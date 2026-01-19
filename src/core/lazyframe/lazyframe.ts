import { DataFrame } from '../dataframe';
import type { Series } from '../series';
import type { DTypeKind, InferSchema, Schema } from '../types';
import { ChunkCache, type ChunkData } from './chunk-cache';
import type { ILazyFrame, LazyFrameConfig, LazyFrameResult } from './interface';
import { parseChunk, parseChunkBytes } from './parser';
import { RowIndex } from './row-index';
import { LazyFrameColumnView } from './view';

/**
 * LazyFrame - Memory-efficient DataFrame for large CSV files.
 *
 * Keeps data on disk and loads rows on-demand using a chunked LRU cache.
 * Suitable for files that exceed available RAM.
 */
export class LazyFrame<S extends Schema> implements ILazyFrame<S> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];
  readonly path: string;

  private readonly _file: ReturnType<typeof Bun.file>;
  private readonly _rowIndex: RowIndex;
  private readonly _columnOrder: (keyof S)[];
  private readonly _cache: ChunkCache;
  private readonly _hasHeader: boolean;
  private readonly _delimiter: string;
  private readonly _config: LazyFrameConfig;

  /**
   * Private constructor - use scanCsv() factory function instead.
   */
  private constructor(
    path: string,
    file: ReturnType<typeof Bun.file>,
    schema: S,
    columnOrder: (keyof S)[],
    rowIndex: RowIndex,
    config: LazyFrameConfig,
    hasHeader: boolean,
    delimiter: string,
  ) {
    this.path = path;
    this._file = file;
    this.schema = schema;
    this._columnOrder = columnOrder;
    this._rowIndex = rowIndex;
    this.shape = [rowIndex.rowCount, columnOrder.length] as const;
    this._config = config;
    this._cache = new ChunkCache({
      maxMemoryBytes: config.maxCacheMemory ?? 100 * 1024 * 1024,
      chunkSize: config.chunkSize ?? 10_000,
    });
    this._hasHeader = hasHeader;
    this._delimiter = delimiter;
  }

  /**
   * Create a LazyFrame from file path - internal factory method.
   */
  static async _create<S extends Schema>(
    path: string,
    schema: S,
    columnOrder: (keyof S)[],
    config: LazyFrameConfig = {},
    hasHeader = true,
    delimiter = ',',
  ): Promise<LazyFrame<S>> {
    const file = Bun.file(path);
    const rowIndex = await RowIndex.build(file, hasHeader);

    return new LazyFrame<S>(
      path,
      file,
      schema,
      columnOrder,
      rowIndex,
      config,
      hasHeader,
      delimiter,
    );
  }

  // Column Access
  // ===============================================================

  /**
   * Get a column as Series (loads all data for that column).
   */
  async col<K extends keyof S>(name: K): Promise<Series<S[K]['kind']>> {
    const result = await this.collect();
    if (result.memoryError) throw result.memoryError;
    return result.data!.col(name);
  }

  /**
   * Get column names in order.
   */
  columns(): (keyof S)[] {
    return [...this._columnOrder];
  }

  // Row Operations
  // ===============================================================

  /**
   * Get first n rows as DataFrame.
   */
  async head(n = 5): Promise<DataFrame<S>> {
    const count = Math.min(n, this.shape[0]);
    const cols = await this._loadColumns(0, count);
    return this._toDataFrame(cols, count);
  }

  /**
   * Get last n rows as DataFrame.
   */
  async tail(n = 5): Promise<DataFrame<S>> {
    const count = Math.min(n, this.shape[0]);
    const startRow = this.shape[0] - count;
    const cols = await this._loadColumns(startRow, count);
    return this._toDataFrame(cols, count);
  }

  /**
   * Select specific columns (returns new LazyFrame).
   */
  select<K extends keyof S>(...cols: K[]): ILazyFrame<Pick<S, K>> {
    const newSchema = {} as Pick<S, K>;
    for (const colName of cols) {
      newSchema[colName] = this.schema[colName];
    }
    return new LazyFrameColumnView<S, K>(this, cols, newSchema);
  }

  /**
   * Count rows matching a predicate.
   */
  async count(predicate?: (row: InferSchema<S>, index: number) => boolean): Promise<number> {
    if (!predicate) return this.shape[0];

    let matchCount = 0;
    const chunkSize = this._cache.chunkSize;

    for (let startRow = 0; startRow < this.shape[0]; startRow += chunkSize) {
      const count = Math.min(chunkSize, this.shape[0] - startRow);
      const cols = await this._loadColumns(startRow, count);
      const colNames = Object.keys(cols);

      for (let i = 0; i < count; i++) {
        // biome-ignore lint/suspicious/noExplicitAny: <explanation>
        const row = {} as any;
        for (const name of colNames) {
          row[name] = cols[name]?.[i];
        }

        if (predicate(row, startRow + i)) {
          matchCount++;
        }
      }

      this.clearCache();
      if (this._config.forceGc && typeof Bun !== 'undefined' && Bun.gc) {
        // @ts-ignore
        Bun.gc(true);
      }
    }

    return matchCount;
  }

  /**
   * Filter rows by predicate function.
   */
  async filter(fn: (row: InferSchema<S>, index: number) => boolean): Promise<LazyFrameResult<S>> {
    const matchingRows: InferSchema<S>[] = [];
    const chunkSize = this._cache.chunkSize;

    for (let startRow = 0; startRow < this.shape[0]; startRow += chunkSize) {
      const count = Math.min(chunkSize, this.shape[0] - startRow);
      const cols = await this._loadColumns(startRow, count);
      const colNames = Object.keys(cols);

      for (let i = 0; i < count; i++) {
        // biome-ignore lint/suspicious/noExplicitAny: <explanation>
        const row = {} as any;
        for (const name of colNames) {
          row[name] = cols[name]?.[i];
        }

        if (fn(row, startRow + i)) {
          matchingRows.push(row);
        }
      }

      if (startRow % (chunkSize * 10) === 0) {
        this.clearCache();
        if (this._config.forceGc && typeof Bun !== 'undefined' && Bun.gc) {
          // @ts-ignore
          Bun.gc(true);
        }
      }
    }

    const df = DataFrame.from(this.schema, matchingRows);
    this.clearCache();
    return { data: df };
  }

  /**
   * Collect all rows.
   */
  async collect(limit?: number): Promise<LazyFrameResult<S>> {
    const count = limit !== undefined ? Math.min(limit, this.shape[0]) : this.shape[0];
    const cols = await this._loadColumns(0, count);

    const estimatedSize = ChunkCache.estimateSize(cols, count);
    const allocation = this._cache.checkAllocation(estimatedSize);

    const df = this._toDataFrame(cols, count);
    this.clearCache();

    return {
      data: df,
      memoryError: allocation.success ? undefined : allocation.error,
    };
  }

  /**
   * Get info about the LazyFrame.
   */
  info(): { rows: number; columns: number; dtypes: Record<string, string>; cached: number } {
    const dtypes: Record<string, string> = {};
    for (const colName of this._columnOrder) {
      const dtype = this.schema[colName];
      if (dtype) dtypes[colName as string] = dtype.kind;
    }

    return {
      rows: this.shape[0],
      columns: this.shape[1],
      dtypes,
      cached: this._cache.size,
    };
  }

  /**
   * Print sample of data to console.
   */
  async print(): Promise<void> {
    const df = await this.head(10);
    console.log(`LazyFrame [${this.path}]`);
    df.print();
    if (this.shape[0] > 10) {
      console.log(`... ${this.shape[0] - 10} more rows`);
    }
  }

  clearCache(): void {
    this._cache.clear();
  }

  destroy(): void {
    this._cache.destroy();
  }

  // Internal: Column Loading
  // ===============================================================

  /**
   * Load columns from file (with caching).
   */
  private async _loadColumns(startRow: number, count: number): Promise<Record<string, unknown[]>> {
    const colNames = this._columnOrder as string[];
    const results: Record<string, unknown[]> = {};
    for (const name of colNames) {
      results[name] = new Array(count);
    }

    const chunkSize = this._cache.chunkSize;
    const startChunk = this._cache.getChunkIndex(startRow);
    const endChunk = this._cache.getChunkIndex(startRow + count - 1);

    for (let chunkIdx = startChunk; chunkIdx <= endChunk; chunkIdx++) {
      let chunk = this._cache.get(chunkIdx);

      if (!chunk) {
        chunk = await this._loadChunk(chunkIdx);
        this._cache.set(chunkIdx, chunk);
      }

      const chunkStartRow = chunkIdx * chunkSize;
      const globalStart = Math.max(startRow, chunkStartRow);
      const globalEnd = Math.min(startRow + count, chunkStartRow + chunk.rowCount);
      const length = globalEnd - globalStart;

      if (length <= 0) continue;

      const srcStart = globalStart - chunkStartRow;
      const destStart = globalStart - startRow;

      for (const colName of colNames) {
        const srcArray = chunk.columns[colName];
        const destArray = results[colName];
        if (srcArray && destArray) {
          for (let i = 0; i < length; i++) {
            destArray[destStart + i] = srcArray[srcStart + i];
          }
        }
      }
    }

    return results;
  }

  /**
   * Async Iterator - Yields chunks of DataFrames (Zero Copy!)
   */
  async *[Symbol.asyncIterator](): AsyncIterator<DataFrame<S>> {
    const chunkSize = this._cache.chunkSize;

    for (let startRow = 0; startRow < this.shape[0]; startRow += chunkSize) {
      const chunkIdx = this._cache.getChunkIndex(startRow);
      let chunk = this._cache.get(chunkIdx);

      if (!chunk) {
        chunk = await this._loadChunk(chunkIdx);
        this._cache.set(chunkIdx, chunk);
      }

      // Create DataFrame using existing schema (Zero Copy logic applied in loadChunk/toDataFrame construction effectively)
      const df = this._toDataFrame(chunk.columns, chunk.rowCount);

      yield df;

      this.clearCache();

      if (this._config.forceGc && typeof Bun !== 'undefined' && Bun.gc) {
        // @ts-ignore
        Bun.gc(true);
      }
    }
  }

  /**
   * Load a specific chunk from file.
   */
  private async _loadChunk(chunkIndex: number): Promise<ChunkData> {
    const chunkSize = this._cache.chunkSize;
    const startRow = chunkIndex * chunkSize;
    const endRow = Math.min(startRow + chunkSize, this.shape[0]);
    const count = endRow - startRow;

    if (count <= 0) {
      return { startRow, columns: {}, rowCount: 0, sizeBytes: 0 };
    }

    const [startByte, endByte] = this._rowIndex.getRowsRange(startRow, endRow);
    const blob = this._file.slice(startByte, endByte);

    let columns: Record<string, unknown[]>;

    if (this._config.raw) {
      const arrayBuffer = await blob.arrayBuffer();
      const bytes = new Uint8Array(arrayBuffer);
      columns = parseChunkBytes(
        bytes,
        count,
        this._columnOrder as string[],
        this.schema,
        this._delimiter.charCodeAt(0),
      );
    } else {
      const chunkText = await blob.text();
      columns = parseChunk(
        chunkText,
        count,
        this._columnOrder as string[],
        this.schema,
        this._delimiter,
      );
    }

    const sizeBytes = ChunkCache.estimateSize(columns, count);

    return { startRow, columns, rowCount: count, sizeBytes };
  }

  /**
   * Convert raw column data to typed DataFrame using this.schema.
   */
  private _toDataFrame(data: Record<string, unknown[]>, rowCount: number): DataFrame<S> {
    const columns = new Map<keyof S, Series<DTypeKind>>();

    for (const colName of this._columnOrder) {
      const values = data[colName as string];
      if (!values) {
        throw new Error(`Missing column data for '${String(colName)}'`);
      }
      const dtype = this.schema[colName]!;
      columns.set(colName, DataFrame._createSeries(dtype, values));
    }

    return DataFrame._fromColumns(this.schema, columns, this._columnOrder, rowCount);
  }
}
