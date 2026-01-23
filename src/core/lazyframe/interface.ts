import type { MemoryLimitError } from '../../errors';
import type { DataFrame } from '../dataframe';
import type { Series } from '../series';
import type { InferSchema, Schema } from '../types';

/**
 * Result of a LazyFrame operation that might hit memory limits.
 */
export interface LazyFrameResult<S extends Schema, T = DataFrame<S>> {
  /** The resulting data if operation succeeded (fully or partially) */
  data?: T;
  /** Memory limit error if operation was throttled or aborted */
  memoryError?: MemoryLimitError;
}

/**
 * Read-only view interface for LazyFrame.
 */
export interface LazyFrameView<S extends Schema> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];

  col<K extends keyof S>(name: K): Promise<Series<S[K]['kind']>>;
}

/**
 * Configuration options for LazyFrame operations.
 */
export interface LazyFrameConfig {
  /** Maximum memory for row cache in bytes (default: 100MB) */
  maxCacheMemory?: number;

  /** Number of rows per cache chunk (default: 10,000) */
  chunkSize?: number;

  /**
   * If true, keeps string columns as Uint8Array (bytes) instead of decoding.
   * Useful for extreme memory optimization when specific string comparisons can be done on bytes.
   */
  raw?: boolean;

  /**
   * If true, forces garbage collection after processing each chunk.
   * Only works in runtimes that expose a GC API (e.g. Bun with --expose-gc, or Bun default in some versions).
   * @default false
   */
  forceGc?: boolean;
}

/**
 * Base interface for LazyFrame - a memory-efficient DataFrame for large files.
 *
 * Unlike DataFrame, LazyFrame keeps data on disk and loads rows on-demand.
 * Operations return new LazyFrame instances with deferred execution.
 */
export interface ILazyFrame<S extends Schema> extends LazyFrameView<S> {
  /** Get first n rows as DataFrame */
  head(n?: number): Promise<DataFrame<S>>;

  /** Get last n rows as DataFrame */
  tail(n?: number): Promise<DataFrame<S>>;

  /** Select specific columns */
  select<K extends keyof S>(...cols: K[]): ILazyFrame<Pick<S, K>>;

  /**
   * Count rows matching a predicate (streaming - very low memory).
   * If no predicate provided, returns total row count.
   */
  count(predicate?: (row: InferSchema<S>, index: number) => boolean): Promise<number>;

  /** Filter rows by predicate (streaming - low memory) */
  filter(fn: (row: InferSchema<S>, index: number) => boolean): Promise<LazyFrameResult<S>>;

  /** Collect all data into a regular DataFrame (loads everything into memory) */
  collect(): Promise<LazyFrameResult<S>>;

  /** Collect with row limit */
  collect(limit: number): Promise<LazyFrameResult<S>>;

  /** Get column names */
  columns(): (keyof S)[];

  /** Print sample of data to console */
  print(): Promise<void>;

  /** Get DataFrame info */
  info(): { rows: number; columns: number; dtypes: Record<string, string>; cached: number };

  /** Clear cached data */
  clearCache(): void;

  /** Release all memory and trackings associated with this LazyFrame */
  destroy(): void;

  /** File path */
  readonly path: string;

  /** Async iterator for streaming chunks (DataFrame) */
  [Symbol.asyncIterator](): AsyncIterator<DataFrame<S>>;

  /**
   * Group by keys and aggregate (Binary-Optimized).
   * @param keys Column names to group by
   * @param aggs Aggregations to perform
   */
  groupby(keys: string[], aggs: AggDef[]): Promise<LazyFrameResult<S>>;
}

export type AggFunc = 'sum' | 'count' | 'mean' | 'min' | 'max';

export interface AggDef {
  col: string;
  func: AggFunc;
  outName: string;
}
