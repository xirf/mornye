import type { DataFrame } from '../dataframe';
import type { Series } from '../series';
import type { InferSchema, Schema } from '../types';
import type { ILazyFrame, LazyFrameResult } from './interface';

/**
 * Column view wrapper for select() operation.
 */
export class LazyFrameColumnView<S extends Schema, K extends keyof S>
  implements ILazyFrame<Pick<S, K>>
{
  readonly schema: Pick<S, K>;
  readonly shape: readonly [rows: number, cols: number];
  readonly path: string;

  private readonly _source: ILazyFrame<S>;
  private readonly _columns: K[];

  constructor(source: ILazyFrame<S>, columns: K[], schema: Pick<S, K>) {
    this._source = source;
    this._columns = columns;
    this.schema = schema;
    this.shape = [source.shape[0], columns.length] as const;
    this.path = source.path;
  }

  async col<C extends K>(name: C): Promise<Series<Pick<S, K>[C]['kind']>> {
    return this._source.col(name) as unknown as Promise<Series<Pick<S, K>[C]['kind']>>;
  }

  columns(): K[] {
    return [...this._columns];
  }

  async head(n = 5): Promise<DataFrame<Pick<S, K>>> {
    const df = await this._source.head(n);
    return df.select(...this._columns);
  }

  async tail(n = 5): Promise<DataFrame<Pick<S, K>>> {
    const df = await this._source.tail(n);
    return df.select(...this._columns);
  }

  select<C extends K>(...cols: C[]): ILazyFrame<Pick<Pick<S, K>, C>> {
    const newSchema = {} as Pick<Pick<S, K>, C>;
    for (const colName of cols) {
      newSchema[colName] = this.schema[colName];
    }
    return new LazyFrameColumnView<S, C>(
      this._source,
      cols,
      newSchema as Pick<S, C>,
    ) as unknown as ILazyFrame<Pick<Pick<S, K>, C>>;
  }

  async count(
    predicate?: (row: InferSchema<Pick<S, K>>, index: number) => boolean,
  ): Promise<number> {
    if (!predicate) return this._source.count();

    return this._source.count((row, idx) => {
      const projected = {} as InferSchema<Pick<S, K>>;
      for (const col of this._columns) {
        (projected as Record<string, unknown>)[col as string] = (row as Record<string, unknown>)[
          col as string
        ];
      }
      return predicate(projected, idx);
    });
  }

  async filter(
    fn: (row: InferSchema<Pick<S, K>>, index: number) => boolean,
  ): Promise<LazyFrameResult<Pick<S, K>>> {
    const result = await this._source.filter((row, idx) => {
      const projected = {} as InferSchema<Pick<S, K>>;
      for (const col of this._columns) {
        (projected as Record<string, unknown>)[col as string] = (row as Record<string, unknown>)[
          col as string
        ];
      }
      return fn(projected, idx);
    });

    if (result.data) {
      return {
        data: result.data.select(...this._columns),
        memoryError: result.memoryError,
      } as unknown as LazyFrameResult<Pick<S, K>>;
    }
    return result as unknown as LazyFrameResult<Pick<S, K>>;
  }

  async collect(limit?: number): Promise<LazyFrameResult<Pick<S, K>>> {
    const result = await this._source.collect(limit);
    if (result.data) {
      return {
        data: result.data.select(...this._columns),
        memoryError: result.memoryError,
      } as unknown as LazyFrameResult<Pick<S, K>>;
    }
    return result as unknown as LazyFrameResult<Pick<S, K>>;
  }

  info(): { rows: number; columns: number; dtypes: Record<string, string>; cached: number } {
    const baseInfo = this._source.info();
    const dtypes: Record<string, string> = {};
    for (const col of this._columns) {
      dtypes[col as string] = baseInfo.dtypes[col as string] ?? 'unknown';
    }
    return {
      rows: baseInfo.rows,
      columns: this._columns.length,
      dtypes,
      cached: baseInfo.cached,
    };
  }

  async *[Symbol.asyncIterator](): AsyncIterator<DataFrame<Pick<S, K>>> {
    for await (const chunk of this._source) {
      yield chunk.select(...this._columns);
    }
  }

  async print(): Promise<void> {
    const result = await this.head(10);
    console.log(`LazyFrame [${this.path}] (${this._columns.length} columns selected)`);
    result.print();
    if (this.shape[0] > 10) {
      console.log(`... ${this.shape[0] - 10} more rows`);
    }
  }

  clearCache(): void {
    this._source.clearCache();
  }

  destroy(): void {
    this._source.destroy();
  }
}
