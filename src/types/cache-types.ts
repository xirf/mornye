/**
 * Type definitions for the cache system
 */

/**
 * Cache operation types for computed columns
 */
export type CacheOperation =
  | 'groupby-count'
  | 'groupby-sum'
  | 'groupby-mean'
  | 'groupby-min'
  | 'groupby-max'
  | 'groupby-first'
  | 'groupby-last'
  | 'filter'
  | 'select'
  | 'sort';

/**
 * Parameters for cache operations
 */
export interface CacheParams {
  /** Column names involved in the operation */
  columns: string[];
  /** Group keys for groupby operations */
  groupKeys?: string[];
  /** Aggregation function for groupby */
  aggFunc?: 'count' | 'sum' | 'mean' | 'min' | 'max' | 'first' | 'last';
  /** Filter operator for filter operations */
  filterOp?: '==' | '!=' | '>' | '<' | '>=' | '<=' | 'in' | 'not-in';
  /** Filter value for filter operations */
  filterValue?: string | number | bigint | (string | number | bigint)[];
  /** Sort direction */
  sortDir?: 'asc' | 'desc';
}

/**
 * String Dictionary Cache interface
 * Reference to the existing global dictionary
 */
export interface StringDictionaryCache {
  intern(str: string): number;
  getString(id: number): string | undefined;
  getMemoryUsage(): number;
  clear(): void;
}

/**
 * Cache statistics for debugging and monitoring
 */
export interface CacheStats {
  totalUsage: number;
  budget: number;
  usagePercent: number;
  typeConversionCaches: number;
  computedCacheUsage: number;
}
