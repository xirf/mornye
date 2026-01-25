/**
 * Three-tier cache system for Molniya
 *
 * Tier 1: String Dictionary (Interning Cache)
 *   - Global per session
 *   - Shared across all DataFrames
 *   - LRU eviction when hitting budget
 *
 * Tier 2: Type Conversion Cache
 *   - Per DataFrame/Column
 *   - Caches parsed values (string→number, string→datetime)
 *   - Cleared after operations or on memory pressure
 *
 * Tier 3: Computed Column Cache
 *   - Per operation
 *   - Memoizes expensive aggregation results
 *   - Cleared after operation completes
 *   - Only cache if result size < 10% of memory budget
 */

import type { CacheOperation, CacheParams, CacheStats } from '../types/cache-types';
import type { DType } from '../types/dtypes';

// Re-export types for convenience
export type {
  CacheOperation,
  CacheParams,
  CacheStats,
  StringDictionaryCache,
} from '../types/cache-types';

/**
 * LRU Cache implementation
 */
class LRUCache<K, V> {
  private cache = new Map<K, V>();
  private readonly maxSize: number;

  constructor(maxSize: number) {
    this.maxSize = maxSize;
  }

  get(key: K): V | undefined {
    const value = this.cache.get(key);
    if (value !== undefined) {
      // Move to end (most recently used)
      this.cache.delete(key);
      this.cache.set(key, value);
    }
    return value;
  }

  set(key: K, value: V): void {
    // Remove if already exists (to update position)
    this.cache.delete(key);

    // Add to end
    this.cache.set(key, value);

    // Evict oldest if over capacity
    if (this.cache.size > this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      if (firstKey !== undefined) {
        this.cache.delete(firstKey);
      }
    }
  }

  has(key: K): boolean {
    return this.cache.has(key);
  }

  delete(key: K): boolean {
    return this.cache.delete(key);
  }

  clear(): void {
    this.cache.clear();
  }

  get size(): number {
    return this.cache.size;
  }

  keys(): IterableIterator<K> {
    return this.cache.keys();
  }

  getMemoryUsage(): number {
    // Rough estimate: each entry is ~100 bytes overhead
    return this.cache.size * 100;
  }
}

/**
 * Tier 2: Type Conversion Cache
 * Caches parsed values for a specific column
 */
export class TypeConversionCache {
  // Cache key: raw string value, Cache value: parsed value
  private cache: LRUCache<string, number | bigint | boolean>;
  private readonly columnName: string;
  private readonly dtype: DType;
  private memoryUsage = 0;

  constructor(columnName: string, dtype: DType, maxEntries = 10000) {
    this.columnName = columnName;
    this.dtype = dtype;
    this.cache = new LRUCache(maxEntries);
  }

  /**
   * Get cached parsed value
   */
  get(rawValue: string): number | bigint | boolean | undefined {
    return this.cache.get(rawValue);
  }

  /**
   * Store parsed value
   */
  set(rawValue: string, parsedValue: number | bigint | boolean): void {
    this.cache.set(rawValue, parsedValue);

    // Update memory usage estimate
    const valueSize = typeof parsedValue === 'bigint' ? 8 : 8; // Float64 or BigInt64
    this.memoryUsage += rawValue.length * 2 + valueSize; // UTF-16 string + value
  }

  /**
   * Clear all cached values
   */
  clear(): void {
    this.cache.clear();
    this.memoryUsage = 0;
  }

  /**
   * Get estimated memory usage in bytes
   */
  getMemoryUsage(): number {
    return this.memoryUsage + this.cache.getMemoryUsage();
  }

  /**
   * Evict 50% of cache entries (LRU)
   */
  evict50Percent(): void {
    const targetSize = Math.floor(this.cache.size / 2);
    while (this.cache.size > targetSize) {
      // Remove oldest entry (Map keys() returns iterator, first key is oldest in insertion order)
      const firstKey = this.cache.keys().next().value;
      if (firstKey === undefined) break;
      this.cache.delete(firstKey as string);
    }
    // Recalculate memory usage
    this.memoryUsage = Math.floor(this.memoryUsage / 2);
  }
}

/**
 * Tier 3: Computed Column Cache
 * Caches results of expensive operations like aggregations
 */
export class ComputedCache {
  // Cache key: operation signature, Cache value: result data
  private cache = new Map<string, Uint8Array>();
  private memoryUsage = 0;
  private readonly maxMemoryBytes: number;

  constructor(maxMemoryBytes: number) {
    this.maxMemoryBytes = maxMemoryBytes;
  }

  /**
   * Generate cache key from operation parameters
   */
  private static getCacheKey(operation: CacheOperation, params: CacheParams): string {
    // Create deterministic key by sorting object keys
    const sortedParams = {
      columns: (params.columns ?? []).slice().sort(),
      ...(params.groupKeys && { groupKeys: params.groupKeys.slice().sort() }),
      ...(params.aggFunc && { aggFunc: params.aggFunc }),
      ...(params.filterOp && { filterOp: params.filterOp }),
      ...(params.filterValue !== undefined && { filterValue: params.filterValue }),
      ...(params.sortDir && { sortDir: params.sortDir }),
    };
    return `${operation}:${JSON.stringify(sortedParams)}`;
  }

  /**
   * Get cached result
   */
  get(operation: CacheOperation, params: CacheParams): Uint8Array | undefined {
    const key = ComputedCache.getCacheKey(operation, params);
    return this.cache.get(key);
  }

  /**
   * Store computed result
   * Only stores if result size is under the memory budget
   */
  set(operation: CacheOperation, params: CacheParams, result: Uint8Array): boolean {
    const key = ComputedCache.getCacheKey(operation, params);

    // Don't cache if result is too large
    if (result.byteLength > this.maxMemoryBytes) {
      return false;
    }

    // Don't cache if would exceed total budget
    if (this.memoryUsage + result.byteLength > this.maxMemoryBytes) {
      return false;
    }

    this.cache.set(key, result);
    this.memoryUsage += result.byteLength;
    return true;
  }

  /**
   * Clear all cached results
   */
  clear(): void {
    this.cache.clear();
    this.memoryUsage = 0;
  }

  /**
   * Get current memory usage in bytes
   */
  getMemoryUsage(): number {
    return this.memoryUsage;
  }
}

/**
 * Cache Manager - Coordinates all three cache tiers
 */
export class CacheManager {
  private readonly memoryBudget: number;
  private readonly typeConversionCaches = new Map<string, TypeConversionCache>();
  private readonly computedCache: ComputedCache;

  // Budget allocation (based on plan.md)
  private readonly stringDictionaryBudget: number; // 20%
  private readonly typeConversionBudget: number; // 30%
  private readonly workingBufferBudget: number; // 30%
  private readonly computedCacheBudget: number; // 10%

  constructor(memoryBudget = 512 * 1024 * 1024) {
    // 512MB default
    this.memoryBudget = memoryBudget;

    this.stringDictionaryBudget = Math.floor(memoryBudget * 0.2);
    this.typeConversionBudget = Math.floor(memoryBudget * 0.3);
    this.workingBufferBudget = Math.floor(memoryBudget * 0.3);
    this.computedCacheBudget = Math.floor(memoryBudget * 0.1);

    this.computedCache = new ComputedCache(this.computedCacheBudget);
  }

  /**
   * Get or create a type conversion cache for a column
   */
  getTypeConversionCache(columnName: string, dtype: DType): TypeConversionCache {
    let cache = this.typeConversionCaches.get(columnName);
    if (!cache) {
      cache = new TypeConversionCache(columnName, dtype);
      this.typeConversionCaches.set(columnName, cache);
    }
    return cache;
  }

  /**
   * Get the computed cache
   */
  getComputedCache(): ComputedCache {
    return this.computedCache;
  }

  /**
   * Clear all type conversion caches
   */
  clearTypeConversionCaches(): void {
    for (const cache of this.typeConversionCaches.values()) {
      cache.clear();
    }
  }

  /**
   * Clear computed cache
   */
  clearComputedCache(): void {
    this.computedCache.clear();
  }

  /**
   * Get total memory usage across all caches
   */
  getTotalMemoryUsage(): number {
    let total = 0;

    // Type conversion caches
    for (const cache of this.typeConversionCaches.values()) {
      total += cache.getMemoryUsage();
    }

    // Computed cache
    total += this.computedCache.getMemoryUsage();

    return total;
  }

  /**
   * Check if memory usage is approaching the warning threshold (78% of budget)
   */
  isNearLimit(): boolean {
    const warningThreshold = this.memoryBudget * 0.78;
    return this.getTotalMemoryUsage() > warningThreshold;
  }

  /**
   * Apply degradation strategy when exceeding memory budget
   * Returns true if degradation was applied
   */
  applyDegradation(): boolean {
    const usage = this.getTotalMemoryUsage();
    const warningThreshold = this.memoryBudget * 0.78;

    if (usage <= warningThreshold) {
      return false; // No degradation needed
    }

    // Step 1: Drop computed column cache
    this.computedCache.clear();

    if (this.getTotalMemoryUsage() <= warningThreshold) {
      return true;
    }

    // Step 2: Reduce type conversion cache (LRU evict 50%)
    for (const cache of this.typeConversionCaches.values()) {
      cache.evict50Percent();
    }

    return true;
  }

  /**
   * Get cache statistics for debugging
   */
  getStats(): CacheStats {
    const totalUsage = this.getTotalMemoryUsage();
    return {
      totalUsage,
      budget: this.memoryBudget,
      usagePercent: (totalUsage / this.memoryBudget) * 100,
      typeConversionCaches: this.typeConversionCaches.size,
      computedCacheUsage: this.computedCache.getMemoryUsage(),
    };
  }
}

/**
 * Global cache manager instance
 */
let globalCacheManager: CacheManager | null = null;

/**
 * Get the global cache manager (singleton)
 */
export function getCacheManager(memoryBudget?: number): CacheManager {
  if (!globalCacheManager) {
    globalCacheManager = new CacheManager(memoryBudget);
  }
  return globalCacheManager;
}

/**
 * Reset the global cache manager (for testing)
 */
export function resetCacheManager(): void {
  globalCacheManager = null;
}
