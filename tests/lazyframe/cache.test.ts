import { describe, expect, test } from 'bun:test';
import {
  CacheManager,
  ComputedCache,
  TypeConversionCache,
  getCacheManager,
  resetCacheManager,
} from '../../src/lazyframe/cache';
import { DType } from '../../src/types/dtypes';

describe('TypeConversionCache', () => {
  test('stores and retrieves parsed values', () => {
    const cache = new TypeConversionCache('price', DType.Float64);

    cache.set('100.5', 100.5);
    expect(cache.get('100.5')).toBe(100.5);
  });

  test('returns undefined for non-existent values', () => {
    const cache = new TypeConversionCache('price', DType.Float64);
    expect(cache.get('nonexistent')).toBeUndefined();
  });

  test('handles bigint values', () => {
    const cache = new TypeConversionCache('timestamp', DType.DateTime);

    cache.set('1234567890', 1234567890n);
    expect(cache.get('1234567890')).toBe(1234567890n);
  });

  test('handles boolean values', () => {
    const cache = new TypeConversionCache('active', DType.Bool);

    cache.set('true', true);
    cache.set('false', false);
    expect(cache.get('true')).toBe(true);
    expect(cache.get('false')).toBe(false);
  });

  test('tracks memory usage', () => {
    const cache = new TypeConversionCache('price', DType.Float64);

    const initialMemory = cache.getMemoryUsage();
    cache.set('100.5', 100.5);
    const afterMemory = cache.getMemoryUsage();

    expect(afterMemory).toBeGreaterThan(initialMemory);
  });

  test('clear removes all entries', () => {
    const cache = new TypeConversionCache('price', DType.Float64);

    cache.set('100.5', 100.5);
    cache.set('200.5', 200.5);
    expect(cache.getMemoryUsage()).toBeGreaterThan(0);

    cache.clear();
    expect(cache.get('100.5')).toBeUndefined();
    expect(cache.get('200.5')).toBeUndefined();
    expect(cache.getMemoryUsage()).toBe(0);
  });

  test('evict50Percent removes half of entries', () => {
    const cache = new TypeConversionCache('price', DType.Float64);

    for (let i = 0; i < 100; i++) {
      cache.set(`value${i}`, i);
    }

    const beforeSize = cache.getMemoryUsage();
    cache.evict50Percent();
    const afterSize = cache.getMemoryUsage();

    expect(afterSize).toBeLessThan(beforeSize);
    expect(afterSize).toBeCloseTo(beforeSize / 2, 0);
  });
});

describe('ComputedCache', () => {
  test('stores and retrieves computed results', () => {
    const cache = new ComputedCache(1024 * 1024); // 1MB

    const result = new Uint8Array([1, 2, 3, 4]);
    const stored = cache.set('sum', { column: 'price' }, result);

    expect(stored).toBe(true);
    expect(cache.get('sum', { column: 'price' })).toBe(result);
  });

  test('returns undefined for non-existent results', () => {
    const cache = new ComputedCache(1024 * 1024);
    expect(cache.get('sum', { column: 'price' })).toBeUndefined();
  });

  test('rejects results larger than budget', () => {
    const cache = new ComputedCache(100); // 100 bytes

    const largeResult = new Uint8Array(200);
    const stored = cache.set('sum', { column: 'price' }, largeResult);

    expect(stored).toBe(false);
    expect(cache.get('sum', { column: 'price' })).toBeUndefined();
  });

  test('rejects results that would exceed total budget', () => {
    const cache = new ComputedCache(100);

    const result1 = new Uint8Array(60);
    const result2 = new Uint8Array(60);

    expect(cache.set('sum1', {}, result1)).toBe(true);
    expect(cache.set('sum2', {}, result2)).toBe(false); // Would exceed budget
  });

  test('tracks memory usage', () => {
    const cache = new ComputedCache(1024 * 1024);

    expect(cache.getMemoryUsage()).toBe(0);

    const result = new Uint8Array(100);
    cache.set('sum', {}, result);

    expect(cache.getMemoryUsage()).toBe(100);
  });

  test('clear removes all results', () => {
    const cache = new ComputedCache(1024 * 1024);

    cache.set('sum1', {}, new Uint8Array(50));
    cache.set('sum2', {}, new Uint8Array(50));
    expect(cache.getMemoryUsage()).toBe(100);

    cache.clear();
    expect(cache.getMemoryUsage()).toBe(0);
    expect(cache.get('sum1', {})).toBeUndefined();
  });

  test('different operations with same params get different cache keys', () => {
    const cache = new ComputedCache(1024 * 1024);

    const result1 = new Uint8Array([1, 2, 3]);
    const result2 = new Uint8Array([4, 5, 6]);

    cache.set('sum', { column: 'price' }, result1);
    cache.set('mean', { column: 'price' }, result2);

    expect(cache.get('sum', { column: 'price' })).toBe(result1);
    expect(cache.get('mean', { column: 'price' })).toBe(result2);
  });

  test('same operation with different params get different cache keys', () => {
    const cache = new ComputedCache(1024 * 1024);

    const result1 = new Uint8Array([1, 2, 3]);
    const result2 = new Uint8Array([4, 5, 6]);

    cache.set('sum', { columns: ['price'] }, result1);
    cache.set('sum', { columns: ['volume'] }, result2);

    expect(cache.get('sum', { columns: ['price'] })).toBe(result1);
    expect(cache.get('sum', { columns: ['volume'] })).toBe(result2);
  });
});

describe('CacheManager', () => {
  test('creates with default 512MB budget', () => {
    const manager = new CacheManager();
    const stats = manager.getStats();

    expect(stats.budget).toBe(512 * 1024 * 1024);
  });

  test('creates with custom budget', () => {
    const manager = new CacheManager(256 * 1024 * 1024);
    const stats = manager.getStats();

    expect(stats.budget).toBe(256 * 1024 * 1024);
  });

  test('gets type conversion cache for column', () => {
    const manager = new CacheManager();
    const cache = manager.getTypeConversionCache('price', DType.Float64);

    expect(cache).toBeInstanceOf(TypeConversionCache);
  });

  test('reuses same cache for same column', () => {
    const manager = new CacheManager();
    const cache1 = manager.getTypeConversionCache('price', DType.Float64);
    const cache2 = manager.getTypeConversionCache('price', DType.Float64);

    expect(cache1).toBe(cache2);
  });

  test('gets computed cache', () => {
    const manager = new CacheManager();
    const cache = manager.getComputedCache();

    expect(cache).toBeInstanceOf(ComputedCache);
  });

  test('clearTypeConversionCaches clears all type caches', () => {
    const manager = new CacheManager();
    const cache1 = manager.getTypeConversionCache('price', DType.Float64);
    const cache2 = manager.getTypeConversionCache('volume', DType.Int32);

    cache1.set('100.5', 100.5);
    cache2.set('50', 50);

    manager.clearTypeConversionCaches();

    expect(cache1.get('100.5')).toBeUndefined();
    expect(cache2.get('50')).toBeUndefined();
  });

  test('clearComputedCache clears computed cache', () => {
    const manager = new CacheManager();
    const cache = manager.getComputedCache();

    cache.set('sum', {}, new Uint8Array(100));
    expect(cache.getMemoryUsage()).toBe(100);

    manager.clearComputedCache();
    expect(cache.getMemoryUsage()).toBe(0);
  });

  test('getTotalMemoryUsage sums all caches', () => {
    const manager = new CacheManager();

    expect(manager.getTotalMemoryUsage()).toBe(0);

    const typeCache = manager.getTypeConversionCache('price', DType.Float64);
    typeCache.set('100.5', 100.5);

    const computedCache = manager.getComputedCache();
    computedCache.set('sum', {}, new Uint8Array(100));

    expect(manager.getTotalMemoryUsage()).toBeGreaterThan(100);
  });

  test('isNearLimit returns false when well below threshold', () => {
    const manager = new CacheManager(1000);
    expect(manager.isNearLimit()).toBe(false);
  });

  test('isNearLimit returns true when above 78% threshold', () => {
    const manager = new CacheManager(1000);
    const typeCache = manager.getTypeConversionCache('price', DType.Float64);

    // Fill type conversion cache to push usage above 78%
    // Each entry is roughly ~20 bytes, so 40 entries ~ 800 bytes
    for (let i = 0; i < 40; i++) {
      typeCache.set(`value${i}`, i);
    }

    expect(manager.isNearLimit()).toBe(true);
  });

  test('applyDegradation clears computed cache first', () => {
    const manager = new CacheManager(1000);
    const computedCache = manager.getComputedCache();
    const typeCache = manager.getTypeConversionCache('price', DType.Float64);

    // Add some data to computed cache (within its 10% budget = 100 bytes)
    computedCache.set('sum', {}, new Uint8Array(50));

    // Fill type conversion cache to push total usage above 78%
    for (let i = 0; i < 40; i++) {
      typeCache.set(`value${i}`, i);
    }

    expect(manager.isNearLimit()).toBe(true);

    const degraded = manager.applyDegradation();

    expect(degraded).toBe(true);
    expect(computedCache.getMemoryUsage()).toBe(0);
  });

  test('applyDegradation does nothing when below threshold', () => {
    const manager = new CacheManager(1000);
    const degraded = manager.applyDegradation();

    expect(degraded).toBe(false);
  });

  test('getStats returns cache statistics', () => {
    const manager = new CacheManager(512 * 1024 * 1024);
    const stats = manager.getStats();

    expect(stats.totalUsage).toBe(0);
    expect(stats.budget).toBe(512 * 1024 * 1024);
    expect(stats.usagePercent).toBe(0);
    expect(stats.typeConversionCaches).toBe(0);
    expect(stats.computedCacheUsage).toBe(0);
  });
});

describe('Global CacheManager', () => {
  test('getCacheManager returns singleton', () => {
    resetCacheManager();
    const manager1 = getCacheManager();
    const manager2 = getCacheManager();

    expect(manager1).toBe(manager2);
  });

  test('getCacheManager uses custom budget on first call', () => {
    resetCacheManager();
    const manager = getCacheManager(256 * 1024 * 1024);
    const stats = manager.getStats();

    expect(stats.budget).toBe(256 * 1024 * 1024);
  });

  test('resetCacheManager clears singleton', () => {
    const manager1 = getCacheManager();
    resetCacheManager();
    const manager2 = getCacheManager();

    expect(manager1).not.toBe(manager2);
  });
});
