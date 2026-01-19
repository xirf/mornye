import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import {
  clearAllAllocations,
  configure,
  getConfig,
  getDefaultConfig,
  getMemoryStats,
  releaseAllocation,
  requestAllocation,
  resetConfig,
} from '../../src/core/config';

describe('Memory Configuration', () => {
  beforeEach(() => {
    resetConfig();
    clearAllAllocations();
  });

  afterEach(() => {
    resetConfig();
    clearAllAllocations();
  });

  describe('configure()', () => {
    test('updates global limit', () => {
      configure({ globalLimitBytes: 512 * 1024 * 1024 });
      expect(getConfig().globalLimitBytes).toBe(512 * 1024 * 1024);
    });

    test('updates max task share percent', () => {
      configure({ maxTaskSharePercent: 0.5 });
      expect(getConfig().maxTaskSharePercent).toBe(0.5);
    });

    test('disables memory tracking', () => {
      configure({ enabled: false });
      expect(getConfig().enabled).toBe(false);
    });

    test('partial updates preserve other settings', () => {
      const before = getConfig();
      configure({ globalLimitBytes: 100 });
      expect(getConfig().maxTaskSharePercent).toBe(before.maxTaskSharePercent);
    });
  });

  describe('resetConfig()', () => {
    test('restores default values', () => {
      configure({ globalLimitBytes: 100, maxTaskSharePercent: 0.1 });
      resetConfig();

      const defaults = getDefaultConfig();
      const current = getConfig();

      expect(current.globalLimitBytes).toBe(defaults.globalLimitBytes);
      expect(current.maxTaskSharePercent).toBe(defaults.maxTaskSharePercent);
      expect(current.enabled).toBe(defaults.enabled);
    });
  });
});

describe('Memory Tracker', () => {
  beforeEach(() => {
    resetConfig();
    clearAllAllocations();
  });

  afterEach(() => {
    resetConfig();
    clearAllAllocations();
  });

  describe('requestAllocation()', () => {
    test('succeeds for single task within global limit', () => {
      configure({ globalLimitBytes: 1000 });

      const result = requestAllocation('task1', 500);

      expect(result.success).toBe(true);
      expect(result.allocatedBytes).toBe(500);
      expect(result.error).toBeUndefined();
    });

    test('fails when exceeding global limit', () => {
      configure({ globalLimitBytes: 1000 });

      const result = requestAllocation('task1', 1500);

      expect(result.success).toBe(false);
      expect(result.allocatedBytes).toBe(0);
      expect(result.error).toBeDefined();
      expect(result.error?.requestedBytes).toBe(1500);
      expect(result.error?.globalLimitBytes).toBe(1000);
    });

    test('single task can use 100% of global limit', () => {
      configure({ globalLimitBytes: 1000 });

      const result = requestAllocation('task1', 1000);

      expect(result.success).toBe(true);
      expect(result.allocatedBytes).toBe(1000);
    });

    test('multiple tasks share with 70% max per task', () => {
      configure({ globalLimitBytes: 1000, maxTaskSharePercent: 0.7 });

      // First task gets allocation
      const result1 = requestAllocation('task1', 500);
      expect(result1.success).toBe(true);

      // Second task's max is min(70% of 1000, remaining 500) = 500
      const result2 = requestAllocation('task2', 400);
      expect(result2.success).toBe(true);
      expect(result2.allocatedBytes).toBe(400);
    });

    test('second task limited to 70% when first task exists', () => {
      configure({ globalLimitBytes: 1000, maxTaskSharePercent: 0.7 });

      // First task gets 200
      requestAllocation('task1', 200);

      // Second task wants 800 but limited to 70% = 700
      // Available = 1000 - 200 = 800
      // Max for task = min(700, 800) = 700
      const result2 = requestAllocation('task2', 750);
      expect(result2.success).toBe(false); // 750 > 700
    });

    test('always succeeds when tracking is disabled', () => {
      configure({ enabled: false, globalLimitBytes: 100 });

      const result = requestAllocation('task1', 1000000);

      expect(result.success).toBe(true);
      expect(result.allocatedBytes).toBe(1000000);
    });
  });

  describe('releaseAllocation()', () => {
    test('frees memory for new allocations', () => {
      configure({ globalLimitBytes: 1000 });

      requestAllocation('task1', 800);
      releaseAllocation('task1');

      const result = requestAllocation('task2', 800);
      expect(result.success).toBe(true);
    });
  });

  describe('getMemoryStats()', () => {
    test('returns correct statistics', () => {
      configure({ globalLimitBytes: 2000 });

      requestAllocation('task1', 500);
      requestAllocation('task2', 300);

      const stats = getMemoryStats();

      expect(stats.totalAllocatedBytes).toBe(800);
      expect(stats.globalLimitBytes).toBe(2000);
      expect(stats.availableBytes).toBe(1200);
      expect(stats.activeTaskCount).toBe(2);
      expect(stats.tasks.size).toBe(2);
    });

    test('tracks released allocations', () => {
      configure({ globalLimitBytes: 1000 });

      requestAllocation('task1', 500);
      releaseAllocation('task1');

      const stats = getMemoryStats();

      expect(stats.totalAllocatedBytes).toBe(0);
      expect(stats.availableBytes).toBe(1000);
      expect(stats.activeTaskCount).toBe(0);
    });
  });
});
