import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { clearAllAllocations, configure, getMemoryStats, resetConfig } from '../../src/core/config';
import { readCsv } from '../../src/io/csv';

describe('CSV Memory Limits', () => {
  const testCsvPath = './tests/fixtures/memory-test.csv';
  const testCsvContent = `name,age,score,active
Alice,25,95.5,true
Bob,30,87.2,false
Carol,22,91.8,true
David,28,88.5,true
Eve,35,92.1,false
`;

  beforeAll(async () => {
    await Bun.write(testCsvPath, testCsvContent);
  });

  afterAll(async () => {
    const file = Bun.file(testCsvPath);
    if (await file.exists()) {
      await file.delete().catch(() => {});
    }
  });

  beforeEach(() => {
    resetConfig();
    clearAllAllocations();
  });

  afterEach(() => {
    resetConfig();
    clearAllAllocations();
  });

  describe('memoryLimitBytes option', () => {
    test('succeeds when file is within memory limit', async () => {
      const { df, memoryError } = await readCsv(testCsvPath, {
        memoryLimitBytes: 100 * 1024 * 1024, // 100MB - plenty of room
      });

      expect(memoryError).toBeUndefined();
      expect(df.shape[0]).toBe(5);
    });

    test('returns memoryError when file exceeds per-task limit', async () => {
      const { df, memoryError } = await readCsv(testCsvPath, {
        memoryLimitBytes: 10, // 10 bytes - way too small
      });

      expect(memoryError).toBeDefined();
      expect(memoryError?.code).toBe('MEMORY_LIMIT_EXCEEDED');
      expect(df.shape[0]).toBe(0); // Empty DataFrame
    });

    test('memoryError contains useful information', async () => {
      const { memoryError } = await readCsv(testCsvPath, {
        memoryLimitBytes: 10,
      });

      expect(memoryError).toBeDefined();
      expect(memoryError!.requestedBytes).toBeGreaterThan(10);
      expect(memoryError!.availableBytes).toBe(10);
      expect(memoryError!.hint).toBeDefined();
    });

    test('format() produces readable error message', async () => {
      const { memoryError } = await readCsv(testCsvPath, {
        memoryLimitBytes: 10,
      });

      expect(memoryError).toBeDefined();
      const formatted = memoryError!.format();

      expect(formatted).toContain('Memory limit exceeded');
      expect(formatted).toContain('help:');
    });
  });

  describe('global allocation integration', () => {
    test('releases allocation after successful read', async () => {
      configure({ globalLimitBytes: 1024 * 1024 * 1024 }); // 1GB

      const statsBefore = getMemoryStats();

      await readCsv(testCsvPath, {
        memoryLimitBytes: 100 * 1024 * 1024,
      });

      const statsAfter = getMemoryStats();

      // Allocation should be released after read completes
      expect(statsAfter.activeTaskCount).toBe(statsBefore.activeTaskCount);
    });
  });

  describe('no memory limit (default behavior)', () => {
    test('works without memoryLimitBytes option', async () => {
      const { df, memoryError } = await readCsv(testCsvPath);

      expect(memoryError).toBeUndefined();
      expect(df.shape[0]).toBe(5);
    });
  });
});

describe('Concurrent Memory Limits', () => {
  const testCsvPath1 = './tests/fixtures/concurrent-test1.csv';
  const testCsvPath2 = './tests/fixtures/concurrent-test2.csv';

  beforeAll(async () => {
    const content = 'a,b\n1,2\n3,4\n';
    await Bun.write(testCsvPath1, content);
    await Bun.write(testCsvPath2, content);
  });

  afterAll(async () => {
    for (const path of [testCsvPath1, testCsvPath2]) {
      const file = Bun.file(path);
      if (await file.exists()) {
        await file.delete().catch(() => {});
      }
    }
  });

  beforeEach(() => {
    resetConfig();
    clearAllAllocations();
  });

  afterEach(() => {
    resetConfig();
    clearAllAllocations();
  });

  test('concurrent reads share global memory budget', async () => {
    configure({ globalLimitBytes: 10 * 1024 * 1024 }); // 10MB

    // Run two reads concurrently
    const [result1, result2] = await Promise.all([
      readCsv(testCsvPath1, { memoryLimitBytes: 5 * 1024 * 1024 }),
      readCsv(testCsvPath2, { memoryLimitBytes: 5 * 1024 * 1024 }),
    ]);

    // Both should succeed (small files)
    expect(result1.memoryError).toBeUndefined();
    expect(result2.memoryError).toBeUndefined();
  });
});
