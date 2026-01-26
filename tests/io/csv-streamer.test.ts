import { afterEach, describe, expect, test } from 'bun:test';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { streamCsvBatches } from '../../src/io/csv-streamer';
import { createMemoryBudget } from '../../src/memory/budget';
import { DType } from '../../src/types/dtypes';

const TEST_CACHE_DIR = path.join(process.cwd(), '.test_cache');

function ensureCacheDir(): void {
  if (!fs.existsSync(TEST_CACHE_DIR)) {
    fs.mkdirSync(TEST_CACHE_DIR, { recursive: true });
  }
}

afterEach(() => {
  if (!fs.existsSync(TEST_CACHE_DIR)) return;
  for (const file of fs.readdirSync(TEST_CACHE_DIR)) {
    fs.unlinkSync(path.join(TEST_CACHE_DIR, file));
  }
});

describe('streamCsvBatches', () => {
  test('streams batches with header', async () => {
    ensureCacheDir();

    const filePath = path.join(TEST_CACHE_DIR, 'stream-header.csv');
    const csv = 'id,value,name,active\n1,1.5,Alice,1\n2,3.0,Bob,0\n3,4.5,Carol,1\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      id: DType.Int32,
      value: DType.Float64,
      name: DType.String,
      active: DType.Bool,
    };

    const result = await streamCsvBatches(filePath, {
      schema,
      batchSizeBytes: 64,
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    let totalRows = 0;
    const batches = [];
    for await (const batch of result.data) {
      batches.push(batch);
      totalRows += batch.rowCount;
    }

    expect(totalRows).toBe(3);
    expect(batches.length).toBeGreaterThan(0);

    const first = batches[0];
    expect(first?.columns.id?.data).toBeInstanceOf(Int32Array);
    expect(first?.columns.value?.data).toBeInstanceOf(Float64Array);
    expect(first?.columns.active?.data).toBeInstanceOf(Uint8Array);
  });

  test('streams batches without header', async () => {
    ensureCacheDir();

    const filePath = path.join(TEST_CACHE_DIR, 'stream-no-header.csv');
    const csv = '1,2.5,Alice\n2,3.5,Bob\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      id: DType.Int32,
      value: DType.Float64,
      name: DType.String,
    };

    const result = await streamCsvBatches(filePath, {
      schema,
      hasHeader: false,
      batchSizeBytes: 64,
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    let totalRows = 0;
    for await (const batch of result.data) {
      totalRows += batch.rowCount;
    }

    expect(totalRows).toBe(2);
    expect(result.data.columnOrder).toEqual(['id', 'value', 'name']);
  });

  test('drops invalid rows when schema enforcement enabled', async () => {
    ensureCacheDir();

    const filePath = path.join(TEST_CACHE_DIR, 'stream-invalid.csv');
    const csv = 'id,value\n1,2.5\nX,3.1\n2,not-a-number\n3,4.2\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      id: DType.Int32,
      value: DType.Float64,
    };

    const result = await streamCsvBatches(filePath, {
      schema,
      batchSizeBytes: 64,
      dropInvalidRows: true,
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    let totalRows = 0;
    for await (const batch of result.data) {
      totalRows += batch.rowCount;
    }

    expect(totalRows).toBe(2);
  });

  test('deletes cache file on early termination', async () => {
    ensureCacheDir();

    const filePath = path.join(TEST_CACHE_DIR, 'stream-early.csv');
    const cachePath = path.join(TEST_CACHE_DIR, 'stream-early.mbin');
    const csv = 'id,value\n1,2.5\n2,3.1\n3,4.2\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      id: DType.Int32,
      value: DType.Float64,
    };

    const result = await streamCsvBatches(filePath, {
      schema,
      batchSizeBytes: 32,
      cachePath,
      deleteCacheOnAbort: true,
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    for await (const batch of result.data) {
      expect(batch.rowCount).toBeGreaterThan(0);
      break; // terminate early
    }

    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(fs.existsSync(cachePath)).toBe(false);
  });

  test('keeps cache file after full consumption', async () => {
    ensureCacheDir();

    const filePath = path.join(TEST_CACHE_DIR, 'stream-complete.csv');
    const cachePath = path.join(TEST_CACHE_DIR, 'stream-complete.mbin');
    const csv = 'id,value\n1,2.5\n2,3.1\n3,4.2\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      id: DType.Int32,
      value: DType.Float64,
    };

    const result = await streamCsvBatches(filePath, {
      schema,
      batchSizeBytes: 32,
      cachePath,
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    let totalRows = 0;
    for await (const batch of result.data) {
      totalRows += batch.rowCount;
    }

    expect(totalRows).toBe(3);
    expect(fs.existsSync(cachePath)).toBe(true);
  });

  test('applies predicate pushdown during streaming', async () => {
    ensureCacheDir();

    const filePath = path.join(TEST_CACHE_DIR, 'stream-predicate.csv');
    const csv = 'id,value\n1,2.5\n2,3.1\n3,4.2\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      id: DType.Int32,
      value: DType.Float64,
    };

    const result = await streamCsvBatches(filePath, {
      schema,
      predicates: [{ columnName: 'value', operator: '>', value: 3 }],
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    let totalRows = 0;
    for await (const batch of result.data) {
      totalRows += batch.rowCount;
    }

    expect(totalRows).toBe(2);
  });

  test('prunes columns during streaming', async () => {
    ensureCacheDir();

    const filePath = path.join(TEST_CACHE_DIR, 'stream-prune.csv');
    const csv = 'id,value,name\n1,2.5,Alice\n2,3.1,Bob\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      id: DType.Int32,
      value: DType.Float64,
      name: DType.String,
    };

    const result = await streamCsvBatches(filePath, {
      schema,
      requiredColumns: new Set(['id', 'name']),
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    let batches = 0;
    for await (const batch of result.data) {
      batches += 1;
      expect(Object.keys(batch.columns)).toEqual(['id', 'name']);
    }

    expect(batches).toBeGreaterThan(0);
  });

  test('tracks memory budget usage', async () => {
    ensureCacheDir();

    const filePath = path.join(TEST_CACHE_DIR, 'stream-budget.csv');
    const csv = 'id,value\n1,2.5\n2,3.1\n3,4.2\n4,5.0\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      id: DType.Int32,
      value: DType.Float64,
    };

    const budget = createMemoryBudget(256);

    const result = await streamCsvBatches(filePath, {
      schema,
      batchSizeBytes: 32,
      memoryBudget: budget,
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    let maxUsage = 0;
    for await (const batch of result.data) {
      maxUsage = Math.max(maxUsage, budget.currentUsage);
      expect(batch.rowCount).toBeGreaterThan(0);
    }

    expect(maxUsage).toBeGreaterThan(0);
    expect(budget.currentUsage).toBe(0);
  });
});
