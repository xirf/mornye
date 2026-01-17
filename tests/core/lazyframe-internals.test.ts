import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { ChunkCache, RowIndex } from '../../src/core/lazyframe';
import { scanCsv } from '../../src/io/csv';

const TMP_DIR = './tests/fixtures';
const QUOTED_PATH = `${TMP_DIR}/lazy-quoted.csv`;

beforeAll(async () => {
  const quoted = 'id,name,flag\n1,"hello, world",True\n2,"He said ""Hi""",false\n';
  await Bun.write(QUOTED_PATH, quoted);
});

afterAll(async () => {
  const file = Bun.file(QUOTED_PATH);
  if (await file.exists()) {
    await file.delete().catch(() => {});
  }
});

describe('ChunkCache', () => {
  test('evicts least recently used chunk when memory exceeded', () => {
    const cache = new ChunkCache<{ id: number }>({ maxMemoryBytes: 60, chunkSize: 2 });

    cache.set(0, { startRow: 0, rows: [{ id: 1 }], sizeBytes: 40 });
    cache.set(1, { startRow: 2, rows: [{ id: 3 }], sizeBytes: 30 });

    expect(cache.size).toBe(1);
    expect(cache.get(0)).toBeUndefined();
    expect(cache.get(1)?.rows[0]?.id).toBe(3);
  });

  test('getRows returns undefined for uncached rows and fills cached ones', () => {
    const cache = new ChunkCache<{ id: number }>({ maxMemoryBytes: 200, chunkSize: 2 });
    cache.set(0, { startRow: 0, rows: [{ id: 1 }, { id: 2 }], sizeBytes: 50 });

    const rows = cache.getRows(0, 3);
    expect(rows).toEqual([{ id: 1 }, { id: 2 }, undefined]);
  });

  test('estimateSize uses field count when no rows', () => {
    const estimate = ChunkCache.estimateSize([], 3);
    expect(estimate).toBe(0);
  });

  test('provides chunk utilities and clear resets memory', () => {
    const cache = new ChunkCache<{ id: number }>({ maxMemoryBytes: 1_000, chunkSize: 5 });

    expect(cache.getChunkIndex(9)).toBe(1);
    expect(cache.getChunkRange(2)).toEqual([10, 15]);

    cache.set(0, { startRow: 0, rows: [{ id: 1 }], sizeBytes: 100 });
    expect(cache.size).toBe(1);
    expect(cache.memoryUsed).toBe(100);

    cache.clear();
    expect(cache.size).toBe(0);
    expect(cache.memoryUsed).toBe(0);
  });
});

describe('RowIndex', () => {
  test('builds offsets and enforces bounds', async () => {
    const csv = 'a,b\n1,2\n3,4\n';
    const buffer = Buffer.from(csv);
    const idx = RowIndex.build(buffer, true);

    expect(idx.rowCount).toBe(2);
    expect(idx.getRowOffset(0)).toBe(csv.indexOf('1'));
    expect(idx.getRowRange(1)[0]).toBe(csv.indexOf('3'));
    expect(() => idx.getRowOffset(5)).toThrow();
  });

  test('getRowsRange returns contiguous range to file end', () => {
    const csv = 'a\n1\n2\n';
    const buffer = Buffer.from(csv);
    const idx = RowIndex.build(buffer, false);

    const [start, end] = idx.getRowsRange(0, 3);
    expect(start).toBe(0);
    expect(end).toBe(buffer.length);
  });
});

describe('LazyFrame parse internals', () => {
  test('parses quoted fields and booleans via scanCsv', async () => {
    const lazy = await scanCsv(QUOTED_PATH);
    const head = await lazy.head(2);

    expect(head.col('name').at(0)).toBe('hello, world');
    expect(head.col('name').at(1)).toBe('He said "Hi"');
    expect(head.col('flag').at(0)).toBe(true);
  });

  test('column view retains behavior for head/tail/collect/info', async () => {
    const lazy = await scanCsv(QUOTED_PATH);
    const view = lazy.select('id', 'flag');

    const head = await view.head(1);
    expect(head.columns()).toEqual(['id', 'flag']);

    const tail = await view.tail(1);
    expect(tail.shape[0]).toBe(1);

    const collected = await view.collect(1);
    expect(collected.shape[1]).toBe(2);

    const info = view.info();
    expect(info.columns).toBe(2);
  });

  test('internal parse line/value helpers handle quotes and bools', async () => {
    const lazy = await scanCsv(QUOTED_PATH);
    const parseLine = (
      lazy as unknown as { _parseLine: (line: string) => string[] }
    )._parseLine.bind(lazy);
    const parseValue = (
      lazy as unknown as { _parseValue: (v: string, d: unknown) => unknown }
    )._parseValue.bind(lazy);

    expect(parseLine('"a,b",c')).toEqual(['a,b', 'c']);

    const boolTrue = parseValue('True', { kind: 'bool' });
    const boolFalse = parseValue('0', { kind: 'bool' });
    expect(boolTrue).toBe(true);
    expect(boolFalse).toBe(false);
  });
});
