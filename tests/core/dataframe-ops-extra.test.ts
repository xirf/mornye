import { describe, expect, test } from 'bun:test';
import { DataFrame } from '../../src/core/dataframe';
import { m } from '../../src/core/types';
import { ColumnNotFoundError, InvalidOperationError } from '../../src/errors';

const baseSchema = {
  a: m.int32(),
  b: m.string(),
  c: m.float64(),
} as const;

const baseRows = [
  { a: 1, b: 'x-ray', c: -1 },
  { a: 2, b: 'xylophone', c: 5 },
  { a: 2, b: 'yellow', c: 10 },
  { a: 3, b: 'xerox', c: Number.NaN },
];

describe('DataFrame extended operations', () => {
  test('cumulative operations leave non-numeric columns untouched', () => {
    const df = DataFrame.from(baseSchema, baseRows);
    const csum = df.cumsum();
    expect([...csum.col('a')]).toEqual([1, 3, 5, 8]);
    expect([...csum.col('b')]).toEqual(['x-ray', 'xylophone', 'yellow', 'xerox']);

    const cmax = df.cummax();
    expect([...cmax.col('a')]).toEqual([1, 2, 2, 3]);
  });

  test('unique() mirrors dropDuplicates()', () => {
    const df = DataFrame.from(baseSchema, baseRows);
    const uniq = df.unique();
    const dropped = df.dropDuplicates();

    expect(uniq.shape[0]).toBe(dropped.shape[0]);
    expect([...uniq.col('a')]).toEqual([...dropped.col('a')]);
  });

  test('dropDuplicates on subset columns keeps first occurrences', () => {
    const df = DataFrame.from(baseSchema, baseRows);
    const dedup = df.dropDuplicates('a');

    expect(dedup.shape[0]).toBe(3);
    expect([...dedup.col('b')]).toEqual(['x-ray', 'xylophone', 'xerox']);
  });

  test('replace handles NaN across columns', () => {
    const df = DataFrame.from(baseSchema, baseRows);
    const replaced = df.replace(Number.NaN, 0);

    expect([...replaced.col('c')]).toEqual([-1, 5, 10, 0]);
  });

  test('clip limits numeric columns and leaves strings unchanged', () => {
    const df = DataFrame.from(baseSchema, baseRows);
    const clipped = df.clip(0, 6);

    expect([...clipped.col('c')]).toEqual([0, 5, 6, Number.NaN]);
    expect([...clipped.col('b')]).toEqual(['x-ray', 'xylophone', 'yellow', 'xerox']);
  });

  test('ffill and bfill propagate valid values', () => {
    const df = DataFrame.from({ value: m.float64(), label: m.string() }, [
      { value: 1, label: 'start' },
      { value: Number.NaN, label: null as unknown as string },
      { value: Number.NaN, label: 'final' },
    ]);

    const ffilled = df.ffill();
    expect([...ffilled.col('value')]).toEqual([1, 1, 1]);
    expect([...ffilled.col('label')]).toEqual(['start', 'start', 'final']);

    const toBfill = DataFrame.from({ value: m.float64(), label: m.string() }, [
      { value: Number.NaN, label: null as unknown as string },
      { value: 5, label: 'mid' },
      { value: 7, label: 'end' },
    ]);
    const bfilled = toBfill.bfill();
    expect([...bfilled.col('value')]).toEqual([5, 5, 7]);
    expect([...bfilled.col('label')]).toEqual(['mid', 'mid', 'end']);
  });

  test('iloc string slicing supports start:end and validates input', () => {
    const df = DataFrame.from(baseSchema, baseRows);
    const sliced = df.iloc('1:3');

    expect(sliced.shape[0]).toBe(2);
    expect([...sliced.col('a')]).toEqual([2, 2]);

    expect(() => df.iloc('bad')).toThrow(/Invalid row split parameter/);
  });

  test('where supports in and contains operators', () => {
    const df = DataFrame.from(baseSchema, baseRows);
    const inFiltered = df.where('a', 'in', [1, 3]);
    expect([...inFiltered.col('a')]).toEqual([1, 3]);

    const containsFiltered = df.where('b', 'contains', 'x');
    expect(containsFiltered.shape[0]).toBe(3);

    const ltFiltered = df.where('c', '<=', 0);
    expect([...ltFiltered.col('a')]).toEqual([1]);
  });

  test('col throws ColumnNotFoundError', () => {
    const df = DataFrame.from(baseSchema, baseRows);
    expect(() => df.col('missing' as never)).toThrow(ColumnNotFoundError);
  });

  test('groupby exposes size and rejects invalid agg', () => {
    const df = DataFrame.from({ g: m.string(), v: m.int32() }, [
      { g: 'A', v: 1 },
      { g: 'B', v: 2 },
      { g: 'A', v: 3 },
    ]);

    const grouped = df.groupby('g');
    expect(grouped.size).toBe(2);

    const minMax = grouped.agg({ v: 'max' });
    const rowA = minMax.find((r) => r.g === 'A');
    expect(rowA?.v).toBe(3);

    expect(() => grouped.agg({ v: 'median' as unknown as 'sum' })).toThrow(InvalidOperationError);
  });

  test('toString uses ellipsis for large outputs', () => {
    const df = DataFrame.fromColumns({ idx: Array.from({ length: 12 }, (_, i) => i + 1) });
    const str = df.toString();

    expect(str.split('\n').some((line) => line.includes('...'))).toBe(true);
  });
});
