import { describe, expect, test } from 'bun:test';
import { getColumnNames, getRowCount } from '../../src/dataframe/dataframe';
import { fromArrays, head, tail } from '../../src/index';

describe('head()', () => {
  test('returns first 5 rows by default', () => {
    const df = fromArrays({
      id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      value: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    });

    const result = head(df);
    expect(getRowCount(result)).toBe(5);
    expect(getColumnNames(result)).toEqual(['id', 'value']);
  });

  test('returns first N rows when specified', () => {
    const df = fromArrays({
      id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      value: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    });

    const result = head(df, 3);
    expect(getRowCount(result)).toBe(3);

    // Verify first 3 values
    const idCol = result.columns.get('id')!;
    expect(idCol.view.getInt32(0, true)).toBe(1);
    expect(idCol.view.getInt32(4, true)).toBe(2);
    expect(idCol.view.getInt32(8, true)).toBe(3);
  });

  test('returns entire DataFrame when N >= row count', () => {
    const df = fromArrays({
      id: [1, 2, 3],
      value: [10, 20, 30],
    });

    const result = head(df, 10);
    expect(getRowCount(result)).toBe(3);
  });

  test('handles empty DataFrame', () => {
    const df = fromArrays({
      id: [] as number[],
      value: [] as number[],
    });

    const result = head(df);
    expect(getRowCount(result)).toBe(0);
  });

  test('preserves all columns', () => {
    const df = fromArrays({
      a: [1, 2, 3, 4, 5],
      b: [10, 20, 30, 40, 50],
      c: [100, 200, 300, 400, 500],
    });

    const result = head(df, 2);
    expect(getColumnNames(result)).toEqual(['a', 'b', 'c']);
    expect(getRowCount(result)).toBe(2);
  });

  test('works with Float64 columns', () => {
    const df = fromArrays({
      price: [10.5, 20.3, 30.7, 40.2, 50.9],
    });

    const result = head(df, 3);
    expect(getRowCount(result)).toBe(3);

    const priceCol = result.columns.get('price')!;
    expect(priceCol.view.getFloat64(0, true)).toBeCloseTo(10.5);
    expect(priceCol.view.getFloat64(8, true)).toBeCloseTo(20.3);
    expect(priceCol.view.getFloat64(16, true)).toBeCloseTo(30.7);
  });

  test('works with String columns', () => {
    const df = fromArrays({
      name: ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    });

    const result = head(df, 2);
    expect(getRowCount(result)).toBe(2);
    expect(getColumnNames(result)).toEqual(['name']);
  });
});

describe('tail()', () => {
  test('returns last 5 rows by default', () => {
    const df = fromArrays({
      id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      value: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    });

    const result = tail(df);
    expect(getRowCount(result)).toBe(5);
    expect(getColumnNames(result)).toEqual(['id', 'value']);
  });

  test('returns last N rows when specified', () => {
    const df = fromArrays({
      id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      value: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    });

    const result = tail(df, 3);
    expect(getRowCount(result)).toBe(3);

    // Verify last 3 values (8, 9, 10)
    const idCol = result.columns.get('id')!;
    expect(idCol.view.getInt32(0, true)).toBe(8);
    expect(idCol.view.getInt32(4, true)).toBe(9);
    expect(idCol.view.getInt32(8, true)).toBe(10);
  });

  test('returns entire DataFrame when N >= row count', () => {
    const df = fromArrays({
      id: [1, 2, 3],
      value: [10, 20, 30],
    });

    const result = tail(df, 10);
    expect(getRowCount(result)).toBe(3);
  });

  test('handles empty DataFrame', () => {
    const df = fromArrays({
      id: [] as number[],
      value: [] as number[],
    });

    const result = tail(df);
    expect(getRowCount(result)).toBe(0);
  });

  test('preserves all columns', () => {
    const df = fromArrays({
      a: [1, 2, 3, 4, 5],
      b: [10, 20, 30, 40, 50],
      c: [100, 200, 300, 400, 500],
    });

    const result = tail(df, 2);
    expect(getColumnNames(result)).toEqual(['a', 'b', 'c']);
    expect(getRowCount(result)).toBe(2);

    // Verify last 2 values
    const aCol = result.columns.get('a')!;
    expect(aCol.view.getInt32(0, true)).toBe(4);
    expect(aCol.view.getInt32(4, true)).toBe(5);
  });

  test('works with Float64 columns', () => {
    const df = fromArrays({
      price: [10.5, 20.3, 30.7, 40.2, 50.9],
    });

    const result = tail(df, 2);
    expect(getRowCount(result)).toBe(2);

    const priceCol = result.columns.get('price')!;
    expect(priceCol.view.getFloat64(0, true)).toBeCloseTo(40.2);
    expect(priceCol.view.getFloat64(8, true)).toBeCloseTo(50.9);
  });

  test('works with String columns', () => {
    const df = fromArrays({
      name: ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    });

    const result = tail(df, 2);
    expect(getRowCount(result)).toBe(2);
    expect(getColumnNames(result)).toEqual(['name']);
  });

  test('handles single row DataFrame', () => {
    const df = fromArrays({
      id: [42],
      value: [100],
    });

    const result = tail(df, 5);
    expect(getRowCount(result)).toBe(1);

    const idCol = result.columns.get('id')!;
    expect(idCol.view.getInt32(0, true)).toBe(42);
  });
});
