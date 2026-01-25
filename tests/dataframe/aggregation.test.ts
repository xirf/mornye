import { describe, expect, test } from 'bun:test';
import { fromArrays, max, mean, median, min, mode, sum } from '../../src/index';

describe('median()', () => {
  test('calculates median for single column (odd count)', () => {
    const df = fromArrays({
      values: [1, 2, 3, 4, 5],
    });

    const result = median(df, 'values');
    expect(result).toBe(3);
  });

  test('calculates median for single column (even count)', () => {
    const df = fromArrays({
      values: [1, 2, 3, 4, 5, 6],
    });

    const result = median(df, 'values');
    expect(result).toBe(3.5); // (3 + 4) / 2
  });

  test('calculates median for all numeric columns', () => {
    const df = fromArrays({
      a: [1, 2, 3, 4, 5],
      b: [10, 20, 30, 40, 50],
      c: [100, 200, 300, 400, 500],
    });

    const result = median(df);
    expect(result.a).toBe(3);
    expect(result.b).toBe(30);
    expect(result.c).toBe(300);
  });

  test('handles unsorted data', () => {
    const df = fromArrays({
      values: [5, 1, 4, 2, 3],
    });

    const result = median(df, 'values');
    expect(result).toBe(3);
  });

  test('handles duplicate values', () => {
    const df = fromArrays({
      values: [1, 2, 2, 3, 3, 3],
    });

    const result = median(df, 'values');
    expect(result).toBe(2.5); // (2 + 3) / 2
  });

  test('handles Float64 columns', () => {
    const df = fromArrays({
      prices: [10.5, 20.3, 30.7, 40.2, 50.9],
    });

    const result = median(df, 'prices');
    expect(result).toBeCloseTo(30.7);
  });

  test('returns 0 for empty column', () => {
    const df = fromArrays({
      values: [] as number[],
    });

    const result = median(df, 'values');
    expect(result).toBe(0);
  });

  test('throws error for non-numeric column', () => {
    const df = fromArrays({
      names: ['Alice', 'Bob', 'Charlie'],
    });

    expect(() => median(df, 'names')).toThrow();
  });

  test('throws error for non-existent column', () => {
    const df = fromArrays({
      values: [1, 2, 3],
    });

    expect(() => median(df, 'nonexistent')).toThrow();
  });

  test('handles single value', () => {
    const df = fromArrays({
      values: [42],
    });

    const result = median(df, 'values');
    expect(result).toBe(42);
  });

  test('handles two values', () => {
    const df = fromArrays({
      values: [10, 20],
    });

    const result = median(df, 'values');
    expect(result).toBe(15);
  });
});

describe('mode()', () => {
  test('finds mode with single most frequent value', () => {
    const df = fromArrays({
      values: [1, 2, 2, 3, 3, 3, 4],
    });

    const result = mode(df, 'values');
    expect(result).toBe(3);
  });

  test('finds mode when all values are same', () => {
    const df = fromArrays({
      values: [5, 5, 5, 5],
    });

    const result = mode(df, 'values');
    expect(result).toBe(5);
  });

  test('finds mode with all unique values (returns first)', () => {
    const df = fromArrays({
      values: [1, 2, 3, 4, 5],
    });

    const result = mode(df, 'values');
    // Should return first value when all have same frequency
    expect([1, 2, 3, 4, 5]).toContain(result);
  });

  test('calculates mode for all numeric columns', () => {
    const df = fromArrays({
      a: [1, 1, 2, 3],
      b: [10, 20, 20, 30],
      c: [100, 100, 100, 200],
    });

    const result = mode(df);
    expect(result.a).toBe(1);
    expect(result.b).toBe(20);
    expect(result.c).toBe(100);
  });

  test('handles Float64 columns', () => {
    const df = fromArrays({
      prices: [10.5, 20.3, 20.3, 30.7],
    });

    const result = mode(df, 'prices');
    expect(result).toBeCloseTo(20.3);
  });

  test('returns 0 for empty column', () => {
    const df = fromArrays({
      values: [] as number[],
    });

    const result = mode(df, 'values');
    expect(result).toBe(0);
  });

  test('throws error for non-numeric column', () => {
    const df = fromArrays({
      names: ['Alice', 'Bob', 'Charlie'],
    });

    expect(() => mode(df, 'names')).toThrow();
  });

  test('throws error for non-existent column', () => {
    const df = fromArrays({
      values: [1, 2, 3],
    });

    expect(() => mode(df, 'nonexistent')).toThrow();
  });

  test('handles single value', () => {
    const df = fromArrays({
      values: [42],
    });

    const result = mode(df, 'values');
    expect(result).toBe(42);
  });

  test('handles bimodal distribution (returns one)', () => {
    const df = fromArrays({
      values: [1, 1, 2, 2, 3],
    });

    const result = mode(df, 'values');
    // Should return either 1 or 2
    expect([1, 2]).toContain(result);
  });
});

describe('aggregation functions integration', () => {
  test('all aggregation functions work together', () => {
    const df = fromArrays({
      values: [10, 20, 30, 40, 50],
    });

    expect(sum(df, 'values')).toBe(150);
    expect(mean(df, 'values')).toBe(30);
    expect(min(df, 'values')).toBe(10);
    expect(max(df, 'values')).toBe(50);
    expect(median(df, 'values')).toBe(30);
    expect(mode(df, 'values')).toBe(10); // All same frequency
  });

  test('aggregations handle mixed distributions', () => {
    const df = fromArrays({
      values: [5, 10, 10, 15, 20, 20, 20, 25],
    });

    expect(sum(df, 'values')).toBe(125);
    expect(mean(df, 'values')).toBe(15.625);
    expect(min(df, 'values')).toBe(5);
    expect(max(df, 'values')).toBe(25);
    expect(median(df, 'values')).toBe(17.5); // (15 + 20) / 2
    expect(mode(df, 'values')).toBe(20); // Most frequent
  });

  test('all-column aggregations return consistent results', () => {
    const df = fromArrays({
      a: [1, 2, 3],
      b: [10, 20, 30],
    });

    const sums = sum(df);
    const medians = median(df);
    const modes = mode(df);

    expect(sums.a).toBe(6);
    expect(sums.b).toBe(60);
    expect(medians.a).toBe(2);
    expect(medians.b).toBe(20);
    expect(modes.a).toBe(1);
    expect(modes.b).toBe(10);
  });
});
