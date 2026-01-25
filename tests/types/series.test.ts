import { describe, expect, test } from 'bun:test';
import { createColumn } from '../../src/core/column';
import { DType } from '../../src/types/dtypes';
import { Series } from '../../src/types/series';

/**
 * Helper to create a Series from array of numbers
 */
function createNumericSeries(values: number[], dtype: DType = DType.Int32): Series<number> {
  const colResult = createColumn(dtype, values.length, 'test');
  if (!colResult.ok) {
    throw new Error(`Failed to create column: ${colResult.error}`);
  }
  const col = colResult.data;

  // Fill column with values
  if (dtype === DType.Float64) {
    for (let i = 0; i < values.length; i++) {
      col.view.setFloat64(i * 8, values[i]!, true);
    }
  } else if (dtype === DType.Int32) {
    for (let i = 0; i < values.length; i++) {
      col.view.setInt32(i * 4, values[i]!, true);
    }
  }

  return new Series<number>(col, undefined, 'test');
}

describe('Series.median()', () => {
  test('calculates median for odd count', () => {
    const series = createNumericSeries([1, 2, 3, 4, 5]);
    expect(series.median()).toBe(3);
  });

  test('calculates median for even count', () => {
    const series = createNumericSeries([1, 2, 3, 4, 5, 6]);
    expect(series.median()).toBe(3.5);
  });

  test('handles unsorted data', () => {
    const series = createNumericSeries([5, 1, 4, 2, 3]);
    expect(series.median()).toBe(3);
  });

  test('handles Float64 values', () => {
    const series = createNumericSeries([10.5, 20.3, 30.7, 40.2, 50.9], DType.Float64);
    expect(series.median()).toBeCloseTo(30.7);
  });

  test('returns 0 for empty series', () => {
    const series = createNumericSeries([]);
    expect(series.median()).toBe(0);
  });

  test('handles single value', () => {
    const series = createNumericSeries([42]);
    expect(series.median()).toBe(42);
  });
});

describe('Series.mode()', () => {
  test('finds most frequent value', () => {
    const series = createNumericSeries([1, 2, 2, 3, 3, 3, 4]);
    expect(series.mode()).toBe(3);
  });

  test('handles all same values', () => {
    const series = createNumericSeries([5, 5, 5, 5]);
    expect(series.mode()).toBe(5);
  });

  test('handles Float64 values', () => {
    const series = createNumericSeries([10.5, 20.3, 20.3, 30.7], DType.Float64);
    expect(series.mode()).toBeCloseTo(20.3);
  });

  test('returns 0 for empty series', () => {
    const series = createNumericSeries([]);
    expect(series.mode()).toBe(0);
  });

  test('handles single value', () => {
    const series = createNumericSeries([42]);
    expect(series.mode()).toBe(42);
  });
});

describe('Series.cumsum()', () => {
  test('calculates cumulative sum', () => {
    const series = createNumericSeries([1, 2, 3, 4, 5]);
    const result = series.cumsum();
    expect(result).toEqual([1, 3, 6, 10, 15]);
  });

  test('handles negative values', () => {
    const series = createNumericSeries([5, -2, 3, -1, 4]);
    const result = series.cumsum();
    expect(result).toEqual([5, 3, 6, 5, 9]);
  });

  test('handles Float64 values', () => {
    const series = createNumericSeries([1.5, 2.5, 3.5], DType.Float64);
    const result = series.cumsum();
    expect(result[0]).toBeCloseTo(1.5);
    expect(result[1]).toBeCloseTo(4.0);
    expect(result[2]).toBeCloseTo(7.5);
  });

  test('handles empty series', () => {
    const series = createNumericSeries([]);
    const result = series.cumsum();
    expect(result).toEqual([]);
  });

  test('handles single value', () => {
    const series = createNumericSeries([42]);
    const result = series.cumsum();
    expect(result).toEqual([42]);
  });

  test('handles zeros', () => {
    const series = createNumericSeries([1, 0, 2, 0, 3]);
    const result = series.cumsum();
    expect(result).toEqual([1, 1, 3, 3, 6]);
  });
});

describe('Series.cummax()', () => {
  test('calculates cumulative maximum', () => {
    const series = createNumericSeries([1, 3, 2, 5, 4]);
    const result = series.cummax();
    expect(result).toEqual([1, 3, 3, 5, 5]);
  });

  test('handles decreasing values', () => {
    const series = createNumericSeries([5, 4, 3, 2, 1]);
    const result = series.cummax();
    expect(result).toEqual([5, 5, 5, 5, 5]);
  });

  test('handles increasing values', () => {
    const series = createNumericSeries([1, 2, 3, 4, 5]);
    const result = series.cummax();
    expect(result).toEqual([1, 2, 3, 4, 5]);
  });

  test('handles negative values', () => {
    const series = createNumericSeries([-5, -2, -8, -1, -10]);
    const result = series.cummax();
    expect(result).toEqual([-5, -2, -2, -1, -1]);
  });

  test('handles Float64 values', () => {
    const series = createNumericSeries([1.5, 3.2, 2.1, 4.7], DType.Float64);
    const result = series.cummax();
    expect(result[0]).toBeCloseTo(1.5);
    expect(result[1]).toBeCloseTo(3.2);
    expect(result[2]).toBeCloseTo(3.2);
    expect(result[3]).toBeCloseTo(4.7);
  });

  test('handles empty series', () => {
    const series = createNumericSeries([]);
    const result = series.cummax();
    expect(result).toEqual([]);
  });

  test('handles single value', () => {
    const series = createNumericSeries([42]);
    const result = series.cummax();
    expect(result).toEqual([42]);
  });
});

describe('Series.cummin()', () => {
  test('calculates cumulative minimum', () => {
    const series = createNumericSeries([5, 2, 4, 1, 3]);
    const result = series.cummin();
    expect(result).toEqual([5, 2, 2, 1, 1]);
  });

  test('handles increasing values', () => {
    const series = createNumericSeries([1, 2, 3, 4, 5]);
    const result = series.cummin();
    expect(result).toEqual([1, 1, 1, 1, 1]);
  });

  test('handles decreasing values', () => {
    const series = createNumericSeries([5, 4, 3, 2, 1]);
    const result = series.cummin();
    expect(result).toEqual([5, 4, 3, 2, 1]);
  });

  test('handles negative values', () => {
    const series = createNumericSeries([-1, -5, -2, -10, -3]);
    const result = series.cummin();
    expect(result).toEqual([-1, -5, -5, -10, -10]);
  });

  test('handles Float64 values', () => {
    const series = createNumericSeries([3.5, 1.2, 2.8, 0.9], DType.Float64);
    const result = series.cummin();
    expect(result[0]).toBeCloseTo(3.5);
    expect(result[1]).toBeCloseTo(1.2);
    expect(result[2]).toBeCloseTo(1.2);
    expect(result[3]).toBeCloseTo(0.9);
  });

  test('handles empty series', () => {
    const series = createNumericSeries([]);
    const result = series.cummin();
    expect(result).toEqual([]);
  });

  test('handles single value', () => {
    const series = createNumericSeries([42]);
    const result = series.cummin();
    expect(result).toEqual([42]);
  });
});

describe('Series cumulative functions integration', () => {
  test('cumsum, cummax, cummin work together correctly', () => {
    const series = createNumericSeries([3, 1, 4, 1, 5]);

    const cumsum = series.cumsum();
    const cummax = series.cummax();
    const cummin = series.cummin();

    expect(cumsum).toEqual([3, 4, 8, 9, 14]);
    expect(cummax).toEqual([3, 3, 4, 4, 5]);
    expect(cummin).toEqual([3, 1, 1, 1, 1]);
  });

  test('handles mixed positive and negative values', () => {
    const series = createNumericSeries([5, -3, 8, -2, 1]);

    const cumsum = series.cumsum();
    const cummax = series.cummax();
    const cummin = series.cummin();

    expect(cumsum).toEqual([5, 2, 10, 8, 9]);
    expect(cummax).toEqual([5, 5, 8, 8, 8]);
    expect(cummin).toEqual([5, -3, -3, -3, -3]);
  });

  test('all Series aggregations work together', () => {
    const series = createNumericSeries([10, 20, 30, 40, 50]);

    expect(series.sum()).toBe(150);
    expect(series.mean()).toBe(30);
    expect(series.min()).toBe(10);
    expect(series.max()).toBe(50);
    expect(series.median()).toBe(30);
    expect(series.count()).toBe(5);

    const cumsum = series.cumsum();
    expect(cumsum[4]).toBe(150); // Final cumsum = total sum
  });
});
