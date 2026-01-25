import { DType } from '../types/dtypes';
import { type DataFrame, getColumn, getColumnNames } from './dataframe';

/**
 * Helper to sort numeric values for median/quantile calculations
 * @param values - Array of numbers
 * @returns Sorted array
 */
function sortNumbers(values: number[]): number[] {
  return values.slice().sort((a, b) => a - b);
}

/**
 * Compute sum of a numeric column using raw DataView access
 * @param df - Source DataFrame
 * @param columnName - Column name (if omitted, computes for all numeric columns)
 * @returns Sum value or record of sums
 * @throws Error if column not found or not numeric
 */
export function sum<T>(df: DataFrame<T>): Record<string, number>;
export function sum<T>(df: DataFrame<T>, columnName: string): number;
export function sum<T>(df: DataFrame<T>, columnName?: string): number | Record<string, number> {
  if (columnName) {
    // Single column - raw DataView access
    const colResult = getColumn(df, columnName as keyof T & string);
    if (!colResult.ok) {
      throw new Error(colResult.error);
    }
    const col = colResult.data;

    if (col.dtype !== DType.Float64 && col.dtype !== DType.Int32) {
      throw new Error(`Column '${columnName}' is not numeric (dtype: ${col.dtype})`);
    }

    let total = 0;
    if (col.dtype === DType.Float64) {
      for (let i = 0; i < col.length; i++) {
        total += col.view.getFloat64(i * 8, true);
      }
    } else {
      for (let i = 0; i < col.length; i++) {
        total += col.view.getInt32(i * 4, true);
      }
    }
    return total;
  }

  // All numeric columns
  const result: Record<string, number> = {};
  const allColumns = getColumnNames(df);

  for (const colName of allColumns) {
    const colResult = getColumn(df, colName as keyof T & string);
    if (!colResult.ok) continue;

    const col = colResult.data;
    if (col.dtype === DType.Float64 || col.dtype === DType.Int32) {
      let total = 0;
      if (col.dtype === DType.Float64) {
        for (let i = 0; i < col.length; i++) {
          total += col.view.getFloat64(i * 8, true);
        }
      } else {
        for (let i = 0; i < col.length; i++) {
          total += col.view.getInt32(i * 4, true);
        }
      }
      result[colName] = total;
    }
  }

  return result;
}

/**
 * Compute mean of a numeric column using raw DataView access
 * @param df - Source DataFrame
 * @param columnName - Column name (if omitted, computes for all numeric columns)
 * @returns Mean value or record of means
 * @throws Error if column not found or not numeric
 */
export function mean<T>(df: DataFrame<T>): Record<string, number>;
export function mean<T>(df: DataFrame<T>, columnName: string): number;
export function mean<T>(df: DataFrame<T>, columnName?: string): number | Record<string, number> {
  if (columnName) {
    // Single column - raw DataView access
    const colResult = getColumn(df, columnName as keyof T & string);
    if (!colResult.ok) {
      throw new Error(colResult.error);
    }
    const col = colResult.data;

    if (col.dtype !== DType.Float64 && col.dtype !== DType.Int32) {
      throw new Error(`Column '${columnName}' is not numeric (dtype: ${col.dtype})`);
    }
    if (col.length === 0) return 0;

    let total = 0;
    if (col.dtype === DType.Float64) {
      for (let i = 0; i < col.length; i++) {
        total += col.view.getFloat64(i * 8, true);
      }
    } else {
      for (let i = 0; i < col.length; i++) {
        total += col.view.getInt32(i * 4, true);
      }
    }
    return total / col.length;
  }

  // All numeric columns
  const result: Record<string, number> = {};
  const allColumns = getColumnNames(df);

  for (const colName of allColumns) {
    const colResult = getColumn(df, colName as keyof T & string);
    if (!colResult.ok) continue;

    const col = colResult.data;
    if (col.dtype === DType.Float64 || col.dtype === DType.Int32) {
      if (col.length === 0) {
        result[colName] = 0;
        continue;
      }

      let total = 0;
      if (col.dtype === DType.Float64) {
        for (let i = 0; i < col.length; i++) {
          total += col.view.getFloat64(i * 8, true);
        }
      } else {
        for (let i = 0; i < col.length; i++) {
          total += col.view.getInt32(i * 4, true);
        }
      }
      result[colName] = total / col.length;
    }
  }

  return result;
}

/**
 * Compute minimum value of a numeric column using raw DataView access
 * @param df - Source DataFrame
 * @param columnName - Column name (if omitted, computes for all numeric columns)
 * @returns Minimum value or record of minimums
 * @throws Error if column not found or not numeric
 */
export function min<T>(df: DataFrame<T>): Record<string, number>;
export function min<T>(df: DataFrame<T>, columnName: string): number;
export function min<T>(df: DataFrame<T>, columnName?: string): number | Record<string, number> {
  if (columnName) {
    // Single column - raw DataView access
    const colResult = getColumn(df, columnName as keyof T & string);
    if (!colResult.ok) {
      throw new Error(colResult.error);
    }
    const col = colResult.data;

    if (col.dtype !== DType.Float64 && col.dtype !== DType.Int32) {
      throw new Error(`Column '${columnName}' is not numeric (dtype: ${col.dtype})`);
    }
    if (col.length === 0) return Number.POSITIVE_INFINITY;

    let minVal = Number.POSITIVE_INFINITY;
    if (col.dtype === DType.Float64) {
      for (let i = 0; i < col.length; i++) {
        const val = col.view.getFloat64(i * 8, true);
        if (val < minVal) minVal = val;
      }
    } else {
      for (let i = 0; i < col.length; i++) {
        const val = col.view.getInt32(i * 4, true);
        if (val < minVal) minVal = val;
      }
    }
    return minVal;
  }

  // All numeric columns
  const result: Record<string, number> = {};
  const allColumns = getColumnNames(df);

  for (const colName of allColumns) {
    const colResult = getColumn(df, colName as keyof T & string);
    if (!colResult.ok) continue;

    const col = colResult.data;
    if (col.dtype === DType.Float64 || col.dtype === DType.Int32) {
      if (col.length === 0) {
        result[colName] = Number.POSITIVE_INFINITY;
        continue;
      }

      let minVal = Number.POSITIVE_INFINITY;
      if (col.dtype === DType.Float64) {
        for (let i = 0; i < col.length; i++) {
          const val = col.view.getFloat64(i * 8, true);
          if (val < minVal) minVal = val;
        }
      } else {
        for (let i = 0; i < col.length; i++) {
          const val = col.view.getInt32(i * 4, true);
          if (val < minVal) minVal = val;
        }
      }
      result[colName] = minVal;
    }
  }

  return result;
}

/**
 * Compute maximum value of a numeric column using raw DataView access
 * @param df - Source DataFrame
 * @param columnName - Column name (if omitted, computes for all numeric columns)
 * @returns Maximum value or record of maximums
 * @throws Error if column not found or not numeric
 */
export function max<T>(df: DataFrame<T>): Record<string, number>;
export function max<T>(df: DataFrame<T>, columnName: string): number;
export function max<T>(df: DataFrame<T>, columnName?: string): number | Record<string, number> {
  if (columnName) {
    // Single column - raw DataView access
    const colResult = getColumn(df, columnName as keyof T & string);
    if (!colResult.ok) {
      throw new Error(colResult.error);
    }
    const col = colResult.data;

    if (col.dtype !== DType.Float64 && col.dtype !== DType.Int32) {
      throw new Error(`Column '${columnName}' is not numeric (dtype: ${col.dtype})`);
    }
    if (col.length === 0) return Number.NEGATIVE_INFINITY;

    let maxVal = Number.NEGATIVE_INFINITY;
    if (col.dtype === DType.Float64) {
      for (let i = 0; i < col.length; i++) {
        const val = col.view.getFloat64(i * 8, true);
        if (val > maxVal) maxVal = val;
      }
    } else {
      for (let i = 0; i < col.length; i++) {
        const val = col.view.getInt32(i * 4, true);
        if (val > maxVal) maxVal = val;
      }
    }
    return maxVal;
  }

  // All numeric columns
  const result: Record<string, number> = {};
  const allColumns = getColumnNames(df);

  for (const colName of allColumns) {
    const colResult = getColumn(df, colName as keyof T & string);
    if (!colResult.ok) continue;

    const col = colResult.data;
    if (col.dtype === DType.Float64 || col.dtype === DType.Int32) {
      if (col.length === 0) {
        result[colName] = Number.NEGATIVE_INFINITY;
        continue;
      }

      let maxVal = Number.NEGATIVE_INFINITY;
      if (col.dtype === DType.Float64) {
        for (let i = 0; i < col.length; i++) {
          const val = col.view.getFloat64(i * 8, true);
          if (val > maxVal) maxVal = val;
        }
      } else {
        for (let i = 0; i < col.length; i++) {
          const val = col.view.getInt32(i * 4, true);
          if (val > maxVal) maxVal = val;
        }
      }
      result[colName] = maxVal;
    }
  }

  return result;
}

/**
 * Compute median of a numeric column using raw DataView access
 * @param df - Source DataFrame
 * @param columnName - Column name (if omitted, computes for all numeric columns)
 * @returns Median value or record of medians
 * @throws Error if column not found or not numeric
 */
export function median<T>(df: DataFrame<T>): Record<string, number>;
export function median<T>(df: DataFrame<T>, columnName: string): number;
export function median<T>(df: DataFrame<T>, columnName?: string): number | Record<string, number> {
  if (columnName) {
    // Single column - raw DataView access
    const colResult = getColumn(df, columnName as keyof T & string);
    if (!colResult.ok) {
      throw new Error(colResult.error);
    }
    const col = colResult.data;

    if (col.dtype !== DType.Float64 && col.dtype !== DType.Int32) {
      throw new Error(`Column '${columnName}' is not numeric (dtype: ${col.dtype})`);
    }
    if (col.length === 0) return 0;

    // Collect values
    const values: number[] = [];
    if (col.dtype === DType.Float64) {
      for (let i = 0; i < col.length; i++) {
        values.push(col.view.getFloat64(i * 8, true));
      }
    } else {
      for (let i = 0; i < col.length; i++) {
        values.push(col.view.getInt32(i * 4, true));
      }
    }

    // Sort and find median
    const sorted = sortNumbers(values);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) {
      return (sorted[mid - 1]! + sorted[mid]!) / 2;
    }
    return sorted[mid]!;
  }

  // All numeric columns
  const result: Record<string, number> = {};
  const allColumns = getColumnNames(df);

  for (const colName of allColumns) {
    const colResult = getColumn(df, colName as keyof T & string);
    if (!colResult.ok) continue;

    const col = colResult.data;
    if (col.dtype === DType.Float64 || col.dtype === DType.Int32) {
      if (col.length === 0) {
        result[colName] = 0;
        continue;
      }

      const values: number[] = [];
      if (col.dtype === DType.Float64) {
        for (let i = 0; i < col.length; i++) {
          values.push(col.view.getFloat64(i * 8, true));
        }
      } else {
        for (let i = 0; i < col.length; i++) {
          values.push(col.view.getInt32(i * 4, true));
        }
      }

      const sorted = sortNumbers(values);
      const mid = Math.floor(sorted.length / 2);
      if (sorted.length % 2 === 0) {
        result[colName] = (sorted[mid - 1]! + sorted[mid]!) / 2;
      } else {
        result[colName] = sorted[mid]!;
      }
    }
  }

  return result;
}

/**
 * Compute mode (most frequent value) of a numeric column
 * @param df - Source DataFrame
 * @param columnName - Column name (if omitted, computes for all numeric columns)
 * @returns Mode value or record of modes
 * @throws Error if column not found or not numeric
 */
export function mode<T>(df: DataFrame<T>): Record<string, number>;
export function mode<T>(df: DataFrame<T>, columnName: string): number;
export function mode<T>(df: DataFrame<T>, columnName?: string): number | Record<string, number> {
  if (columnName) {
    // Single column - raw DataView access
    const colResult = getColumn(df, columnName as keyof T & string);
    if (!colResult.ok) {
      throw new Error(colResult.error);
    }
    const col = colResult.data;

    if (col.dtype !== DType.Float64 && col.dtype !== DType.Int32) {
      throw new Error(`Column '${columnName}' is not numeric (dtype: ${col.dtype})`);
    }
    if (col.length === 0) return 0;

    // Count frequency of each value
    const freq = new Map<number, number>();
    if (col.dtype === DType.Float64) {
      for (let i = 0; i < col.length; i++) {
        const val = col.view.getFloat64(i * 8, true);
        freq.set(val, (freq.get(val) || 0) + 1);
      }
    } else {
      for (let i = 0; i < col.length; i++) {
        const val = col.view.getInt32(i * 4, true);
        freq.set(val, (freq.get(val) || 0) + 1);
      }
    }

    // Find value with highest frequency
    let modeVal = 0;
    let maxFreq = 0;
    for (const [val, count] of freq) {
      if (count > maxFreq) {
        maxFreq = count;
        modeVal = val;
      }
    }

    return modeVal;
  }

  // All numeric columns
  const result: Record<string, number> = {};
  const allColumns = getColumnNames(df);

  for (const colName of allColumns) {
    const colResult = getColumn(df, colName as keyof T & string);
    if (!colResult.ok) continue;

    const col = colResult.data;
    if (col.dtype === DType.Float64 || col.dtype === DType.Int32) {
      if (col.length === 0) {
        result[colName] = 0;
        continue;
      }

      const freq = new Map<number, number>();
      if (col.dtype === DType.Float64) {
        for (let i = 0; i < col.length; i++) {
          const val = col.view.getFloat64(i * 8, true);
          freq.set(val, (freq.get(val) || 0) + 1);
        }
      } else {
        for (let i = 0; i < col.length; i++) {
          const val = col.view.getInt32(i * 4, true);
          freq.set(val, (freq.get(val) || 0) + 1);
        }
      }

      let modeVal = 0;
      let maxFreq = 0;
      for (const [val, count] of freq) {
        if (count > maxFreq) {
          maxFreq = count;
          modeVal = val;
        }
      }

      result[colName] = modeVal;
    }
  }

  return result;
}

/**
 * Count non-null values per column
 * For simplicity, returns row count (null tracking to be implemented)
 * @param df - Source DataFrame
 * @param columnName - Column name (if omitted, computes for all columns)
 * @returns Count or record of counts
 * @throws Error if column not found
 */
export function count<T>(df: DataFrame<T>): Record<string, number>;
export function count<T>(df: DataFrame<T>, columnName: string): number;
export function count<T>(df: DataFrame<T>, columnName?: string): number | Record<string, number> {
  if (columnName) {
    const colResult = getColumn(df, columnName as keyof T & string);
    if (!colResult.ok) {
      throw new Error(colResult.error);
    }
    // Return length (null bitmap check to be added)
    return colResult.data.length;
  }

  // All columns
  const result: Record<string, number> = {};
  const allColumns = getColumnNames(df);

  for (const colName of allColumns) {
    const colResult = getColumn(df, colName as keyof T & string);
    if (!colResult.ok) continue;
    result[colName] = colResult.data.length;
  }

  return result;
}
