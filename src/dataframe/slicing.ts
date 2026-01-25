/**
 * DataFrame slicing operations: head, tail, slice
 * Raw DataView operations for performance
 */

import { DType } from '../types/dtypes';
import {
  type DataFrame,
  addColumn,
  createDataFrame,
  getColumn,
  getColumnNames,
  getRowCount,
} from './dataframe';

/**
 * Get first N rows of DataFrame
 * @param df - Source DataFrame
 * @param n - Number of rows to return (default: 5)
 * @returns New DataFrame with first N rows
 */
export function head<T>(df: DataFrame<T>, n = 5): DataFrame<T> {
  const rowCount = getRowCount(df);
  const actualN = Math.min(n, rowCount);

  if (actualN === rowCount) {
    // Return entire DataFrame if N >= row count
    return df;
  }

  const resultDf = createDataFrame<T>();
  resultDf.dictionary = df.dictionary; // Share dictionary

  // Copy first N rows of each column
  const allColumns = getColumnNames(df);
  for (const colName of allColumns) {
    const sourceColResult = getColumn(df, colName as keyof T & string);
    if (!sourceColResult.ok) {
      throw new Error(sourceColResult.error);
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, colName, sourceCol.dtype, actualN);

    if (!addResult.ok) {
      throw new Error(addResult.error);
    }

    const destColResult = getColumn(resultDf, colName as keyof T & string);
    if (!destColResult.ok) {
      throw new Error(destColResult.error);
    }

    const destCol = destColResult.data;

    // Raw buffer copy for performance
    const bytesPerElement = getBytesPerElement(sourceCol.dtype);
    const sourceBytes = actualN * bytesPerElement;
    destCol.data.set(sourceCol.data.subarray(0, sourceBytes));

    // Copy null bitmap if exists
    if (sourceCol.nullBitmap && destCol.nullBitmap) {
      const nullBytes = Math.ceil(actualN / 8);
      destCol.nullBitmap.data.set(sourceCol.nullBitmap.data.subarray(0, nullBytes));
    }
  }

  return resultDf;
}

/**
 * Get last N rows of DataFrame
 * @param df - Source DataFrame
 * @param n - Number of rows to return (default: 5)
 * @returns New DataFrame with last N rows
 */
export function tail<T>(df: DataFrame<T>, n = 5): DataFrame<T> {
  const rowCount = getRowCount(df);
  const actualN = Math.min(n, rowCount);

  if (actualN === rowCount) {
    // Return entire DataFrame if N >= row count
    return df;
  }

  const resultDf = createDataFrame<T>();
  resultDf.dictionary = df.dictionary; // Share dictionary

  const startRow = rowCount - actualN;

  // Copy last N rows of each column
  const allColumns = getColumnNames(df);
  for (const colName of allColumns) {
    const sourceColResult = getColumn(df, colName as keyof T & string);
    if (!sourceColResult.ok) {
      throw new Error(sourceColResult.error);
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, colName, sourceCol.dtype, actualN);

    if (!addResult.ok) {
      throw new Error(addResult.error);
    }

    const destColResult = getColumn(resultDf, colName as keyof T & string);
    if (!destColResult.ok) {
      throw new Error(destColResult.error);
    }

    const destCol = destColResult.data;

    // Raw buffer copy from start position
    const bytesPerElement = getBytesPerElement(sourceCol.dtype);
    const sourceStartByte = startRow * bytesPerElement;
    const sourceBytes = actualN * bytesPerElement;
    destCol.data.set(sourceCol.data.subarray(sourceStartByte, sourceStartByte + sourceBytes));

    // Copy null bitmap if exists
    if (sourceCol.nullBitmap && destCol.nullBitmap) {
      // For null bitmap, we need to copy bits from the correct byte position
      const startBit = startRow;
      const startByte = Math.floor(startBit / 8);
      const bitOffset = startBit % 8;

      if (bitOffset === 0) {
        // Aligned - simple copy
        const nullBytes = Math.ceil(actualN / 8);
        destCol.nullBitmap.data.set(
          sourceCol.nullBitmap.data.subarray(startByte, startByte + nullBytes),
        );
      } else {
        // Unaligned - need to shift bits (simplified for now, just mark all as not null)
        destCol.nullBitmap.data.fill(0);
      }
    }
  }

  return resultDf;
}

/**
 * Get bytes per element for a dtype
 * @param dtype - Data type
 * @returns Number of bytes per element
 */
function getBytesPerElement(dtype: DType): number {
  switch (dtype) {
    case DType.Float64:
    case DType.DateTime:
    case DType.Date:
      return 8;
    case DType.Int32:
    case DType.String:
      return 4;
    case DType.Bool:
      return 1;
    default:
      return 8;
  }
}
