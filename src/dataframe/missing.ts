/**
 * Missing data operations for DataFrames
 * All operations work directly on Uint8Array buffers for maximum performance
 */

import { createColumn, enableNullTracking, getColumnValue, setColumnValue } from '../core/column';
import { DType, getDTypeSize } from '../types/dtypes';
import { type Result, err, ok } from '../types/result';
import { isNull, setNull } from '../utils/nulls';
import {
  type DataFrame,
  addColumn,
  createDataFrame,
  getColumn,
  getColumnNames,
  getRowCount,
} from './dataframe';

/**
 * Creates a boolean DataFrame indicating which values are null
 * Returns a new DataFrame with boolean columns showing null positions
 *
 * @param df - Source DataFrame
 * @returns Result with boolean DataFrame or error
 */
export function isna(df: DataFrame): Result<DataFrame, Error> {
  const rowCount = getRowCount(df);
  const resultDf = createDataFrame();
  const columnNames = getColumnNames(df);

  for (const colName of columnNames) {
    const columnResult = getColumn(df, colName);
    if (!columnResult.ok) {
      return err(new Error(columnResult.error));
    }

    const sourceCol = columnResult.data;

    // Create boolean column for null indicators
    const addResult = addColumn(resultDf, colName, DType.Bool, rowCount);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // HOT LOOP: Check each value for null - work directly with Uint8Array
    for (let i = 0; i < rowCount; i++) {
      const isNullValue = sourceCol.nullBitmap ? isNull(sourceCol.nullBitmap, i) : false;
      // Write boolean directly to Uint8Array (0 = false, 1 = true)
      destCol.view.setUint8(i, isNullValue ? 1 : 0);
    }
  }

  return ok(resultDf);
}

/**
 * Creates a boolean DataFrame indicating which values are not null
 * Returns a new DataFrame with boolean columns showing non-null positions
 *
 * @param df - Source DataFrame
 * @returns Result with boolean DataFrame or error
 */
export function notna(df: DataFrame): Result<DataFrame, Error> {
  const rowCount = getRowCount(df);
  const resultDf = createDataFrame();
  const columnNames = getColumnNames(df);

  for (const colName of columnNames) {
    const columnResult = getColumn(df, colName);
    if (!columnResult.ok) {
      return err(new Error(columnResult.error));
    }

    const sourceCol = columnResult.data;

    // Create boolean column for non-null indicators
    const addResult = addColumn(resultDf, colName, DType.Bool, rowCount);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // HOT LOOP: Check each value for non-null - work directly with Uint8Array
    for (let i = 0; i < rowCount; i++) {
      const isNullValue = sourceCol.nullBitmap ? isNull(sourceCol.nullBitmap, i) : false;
      // Write boolean directly to Uint8Array (0 = false, 1 = true)
      destCol.view.setUint8(i, isNullValue ? 0 : 1);
    }
  }

  return ok(resultDf);
}

/**
 * Drop rows with missing values
 * Returns a new DataFrame with rows containing null values removed
 *
 * @param df - Source DataFrame
 * @param options - Drop options
 * @returns Result with filtered DataFrame or error
 */
export function dropna(
  df: DataFrame,
  options?: {
    /** How to determine if row should be dropped. 'any' = drop if any null, 'all' = drop if all null */
    how?: 'any' | 'all';
    /** Only consider these columns for null detection */
    subset?: string[];
  },
): Result<DataFrame, Error> {
  const rowCount = getRowCount(df);
  const how = options?.how ?? 'any';
  const columnNames = options?.subset ?? getColumnNames(df);

  // Validate subset columns exist
  for (const colName of columnNames) {
    const colResult = getColumn(df, colName);
    if (!colResult.ok) {
      return err(new Error(`Column '${colName}' not found in subset`));
    }
  }

  // Get columns to check
  const columnsToCheck = columnNames
    .map((name) => {
      const colResult = getColumn(df, name);
      return colResult.ok ? colResult.data : null;
    })
    .filter((col) => col !== null);

  // Build list of rows to keep
  const keepIndices: number[] = [];

  // HOT LOOP: Check each row for nulls
  for (let rowIdx = 0; rowIdx < rowCount; rowIdx++) {
    let hasNull = false;
    let allNull = true;

    for (const col of columnsToCheck) {
      const isNullValue = col.nullBitmap ? isNull(col.nullBitmap, rowIdx) : false;

      if (isNullValue) {
        hasNull = true;
      } else {
        allNull = false;
      }
    }

    // Determine if row should be kept
    const shouldKeep = how === 'any' ? !hasNull : !allNull;

    if (shouldKeep) {
      keepIndices.push(rowIdx);
    }
  }

  // Create new DataFrame with kept rows
  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary; // Share dictionary

  const resultRowCount = keepIndices.length;

  // If no rows kept, return empty DataFrame with schema
  if (resultRowCount === 0) {
    const allColumns = getColumnNames(df);
    for (const colName of allColumns) {
      const sourceColResult = getColumn(df, colName);
      if (!sourceColResult.ok) {
        return err(new Error(sourceColResult.error));
      }
      const sourceCol = sourceColResult.data;
      const addResult = addColumn(resultDf, colName, sourceCol.dtype, 0);
      if (!addResult.ok) {
        return err(new Error(addResult.error));
      }
    }
    return ok(resultDf);
  }

  // Copy all columns with kept rows
  const allColumns = getColumnNames(df);
  for (const colName of allColumns) {
    const sourceColResult = getColumn(df, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, colName, sourceCol.dtype, resultRowCount);

    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // Enable null tracking if source has it
    if (sourceCol.nullBitmap) {
      enableNullTracking(destCol);
    }

    const bytesPerElement = getDTypeSize(sourceCol.dtype);

    // HOT LOOP: Copy kept rows - work directly with Uint8Array
    for (let i = 0; i < resultRowCount; i++) {
      const sourceRowIdx = keepIndices[i];
      if (sourceRowIdx === undefined) continue;

      // Copy value directly from buffer
      const sourceOffset = sourceRowIdx * bytesPerElement;
      const destOffset = i * bytesPerElement;

      // Copy bytes directly for maximum performance
      for (let b = 0; b < bytesPerElement; b++) {
        const byte = sourceCol.data[sourceOffset + b];
        if (byte !== undefined) {
          destCol.data[destOffset + b] = byte;
        }
      }

      // Copy null status if present
      if (sourceCol.nullBitmap && destCol.nullBitmap) {
        if (isNull(sourceCol.nullBitmap, sourceRowIdx)) {
          setNull(destCol.nullBitmap, i);
        }
      }
    }
  }

  return ok(resultDf);
}

/**
 * Fill null values with a specified value
 * Returns a new DataFrame with nulls replaced
 *
 * @param df - Source DataFrame
 * @param value - Value to fill nulls with (number, bigint, or column-specific object)
 * @param options - Fill options
 * @returns Result with filled DataFrame or error
 */
export function fillna(
  df: DataFrame,
  value: number | bigint | string | Record<string, number | bigint | string>,
  options?: {
    /** Only fill nulls in these columns */
    subset?: string[];
  },
): Result<DataFrame, Error> {
  const rowCount = getRowCount(df);
  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary; // Share dictionary

  const columnNames = options?.subset ?? getColumnNames(df);
  const allColumns = getColumnNames(df);

  // Parse fill values
  const fillValues = new Map<string, number | bigint | string>();

  if (typeof value === 'object' && !Array.isArray(value)) {
    // Column-specific fill values
    for (const [colName, fillVal] of Object.entries(value)) {
      fillValues.set(colName, fillVal);
    }
  } else {
    // Single fill value for all columns
    const scalarValue = value as number | bigint | string;
    for (const colName of columnNames) {
      fillValues.set(colName, scalarValue);
    }
  }

  // Copy all columns
  for (const colName of allColumns) {
    const sourceColResult = getColumn(df, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, colName, sourceCol.dtype, rowCount);

    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // Enable null tracking if source has it
    if (sourceCol.nullBitmap) {
      enableNullTracking(destCol);
    }

    const bytesPerElement = getDTypeSize(sourceCol.dtype);
    const shouldFill = fillValues.has(colName);
    const fillValue = fillValues.get(colName);

    // HOT LOOP: Copy values, filling nulls - work directly with Uint8Array
    for (let i = 0; i < rowCount; i++) {
      const isNullValue = sourceCol.nullBitmap ? isNull(sourceCol.nullBitmap, i) : false;

      if (isNullValue && shouldFill && fillValue !== undefined) {
        // Fill with specified value
        let fillVal: number | bigint = 0;

        if (sourceCol.dtype === DType.String && typeof fillValue === 'string' && df.dictionary) {
          // For string columns, get dictionary ID
          const dictId = df.dictionary.stringToId.get(fillValue);
          if (dictId !== undefined) {
            fillVal = dictId;
          } else {
            // Add to dictionary
            const newId = df.dictionary.idToString.size;
            df.dictionary.stringToId.set(fillValue, newId);
            df.dictionary.idToString.set(newId, fillValue);
            fillVal = newId;
          }
        } else if (typeof fillValue === 'number' || typeof fillValue === 'bigint') {
          fillVal = fillValue;
        }

        setColumnValue(destCol, i, fillVal);

        // Clear null bit if we have null bitmap
        if (destCol.nullBitmap?.data) {
          // Mark as not null by clearing the bit
          const byteIndex = Math.floor(i / 8);
          const bitIndex = i % 8;
          destCol.nullBitmap.data[byteIndex]! &= ~(1 << bitIndex);
        }
      } else {
        // Copy original value directly from buffer
        const sourceOffset = i * bytesPerElement;
        const destOffset = i * bytesPerElement;

        for (let b = 0; b < bytesPerElement; b++) {
          const byte = sourceCol.data[sourceOffset + b];
          if (byte !== undefined) {
            destCol.data[destOffset + b] = byte;
          }
        }

        // Copy null status if present
        if (sourceCol.nullBitmap && destCol.nullBitmap && isNullValue) {
          setNull(destCol.nullBitmap, i);
        }
      }
    }
  }

  return ok(resultDf);
}
