/**
 * Basic DataFrame manipulation operations
 * All operations work directly on Uint8Array buffers for maximum performance
 */

import { enableNullTracking } from '../core/column';
import { getDTypeSize } from '../types/dtypes';
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
 * Drop columns or rows from a DataFrame
 * Returns a new DataFrame with specified columns or rows removed
 *
 * @param df - Source DataFrame
 * @param options - Drop options
 * @returns Result with modified DataFrame or error
 */
export function drop(
  df: DataFrame,
  options: {
    /** Columns to drop (by name) */
    columns?: string[];
    /** Row indices to drop */
    index?: number[];
  },
): Result<DataFrame, Error> {
  if (!options.columns && !options.index) {
    return err(new Error('Must specify either columns or index to drop'));
  }

  // Handle column dropping
  if (options.columns && !options.index) {
    return dropColumns(df, options.columns);
  }

  // Handle row dropping
  if (options.index && !options.columns) {
    return dropRows(df, options.index);
  }

  // Handle both - drop columns first, then rows
  if (options.columns && options.index) {
    const colResult = dropColumns(df, options.columns);
    if (!colResult.ok) {
      return colResult;
    }
    return dropRows(colResult.data, options.index);
  }

  return err(new Error('Invalid drop options'));
}

/**
 * Drop columns from DataFrame
 * @param df - Source DataFrame
 * @param columnsToDrop - Array of column names to drop
 * @returns Result with new DataFrame or error
 */
function dropColumns(df: DataFrame, columnsToDrop: string[]): Result<DataFrame, Error> {
  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary; // Share dictionary

  const allColumns = getColumnNames(df);
  const dropSet = new Set(columnsToDrop);

  // Validate that at least one column will remain
  const remainingColumns = allColumns.filter((name) => !dropSet.has(name));
  if (remainingColumns.length === 0) {
    return err(new Error('Cannot drop all columns from DataFrame'));
  }

  const rowCount = getRowCount(df);

  // Copy columns that are not being dropped
  for (const colName of allColumns) {
    if (dropSet.has(colName)) {
      continue; // Skip dropped columns
    }

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

    // HOT LOOP: Copy all data directly using Uint8Array
    const totalBytes = rowCount * bytesPerElement;
    for (let b = 0; b < totalBytes; b++) {
      destCol.data[b] = sourceCol.data[b] ?? 0;
    }

    // Copy null bitmap if present
    if (sourceCol.nullBitmap && destCol.nullBitmap) {
      const bitmapBytes = sourceCol.nullBitmap.data.byteLength;
      for (let b = 0; b < bitmapBytes; b++) {
        destCol.nullBitmap.data[b] = sourceCol.nullBitmap.data[b] ?? 0;
      }
    }
  }

  return ok(resultDf);
}

/**
 * Drop rows from DataFrame
 * @param df - Source DataFrame
 * @param indicesToDrop - Array of row indices to drop
 * @returns Result with new DataFrame or error
 */
function dropRows(df: DataFrame, indicesToDrop: number[]): Result<DataFrame, Error> {
  const rowCount = getRowCount(df);
  const dropSet = new Set(indicesToDrop);

  // Validate indices
  for (const idx of indicesToDrop) {
    if (idx < 0 || idx >= rowCount) {
      return err(new Error(`Row index ${idx} out of bounds [0, ${rowCount})`));
    }
  }

  // Build list of rows to keep
  const keepIndices: number[] = [];
  for (let i = 0; i < rowCount; i++) {
    if (!dropSet.has(i)) {
      keepIndices.push(i);
    }
  }

  const resultRowCount = keepIndices.length;

  // If all rows dropped, return empty DataFrame with schema
  if (resultRowCount === 0) {
    const resultDf = createDataFrame();
    resultDf.dictionary = df.dictionary;

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

  // Create new DataFrame with kept rows
  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary; // Share dictionary

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

      // Copy bytes directly for maximum performance
      const sourceOffset = sourceRowIdx * bytesPerElement;
      const destOffset = i * bytesPerElement;

      for (let b = 0; b < bytesPerElement; b++) {
        destCol.data[destOffset + b] = sourceCol.data[sourceOffset + b] ?? 0;
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
 * Rename columns in a DataFrame
 * Returns a new DataFrame with renamed columns
 *
 * @param df - Source DataFrame
 * @param mapping - Object mapping old column names to new names
 * @returns Result with renamed DataFrame or error
 */
export function rename(df: DataFrame, mapping: Record<string, string>): Result<DataFrame, Error> {
  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary; // Share dictionary

  const allColumns = getColumnNames(df);
  const rowCount = getRowCount(df);

  // Validate that new names don't conflict
  const newNames = new Set<string>();
  for (const colName of allColumns) {
    const newName = mapping[colName] ?? colName;

    if (newNames.has(newName)) {
      return err(new Error(`Duplicate column name after rename: '${newName}'`));
    }
    newNames.add(newName);
  }

  // Copy all columns with new names
  for (const colName of allColumns) {
    const newName = mapping[colName] ?? colName;

    const sourceColResult = getColumn(df, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, newName, sourceCol.dtype, rowCount);

    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, newName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // Enable null tracking if source has it
    if (sourceCol.nullBitmap) {
      enableNullTracking(destCol);
    }

    const bytesPerElement = getDTypeSize(sourceCol.dtype);

    // HOT LOOP: Copy all data directly using Uint8Array
    const totalBytes = rowCount * bytesPerElement;
    for (let b = 0; b < totalBytes; b++) {
      destCol.data[b] = sourceCol.data[b] ?? 0;
    }

    // Copy null bitmap if present
    if (sourceCol.nullBitmap && destCol.nullBitmap) {
      const bitmapBytes = sourceCol.nullBitmap.data.byteLength;
      for (let b = 0; b < bitmapBytes; b++) {
        destCol.nullBitmap.data[b] = sourceCol.nullBitmap.data[b] ?? 0;
      }
    }
  }

  return ok(resultDf);
}
