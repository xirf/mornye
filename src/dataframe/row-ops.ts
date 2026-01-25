/**
 * Row-level operations for DataFrames
 * Includes append, duplicate, dropDuplicates, unique
 */

import { enableNullTracking, getColumnValue, resizeColumn, setColumnValue } from '../core/column';
import { type Result, err, ok } from '../types/result';
import { isNull, resizeNullBitmap, setNotNull, setNull } from '../utils/nulls';
import {
  type DataFrame,
  addColumn,
  createDataFrame,
  getColumn,
  getColumnNames,
  getRowCount,
  getSchema,
} from './dataframe';

/**
 * Append rows to a DataFrame
 * Creates a new DataFrame with additional rows
 *
 * @param df - Source DataFrame
 * @param rows - Array of row objects with column values
 * @returns Result with new DataFrame or error
 */
export function append(
  df: DataFrame,
  rows: Record<string, number | bigint | null>[],
): Result<DataFrame, Error> {
  if (rows.length === 0) {
    return ok(df); // No rows to append, return original
  }

  const schema = getSchema(df);
  const existingRowCount = getRowCount(df);
  const newRowCount = existingRowCount + rows.length;

  // Create new DataFrame with same schema
  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary; // Share dictionary

  // Copy and extend each column
  for (const colName of getColumnNames(df)) {
    const sourceColResult = getColumn(df, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }
    const sourceCol = sourceColResult.data;

    // Add column with new size
    const addResult = addColumn(resultDf, colName, sourceCol.dtype, newRowCount);
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

    // Copy existing data
    const bytesPerElement = sourceCol.data.byteLength / sourceCol.length;
    const existingBytes = sourceCol.length * bytesPerElement;

    for (let b = 0; b < existingBytes; b++) {
      destCol.data[b] = sourceCol.data[b]!;
    }

    // Copy null bitmap for existing rows
    if (sourceCol.nullBitmap && destCol.nullBitmap) {
      const bitmapBytes = sourceCol.nullBitmap.data.byteLength;
      for (let b = 0; b < bitmapBytes; b++) {
        destCol.nullBitmap.data[b] = sourceCol.nullBitmap.data[b]!;
      }
    }

    // Append new rows
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i]!;
      const rowIndex = existingRowCount + i;

      if (!(colName in row)) {
        // Column not present in row data, set as null
        if (!destCol.nullBitmap) {
          enableNullTracking(destCol);
        }
        if (destCol.nullBitmap) {
          setNull(destCol.nullBitmap, rowIndex);
        }
        continue;
      }

      const value = row[colName];
      if (value === null || value === undefined) {
        if (!destCol.nullBitmap) {
          enableNullTracking(destCol);
        }
        if (destCol.nullBitmap) {
          setNull(destCol.nullBitmap, rowIndex);
        }
      } else {
        setColumnValue(destCol, rowIndex, value);
        if (destCol.nullBitmap) {
          setNotNull(destCol.nullBitmap, rowIndex);
        }
      }
    }
  }

  return ok(resultDf);
}

/**
 * Create a deep copy of a DataFrame
 * All data is duplicated, but dictionary can be shared
 *
 * @param df - Source DataFrame
 * @param shareDictionary - Whether to share dictionary (default: true for efficiency)
 * @returns Result with duplicated DataFrame or error
 */
export function duplicate(df: DataFrame, shareDictionary = true): Result<DataFrame, Error> {
  const rowCount = getRowCount(df);
  const resultDf = createDataFrame();

  // Handle dictionary
  if (shareDictionary) {
    resultDf.dictionary = df.dictionary;
  } else {
    // Deep copy dictionary
    if (df.dictionary) {
      resultDf.dictionary = {
        stringToId: new Map(df.dictionary.stringToId),
        idToString: new Map(df.dictionary.idToString),
        nextId: df.dictionary.nextId,
      };
    }
  }

  // Copy each column
  for (const colName of getColumnNames(df)) {
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

    // Copy data buffer
    const totalBytes = sourceCol.data.byteLength;
    for (let b = 0; b < totalBytes; b++) {
      destCol.data[b] = sourceCol.data[b]!;
    }

    // Copy null bitmap
    if (sourceCol.nullBitmap) {
      enableNullTracking(destCol);
      if (destCol.nullBitmap) {
        const bitmapBytes = sourceCol.nullBitmap.data.byteLength;
        for (let b = 0; b < bitmapBytes; b++) {
          destCol.nullBitmap.data[b] = sourceCol.nullBitmap.data[b]!;
        }
      }
    }
  }

  return ok(resultDf);
}

/**
 * Hash a row for deduplication
 * Creates a string hash from specified columns
 */
function hashRow(df: DataFrame, rowIndex: number, columns: string[]): string {
  const parts: string[] = [];

  for (const colName of columns) {
    const colResult = getColumn(df, colName);
    if (!colResult.ok) continue;

    const col = colResult.data;

    // Check if null
    if (col.nullBitmap && isNull(col.nullBitmap, rowIndex)) {
      parts.push('NULL');
      continue;
    }

    const value = getColumnValue(col, rowIndex);
    parts.push(String(value));
  }

  return parts.join('|');
}

/**
 * Drop duplicate rows from DataFrame
 * Keeps first or last occurrence of duplicates
 *
 * @param df - Source DataFrame
 * @param options - Deduplication options
 * @returns Result with deduplicated DataFrame or error
 */
export function dropDuplicates(
  df: DataFrame,
  options?: {
    subset?: string[]; // Columns to consider for duplicates (default: all)
    keep?: 'first' | 'last'; // Which duplicate to keep (default: 'first')
  },
): Result<DataFrame, Error> {
  const rowCount = getRowCount(df);
  const columnNames = getColumnNames(df);

  // Determine which columns to use for deduplication
  const subsetCols = options?.subset || columnNames;
  const keep = options?.keep || 'first';

  // Validate subset columns exist
  for (const colName of subsetCols) {
    if (!columnNames.includes(colName)) {
      return err(new Error(`Column '${colName}' not found in DataFrame`));
    }
  }

  // Track seen row hashes
  const seen = new Map<string, number>(); // hash -> row index
  const keepIndices: number[] = [];

  for (let i = 0; i < rowCount; i++) {
    const hash = hashRow(df, i, subsetCols);

    if (!seen.has(hash)) {
      // First occurrence
      seen.set(hash, i);
      keepIndices.push(i);
    } else if (keep === 'last') {
      // Replace previous occurrence with current
      const prevIndex = seen.get(hash)!;
      const keepIdx = keepIndices.indexOf(prevIndex);
      if (keepIdx !== -1) {
        keepIndices[keepIdx] = i; // Replace with current row index
      }
      seen.set(hash, i);
    }
    // If keep === 'first', do nothing (already have first occurrence)
  }

  // Create result DataFrame with only kept rows
  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  for (const colName of columnNames) {
    const sourceColResult = getColumn(df, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }
    const sourceCol = sourceColResult.data;

    const addResult = addColumn(resultDf, colName, sourceCol.dtype, keepIndices.length);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }
    const destCol = destColResult.data;

    if (sourceCol.nullBitmap) {
      enableNullTracking(destCol);
    }

    // Copy only the kept rows
    for (let destIdx = 0; destIdx < keepIndices.length; destIdx++) {
      const sourceIdx = keepIndices[destIdx]!;

      if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, sourceIdx)) {
        if (destCol.nullBitmap) setNull(destCol.nullBitmap, destIdx);
        continue;
      }

      const value = getColumnValue(sourceCol, sourceIdx);
      if (value !== undefined) {
        setColumnValue(destCol, destIdx, value);
      }
    }
  }

  return ok(resultDf);
}

/**
 * Get unique rows from DataFrame
 * Equivalent to dropDuplicates with all columns
 *
 * @param df - Source DataFrame
 * @returns Result with unique rows or error
 */
export function unique(df: DataFrame): Result<DataFrame, Error> {
  return dropDuplicates(df, { subset: getColumnNames(df), keep: 'first' });
}
