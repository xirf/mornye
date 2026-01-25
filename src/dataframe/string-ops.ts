/**
 * String operations for DataFrame columns
 * All operations work with dictionary-encoded strings for memory efficiency
 */

import { enableNullTracking, getColumnValue, setColumnValue } from '../core/column';
import { getString, internString } from '../memory/dictionary';
import { DType } from '../types/dtypes';
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
 * Convert strings in a column to lowercase
 * Returns a new DataFrame with transformed strings
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column to transform
 * @returns Result with new DataFrame or error
 */
export function strLower(df: DataFrame, columnName: string): Result<DataFrame, Error> {
  const colResult = getColumn(df, columnName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const sourceCol = colResult.data;
  if (sourceCol.dtype !== DType.String) {
    return err(new Error(`Column '${columnName}' must be String type for str operations`));
  }

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary; // Share dictionary

  // Copy all columns
  const rowCount = getRowCount(df);
  for (const colName of getColumnNames(df)) {
    const col = getColumn(df, colName);
    if (!col.ok) continue;

    const addResult = addColumn(resultDf, colName, col.data.dtype, rowCount);
    if (!addResult.ok) continue;

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) continue;
    const destCol = destColResult.data;

    if (col.data.nullBitmap) {
      enableNullTracking(destCol);
    }

    if (colName === columnName) {
      // Transform this column
      for (let i = 0; i < rowCount; i++) {
        if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
          if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
          continue;
        }

        const dictId = getColumnValue(sourceCol, i);
        if (dictId === undefined) continue;

        const str = getString(df.dictionary!, Number(dictId))!;
        const transformed = str.toLowerCase();
        const newId = internString(resultDf.dictionary!, transformed);

        setColumnValue(destCol, i, newId);
      }
    } else {
      // Copy other columns as-is
      const bytesPerElement = col.data.data.byteLength / col.data.length;
      const totalBytes = rowCount * bytesPerElement;

      for (let b = 0; b < totalBytes; b++) {
        destCol.data[b] = col.data.data[b]!;
      }

      if (col.data.nullBitmap && destCol.nullBitmap) {
        const bitmapBytes = col.data.nullBitmap.data.byteLength;
        for (let b = 0; b < bitmapBytes; b++) {
          destCol.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  return ok(resultDf);
}

/**
 * Convert strings in a column to uppercase
 * Returns a new DataFrame with transformed strings
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column to transform
 * @returns Result with new DataFrame or error
 */
export function strUpper(df: DataFrame, columnName: string): Result<DataFrame, Error> {
  const colResult = getColumn(df, columnName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const sourceCol = colResult.data;
  if (sourceCol.dtype !== DType.String) {
    return err(new Error(`Column '${columnName}' must be String type for str operations`));
  }

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  const rowCount = getRowCount(df);
  for (const colName of getColumnNames(df)) {
    const col = getColumn(df, colName);
    if (!col.ok) continue;

    const addResult = addColumn(resultDf, colName, col.data.dtype, rowCount);
    if (!addResult.ok) continue;

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) continue;
    const destCol = destColResult.data;

    if (col.data.nullBitmap) {
      enableNullTracking(destCol);
    }

    if (colName === columnName) {
      for (let i = 0; i < rowCount; i++) {
        if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
          if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
          continue;
        }

        const dictId = getColumnValue(sourceCol, i);
        if (dictId === undefined) continue;

        const str = getString(df.dictionary!, Number(dictId));
        const transformed = str!.toUpperCase();
        const newId = internString(resultDf.dictionary!, transformed);

        setColumnValue(destCol, i, newId);
      }
    } else {
      const bytesPerElement = col.data.data.byteLength / col.data.length;
      const totalBytes = rowCount * bytesPerElement;

      for (let b = 0; b < totalBytes; b++) {
        destCol.data[b] = col.data.data[b]!;
      }

      if (col.data.nullBitmap && destCol.nullBitmap) {
        const bitmapBytes = col.data.nullBitmap.data.byteLength;
        for (let b = 0; b < bitmapBytes; b++) {
          destCol.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  return ok(resultDf);
}

/**
 * Strip whitespace from strings in a column
 * Returns a new DataFrame with trimmed strings
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column to transform
 * @returns Result with new DataFrame or error
 */
export function strStrip(df: DataFrame, columnName: string): Result<DataFrame, Error> {
  const colResult = getColumn(df, columnName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const sourceCol = colResult.data;
  if (sourceCol.dtype !== DType.String) {
    return err(new Error(`Column '${columnName}' must be String type for str operations`));
  }

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  const rowCount = getRowCount(df);
  for (const colName of getColumnNames(df)) {
    const col = getColumn(df, colName);
    if (!col.ok) continue;

    const addResult = addColumn(resultDf, colName, col.data.dtype, rowCount);
    if (!addResult.ok) continue;

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) continue;
    const destCol = destColResult.data;

    if (col.data.nullBitmap) {
      enableNullTracking(destCol);
    }

    if (colName === columnName) {
      for (let i = 0; i < rowCount; i++) {
        if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
          if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
          continue;
        }

        const dictId = getColumnValue(sourceCol, i);
        if (dictId === undefined) continue;

        const str = getString(df.dictionary!, Number(dictId));
        const transformed = str!.trim();
        const newId = internString(resultDf.dictionary!, transformed);

        setColumnValue(destCol, i, newId);
      }
    } else {
      const bytesPerElement = col.data.data.byteLength / col.data.length;
      const totalBytes = rowCount * bytesPerElement;

      for (let b = 0; b < totalBytes; b++) {
        destCol.data[b] = col.data.data[b]!;
      }

      if (col.data.nullBitmap && destCol.nullBitmap) {
        const bitmapBytes = col.data.nullBitmap.data.byteLength;
        for (let b = 0; b < bitmapBytes; b++) {
          destCol.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  return ok(resultDf);
}

/**
 * Check if strings contain a substring
 * Returns a new DataFrame with boolean column indicating matches
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column to check
 * @param substring - Substring to search for
 * @param caseSensitive - Whether search is case sensitive (default: true)
 * @returns Result with new DataFrame or error
 */
export function strContains(
  df: DataFrame,
  columnName: string,
  substring: string,
  caseSensitive = true,
): Result<DataFrame, Error> {
  const colResult = getColumn(df, columnName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const sourceCol = colResult.data;
  if (sourceCol.dtype !== DType.String) {
    return err(new Error(`Column '${columnName}' must be String type for str operations`));
  }

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  const rowCount = getRowCount(df);
  const searchStr = caseSensitive ? substring : substring.toLowerCase();

  // Add boolean result column
  const resultColName = `${columnName}_contains`;
  const addResult = addColumn(resultDf, resultColName, DType.Bool, rowCount);
  if (!addResult.ok) {
    return err(new Error(addResult.error));
  }

  const destColResult = getColumn(resultDf, resultColName);
  if (!destColResult.ok) {
    return err(new Error(destColResult.error));
  }
  const destCol = destColResult.data;

  if (sourceCol.nullBitmap) {
    enableNullTracking(destCol);
  }

  // Check each string
  for (let i = 0; i < rowCount; i++) {
    if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
      if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
      continue;
    }

    const dictId = getColumnValue(sourceCol, i);
    if (dictId === undefined) continue;

    const str = getString(df.dictionary!, Number(dictId))!;
    const checkStr = caseSensitive ? str : str.toLowerCase();
    const contains = checkStr.includes(searchStr);

    setColumnValue(destCol, i, contains ? 1 : 0);
  }

  return ok(resultDf);
}

/**
 * Check if strings start with a prefix
 * Returns a new DataFrame with boolean column indicating matches
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column to check
 * @param prefix - Prefix to search for
 * @param caseSensitive - Whether search is case sensitive (default: true)
 * @returns Result with new DataFrame or error
 */
export function strStartsWith(
  df: DataFrame,
  columnName: string,
  prefix: string,
  caseSensitive = true,
): Result<DataFrame, Error> {
  const colResult = getColumn(df, columnName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const sourceCol = colResult.data;
  if (sourceCol.dtype !== DType.String) {
    return err(new Error(`Column '${columnName}' must be String type for str operations`));
  }

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  const rowCount = getRowCount(df);
  const searchStr = caseSensitive ? prefix : prefix.toLowerCase();

  const resultColName = `${columnName}_startswith`;
  const addResult = addColumn(resultDf, resultColName, DType.Bool, rowCount);
  if (!addResult.ok) {
    return err(new Error(addResult.error));
  }

  const destColResult = getColumn(resultDf, resultColName);
  if (!destColResult.ok) {
    return err(new Error(destColResult.error));
  }
  const destCol = destColResult.data;

  if (sourceCol.nullBitmap) {
    enableNullTracking(destCol);
  }

  for (let i = 0; i < rowCount; i++) {
    if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
      if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
      continue;
    }

    const dictId = getColumnValue(sourceCol, i);
    if (dictId === undefined) continue;

    const str = getString(df.dictionary!, Number(dictId))!;
    const checkStr = caseSensitive ? str : str.toLowerCase();
    const matches = checkStr.startsWith(searchStr);

    setColumnValue(destCol, i, matches ? 1 : 0);
  }

  return ok(resultDf);
}

/**
 * Check if strings end with a suffix
 * Returns a new DataFrame with boolean column indicating matches
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column to check
 * @param suffix - Suffix to search for
 * @param caseSensitive - Whether search is case sensitive (default: true)
 * @returns Result with new DataFrame or error
 */
export function strEndsWith(
  df: DataFrame,
  columnName: string,
  suffix: string,
  caseSensitive = true,
): Result<DataFrame, Error> {
  const colResult = getColumn(df, columnName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const sourceCol = colResult.data;
  if (sourceCol.dtype !== DType.String) {
    return err(new Error(`Column '${columnName}' must be String type for str operations`));
  }

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  const rowCount = getRowCount(df);
  const searchStr = caseSensitive ? suffix : suffix.toLowerCase();

  const resultColName = `${columnName}_endswith`;
  const addResult = addColumn(resultDf, resultColName, DType.Bool, rowCount);
  if (!addResult.ok) {
    return err(new Error(addResult.error));
  }

  const destColResult = getColumn(resultDf, resultColName);
  if (!destColResult.ok) {
    return err(new Error(destColResult.error));
  }
  const destCol = destColResult.data;

  if (sourceCol.nullBitmap) {
    enableNullTracking(destCol);
  }

  for (let i = 0; i < rowCount; i++) {
    if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
      if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
      continue;
    }

    const dictId = getColumnValue(sourceCol, i);
    if (dictId === undefined) continue;

    const str = getString(df.dictionary!, Number(dictId))!;
    const checkStr = caseSensitive ? str : str.toLowerCase();
    const matches = checkStr.endsWith(searchStr);

    setColumnValue(destCol, i, matches ? 1 : 0);
  }

  return ok(resultDf);
}

/**
 * Replace substring in strings
 * Returns a new DataFrame with transformed strings
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column to transform
 * @param pattern - Substring to find
 * @param replacement - Replacement string
 * @returns Result with new DataFrame or error
 */
export function strReplace(
  df: DataFrame,
  columnName: string,
  pattern: string,
  replacement: string,
): Result<DataFrame, Error> {
  const colResult = getColumn(df, columnName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const sourceCol = colResult.data;
  if (sourceCol.dtype !== DType.String) {
    return err(new Error(`Column '${columnName}' must be String type for str operations`));
  }

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  const rowCount = getRowCount(df);
  for (const colName of getColumnNames(df)) {
    const col = getColumn(df, colName);
    if (!col.ok) continue;

    const addResult = addColumn(resultDf, colName, col.data.dtype, rowCount);
    if (!addResult.ok) continue;

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) continue;
    const destCol = destColResult.data;

    if (col.data.nullBitmap) {
      enableNullTracking(destCol);
    }

    if (colName === columnName) {
      for (let i = 0; i < rowCount; i++) {
        if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
          if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
          continue;
        }

        const dictId = getColumnValue(sourceCol, i);
        if (dictId === undefined) continue;

        const str = getString(df.dictionary!, Number(dictId))!;
        const transformed = str.replace(new RegExp(pattern, 'g'), replacement);
        const newId = internString(resultDf.dictionary!, transformed);

        setColumnValue(destCol, i, newId);
      }
    } else {
      const bytesPerElement = col.data.data.byteLength / col.data.length;
      const totalBytes = rowCount * bytesPerElement;

      for (let b = 0; b < totalBytes; b++) {
        destCol.data[b] = col.data.data[b]!;
      }

      if (col.data.nullBitmap && destCol.nullBitmap) {
        const bitmapBytes = col.data.nullBitmap.data.byteLength;
        for (let b = 0; b < bitmapBytes; b++) {
          destCol.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  return ok(resultDf);
}

/**
 * Get length of strings
 * Returns a new DataFrame with Int32 column containing string lengths
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column
 * @returns Result with new DataFrame or error
 */
export function strLen(df: DataFrame, columnName: string): Result<DataFrame, Error> {
  const colResult = getColumn(df, columnName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const sourceCol = colResult.data;
  if (sourceCol.dtype !== DType.String) {
    return err(new Error(`Column '${columnName}' must be String type for str operations`));
  }

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  const rowCount = getRowCount(df);
  const resultColName = `${columnName}_len`;
  const addResult = addColumn(resultDf, resultColName, DType.Int32, rowCount);
  if (!addResult.ok) {
    return err(new Error(addResult.error));
  }

  const destColResult = getColumn(resultDf, resultColName);
  if (!destColResult.ok) {
    return err(new Error(destColResult.error));
  }
  const destCol = destColResult.data;

  if (sourceCol.nullBitmap) {
    enableNullTracking(destCol);
  }

  for (let i = 0; i < rowCount; i++) {
    if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
      if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
      continue;
    }

    const dictId = getColumnValue(sourceCol, i);
    if (dictId === undefined) continue;

    const str = getString(df.dictionary!, Number(dictId))!;
    setColumnValue(destCol, i, str.length);
  }

  return ok(resultDf);
}
