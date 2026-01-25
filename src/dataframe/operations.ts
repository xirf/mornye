import { createColumn, getColumnValue } from '../core/column';
import type { Select } from '../lazyframe/types';
import { DType } from '../types/dtypes';
import type { FilterOperator } from '../types/operators';
import { type Result, err, ok } from '../types/result';
import {
  type DataFrame,
  addColumn,
  createDataFrame,
  getColumn,
  getColumnNames,
  getRowCount,
} from './dataframe';
import { getVectorizedFilter, shouldUseVectorized } from './operations-simd';

/**
 * Filters a DataFrame based on a column predicate
 * Returns a new DataFrame with only rows matching the condition
 *
 * @param df - Source DataFrame
 * @param columnName - Column to filter on
 * @param operator - Comparison operator
 * @param value - Value to compare against (or array for 'in'/'not-in')
 * @returns Filtered DataFrame
 * @throws Error if column not found or operation fails
 */
export function filter<T, K extends keyof T>(
  df: DataFrame<T>,
  columnName: unknown extends T ? string : K,
  operator: FilterOperator,
  value: number | bigint | string | boolean | (number | bigint | string | boolean)[],
): DataFrame<T> {
  // Validate column exists
  const colResult = getColumn(df, String(columnName) as keyof T & string);
  if (!colResult.ok) {
    throw new Error(colResult.error);
  }
  const sourceColumn = colResult.data;
  const sourceRows = getRowCount(df);

  // For string columns, optimize by converting filter value to dictionary ID
  const isStringColumn = sourceColumn.dtype === DType.String;
  let compareValue = value;
  let compareArray: (number | bigint | string | boolean)[] | undefined;

  if (isStringColumn && df.dictionary) {
    // Convert string values to dictionary IDs for fast comparison
    if (Array.isArray(value)) {
      compareArray = value.map((v) =>
        typeof v === 'string' ? (df.dictionary!.stringToId.get(v) ?? -1) : v,
      );
    } else if (typeof value === 'string') {
      compareValue = df.dictionary.stringToId.get(value) ?? -1;
    }
  }

  // For 'in' and 'not-in', value must be an array
  if (operator === 'in' || operator === 'not-in') {
    if (!Array.isArray(value)) {
      throw new Error(`Operator '${operator}' requires an array value`);
    }
  } else {
    if (Array.isArray(value)) {
      throw new Error(`Operator '${operator}' requires a single value, not an array`);
    }
  }

  // Try SIMD-optimized path for large numeric datasets
  let matchingIndices: number[] = [];
  if (shouldUseVectorized(sourceColumn, sourceRows) && typeof compareValue === 'number') {
    const vectorizedFilter = getVectorizedFilter(sourceColumn, operator);
    if (vectorizedFilter) {
      matchingIndices = vectorizedFilter(sourceColumn, operator, compareValue);
    }
  }

  // Fall back to scalar path if SIMD not applicable or matchingIndices empty
  if (matchingIndices.length === 0 && sourceRows > 0) {
    // Build list of matching row indices (zero object creation in loop)
    matchingIndices = [];

    // HOT LOOP: Check each row - minimize object creation
    for (let rowIdx = 0; rowIdx < sourceRows; rowIdx++) {
      const rowValue = getColumnValue(sourceColumn, rowIdx);

      let matches = false;

      // Handle nulls
      if (rowValue === null || rowValue === undefined) {
        matches = false;
      } else {
        switch (operator) {
          case '==':
            matches = rowValue === compareValue;
            break;
          case '!=':
            matches = rowValue !== compareValue;
            break;
          case '>':
            if (typeof rowValue === 'number' && typeof compareValue === 'number') {
              matches = rowValue > compareValue;
            } else if (typeof rowValue === 'bigint' && typeof compareValue === 'bigint') {
              matches = rowValue > compareValue;
            }
            break;
          case '<':
            if (typeof rowValue === 'number' && typeof compareValue === 'number') {
              matches = rowValue < compareValue;
            } else if (typeof rowValue === 'bigint' && typeof compareValue === 'bigint') {
              matches = rowValue < compareValue;
            }
            break;
          case '>=':
            if (typeof rowValue === 'number' && typeof compareValue === 'number') {
              matches = rowValue >= compareValue;
            } else if (typeof rowValue === 'bigint' && typeof compareValue === 'bigint') {
              matches = rowValue >= compareValue;
            }
            break;
          case '<=':
            if (typeof rowValue === 'number' && typeof compareValue === 'number') {
              matches = rowValue <= compareValue;
            } else if (typeof rowValue === 'bigint' && typeof compareValue === 'bigint') {
              matches = rowValue <= compareValue;
            }
            break;
          case 'in':
            matches = (compareArray ?? (value as Array<number | bigint | string>)).includes(
              rowValue,
            );
            break;
          case 'not-in':
            matches = !(compareArray ?? (value as Array<number | bigint | string>)).includes(
              rowValue,
            );
            break;
        }
      }

      if (matches) {
        matchingIndices.push(rowIdx);
      }
    }
  }

  // Create new DataFrame with filtered rows
  const resultDf = createDataFrame<T>();
  resultDf.dictionary = df.dictionary; // Share dictionary

  const resultRowCount = matchingIndices.length;

  // If no matches, return empty DataFrame with column structure
  if (resultRowCount === 0) {
    // Add empty columns to preserve schema
    const allColumns = getColumnNames(df);
    for (const colName of allColumns) {
      const sourceColResult = getColumn(df, colName as keyof T & string);
      if (!sourceColResult.ok) {
        throw new Error(sourceColResult.error);
      }
      const sourceCol = sourceColResult.data;
      const addResult = addColumn(resultDf, colName, sourceCol.dtype, 0);
      if (!addResult.ok) {
        throw new Error(addResult.error);
      }
    }
    return resultDf;
  }

  // Copy all columns with filtered rows
  const allColumns = getColumnNames(df);
  for (const colName of allColumns) {
    const sourceColResult = getColumn(df, colName as keyof T & string);
    if (!sourceColResult.ok) {
      throw new Error(sourceColResult.error);
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, colName, sourceCol.dtype, resultRowCount);

    if (!addResult.ok) {
      throw new Error(addResult.error);
    }

    const destColResult = getColumn(resultDf, colName as keyof T & string);
    if (!destColResult.ok) {
      throw new Error(destColResult.error);
    }

    const destCol = destColResult.data;

    // HOT LOOP: Copy matching rows - zero object creation
    for (let i = 0; i < resultRowCount; i++) {
      const sourceRowIdx = matchingIndices[i];
      if (sourceRowIdx === undefined) continue;
      const sourceValue = getColumnValue(sourceCol, sourceRowIdx);
      // Nulls are handled by setColumnValue logic if strict types allow,
      // but low level access via buffers might need strict undefined check.
      // Assuming getColumnValue returns value or undefined/null.

      // We can use helper setColumnValue instead of raw view access to be safe generic-wise
      // But raw access is faster.
      // Re-using the raw access logic from readCsv or similar is verbose here.
      // But preserving original implementation's optimization:

      const bytesPerElement = sourceCol.view.byteLength / sourceCol.length;
      const destOffset = i * bytesPerElement;
      // sourceRowIdx * bytesPerElement works if dense? Yes columns are dense arrays.

      if (sourceValue !== undefined && sourceValue !== null) {
        switch (sourceCol.dtype) {
          case DType.Float64:
            destCol.view.setFloat64(destOffset, sourceValue as number, true);
            break;
          case DType.Int32:
          case DType.String:
            destCol.view.setInt32(destOffset, sourceValue as number, true);
            break;
          case DType.Bool:
            destCol.view.setUint8(destOffset, sourceValue as number);
            break;
          case DType.DateTime:
          case DType.Date:
            destCol.view.setBigInt64(destOffset, sourceValue as bigint, true);
            break;
        }
      }
    }
  }

  return resultDf;
}

/**
 * Selects specific columns from a DataFrame
 * Returns a new DataFrame with only the specified columns
 *
 * @param df - Source DataFrame
 * @param columnNames - Array of column names to select
 * @returns Projected DataFrame
 * @throws Error if any column not found
 */
export function select<T, K extends keyof T>(
  df: DataFrame<T>,
  columnNames: (unknown extends T ? string : K)[],
): DataFrame<Select<T, K>> {
  // Validate all columns exist
  for (const colName of columnNames) {
    const colResult = getColumn(df, String(colName) as keyof T & string);
    if (!colResult.ok) {
      throw new Error(`Column '${String(colName)}' not found`);
    }
  }

  const resultDf = createDataFrame<Select<T, K>>();
  resultDf.dictionary = df.dictionary; // Share dictionary

  const rowCount = getRowCount(df);

  // Copy selected columns (zero copy sharing if possible, but strict independence requested usually?
  // Existing implementation copied data.
  // We will copy data to be safe and consistent.

  for (const colName of columnNames) {
    const sourceColResult = getColumn(df, String(colName) as keyof T & string);
    if (!sourceColResult.ok) {
      throw new Error(sourceColResult.error);
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, String(colName), sourceCol.dtype, rowCount);

    if (!addResult.ok) {
      throw new Error(addResult.error);
    }

    if (rowCount > 0) {
      const destColResult = getColumn(resultDf, String(colName) as keyof Select<T, K> & string);
      if (!destColResult.ok) {
        throw new Error(destColResult.error);
      }
      const destCol = destColResult.data;

      // Buffer copy for speed
      destCol.data.set(sourceCol.data);
      // And null bitmap if exists
      if (sourceCol.nullBitmap && destCol.nullBitmap) {
        destCol.nullBitmap.data.set(sourceCol.nullBitmap.data);
      }
    }
  }

  return resultDf;
}
