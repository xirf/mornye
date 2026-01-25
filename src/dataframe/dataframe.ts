import { type Column, createColumn, getColumnDType } from '../core/column';
import type { Schema } from '../core/schema';
import { type StringDictionary, createDictionary } from '../memory/dictionary';
import type { DType } from '../types/dtypes';
import { type Result, err, ok, unwrapErr } from '../types/result';

/**
 * DataFrame represents a collection of columns with the same row count
 */
export interface DataFrame<T = unknown> {
  /** Columns stored in insertion order */
  columns: Map<string, Column>;
  /** Column names in order */
  columnOrder: string[];
  /** String dictionary for all String columns */
  dictionary?: StringDictionary;
}

/**
 * Creates a new empty DataFrame
 * @returns DataFrame instance
 */
export function createDataFrame<T = unknown>(): DataFrame<T> {
  return {
    columns: new Map(),
    columnOrder: [],
    dictionary: createDictionary(),
  };
}

/**
 * Gets all column names in order
 * @param df - The DataFrame
 * @returns Array of column names
 */
export function getColumnNames<T>(df: DataFrame<T>): string[] {
  return df.columnOrder;
}

/**
 * Gets the number of rows in the DataFrame
 * @param df - The DataFrame
 * @returns Row count (0 if no columns)
 */
export function getRowCount<T>(df: DataFrame<T>): number {
  if (df.columnOrder.length === 0) {
    return 0;
  }
  // All columns must have same length, so return first column's length
  const firstColName = df.columnOrder[0];
  if (!firstColName) {
    return 0;
  }
  const firstCol = df.columns.get(firstColName);
  return firstCol ? firstCol.length : 0;
}

/**
 * Gets a column by name
 * @param df - The DataFrame
 * @param columnName - The column name
 * @returns Result with Column or error
 */
export function getColumn<T>(
  df: DataFrame<T>,
  columnName: unknown extends T ? string : keyof T & string,
): Result<Column, string> {
  const col = df.columns.get(columnName);
  if (!col) {
    return err(`Column '${columnName}' not found`);
  }
  return ok(col);
}

/**
 * Adds a new column to the DataFrame
 * @param df - The DataFrame
 * @param columnName - The column name
 * @param dtype - The data type
 * @param length - Number of rows
 * @returns Result indicating success or error
 */
export function addColumn<T>(
  df: DataFrame<T>,
  columnName: string,
  dtype: DType,
  length: number,
): Result<true, string> {
  // Check for duplicate name
  if (df.columns.has(columnName)) {
    return err(`Column '${columnName}' already exists`);
  }

  // If DataFrame has columns, new column must match row count
  const currentRowCount = getRowCount(df);
  if (currentRowCount > 0 && length !== currentRowCount) {
    return err(`Column length ${length} does not match DataFrame row count ${currentRowCount}`);
  }

  // Create the column
  const colResult = createColumn(dtype, length, columnName);
  if (!colResult.ok) {
    return err(unwrapErr(colResult));
  }

  // Add to DataFrame
  df.columns.set(columnName, colResult.data);
  df.columnOrder.push(columnName);

  return ok(true);
}

/**
 * Gets the schema (column names -> dtypes) of the DataFrame
 * @param df - The DataFrame
 * @returns Schema object
 */
export function getSchema<T>(df: DataFrame<T>): Schema {
  const schema: Schema = {};
  for (const colName of df.columnOrder) {
    const col = df.columns.get(colName);
    if (col) {
      schema[colName] = getColumnDType(col);
    }
  }
  return schema;
}
