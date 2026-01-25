import type { DType } from '../types/dtypes';
import { type Result, err, ok } from '../types/result';

/**
 * Schema definition: maps column names to data types
 */
export type Schema = Record<string, DType>;

/**
 * Validates a schema
 * @param schema - The schema to validate
 * @returns Result indicating success or error
 */
export function validateSchema(schema: Schema): Result<true, string> {
  const columns = Object.keys(schema);

  // Check for empty schema
  if (columns.length === 0) {
    return err('Schema cannot be empty');
  }

  // Valid dtypes
  const validDTypes = new Set(['float64', 'int32', 'string', 'bool', 'datetime', 'date']);

  // Check for empty column names
  for (const col of columns) {
    if (col.trim().length === 0) {
      return err('Schema contains empty column name');
    }
  }

  // Check for invalid dtypes
  for (const [col, dtype] of Object.entries(schema)) {
    if (!validDTypes.has(dtype)) {
      return err(`Column '${col}' has invalid dtype: ${dtype}`);
    }
  }

  // Check for duplicate column names (case insensitive)
  const lowerCaseNames = new Set<string>();
  for (const col of columns) {
    const lower = col.toLowerCase();
    if (lowerCaseNames.has(lower)) {
      return err(`Schema contains duplicate column name (case insensitive): ${col}`);
    }
    lowerCaseNames.add(lower);
  }

  return ok(true);
}

/**
 * Get the dtype for a specific column
 * @param schema - The schema
 * @param columnName - The column name
 * @returns Result with dtype or error if column not found
 */
export function getColumnDType(schema: Schema, columnName: string): Result<DType, string> {
  const dtype = schema[columnName];
  if (dtype === undefined) {
    return err(`Column '${columnName}' not found in schema`);
  }
  return ok(dtype);
}
