/**
 * Predicate pushdown optimization for CSV scanning
 * Applies filters during CSV parsing to avoid loading filtered-out rows
 */

import { type DataFrame, addColumn, createDataFrame } from '../dataframe/dataframe';
import type { StringDictionary } from '../memory/dictionary';
import type { DType } from '../types/dtypes';
import type { FilterOperator } from '../types/operators';
import { type Result, err, ok, unwrapErr } from '../types/result';
import type { PlanNode } from './plan';

/**
 * Filter predicate to apply during CSV scanning
 */
export interface ScanPredicate {
  columnName: string;
  operator: FilterOperator;
  value: number | bigint | string | boolean;
}

/**
 * Configuration for CSV scanning with predicate pushdown
 */
export interface PredicatePushdownConfig {
  // Filter predicates to apply during scanning
  predicates: ScanPredicate[];
  // Expected schema
  schema?: Map<string, DType>;
  // Columns to load (for combined column pruning + predicate pushdown)
  requiredColumns?: Set<string>;
}

/**
 * Scans CSV with predicate pushdown
 * Only loads rows that match all predicates
 *
 * @param filepath - Path to CSV file
 * @param config - Pushdown configuration
 * @returns DataFrame with only matching rows
 */
export async function scanCsvWithPredicates(
  filepath: string,
  config: PredicatePushdownConfig,
): Promise<Result<DataFrame, Error>> {
  try {
    const file = Bun.file(filepath);
    const content = await file.text();
    return parseCsvWithPredicates(content, config);
  } catch (error) {
    return err(new Error(`Failed to read file: ${error}`));
  }
}

/**
 * Parses CSV content with predicate pushdown
 *
 * @param content - CSV content as string
 * @param config - Pushdown configuration
 * @returns DataFrame with only matching rows
 */
export function parseCsvWithPredicates(
  content: string,
  config: PredicatePushdownConfig,
): Result<DataFrame, Error> {
  const lines = content.trim().split('\n');

  if (lines.length === 0) {
    return err(new Error('Empty CSV file'));
  }

  // Parse header
  const headerLine = lines[0];
  if (!headerLine) {
    return err(new Error('CSV header is missing'));
  }
  const headers = parseCSVLine(headerLine);

  // Build column index map
  const columnIndices = new Map<string, number>();
  const requiredColumns = config.requiredColumns ?? new Set(headers);

  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    if (header && requiredColumns.has(header)) {
      columnIndices.set(header, i);
    }
  }

  // Build predicate index map (which column index for each predicate)
  const predicateIndices = new Map<ScanPredicate, number>();
  for (const pred of config.predicates) {
    const idx = headers.indexOf(pred.columnName);
    if (idx === -1) {
      return err(new Error(`Predicate column '${pred.columnName}' not found in CSV`));
    }
    predicateIndices.set(pred, idx);
    // Ensure predicate column is loaded
    if (!columnIndices.has(pred.columnName)) {
      columnIndices.set(pred.columnName, idx);
    }
  }

  // First pass: Count matching rows
  let matchCount = 0;
  const totalRows = lines.length - 1;

  for (let rowIdx = 0; rowIdx < totalRows; rowIdx++) {
    const line = lines[rowIdx + 1];
    if (!line) {
      continue;
    }
    const values = parseCSVLine(line);

    // Check all predicates
    if (evaluatePredicates(config.predicates, predicateIndices, values)) {
      matchCount++;
    }
  }

  // Create DataFrame with matching row count
  const df = createDataFrame();

  // Add columns
  const columnNames = Array.from(columnIndices.keys());
  const columns = new Map<string, { dtype: DType; index: number }>();

  for (const colName of columnNames) {
    const dtype = config.schema?.get(colName) ?? 'string';
    const addResult = addColumn(df, colName, dtype, matchCount);

    if (!addResult.ok) {
      return err(new Error(`Failed to add column '${colName}': ${unwrapErr(addResult)}`));
    }

    columns.set(colName, { dtype, index: columnIndices.get(colName)! });
  }

  // Second pass: Load only matching rows
  let writeIdx = 0;
  for (let rowIdx = 0; rowIdx < totalRows; rowIdx++) {
    const line = lines[rowIdx + 1];
    if (!line) {
      continue;
    }
    const values = parseCSVLine(line);

    // Check predicates again
    if (!evaluatePredicates(config.predicates, predicateIndices, values)) {
      continue; // Skip this row
    }

    // Store values for this row
    for (const [colName, { dtype, index }] of columns) {
      if (index >= values.length) {
        return err(new Error(`Row ${rowIdx + 1}: Missing value for column '${colName}'`));
      }

      const value = values[index];
      if (value === undefined) {
        return err(new Error(`Row ${rowIdx + 1}: Undefined value for column '${colName}'`));
      }

      const column = df.columns.get(colName);

      if (!column) {
        return err(new Error(`Column '${colName}' not found in DataFrame`));
      }

      const storeResult = storeValue(column, writeIdx, value, dtype, df.dictionary);
      if (!storeResult.ok) {
        return err(new Error(`Row ${rowIdx + 1}, column '${colName}': ${unwrapErr(storeResult)}`));
      }
    }

    writeIdx++;
  }

  return ok(df);
}

/**
 * Evaluate all predicates against a row's values
 * Returns true if all predicates match
 */
function evaluatePredicates(
  predicates: ScanPredicate[],
  indices: Map<ScanPredicate, number>,
  values: string[],
): boolean {
  for (const pred of predicates) {
    const idx = indices.get(pred)!;
    if (idx >= values.length) {
      return false; // Missing value, skip row
    }

    const value = values[idx];

    // Evaluate predicate (string comparison before type conversion)
    if (value === undefined || !evaluatePredicate(pred, value)) {
      return false;
    }
  }

  return true; // All predicates matched
}

/**
 * Evaluate a single predicate against a string value
 * Performs string comparison for efficiency (before type conversion)
 */
function evaluatePredicate(pred: ScanPredicate, value: string): boolean {
  const { operator, value: filterValue } = pred;

  // For numeric comparisons, parse both sides
  if (operator === '>' || operator === '<' || operator === '>=' || operator === '<=') {
    const numValue = Number.parseFloat(value);
    const numFilter =
      typeof filterValue === 'number' ? filterValue : Number.parseFloat(String(filterValue));

    if (Number.isNaN(numValue) || Number.isNaN(numFilter)) {
      return false;
    }

    switch (operator) {
      case '>':
        return numValue > numFilter;
      case '<':
        return numValue < numFilter;
      case '>=':
        return numValue >= numFilter;
      case '<=':
        return numValue <= numFilter;
    }
  }

  // For equality, use string comparison (faster, no parsing)
  const strFilter = String(filterValue);

  switch (operator) {
    case '==':
      return value === strFilter;
    case '!=':
      return value !== strFilter;
    default:
      return true; // 'in' and 'not-in' not supported in pushdown
  }
}

/**
 * Parse a CSV line handling quoted values
 */
function parseCSVLine(line: string): string[] {
  const values: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      values.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }

  values.push(current.trim());
  return values;
}

/**
 * Store a parsed value in the appropriate column format
 */
function storeValue(
  column: { data: Uint8Array; view: DataView; length: number; dtype: DType },
  rowIdx: number,
  value: string,
  dtype: DType,
  dictionary: StringDictionary | undefined,
): Result<void, string> {
  const view = column.view;

  switch (dtype) {
    case 'float64': {
      const num = Number.parseFloat(value);
      if (Number.isNaN(num)) {
        return err(`Invalid float64 value: '${value}'`);
      }
      view.setFloat64(rowIdx * 8, num, true);
      return ok(undefined);
    }

    case 'int32': {
      const num = Number.parseInt(value, 10);
      if (Number.isNaN(num)) {
        return err(`Invalid int32 value: '${value}'`);
      }
      view.setInt32(rowIdx * 4, num, true);
      return ok(undefined);
    }

    case 'string': {
      if (!dictionary) {
        return err('Dictionary not initialized for string column');
      }

      let id = dictionary.stringToId.get(value);
      if (id === undefined) {
        id = dictionary.nextId++;
        dictionary.stringToId.set(value, id);
        dictionary.idToString.set(id, value);
      }

      view.setInt32(rowIdx * 4, id, true);
      return ok(undefined);
    }

    default:
      return err(`Unsupported dtype: ${dtype}`);
  }
}

/**
 * Analyzes query plan to extract filter predicates that can be pushed down
 */
export function extractPushdownPredicates(plan: PlanNode): ScanPredicate[] {
  const predicates: ScanPredicate[] = [];

  // Recursively traverse plan to find filters
  function traverse(node: PlanNode): void {
    if (!node) return;

    if (node.type === 'filter') {
      // Check if this filter can be pushed down
      // Only push down scalar values, not arrays
      if (canPushDown(node.operator) && !Array.isArray(node.value)) {
        predicates.push({
          columnName: node.column,
          operator: node.operator,
          value: node.value,
        });
      }

      // Continue traversing
      if ('input' in node && node.input) {
        traverse(node.input);
      }
    } else if ('input' in node && node.input) {
      traverse(node.input);
    }
  }

  traverse(plan);
  return predicates;
}

/**
 * Check if a filter operator can be pushed down to CSV scanning
 */
function canPushDown(operator: FilterOperator): boolean {
  // Support simple comparison operators
  // 'in' and 'not-in' are harder to push down efficiently
  return (
    operator === '==' ||
    operator === '!=' ||
    operator === '>' ||
    operator === '<' ||
    operator === '>=' ||
    operator === '<='
  );
}

/**
 * Estimates memory savings from predicate pushdown
 */
export function estimatePushdownSavings(
  totalRows: number,
  matchingRows: number,
  columns: number,
  avgBytesPerCell = 8,
): { totalBytes: number; loadedBytes: number; savingsPercent: number } {
  const totalBytes = totalRows * columns * avgBytesPerCell;
  const loadedBytes = matchingRows * columns * avgBytesPerCell;
  const savingsPercent = ((totalBytes - loadedBytes) / totalBytes) * 100;

  return {
    totalBytes,
    loadedBytes,
    savingsPercent,
  };
}
