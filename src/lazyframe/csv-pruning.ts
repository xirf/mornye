/**
 * Optimized CSV scanner with column pruning support
 * Only parses columns that are actually needed, skipping the rest
 */

import { type DataFrame, addColumn, createDataFrame } from '../dataframe/dataframe';
import type { StringDictionary } from '../memory/dictionary';
import type { DType } from '../types/dtypes';
import { type Result, err, ok, unwrapErr } from '../types/result';

/**
 * Configuration for which columns to parse from CSV
 */
export interface ColumnPruningConfig {
  // Columns to actually parse (by name)
  requiredColumns: Set<string>;
  // Map of column name -> expected dtype
  schema?: Map<string, DType>;
}

/**
 * Scans a CSV file with column pruning
 * Only parses columns specified in config, skips the rest
 *
 * @param filepath - Path to CSV file
 * @param config - Pruning configuration
 * @returns DataFrame with only required columns
 */
export async function scanCsvWithPruning(
  filepath: string,
  config: ColumnPruningConfig,
): Promise<Result<DataFrame, Error>> {
  try {
    const file = Bun.file(filepath);
    const content = await file.text();
    return parseCsvWithPruning(content, config);
  } catch (error) {
    return err(new Error(`Failed to read file: ${error}`));
  }
}

/**
 * Parses CSV content with column pruning
 *
 * @param content - CSV content as string
 * @param config - Pruning configuration
 * @returns DataFrame with only required columns
 */
export function parseCsvWithPruning(
  content: string,
  config: ColumnPruningConfig,
): Result<DataFrame, Error> {
  const lines = content.trim().split('\n');

  if (lines.length === 0) {
    return err(new Error('Empty CSV file'));
  }

  // Parse header
  const headerLine = lines[0];
  if (!headerLine) {
    return err(new Error('Missing header line'));
  }
  const headers = parseCSVLine(headerLine);

  // Build column index map (only for required columns)
  const columnIndices = new Map<string, number>();
  const columnNames: string[] = [];

  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    if (header && config.requiredColumns.has(header)) {
      columnIndices.set(header, i);
      columnNames.push(header);
    }
  }

  // Validate all required columns exist
  for (const required of config.requiredColumns) {
    if (!columnIndices.has(required)) {
      return err(new Error(`Required column '${required}' not found in CSV`));
    }
  }

  const rowCount = lines.length - 1; // Exclude header

  // Create DataFrame
  const df = createDataFrame();

  // Add columns (only required ones)
  const columns = new Map<string, { dtype: DType; index: number }>();

  for (const colName of columnNames) {
    const dtype = config.schema?.get(colName) ?? 'string'; // Default to string
    const addResult = addColumn(df, colName, dtype, rowCount);

    if (!addResult.ok) {
      return err(new Error(`Failed to add column '${colName}'`));
    }

    columns.set(colName, { dtype, index: columnIndices.get(colName)! });
  }

  // Parse data rows (skip header)
  for (let rowIdx = 0; rowIdx < rowCount; rowIdx++) {
    const line = lines[rowIdx + 1];
    if (!line) continue; // Skip empty lines
    const values = parseCSVLine(line);

    // Only parse required columns
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

      // Parse and store value
      const storeResult = storeValue(column, rowIdx, value, dtype, df.dictionary);
      if (!storeResult.ok) {
        return err(new Error(`Row ${rowIdx + 1}, column '${colName}': ${unwrapErr(storeResult)}`));
      }
    }
  }

  return ok(df);
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
 * Estimates memory savings from column pruning
 */
export function estimatePruningSavings(
  totalColumns: number,
  requiredColumns: number,
  rowCount: number,
  avgBytesPerColumn = 8, // Default: float64
): { totalBytes: number; requiredBytes: number; savingsPercent: number } {
  const totalBytes = totalColumns * rowCount * avgBytesPerColumn;
  const requiredBytes = requiredColumns * rowCount * avgBytesPerColumn;
  const savingsPercent = ((totalBytes - requiredBytes) / totalBytes) * 100;

  return {
    totalBytes,
    requiredBytes,
    savingsPercent,
  };
}
