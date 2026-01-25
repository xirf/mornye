import { getColumnValue } from '../core/column';
import type { StringDictionary } from '../memory/dictionary';
import { getString } from '../memory/dictionary';
import { DType } from '../types/dtypes';
import type { DataFrame } from './dataframe';
import { getColumnNames, getRowCount } from './dataframe';

/**
 * Options for DataFrame formatting
 */
export interface PrintOptions {
  /** Maximum number of rows to display (default: 10) */
  maxRows?: number;
}

/**
 * Format a single value for display based on its data type
 */
export function formatValue(
  dtype: DType,
  value: number | bigint | null | undefined,
  dict?: StringDictionary,
): string {
  // Handle null/undefined
  if (value === null || value === undefined) {
    return 'null';
  }

  switch (dtype) {
    case DType.Float64:
      return String(value);

    case DType.Int32:
      return String(value);

    case DType.Bool:
      return value === 1 ? 'true' : 'false';

    case DType.String: {
      if (!dict) {
        return `<id:${value}>`;
      }
      const str = getString(dict, value as number);
      return str ?? '<unknown>';
    }

    case DType.DateTime:
    case DType.Date: {
      const timestamp = typeof value === 'bigint' ? value : BigInt(value);
      const date = new Date(Number(timestamp));
      return dtype === DType.DateTime
        ? date.toISOString()
        : (date.toISOString().split('T')[0] ?? '');
    }

    default:
      return String(value);
  }
}

/**
 * Format DataFrame as a readable string table
 */
export function formatDataFrame(df: DataFrame, options?: PrintOptions): string {
  const maxRows = options?.maxRows ?? 10;
  const rowCount = getRowCount(df);
  const columnNames = getColumnNames(df);
  const columnCount = columnNames.length;

  // Handle empty DataFrame
  if (columnCount === 0) {
    return 'DataFrame (0 rows x 0 columns)';
  }

  // Build header
  const lines: string[] = [];
  lines.push(`DataFrame (${rowCount} rows x ${columnCount} columns)`);
  lines.push('');

  // Calculate column widths
  const widths = columnNames.map((name) => {
    const column = df.columns.get(name);
    if (!column) return name.length;

    // Check first few values to estimate width
    const sampleSize = Math.min(10, rowCount);
    let maxWidth = name.length;

    for (let i = 0; i < sampleSize; i++) {
      const value = getColumnValue(column, i);
      const formatted = formatValue(column.dtype, value, df.dictionary);
      maxWidth = Math.max(maxWidth, formatted.length);
    }

    return Math.min(maxWidth, 20); // Cap at 20 chars
  }) as number[];

  // Format header row
  const headerRow = columnNames.map((name, i) => name.padEnd(widths[i]!)).join(' │ ');
  lines.push(headerRow);

  // Separator
  const separator = widths.map((w) => '─'.repeat(w)).join('─┼─');
  lines.push(separator);

  // Format data rows
  const displayRows = Math.min(maxRows, rowCount);

  for (let row = 0; row < displayRows; row++) {
    const values = columnNames.map((name, colIdx) => {
      const column = df.columns.get(name);
      if (!column) return 'null'.padEnd(widths[colIdx]!);

      const value = getColumnValue(column, row);
      const formatted = formatValue(column.dtype, value, df.dictionary);

      // Truncate if too long
      const truncated =
        formatted.length > widths[colIdx]!
          ? `${formatted.substring(0, widths[colIdx]! - 1)}…`
          : formatted;

      return truncated.padEnd(widths[colIdx]!);
    });

    lines.push(values.join(' │ '));
  }

  // Add truncation indicator if needed
  if (rowCount > displayRows) {
    lines.push('');
    lines.push(`... (${rowCount - displayRows} more rows)`);
  }

  return lines.join('\n');
}
