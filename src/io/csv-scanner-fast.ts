import { getColumnValue, setColumnValue } from '../core/column';
import type { Schema } from '../core/schema';
import { validateSchema } from '../core/schema';
import type { DataFrame } from '../dataframe/dataframe';
import { addColumn, createDataFrame } from '../dataframe/dataframe';
import { internString } from '../memory/dictionary';
import { DType } from '../types/dtypes';
import type { Result } from '../types/result';
import { err, ok } from '../types/result';

/**
 * CSV scanning options (streaming with chunking)
 */
export interface CsvScanOptions {
  /** Schema definition (required) */
  schema: Schema;
  /** Field delimiter (default: ",") */
  delimiter?: string;
  /** Whether first row is header (default: true) */
  hasHeader?: boolean;
  /** Custom null value representations (default: ["NA", "null", "-", ""]) */
  nullValues?: string[];
  /** Chunk size for processing (default: 50000 rows) */
  chunkSize?: number;
}

// Byte constants
const COMMA = 44; // ,
const CR = 13; // \r
const LF = 10; // \n
const QUOTE = 34; // "
const MINUS = 45; // -
const PLUS = 43; // +
const DOT = 46; // .
const ZERO = 48; // 0
const NINE = 57; // 9

/**
 * Parse float directly from bytes (zero-copy)
 */
function parseFloatFromBytes(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let i = start;
  let negative = false;

  // Handle sign
  if (bytes[i] === MINUS) {
    negative = true;
    i++;
  } else if (bytes[i] === PLUS) {
    i++;
  }

  // Parse integer part
  let intPart = 0;
  while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
    intPart = intPart * 10 + (bytes[i]! - ZERO);
    i++;
  }

  let result = intPart;

  // Parse fractional part
  if (i < end && bytes[i] === DOT) {
    i++;
    let fracPart = 0;
    let fracDigits = 0;
    while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
      fracPart = fracPart * 10 + (bytes[i]! - ZERO);
      fracDigits++;
      i++;
    }
    if (fracDigits > 0) {
      result += fracPart / 10 ** fracDigits;
    }
  }

  return negative ? -result : result;
}

/**
 * Parse int directly from bytes (zero-copy)
 */
function parseIntFromBytes(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let i = start;
  let negative = false;

  if (bytes[i] === MINUS) {
    negative = true;
    i++;
  } else if (bytes[i] === PLUS) {
    i++;
  }

  let result = 0;
  while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
    result = result * 10 + (bytes[i]! - ZERO);
    i++;
  }

  return negative ? -result : result;
}

/**
 * Check if field is null value
 */
function isNullField(bytes: Uint8Array, start: number, end: number, nullValues: string[]): boolean {
  const len = end - start;
  if (len === 0) return true; // Empty field is null

  // Quick check for common null values
  if (len === 1 && bytes[start] === MINUS) return true; // "-"
  if (len === 2 && bytes[start] === 78 && bytes[start + 1] === 65) return true; // "NA"

  // For other null values, convert to string (rare case)
  const decoder = new TextDecoder();
  const value = decoder.decode(bytes.subarray(start, end));
  return nullValues.includes(value);
}

/**
 * Find line starts in buffer
 */
function findLineStarts(bytes: Uint8Array): number[] {
  const starts: number[] = [0];
  const len = bytes.length;

  for (let i = 0; i < len; i++) {
    if (bytes[i] === LF) {
      starts.push(i + 1);
    }
  }

  return starts;
}

/**
 * Parse header line
 */
function parseHeaderLine(
  bytes: Uint8Array,
  start: number,
  end: number,
  delimiter: number,
): string[] {
  const headers: string[] = [];
  let fieldStart = start;
  const decoder = new TextDecoder();

  for (let i = start; i <= end; i++) {
    if (i === end || bytes[i] === delimiter) {
      let fieldEnd = i;
      if (fieldEnd > fieldStart && bytes[fieldEnd - 1] === CR) {
        fieldEnd--;
      }
      const header = decoder.decode(bytes.subarray(fieldStart, fieldEnd)).trim();
      headers.push(header);
      fieldStart = i + 1;
    }
  }

  return headers;
}

/**
 * Scan CSV data into a DataFrame (streaming with chunking)
 * Processes CSV in chunks to handle large files efficiently
 * Works directly on Uint8Array for zero-copy parsing
 *
 * @param data - CSV string or Buffer data
 * @param options - CSV scanning options
 */
export async function scanCsv(
  data: string | Buffer,
  options: CsvScanOptions,
): Promise<Result<DataFrame, Error>> {
  // Validate schema
  const schemaResult = validateSchema(options.schema);
  if (!schemaResult.ok) {
    return err(new Error(schemaResult.error));
  }

  const delimiter = options.delimiter?.charCodeAt(0) ?? COMMA;
  const hasHeader = options.hasHeader ?? true;
  const nullValues = options.nullValues ?? ['NA', 'null', '-', ''];
  const chunkSize = options.chunkSize ?? 50000;

  // Convert to Uint8Array
  const bytes = typeof data === 'string' ? new TextEncoder().encode(data) : new Uint8Array(data);

  if (bytes.length === 0) {
    return err(new Error('Empty CSV data'));
  }

  // Find all line starts
  const lineStarts = findLineStarts(bytes);

  if (lineStarts.length < 1) {
    return err(new Error('No lines found in CSV'));
  }

  // Parse header
  let headerNames: string[];
  let dataStartLine = 0;

  if (hasHeader) {
    const firstLineEnd = lineStarts[1] ?? bytes.length;
    const firstLineStart = lineStarts[0] ?? 0;
    headerNames = parseHeaderLine(bytes, firstLineStart, firstLineEnd - 1, delimiter);
    dataStartLine = 1;
  } else {
    // Use schema keys as column names
    headerNames = Object.keys(options.schema);
  }

  // Validate schema matches header
  const schemaKeys = Object.keys(options.schema);
  if (schemaKeys.length !== headerNames.length) {
    return err(
      new Error(
        `Schema column count (${schemaKeys.length}) doesn't match CSV columns (${headerNames.length})`,
      ),
    );
  }

  // If hasHeader=true, verify schema keys match header names
  if (hasHeader) {
    for (const header of headerNames) {
      if (!schemaKeys.includes(header)) {
        return err(new Error(`Schema missing column from CSV header: ${header}`));
      }
    }
  }

  // Count total data rows
  let totalRows = lineStarts.length - 1 - dataStartLine;

  // Skip empty trailing lines
  while (totalRows > 0) {
    const lineIdx = dataStartLine + totalRows - 1;
    const start = lineStarts[lineIdx];
    const end = lineStarts[lineIdx + 1] ?? bytes.length;
    if (start === undefined || end - start > 1) break; // Non-empty line
    totalRows--;
  }

  // Create empty DataFrame
  const df = createDataFrame();

  // If no data rows, return empty DataFrame
  if (totalRows === 0) {
    return ok(df);
  }

  // Create columns with total capacity
  for (const colName of headerNames) {
    const dtype = options.schema[colName];
    if (!dtype) return err(new Error(`Missing dtype for column: ${colName}`));
    const addResult = addColumn(df, colName, dtype, totalRows);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }
  }

  const decoder = new TextDecoder();
  let currentRow = 0;

  // Process data in chunks
  for (
    let chunkStart = dataStartLine;
    chunkStart < dataStartLine + totalRows;
    chunkStart += chunkSize
  ) {
    const chunkEnd = Math.min(chunkStart + chunkSize, dataStartLine + totalRows);

    // Process each line in the chunk
    for (let lineIdx = chunkStart; lineIdx < chunkEnd; lineIdx++) {
      const lineStart = lineStarts[lineIdx];
      const lineEnd = lineStarts[lineIdx + 1] ?? bytes.length;
      if (lineStart === undefined) continue;

      // Parse fields from this line
      let fieldStart = lineStart;
      let colIdx = 0;

      for (let i = lineStart; i <= lineEnd && colIdx < headerNames.length; i++) {
        const isEndOfLine = i === lineEnd || bytes[i] === LF;
        const isDelimiter = bytes[i] === delimiter;

        if (isDelimiter || isEndOfLine) {
          let fieldEnd: number = i;

          // Trim CR if present
          if (fieldEnd > fieldStart && bytes[fieldEnd - 1] === CR) {
            fieldEnd--;
          }

          const colName = headerNames[colIdx];
          const dtype = colName ? options.schema[colName] : undefined;
          const column = colName && dtype ? df.columns.get(colName) : undefined;

          if (colName && dtype && !column) {
            return err(new Error(`Column not found: ${colName}`));
          }

          // Check for null
          const isNull = isNullField(bytes, fieldStart ?? 0, fieldEnd ?? 0, nullValues);

          // Parse value based on dtype
          let value: number | bigint = 0;

          if (column && dtype && !isNull) {
            switch (dtype) {
              case DType.Float64:
                value = parseFloatFromBytes(bytes, fieldStart ?? 0, fieldEnd ?? 0);
                break;

              case DType.Int32:
                value = parseIntFromBytes(bytes, fieldStart ?? 0, fieldEnd ?? 0);
                break;

              case DType.Bool: {
                const byte = fieldStart !== undefined ? bytes[fieldStart] : undefined;
                value = byte === 116 || byte === 84 || byte === 49 ? 1 : 0; // t, T, 1
                break;
              }

              case DType.String: {
                const str = decoder.decode(bytes.subarray(fieldStart, fieldEnd));
                if (df.dictionary) {
                  value = internString(df.dictionary, str);
                }
                break;
              }

              case DType.DateTime:
              case DType.Date: {
                // For now, just parse as number (TODO: proper datetime parsing)
                value = BigInt(
                  Math.floor(parseFloatFromBytes(bytes, fieldStart ?? 0, fieldEnd ?? 0)),
                );
                break;
              }
            }
          }

          if (column && value !== undefined) {
            setColumnValue(column, currentRow, value);
          }

          fieldStart = i + 1;
          colIdx++;
        }
      }

      currentRow++;
    }
  }

  return ok(df);
}
