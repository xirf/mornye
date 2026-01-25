import { setColumnValue } from '../core/column';
import type { Schema } from '../core/schema';
import { validateSchema } from '../core/schema';
import type { DataFrame } from '../dataframe/dataframe';
import { addColumn, createDataFrame } from '../dataframe/dataframe';
import { internString } from '../memory/dictionary';
import { getDTypeSize } from '../types/dtypes';
import { DType } from '../types/dtypes';
import type { Result } from '../types/result';
import { err, ok } from '../types/result';

/**
 * CSV reading options
 */
export interface CsvOptions {
  /** Schema definition (required) */
  schema: Schema;
  /** Field delimiter (default: ",") */
  delimiter?: string;
  /** Whether first row is header (default: true) */
  hasHeader?: boolean;
  /** Custom null value representations (default: ["NA", "null", "-", ""]) */
  nullValues?: string[];
}

// Byte constants
const COMMA = 44;
const CR = 13;
const LF = 10;
const MINUS = 45;
const PLUS = 43;
const DOT = 46;
const ZERO = 48;
const NINE = 57;
const CHAR_t = 116;
const CHAR_T = 84;
const CHAR_1 = 49;

/** Parse float from bytes */
function parseFloatFromBytes(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;
  let i = start;
  let negative = false;
  if (bytes[i] === MINUS) {
    negative = true;
    i++;
  } else if (bytes[i] === PLUS) {
    i++;
  }
  let intPart = 0;
  while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
    intPart = intPart * 10 + (bytes[i]! - ZERO);
    i++;
  }
  let result = intPart;
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

/** Parse int from bytes - returns null if invalid */
function parseIntFromBytes(bytes: Uint8Array, start: number, end: number): number | null {
  if (start >= end) return null;
  let i = start;
  let negative = false;
  if (bytes[i] === MINUS) {
    negative = true;
    i++;
  } else if (bytes[i] === PLUS) {
    i++;
  }
  const digitStart = i;
  let result = 0;
  while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
    result = result * 10 + (bytes[i]! - ZERO);
    i++;
  }
  // If we didn't parse any digits or didn't consume all characters, it's invalid
  if (i === digitStart || i < end) return null;
  return negative ? -result : result;
}

/** Check if null */
function isNullField(
  bytes: Uint8Array,
  start: number,
  end: number,
  nullValueBytes: Uint8Array[],
): boolean {
  const len = end - start;

  // Check each null value pattern
  for (const nullBytes of nullValueBytes) {
    if (nullBytes.length !== len) continue;

    let match = true;
    for (let i = 0; i < len; i++) {
      if (bytes[start + i] !== nullBytes[i]) {
        match = false;
        break;
      }
    }
    if (match) return true;
  }

  return false;
}

/** Find line starts */
function findLineStarts(bytes: Uint8Array): number[] {
  const starts: number[] = [0];
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] === LF) starts.push(i + 1);
  }
  return starts;
}

/** Parse header */
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
      // When we hit end of line, include the character at 'end'
      if (i === end && i < bytes.length) fieldEnd = i + 1;
      if (fieldEnd > fieldStart && bytes[fieldEnd - 1] === CR) fieldEnd--;
      const field = decoder.decode(bytes.subarray(fieldStart, fieldEnd)).trim();
      headers.push(field);
      fieldStart = i + 1;
    }
  }
  return headers;
}

/**
 * Internal function that processes CSV data (used by both file and string inputs)
 * Works directly on Uint8Array for zero-copy parsing
 */
async function readCsvInternal<T>(
  data: string | Buffer,
  options: CsvOptions,
): Promise<Result<DataFrame<T>, Error>> {
  const schemaResult = validateSchema(options.schema);
  if (!schemaResult.ok) return err(new Error(schemaResult.error));
  return readCsvInternalImplementation(data, options) as Promise<Result<DataFrame<T>, Error>>;
}

async function readCsvInternalImplementation(
  data: string | Buffer,
  options: CsvOptions,
): Promise<Result<DataFrame<unknown>, Error>> {
  const schemaResult = validateSchema(options.schema);
  if (!schemaResult.ok) return err(new Error(schemaResult.error));

  const delimiter = options.delimiter?.charCodeAt(0) ?? COMMA;
  const hasHeader = options.hasHeader ?? true;
  const nullValues = options.nullValues ?? ['NA', 'null', '-', ''];

  // Pre-encode null values as bytes for efficient comparison
  const encoder = new TextEncoder();
  const nullValueBytes = nullValues.map((v) => encoder.encode(v));

  const bytes = typeof data === 'string' ? new TextEncoder().encode(data) : new Uint8Array(data);
  if (bytes.length === 0) return err(new Error('Empty CSV data'));

  const lineStarts = findLineStarts(bytes);
  if (lineStarts.length < 1) return err(new Error('No lines found in CSV'));

  let headerNames: string[];
  let dataStartLine = 0;

  if (hasHeader) {
    const firstLineEnd = lineStarts[1] ?? bytes.length;
    const firstLineStart = lineStarts[0] ?? 0;
    headerNames = parseHeaderLine(bytes, firstLineStart, firstLineEnd - 1, delimiter);
    dataStartLine = 1;
  } else {
    headerNames = Object.keys(options.schema);
  }

  const schemaKeys = Object.keys(options.schema);
  if (schemaKeys.length !== headerNames.length) {
    return err(
      new Error(
        `Schema column count (${schemaKeys.length}) doesn't match CSV columns (${headerNames.length})`,
      ),
    );
  }

  if (hasHeader) {
    for (const header of headerNames) {
      if (!schemaKeys.includes(header)) {
        return err(new Error(`Schema missing column from CSV header: ${header}`));
      }
    }
  }

  // Calculate total data rows
  // If file doesn't end with newline, we have one more line than lineStarts indicates
  let totalRows = lineStarts.length - dataStartLine;

  // Check if last line is empty (trim trailing empty lines)
  while (totalRows > 0) {
    const lineIdx = dataStartLine + totalRows - 1;
    const start = lineStarts[lineIdx];
    const end = lineIdx + 1 < lineStarts.length ? lineStarts[lineIdx + 1] : bytes.length;
    if (start === undefined || end === undefined) break;

    // Check if line has content (more than just CR/LF)
    let hasContent = false;
    for (let i = start; i < end; i++) {
      if (bytes[i] !== CR && bytes[i] !== LF) {
        hasContent = true;
        break;
      }
    }
    if (hasContent) break;
    totalRows--;
  }

  const df = createDataFrame();
  if (totalRows === 0) return ok(df);

  for (const colName of headerNames) {
    const dtype = options.schema[colName];
    if (!dtype) return err(new Error(`Missing dtype for column: ${colName}`));
    const addResult = addColumn(df, colName, dtype, totalRows);
    if (!addResult.ok) return err(new Error(addResult.error));
  }

  const decoder = new TextDecoder();
  let currentRow = 0;

  // HOT LOOP: Process all lines at once (no chunking)
  for (let lineIdx = dataStartLine; lineIdx < dataStartLine + totalRows; lineIdx++) {
    const lineStart = lineStarts[lineIdx];
    const lineEnd = lineStarts[lineIdx + 1] ?? bytes.length;
    if (lineStart === undefined) continue;
    let fieldStart = lineStart;
    let colIdx = 0;

    // HOT LOOP: Parse fields - zero object creation
    for (let i = lineStart; i <= lineEnd && colIdx < headerNames.length; i++) {
      const isEndOfLine = i === lineEnd || bytes[i] === LF;
      const isDelimiter = bytes[i] === delimiter;

      if (isDelimiter || isEndOfLine) {
        let fieldEnd: number = i;
        if (fieldEnd > fieldStart && bytes[fieldEnd - 1] === CR) fieldEnd--;

        const colName = headerNames[colIdx];
        const dtype = colName ? options.schema[colName] : undefined;
        const column = colName && dtype ? df.columns.get(colName) : undefined;
        if (colName && dtype && !column) return err(new Error(`Column not found: ${colName}`));

        const isNull = isNullField(bytes, fieldStart, fieldEnd, nullValueBytes);
        const bytesPerElement = dtype ? getDTypeSize(dtype) : 0;
        const byteOffset = currentRow * bytesPerElement;

        if (column && dtype && !isNull) {
          switch (dtype) {
            case DType.Float64:
              column.view.setFloat64(
                byteOffset,
                parseFloatFromBytes(bytes, fieldStart ?? 0, fieldEnd ?? 0),
                true,
              );
              break;
            case DType.Int32: {
              const parsed = parseIntFromBytes(bytes, fieldStart ?? 0, fieldEnd ?? 0);
              if (parsed === null) {
                const badValue = decoder.decode(bytes.subarray(fieldStart, fieldEnd));
                return err(
                  new Error(
                    `Invalid integer value at row ${currentRow + 1}, column '${colName}': ${badValue}`,
                  ),
                );
              }
              column.view.setInt32(byteOffset, parsed, true);
              break;
            }
            case DType.Bool: {
              const byte = fieldStart !== undefined ? bytes[fieldStart] : undefined;
              column.view.setUint8(
                byteOffset,
                byte === CHAR_t || byte === CHAR_T || byte === CHAR_1 ? 1 : 0,
              );
              break;
            }
            case DType.String: {
              const str = decoder.decode(bytes.subarray(fieldStart ?? 0, fieldEnd ?? 0));
              const dictId = df.dictionary ? internString(df.dictionary, str) : 0;
              column.view.setInt32(byteOffset, dictId, true);
              break;
            }
            case DType.DateTime:
            case DType.Date: {
              const timestamp = BigInt(
                Math.floor(parseFloatFromBytes(bytes, fieldStart ?? 0, fieldEnd ?? 0)),
              );
              column.view.setBigInt64(byteOffset, timestamp, true);
              break;
            }
          }
        }

        fieldStart = i + 1;
        colIdx++;
      }
    }

    currentRow++;
  }

  return ok(df);
}

/**
 * Read CSV from a file path into a DataFrame (eager loading - all at once)
 * Handles file reading internally using Bun's efficient file I/O
 *
 * @param path - Path to CSV file
 * @param options - CSV parsing options
 * @example
 * ```ts
 * const result = await readCsv('data.csv', {
 *   schema: { id: DType.Int32, name: DType.String }
 * });
 * if (result.ok) {
 *   console.log(result.value);
 * }
 * ```
 */
export async function readCsv<S extends Record<string, DType>>(
  path: string,
  options: CsvOptions & { schema: S },
): Promise<Result<DataFrame<InferSchemaType<S>>, Error>> {
  try {
    // Use Bun's file API for efficient file reading
    const file = Bun.file(path);
    const text = await file.text();
    // We cast the result to the inferred type because at runtime we just check the schema matches the data
    return readCsvInternal(text, options) as unknown as Promise<
      Result<DataFrame<InferSchemaType<S>>, Error>
    >;
  } catch (error) {
    return err(
      new Error(
        `Failed to read file '${path}': ${error instanceof Error ? error.message : String(error)}`,
      ),
    );
  }
}

/**
 * Read CSV from a string into a DataFrame (eager loading - all at once)
 * Use this when you already have CSV data in memory
 *
 * @param data - CSV string or Buffer data
 * @param options - CSV parsing options
 * @example
 * ```ts
 * const csvData = "id,name\n1,Alice\n2,Bob";
 * const result = await readCsvFromString(csvData, {
 *   schema: { id: DType.Int32, name: DType.String }
 * });
 * ```
 */
export async function readCsvFromString<S extends Record<string, DType>>(
  data: string | Buffer,
  options: CsvOptions & { schema: S },
): Promise<Result<DataFrame<InferSchemaType<S>>, Error>> {
  return readCsvInternal(data, options) as unknown as Promise<
    Result<DataFrame<InferSchemaType<S>>, Error>
  >;
}

// Helper type for type inference
import type { InferSchemaType } from '../lazyframe/types';
