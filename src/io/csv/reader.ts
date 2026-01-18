import type { Schema } from '../../core/types';
import { type CsvOptions, resolveOptions } from './options';
import type { CsvReadResult } from './parse-result';
import { readCsvWithHybridParser } from './reader-quoted';
import { computeLineStarts } from './reader-shared';
import { readCsvUnquoted, supportsUnquotedPath } from './reader-unquoted';

/**
 * Reads a CSV file using Bun.js file system APIs.
 *
 * This function parses the file content directly into typed arrays (Float64Array, Int32Array, etc.),
 * bypassing intermediate string representations for numeric columns.
 *
 * It automatically selects between two parsing strategies:
 * 1. Unquoted Path: For simple CSVs without quoted fields.
 * 2. Hybrid Path: For CSVs containing quoted fields (RFC 4180 compliant).
 *
 * @param path - Absolute or relative path to the CSV file.
 * @param options - Parsing options (delimiter, hasHeader, schema, etc.).
 * @returns A promise resolving to the parsed DataFrame and any non-fatal errors.
 */
export async function readCsv<S extends Schema = Schema>(
  path: string,
  options?: CsvOptions & { schema?: S },
): Promise<CsvReadResult<S>> {
  const opts = resolveOptions(options);
  const providedSchema = options?.schema;
  const trackErrors = opts.trackErrors;

  // Read file - keep both Buffer (for SIMD indexOf) and Uint8Array (for parsing)
  const file = Bun.file(path);
  const arrayBuffer = await file.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);
  const bytes = new Uint8Array(arrayBuffer);
  const len = buffer.length;
  const lineStarts = computeLineStarts(buffer, len);

  // Quote-aware fallback when unquoted path is not supported
  if (!supportsUnquotedPath(buffer)) {
    return readCsvWithHybridParser(buffer, bytes, lineStarts, opts, providedSchema, trackErrors);
  }

  return readCsvUnquoted(buffer, bytes, lineStarts, opts, providedSchema, trackErrors);
}
