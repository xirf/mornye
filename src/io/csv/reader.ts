import type { Schema } from '../../core/types';
import { type CsvOptions, resolveOptions } from './options';
import type { CsvReadResult } from './parse-result';
import { computeLineStarts } from './reader-shared';
import { readCsvUnquoted, supportsUnquotedPath } from './reader-unquoted';
import { readCsvWithHybridParser } from './reader-quoted';

/**
 * Ultra-fast CSV reader using optimized byte-level parsing.
 *
 * Two strategies:
 * 1. No quotes: Direct byte parsing with SIMD line-finding (~1.3s for 387MB)
 * 2. Has quotes: Byte-level quote-aware parsing without materializing string[][]
 *
 * Key optimization: Parses directly into typed arrays without intermediate string[][].
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

  // Quote-aware fallback when fast path is not supported
  if (!supportsUnquotedPath(buffer)) {
    return readCsvWithHybridParser(buffer, bytes, lineStarts, opts, providedSchema, trackErrors);
  }

  return readCsvUnquoted(buffer, bytes, lineStarts, opts, providedSchema, trackErrors);
}
