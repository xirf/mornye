import {
  generateTaskId,
  getConfig,
  releaseAllocation,
  requestAllocation,
  updateUsage,
} from '../../core/config';
import { DataFrame } from '../../core/dataframe';
import type { Schema } from '../../core/types';
import { MemoryLimitError } from '../../errors';
import { type CsvOptions, resolveOptions } from './options';
import type { CsvReadResult } from './parse-result';
import { readCsvWithHybridParser } from './reader-quoted';
import { computeLineStarts } from './reader-shared';
import { readCsvUnquoted, supportsUnquotedPath } from './reader-unquoted';

/**
 * Memory overhead multiplier for estimating memory usage from file size.
 *
 * CSV files typically expand to ~2.5x their size when loaded into typed arrays:
 * - Numbers: 8 bytes (float64) per value vs ~5 bytes in CSV
 * - Strings: overhead for string objects and lazy storage metadata
 * - Additional arrays and data structures
 */
const MEMORY_OVERHEAD_MULTIPLIER = 2.5;

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
 * @param options - Parsing options (delimiter, hasHeader, schema, memoryLimitBytes, etc.).
 * @returns A promise resolving to the parsed DataFrame and any non-fatal errors.
 *
 * @example
 * ```ts
 * // Basic usage
 * const { df } = await readCsv('./data.csv');
 *
 * // With memory limit (opt-in)
 * const { df, memoryError } = await readCsv('./large.csv', {
 *   memoryLimitBytes: 100 * 1024 * 1024, // 100MB
 * });
 *
 * if (memoryError) {
 *   console.log('File too large:', memoryError.format());
 *   // df will be empty in this case
 * }
 * ```
 */
export async function readCsv<S extends Schema = Schema>(
  path: string,
  options?: CsvOptions & { schema?: S },
): Promise<CsvReadResult<S>> {
  const opts = resolveOptions(options);
  const providedSchema = options?.schema;
  const trackErrors = opts.trackErrors;

  // Get file info for memory checking
  const file = Bun.file(path);
  const fileSize = file.size;

  // Memory limit check (opt-in via memoryLimitBytes option)
  const config = getConfig();
  const taskId = generateTaskId();
  let allocationSuccess = true;
  let memoryError: MemoryLimitError | undefined;

  if (config.enabled && options?.memoryLimitBytes !== undefined) {
    // Per-task limit specified - use that
    const estimatedMemory = Math.ceil(fileSize * MEMORY_OVERHEAD_MULTIPLIER);
    const perTaskLimit = options.memoryLimitBytes;

    if (estimatedMemory > perTaskLimit) {
      // Return empty DataFrame with error
      return {
        df: DataFrame.empty({} as S),
        hasErrors: false,
        memoryError: new MemoryLimitError(
          estimatedMemory,
          perTaskLimit,
          config.globalLimitBytes,
          0,
        ),
      };
    }

    // Also check global allocation
    const allocation = requestAllocation(taskId, estimatedMemory);
    if (!allocation.success && allocation.error) {
      return {
        df: DataFrame.empty({} as S),
        hasErrors: false,
        memoryError: MemoryLimitError.fromAllocationError(allocation.error),
      };
    }
    allocationSuccess = allocation.success;
  }

  try {
    // Read file - keep both Buffer (for SIMD indexOf) and Uint8Array (for parsing)
    const arrayBuffer = await file.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    const bytes = new Uint8Array(arrayBuffer);
    const len = buffer.length;

    // Update memory usage tracking
    if (config.enabled && allocationSuccess) {
      updateUsage(taskId, len);
    }

    const lineStarts = computeLineStarts(buffer, len);

    // Quote-aware fallback when unquoted path is not supported
    let result: CsvReadResult<S>;
    if (!supportsUnquotedPath(buffer)) {
      result = await readCsvWithHybridParser(
        buffer,
        bytes,
        lineStarts,
        opts,
        providedSchema,
        trackErrors,
      );
    } else {
      result = readCsvUnquoted(buffer, bytes, lineStarts, opts, providedSchema, trackErrors);
    }

    // Preserve any memory error that occurred
    if (memoryError) {
      result.memoryError = memoryError;
    }

    return result;
  } finally {
    // Always release allocation when done
    if (config.enabled && allocationSuccess) {
      releaseAllocation(taskId);
    }
  }
}
