import { DataFrame } from '../../core/dataframe';
import { Series } from '../../core/series';
import type { DTypeKind, Schema } from '../../core/types';
import { inferColumnType } from './inference';
import { BYTES, type CsvOptions, type ResolvedCsvOptions, resolveOptions } from './options';
import { type CsvReadResult, type ParseFailures, createParseFailures } from './parse-result';
import { hasQuotedFields } from './parser';
import {
  createLazyStringColumn,
  isLazyStringColumn,
  type LazyStringColumn,
} from '../../core/series/lazy-string';

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

  // Check for quotes - use HybridCsvParser for quoted files
  if (hasQuotedFields(buffer)) {
    return readCsvWithHybridParser(buffer, bytes, lineStarts, opts, providedSchema, trackErrors);
  }

  return readCsvFastPath(buffer, bytes, lineStarts, opts, providedSchema, trackErrors);
}

function readCsvFastPath<S extends Schema = Schema>(
  buffer: Buffer,
  bytes: Uint8Array,
  lineStarts: number[],
  opts: ResolvedCsvOptions,
  providedSchema: S | undefined,
  trackErrors: boolean,
): CsvReadResult<S> {
  const len = buffer.length;

  // Fast path: Direct byte parsing for files without quotes
  const COMMA = opts.delimiter;
  const LF = BYTES.LF;
  const CR = BYTES.CR;
  // Get headers
  const firstLineEnd = lineStarts[1]! - 1;
  const headerEnd = bytes[firstLineEnd - 1] === CR ? firstLineEnd - 1 : firstLineEnd;
  const headerLine = buffer.toString('utf-8', 0, headerEnd);
  const delimiter = String.fromCharCode(COMMA);
  const headers = opts.hasHeader
    ? headerLine.split(delimiter)
    : headerLine.split(delimiter).map((_, i) => `column_${i}`);

  const numCols = headers.length;
  const startLineIdx = opts.hasHeader ? 1 : 0;

  // Find actual data lines (skip empty trailing lines)
  let numDataLines = lineStarts.length - 1;
  while (numDataLines > startLineIdx) {
    const start = lineStarts[numDataLines - 1]!;
    const end = lineStarts[numDataLines] ?? len;
    if (end - start > 1) break;
    numDataLines--;
  }

  const rowCount = Math.min(numDataLines - startLineIdx, opts.maxRows);
  if (rowCount === 0) {
    return {
      df: DataFrame.empty({} as S),
      hasErrors: false,
    };
  }

  // Sample for type inference
  const sampleSize = Math.min(opts.sampleRows, rowCount);
  const samples: string[][] = [];
  for (let i = 0; i < sampleSize; i++) {
    const lineIdx = startLineIdx + i;
    const start = lineStarts[lineIdx]!;
    let end = lineStarts[lineIdx + 1]! - 1;
    if (bytes[end - 1] === CR) end--;
    samples.push(buffer.toString('utf-8', start, end).split(delimiter));
  }

  // Infer schema
  const schema: Schema = providedSchema ?? inferSchemaFast(headers, samples);

  // Pre-allocate storage
  const storage: (Float64Array | Int32Array | Uint8Array | string[] | LazyStringColumn)[] = [];
  const colTypes: DTypeKind[] = [];
  const isDictString: boolean[] = [];

  // Error tracking
  const parseErrors = new Map<string, ParseFailures>();

  for (let col = 0; col < numCols; col++) {
    const dtype = schema[headers[col]!];
    colTypes[col] = dtype?.kind ?? 'string';

    switch (dtype?.kind) {
      case 'float64':
        storage[col] = new Float64Array(rowCount);
        break;
      case 'int32':
        storage[col] = new Int32Array(rowCount);
        break;
      case 'bool':
        storage[col] = new Uint8Array(rowCount);
        break;
      default: {
        const useDict = /currency|exchange/i.test(headers[col] ?? '');
        storage[col] = createLazyStringColumn(buffer, rowCount, useDict);
        isDictString[col] = useDict;
        break;
      }
    }

    if (trackErrors) {
      parseErrors.set(headers[col]!, createParseFailures(rowCount));
    }
  }

  // Parse rows directly into columns - optimized byte-level parsing
  for (let rowIdx = 0; rowIdx < rowCount; rowIdx++) {
    const lineIdx = startLineIdx + rowIdx;
    const lineStart = lineStarts[lineIdx]!;
    let lineEnd = (lineStarts[lineIdx + 1] ?? len) - 1;
    if (bytes[lineEnd - 1] === CR) lineEnd--;

    let fieldStart = lineStart;

    for (let col = 0; col < numCols; col++) {
      // Find field end
      let fieldEnd = fieldStart;
      while (fieldEnd < lineEnd && bytes[fieldEnd] !== COMMA) fieldEnd++;

      const dtype = colTypes[col]!;
      const store = storage[col]!;

      switch (dtype) {
        case 'float64':
          (store as Float64Array)[rowIdx] = parseFloatFast(bytes, fieldStart, fieldEnd);
          break;
        case 'int32':
          (store as Int32Array)[rowIdx] = parseIntFast(bytes, fieldStart, fieldEnd);
          break;
        case 'bool':
          (store as Uint8Array)[rowIdx] =
            bytes[fieldStart] === 116 || bytes[fieldStart] === 84 || bytes[fieldStart] === 49
              ? 1
              : 0;
          break;
        default:
          storeLazyString(
            store as LazyStringColumn,
            rowIdx,
            fieldStart,
            fieldEnd,
            false,
            false,
          );
      }

      fieldStart = fieldEnd + 1;
    }
  }

  // Build Series
  const columns = new Map<keyof S, Series<DTypeKind>>();
  for (let col = 0; col < numCols; col++) {
    const header = headers[col]! as keyof S;
    const dtype = colTypes[col]!;
    const store = storage[col]!;

    switch (dtype) {
      case 'float64':
        columns.set(header, Series.float64(store as Float64Array));
        break;
      case 'int32':
        columns.set(header, Series.int32(store as Int32Array));
        break;
      case 'bool':
        columns.set(
          header,
          Series._fromStorage({ kind: 'bool', nullable: false }, store as Uint8Array),
        );
        break;
      default: {
        const stringStore = store as string[] | LazyStringColumn;
        if (isLazyStringColumn(stringStore)) {
          columns.set(header, Series.stringLazy(stringStore));
        } else {
          columns.set(header, Series.string(stringStore as string[]));
        }
        break;
      }
    }
  }

  const df = DataFrame._fromColumns(schema as S, columns, headers as (keyof S)[], rowCount);

  // Filter out columns with no errors
  const filteredErrors = new Map<keyof S, ParseFailures>();
  let hasErrors = false;

  if (trackErrors) {
    for (const [header, tracker] of parseErrors) {
      if (tracker.failureCount > 0) {
        filteredErrors.set(header as keyof S, tracker);
        hasErrors = true;
      }
    }
  }

  return {
    df,
    parseErrors: hasErrors ? filteredErrors : undefined,
    hasErrors,
  };
}

/**
 * Quote-aware parser that writes directly into column storage without
 * materializing intermediate string matrices. Handles escaped quotes and
 * CRLF endings while preserving the fast-path layout.
 */
async function readCsvWithHybridParser<S extends Schema = Schema>(
  buffer: Buffer,
  bytes: Uint8Array,
  lineStarts: number[],
  opts: ResolvedCsvOptions,
  providedSchema: S | undefined,
  trackErrors: boolean,
): Promise<CsvReadResult<S>> {
  if (lineStarts.length === 1) {
    return {
      df: DataFrame.empty({} as S),
      hasErrors: false,
    };
  }

  const delimiter = opts.delimiter;
  const quote = opts.quote;

  const logicalStarts = computeLogicalRowStarts(bytes, buffer.length, quote);
  if (logicalStarts.length === 0) {
    return {
      df: DataFrame.empty({} as S),
      hasErrors: false,
    };
  }

  // Parse header with quote awareness
  const headerNextStart = logicalStarts[1] ?? buffer.length;
  const headerLineEnd = Math.max(0, headerNextStart - 1);
  let headers = parseQuotedLine(buffer, bytes, 0, headerLineEnd, delimiter, quote);
  if (!opts.hasHeader) {
    headers = headers.map((_, i) => `column_${i}`);
  }

  const numCols = headers.length;
  const startLineIdx = opts.hasHeader ? 1 : 0;

  // Find actual data lines (skip empty trailing lines)
  let numDataLines = logicalStarts.length - 1;
  while (numDataLines > 0) {
    const start = logicalStarts[numDataLines - 1]!;
    const end = logicalStarts[numDataLines] ?? buffer.length;
    if (end - start > 1) break;
    numDataLines--;
  }

  const rowCount = Math.min(numDataLines, opts.maxRows);

  if (rowCount <= 0) {
    return {
      df: DataFrame.empty({} as S),
      hasErrors: false,
    };
  }

  // Sample for type inference (quote-aware)
  const sampleSize = Math.min(opts.sampleRows, rowCount);
  const samples: string[][] = [];
  for (let i = 0; i < sampleSize; i++) {
    const lineIdx = startLineIdx + i;
    const start = logicalStarts[lineIdx]!;
    const end = Math.max(start, (logicalStarts[lineIdx + 1] ?? buffer.length) - 1);
    samples.push(parseQuotedLine(buffer, bytes, start, end, delimiter, quote));
  }

  // Infer schema
  const schema: Schema = providedSchema ?? inferSchemaFast(headers, samples);

  // Pre-allocate storage
  const storage: (Float64Array | Int32Array | Uint8Array | string[] | LazyStringColumn)[] = [];
  const colTypes: DTypeKind[] = [];
  const parseErrors = new Map<string, ParseFailures>();
  const isDictString: boolean[] = [];

  for (let col = 0; col < numCols; col++) {
    const dtype = schema[headers[col]!];
    colTypes[col] = dtype?.kind ?? 'string';

    switch (dtype?.kind) {
      case 'float64':
        storage[col] = new Float64Array(rowCount);
        break;
      case 'int32':
        storage[col] = new Int32Array(rowCount);
        break;
      case 'bool':
        storage[col] = new Uint8Array(rowCount);
        break;
      default: {
        const useDict = /currency|exchange/i.test(headers[col] ?? '');
        storage[col] = createLazyStringColumn(buffer, rowCount, useDict);
        isDictString[col] = useDict;
        break;
      }
    }

    if (trackErrors) {
      parseErrors.set(headers[col]!, createParseFailures(rowCount));
    }
  }

  // Parse rows directly into columns with quote support
  for (let rowIdx = 0; rowIdx < rowCount; rowIdx++) {
    const lineIdx = startLineIdx + rowIdx;
    const lineStart = logicalStarts[lineIdx]!;
    const lineEnd = Math.max(lineStart, (logicalStarts[lineIdx + 1] ?? buffer.length) - 1);

    parseQuotedRowIntoStorage(
      buffer,
      bytes,
      lineStart,
      lineEnd,
      delimiter,
      quote,
      colTypes,
      storage,
      rowIdx,
    );
  }

  // Build Series
  const columns = new Map<keyof S, Series<DTypeKind>>();
  for (let col = 0; col < numCols; col++) {
    const header = headers[col]! as keyof S;
    const dtype = colTypes[col]!;
    const store = storage[col]!;

    switch (dtype) {
      case 'float64':
        columns.set(header, Series.float64(store as Float64Array));
        break;
      case 'int32':
        columns.set(header, Series.int32(store as Int32Array));
        break;
      case 'bool':
        columns.set(
          header,
          Series._fromStorage({ kind: 'bool', nullable: false }, store as Uint8Array),
        );
        break;
      default: {
        const stringStore = store as string[] | LazyStringColumn;
        if (isLazyStringColumn(stringStore)) {
          columns.set(header, Series.stringLazy(stringStore));
        } else {
          columns.set(header, Series.string(stringStore as string[]));
        }
        break;
      }
    }
  }

  const df = DataFrame._fromColumns(schema as S, columns, headers as (keyof S)[], rowCount);

  const filteredErrors = new Map<keyof S, ParseFailures>();
  let hasErrors = false;

  if (trackErrors) {
    for (const [header, tracker] of parseErrors) {
      if (tracker.failureCount > 0) {
        filteredErrors.set(header as keyof S, tracker);
        hasErrors = true;
      }
    }
  }

  return {
    df,
    parseErrors: hasErrors ? filteredErrors : undefined,
    hasErrors,
  };
}

function computeLineStarts(buffer: Buffer, len: number): number[] {
  const lineStarts: number[] = [0];
  let pos = 0;
  while (pos < len) {
    const idx = buffer.indexOf(BYTES.LF, pos);
    if (idx === -1) break;
    lineStarts.push(idx + 1);
    pos = idx + 1;
  }
  return lineStarts;
}

function lineEndExclusive(bytes: Uint8Array, lineStarts: number[], lineIdx: number, bufferLen: number): number {
  const nextStart = lineStarts[lineIdx + 1] ?? bufferLen;
  let end = nextStart;
  if (end > lineStarts[lineIdx]! && bytes[end - 1] === BYTES.LF) end--;
  if (end > lineStarts[lineIdx]! && bytes[end - 1] === BYTES.CR) end--;
  return end;
}

function computeLogicalRowStarts(bytes: Uint8Array, bufferLen: number, quote: number): number[] {
  const starts: number[] = [];
  let pos = 0;
  let inQuotes = false;
  starts.push(0);

  while (pos < bufferLen) {
    const byte = bytes[pos]!;

    if (byte === quote) {
      if (inQuotes && pos + 1 < bufferLen && bytes[pos + 1] === quote) {
        pos += 2;
        continue;
      }
      inQuotes = !inQuotes;
      pos++;
      continue;
    }

    if (byte === BYTES.LF && !inQuotes) {
      const next = pos + 1;
      if (next < bufferLen) starts.push(next);
    }
    pos++;
  }

  return starts;
}

function hasEscapedQuotes(bytes: Uint8Array, start: number, end: number, quote: number): boolean {
  for (let i = start; i < end - 1; i++) {
    if (bytes[i] === quote && bytes[i + 1] === quote) return true;
  }
  return false;
}

function storeLazyString(
  store: LazyStringColumn,
  rowIdx: number,
  start: number,
  end: number,
  quoted: boolean,
  hasEscapes: boolean,
): void {
  // Dictionary-coded path
  if (store.codes && store.dict && store.dictLookup) {
    let str = start === end ? '' : store.buffer.toString('utf-8', start, end);
    if (quoted && hasEscapes && str.indexOf('"') !== -1) {
      str = str.replace(/""/g, '"');
    }
    let code = store.dictLookup.get(str);
    if (code === undefined) {
      code = store.dict.length;
      store.dict.push(str);
      store.dictLookup.set(str, code);
    }
    store.codes[rowIdx] = code;
    return;
  }

  store.offsets[rowIdx] = start;
  store.lengths[rowIdx] = end - start;
  if (store.needsUnescape) {
    store.needsUnescape[rowIdx] = quoted && hasEscapes ? 1 : 0;
  }
  store.cache[rowIdx] = null;
}

function parseQuotedLine(
  buffer: Buffer,
  bytes: Uint8Array,
  start: number,
  endExclusive: number,
  delimiter: number,
  quote: number,
): string[] {
  let endClean = endExclusive;
  if (endClean > start && bytes[endClean - 1] === BYTES.CR) endClean--;

  const fields: string[] = [];
  let fieldStart = start;
  let inQuotes = false;

  for (let i = start; i <= endClean; i++) {
    const isLast = i === endClean;
    const byte = isLast ? delimiter : bytes[i]!;

    if (!isLast && byte === quote) {
      if (inQuotes && i + 1 < endExclusive && bytes[i + 1] === quote) {
        i++;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }

    if ((byte === delimiter && !inQuotes) || isLast) {
      const value = decodeQuotedField(buffer, bytes, fieldStart, isLast ? endClean : i, quote);
      fields.push(value);
      fieldStart = i + 1;
    }
  }

  return fields;
}

function decodeQuotedField(
  buffer: Buffer,
  bytes: Uint8Array,
  start: number,
  endExclusive: number,
  quote: number,
): string {
  if (start >= endExclusive) return '';

  let s = start;
  let e = endExclusive;
  let quoted = false;

  if (bytes[s] === quote && e - s >= 1) {
    quoted = true;
    s++;
    if (e > s && bytes[e - 1] === quote) {
      e--;
    }
  }

  let str = buffer.toString('utf-8', s, e);
  if (quoted && str.indexOf('"') !== -1) {
    str = str.replace(/""/g, '"');
  }
  return str;
}

function parseQuotedRowIntoStorage(
  buffer: Buffer,
  bytes: Uint8Array,
  start: number,
  endExclusive: number,
  delimiter: number,
  quote: number,
  colTypes: DTypeKind[],
  storage: (Float64Array | Int32Array | Uint8Array | string[] | LazyStringColumn)[],
  rowIdx: number,
): void {
  let endClean = endExclusive;
  if (endClean > start && bytes[endClean - 1] === BYTES.CR) endClean--;

  let fieldStart = start;
  let inQuotes = false;
  const numCols = colTypes.length;
  let col = 0;

  for (let i = start; i <= endClean && col < numCols; i++) {
    const isLast = i === endClean;
    const byte = isLast ? delimiter : bytes[i]!;

    if (!isLast && byte === quote) {
      if (inQuotes && i + 1 < endExclusive && bytes[i + 1] === quote) {
        i++;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }

    if ((byte === delimiter && !inQuotes) || isLast) {
      let s = fieldStart;
      let e = isLast ? endExclusive : i;
      let quoted = false;

      if (s < e && bytes[s] === quote) {
        quoted = true;
        s++;
        if (e > s && bytes[e - 1] === quote) e--;
      }

      const dtype = colTypes[col]!;
      const store = storage[col]!;

      switch (dtype) {
        case 'float64':
          (store as Float64Array)[rowIdx] = parseFloatFast(bytes, s, e);
          break;
        case 'int32':
          (store as Int32Array)[rowIdx] = parseIntFast(bytes, s, e);
          break;
        case 'bool': {
          const first = s < e ? bytes[s]! : 0;
          (store as Uint8Array)[rowIdx] = first === 116 || first === 84 || first === 49 ? 1 : 0;
          break;
        }
        default:
          storeLazyString(
            store as LazyStringColumn,
            rowIdx,
            s,
            e,
            quoted,
            quoted && hasEscapedQuotes(bytes, s, e, quote),
          );
          break;
      }

      fieldStart = i + 1;
      col++;
    }
  }
}

// Optimized numeric parsers operating directly on bytes
function parseFloatFast(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let i = start;
  let negative = false;
  if (bytes[i] === 45) {
    negative = true;
    i++;
  } else if (bytes[i] === 43) {
    i++;
  }

  let intPart = 0;
  while (i < end && bytes[i]! >= 48 && bytes[i]! <= 57) {
    intPart = intPart * 10 + (bytes[i]! - 48);
    i++;
  }

  let result = intPart;
  if (i < end && bytes[i] === 46) {
    i++;
    let fracPart = 0;
    let fracDigits = 0;
    while (i < end && bytes[i]! >= 48 && bytes[i]! <= 57) {
      fracPart = fracPart * 10 + (bytes[i]! - 48);
      fracDigits++;
      i++;
    }
    if (fracDigits > 0) {
      result += fracPart / 10 ** fracDigits;
    }
  }

  return negative ? -result : result;
}

function parseIntFast(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let i = start;
  let negative = false;
  if (bytes[i] === 45) {
    negative = true;
    i++;
  } else if (bytes[i] === 43) {
    i++;
  }

  let result = 0;
  while (i < end && bytes[i]! >= 48 && bytes[i]! <= 57) {
    result = result * 10 + (bytes[i]! - 48);
    i++;
  }

  return negative ? -result : result;
}

function inferSchemaFast(headers: string[], samples: string[][]): Schema {
  const schema: Schema = {};
  for (let col = 0; col < headers.length; col++) {
    const columnSamples: string[] = [];
    for (const row of samples) {
      if (row[col] !== undefined) columnSamples.push(row[col]!);
    }
    schema[headers[col]!] = inferColumnType(columnSamples);
  }
  return schema;
}
