import { DataFrame } from '../../core/dataframe';
import { Series } from '../../core/series';
import {
  type LazyStringColumn,
  createLazyStringColumn,
  isLazyStringColumn,
} from '../../core/series/lazy-string';
import type { DTypeKind, Schema } from '../../core/types';
import { BYTES, type ResolvedCsvOptions } from './options';
import { type CsvReadResult, type ParseFailures, createParseFailures } from './parse-result';
import {
  type DateTimeParser,
  applyDateTimeSchemaOverrides,
  buildDateTimeParsers,
  computeLogicalRowStarts,
  decodeQuotedField,
  hasEscapedQuotes,
  inferSchemaOptimized,
  parseFloatOptimized,
  parseIntOptimized,
  parseQuotedLine,
  storeLazyString,
} from './reader-shared';

/**
 * Quote-aware parser that writes directly into column storage without
 * materializing intermediate string matrices.
 */
export async function readCsvWithHybridParser<S extends Schema = Schema>(
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

  const headerNextStart = logicalStarts[1] ?? buffer.length;
  const headerLineEnd = Math.max(0, headerNextStart - 1);
  let headers = parseQuotedLine(buffer, bytes, 0, headerLineEnd, delimiter, quote);
  if (!opts.hasHeader) {
    headers = headers.map((_, i) => `column_${i}`);
  }

  const numCols = headers.length;
  const startLineIdx = opts.hasHeader ? 1 : 0;

  const datetimeParsers = buildDateTimeParsers(headers, opts);

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

  const sampleSize = Math.min(opts.sampleRows, rowCount);
  const samples: string[][] = [];
  for (let i = 0; i < sampleSize; i++) {
    const lineIdx = startLineIdx + i;
    const start = logicalStarts[lineIdx]!;
    const end = Math.max(start, (logicalStarts[lineIdx + 1] ?? buffer.length) - 1);
    samples.push(parseQuotedLine(buffer, bytes, start, end, delimiter, quote));
  }

  const schema: Schema = providedSchema ?? inferSchemaOptimized(headers, samples);
  applyDateTimeSchemaOverrides(schema, headers, datetimeParsers);

  const storage: (Float64Array | Int32Array | Uint8Array | string[] | LazyStringColumn)[] = [];
  const colTypes: DTypeKind[] = [];
  const parseErrors = new Map<string, ParseFailures>();
  const isDictString: boolean[] = [];

  for (let col = 0; col < numCols; col++) {
    const dtype = schema[headers[col]!];
    const isDatetime = datetimeParsers[col] !== null;
    colTypes[col] = isDatetime ? 'float64' : (dtype?.kind ?? 'string');

    switch (colTypes[col]) {
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
      datetimeParsers,
      rowIdx,
    );
  }

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

function parseQuotedRowIntoStorage(
  buffer: Buffer,
  bytes: Uint8Array,
  start: number,
  endExclusive: number,
  delimiter: number,
  quote: number,
  colTypes: DTypeKind[],
  storage: (Float64Array | Int32Array | Uint8Array | string[] | LazyStringColumn)[],
  datetimeParsers: (DateTimeParser | null)[],
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
      const dtParser = datetimeParsers[col];

      if (dtParser) {
        const epoch = dtParser(buffer.toString('utf-8', s, e));
        (store as Float64Array)[rowIdx] = epoch;
      } else {
        switch (dtype) {
          case 'float64':
            (store as Float64Array)[rowIdx] = parseFloatOptimized(bytes, s, e);
            break;
          case 'int32':
            (store as Int32Array)[rowIdx] = parseIntOptimized(bytes, s, e);
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
      }

      fieldStart = i + 1;
      col++;
    }
  }
}
