import { DataFrame } from '../../core/dataframe';
import { Series } from '../../core/series';
import { createLazyStringColumn, isLazyStringColumn, type LazyStringColumn } from '../../core/series/lazy-string';
import type { DTypeKind, Schema } from '../../core/types';
import { BYTES, type ResolvedCsvOptions } from './options';
import { type CsvReadResult, type ParseFailures, createParseFailures } from './parse-result';
import { hasQuotedFields } from './parser';
import {
  applyDateTimeSchemaOverrides,
  buildDateTimeParsers,
  inferSchemaFast,
  parseFloatFast,
  parseIntFast,
  storeLazyString,
} from './reader-shared';

/**
 * Unquoted CSV path (fast) for files without quotes.
 */
export function readCsvUnquoted<S extends Schema = Schema>(
  buffer: Buffer,
  bytes: Uint8Array,
  lineStarts: number[],
  opts: ResolvedCsvOptions,
  providedSchema: S | undefined,
  trackErrors: boolean,
): CsvReadResult<S> {
  const len = buffer.length;

  const COMMA = opts.delimiter;
  const LF = BYTES.LF;
  const CR = BYTES.CR;

  const firstLineEnd = lineStarts[1]! - 1;
  const headerEnd = bytes[firstLineEnd - 1] === CR ? firstLineEnd - 1 : firstLineEnd;
  const headerLine = buffer.toString('utf-8', 0, headerEnd);
  const delimiter = String.fromCharCode(COMMA);
  const headers = opts.hasHeader
    ? headerLine.split(delimiter)
    : headerLine.split(delimiter).map((_, i) => `column_${i}`);

  const numCols = headers.length;
  const startLineIdx = opts.hasHeader ? 1 : 0;

  const datetimeParsers = buildDateTimeParsers(headers, opts);

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

  const sampleSize = Math.min(opts.sampleRows, rowCount);
  const samples: string[][] = [];
  for (let i = 0; i < sampleSize; i++) {
    const lineIdx = startLineIdx + i;
    const start = lineStarts[lineIdx]!;
    let end = lineStarts[lineIdx + 1]! - 1;
    if (bytes[end - 1] === CR) end--;
    samples.push(buffer.toString('utf-8', start, end).split(delimiter));
  }

  const schema: Schema = providedSchema ?? inferSchemaFast(headers, samples);
  applyDateTimeSchemaOverrides(schema, headers, datetimeParsers);

  const storage: (Float64Array | Int32Array | Uint8Array | string[] | LazyStringColumn)[] = [];
  const colTypes: DTypeKind[] = [];
  const isDictString: boolean[] = [];
  const parseErrors = new Map<string, ParseFailures>();

  for (let col = 0; col < numCols; col++) {
    const dtype = schema[headers[col]!];
    const isDatetime = datetimeParsers[col] !== null;
    colTypes[col] = isDatetime ? 'float64' : dtype?.kind ?? 'string';

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
    const lineStart = lineStarts[lineIdx]!;
    let lineEnd = (lineStarts[lineIdx + 1] ?? len) - 1;
    if (bytes[lineEnd - 1] === CR) lineEnd--;

    let fieldStart = lineStart;

    for (let col = 0; col < numCols; col++) {
      let fieldEnd = fieldStart;
      while (fieldEnd < lineEnd && bytes[fieldEnd] !== COMMA) fieldEnd++;

      const dtype = colTypes[col]!;
      const store = storage[col]!;
      const dtParser = datetimeParsers[col];

      if (dtParser) {
        const epoch = dtParser(buffer.toString('utf-8', fieldStart, fieldEnd));
        (store as Float64Array)[rowIdx] = epoch;
      } else {
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
      }

      fieldStart = fieldEnd + 1;
    }
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

export function supportsUnquotedPath(buffer: Buffer): boolean {
  return !hasQuotedFields(buffer);
}
