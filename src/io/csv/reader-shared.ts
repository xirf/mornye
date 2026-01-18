import { type LazyStringColumn, createLazyStringColumn } from '../../core/series/lazy-string';
import type { DTypeKind, Schema } from '../../core/types';
import { createDateTimeParser } from '../datetime';
import { inferColumnType } from './inference';
import { BYTES, type ResolvedCsvOptions } from './options';

export type DateTimeParser = ((value: string) => number) | null;

export function buildDateTimeParsers(
  headers: string[],
  opts: ResolvedCsvOptions,
): DateTimeParser[] {
  const parsers: DateTimeParser[] = [];
  for (const header of headers) {
    const cfg = opts.datetime.columns.get(header);
    if (!cfg) {
      parsers.push(null);
      continue;
    }
    parsers.push(
      createDateTimeParser(cfg.format, cfg.offsetMinutes ?? opts.datetime.defaultOffsetMinutes),
    );
  }
  return parsers;
}

export function applyDateTimeSchemaOverrides(
  schema: Schema,
  headers: string[],
  datetimeParsers: DateTimeParser[],
): void {
  for (let i = 0; i < headers.length; i++) {
    if (datetimeParsers[i]) {
      schema[headers[i]!] = { kind: 'float64', nullable: false };
    }
  }
}

export function computeLineStarts(buffer: Buffer, len: number): number[] {
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

export function computeLogicalRowStarts(
  bytes: Uint8Array,
  bufferLen: number,
  quote: number,
): number[] {
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

export function hasEscapedQuotes(
  bytes: Uint8Array,
  start: number,
  end: number,
  quote: number,
): boolean {
  for (let i = start; i < end - 1; i++) {
    if (bytes[i] === quote && bytes[i + 1] === quote) return true;
  }
  return false;
}

export function storeLazyString(
  store: LazyStringColumn,
  rowIdx: number,
  start: number,
  end: number,
  quoted: boolean,
  hasEscapes: boolean,
): void {
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

export function parseQuotedLine(
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

export function decodeQuotedField(
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

export function parseFloatOptimized(bytes: Uint8Array, start: number, end: number): number {
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

export function parseIntOptimized(bytes: Uint8Array, start: number, end: number): number {
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

export function inferSchemaOptimized(headers: string[], samples: string[][]): Schema {
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
