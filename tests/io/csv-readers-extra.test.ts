import { describe, expect, test } from 'bun:test';
import { createLazyStringColumn } from '../../src/core/series/lazy-string';
import { m } from '../../src/core/types';
import { parseValue } from '../../src/io/csv/inference';
import { resolveOptions } from '../../src/io/csv/options';
import { readCsvWithHybridParser } from '../../src/io/csv/reader-quoted';
import {
  computeLineStarts,
  computeLogicalRowStarts,
  parseQuotedLine,
  storeLazyString,
} from '../../src/io/csv/reader-shared';
import { readCsvUnquoted, supportsUnquotedPath } from '../../src/io/csv/reader-unquoted';

const toBytes = (s: string) => new Uint8Array(Buffer.from(s));

describe('reader-shared helpers', () => {
  test('parseQuotedLine splits mixed quoted/unquoted fields', () => {
    const csv = '"a,b",c,"d""e"';
    const buf = Buffer.from(csv);
    const bytes = new Uint8Array(buf);

    const fields = parseQuotedLine(buf, bytes, 0, csv.length, ','.charCodeAt(0), '"'.charCodeAt(0));
    expect(fields).toEqual(['a,b', 'c', 'd"e']);
  });

  test('storeLazyString writes dict codes and unescaped markers', () => {
    const buf = Buffer.from('"hello"');
    const store = createLazyStringColumn(buf, 2, true);

    storeLazyString(store, 0, 1, 6, true, true);
    storeLazyString(store, 1, 1, 6, true, true);

    expect(store.codes?.[0]).toBe(0);
    expect(store.codes?.[1]).toBe(0);
    expect(store.dict?.[0]).toBe('hello');
  });
});

describe('reader-unquoted', () => {
  test('supportsUnquotedPath false when quotes present', () => {
    expect(supportsUnquotedPath(Buffer.from('"quoted"'))).toBe(false);
  });

  test('readCsvUnquoted parses booleans and datetime overrides', () => {
    const csv = 'flag,ts\nTRUE,2021-01-01\n0,2021-01-02\n';
    const buffer = Buffer.from(csv);
    const bytes = new Uint8Array(buffer);
    const lineStarts = computeLineStarts(buffer, buffer.length);
    const opts = resolveOptions({ datetime: { columns: { ts: { format: 'date', zone: 'UTC' } } } });

    const result = readCsvUnquoted(buffer, bytes, lineStarts, opts, undefined, true);

    expect(result.df.col('flag').at(0)).toBe(true);
    expect(result.df.col('flag').at(1)).toBe(false);
    expect(result.df.col('ts').at(0)).toBe(Date.UTC(2021, 0, 1));
  });
});

describe('reader-quoted hybrid path', () => {
  test('returns empty when lineStarts lacks data', async () => {
    const buffer = Buffer.from('');
    const bytes = new Uint8Array(buffer);
    const opts = resolveOptions();
    const result = await readCsvWithHybridParser(buffer, bytes, [0], opts, undefined, true);
    expect(result.df.shape[0]).toBe(0);
    expect(result.hasErrors).toBe(false);
  });

  test('handles empty data and headers without hasHeader', async () => {
    const csv = 'a,b\n';
    const buffer = Buffer.from(csv);
    const bytes = new Uint8Array(buffer);
    const opts = resolveOptions({ hasHeader: false });
    const logical = computeLogicalRowStarts(bytes, buffer.length, '"'.charCodeAt(0));

    const result = await readCsvWithHybridParser(buffer, bytes, logical, opts, undefined, true);
    expect(result.df.shape[0]).toBe(0);
    expect(result.df.columns()).toEqual([]);
  });

  test('tracks parse errors on bool/int/datetime with escapes', async () => {
    const csv =
      'flag,count,ts,comment\n"maybe",2.5,"2021-01-01 00:00:00","He said ""Hi"""\n"1",bad,"2021-01-02 00:00:00","ok"\n';
    const buffer = Buffer.from(csv);
    const bytes = new Uint8Array(buffer);
    const logical = computeLogicalRowStarts(bytes, buffer.length, '"'.charCodeAt(0));
    const opts = resolveOptions({ datetime: { columns: { ts: { format: 'sql', zone: 'UTC' } } } });

    const result = await readCsvWithHybridParser(buffer, bytes, logical, opts, undefined, true);
    const counts = result.df.col('count');
    expect(counts.at(1)).toBe('bad');

    const comments = result.df.col('comment');
    expect(comments.at(0)).toBe('He said "Hi"');
  });
});

describe('inference edge cases', () => {
  test('parseValue handles invalid bool and returns original', () => {
    const result = parseValue('not-bool', m.bool());
    expect(result.success).toBe(false);
    expect(result.original).toBe('not-bool');
  });

  test('parseValue handles int32 NaN and truncates float', () => {
    const nanInt = parseValue('abc', m.int32());
    expect(nanInt.success).toBe(false);
    expect(Number.isNaN(nanInt.value as number)).toBe(true);

    const trunc = parseValue('9.9', m.int32());
    expect(trunc.value).toBe(9);
  });
});
