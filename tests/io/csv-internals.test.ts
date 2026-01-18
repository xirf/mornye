import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { DataFrame } from '../../src/core/dataframe';
import { m } from '../../src/core/types';
import { readCsv } from '../../src/io/csv';
import { inferColumnType, parseValue, parseValueSimple } from '../../src/io/csv/inference';
import { resolveOptions } from '../../src/io/csv/options';
import { createParseFailures, recordFailure } from '../../src/io/csv/parse-result';
import { CsvParser } from '../../src/io/csv/parser';
import { readCsvNode } from '../../src/io/csv/reader-node';
import { readCsvWithHybridParser } from '../../src/io/csv/reader-quoted';
import {
  computeLineStarts,
  computeLogicalRowStarts,
  decodeQuotedField,
  hasEscapedQuotes,
} from '../../src/io/csv/reader-shared';
import { scanCsv } from '../../src/io/csv/scanner';
import { toCsv } from '../../src/io/csv/writer';
import { createDateTimeParser } from '../../src/io/datetime';

describe('CSV inference utilities', () => {
  test('infers string when samples are empty or null-like', () => {
    expect(inferColumnType([]).kind).toBe('string');
    expect(inferColumnType(['', 'NA', 'null']).kind).toBe('string');
  });

  test('infers boolean columns from 0/1 and true/false', () => {
    const dtype = inferColumnType(['1', '0', 'true', 'False']);
    expect(dtype.kind).toBe('bool');
  });

  test('parseValue handles missing, invalid, and integer truncation', () => {
    const missingBool = parseValue('', m.bool());
    expect(missingBool.success).toBe(true);
    expect(missingBool.value).toBeNull();

    const invalidNumber = parseValue('nan-ish', m.float64());
    expect(invalidNumber.success).toBe(false);
    expect(invalidNumber.original).toBe('nan-ish');
    expect(Number.isNaN(invalidNumber.value as number)).toBe(true);

    const intFromFloat = parseValue('42.9', m.int32());
    expect(intFromFloat.success).toBe(true);
    expect(intFromFloat.value).toBe(42);

    expect(parseValueSimple(' padded ', m.string())).toBe('padded');
  });
});

describe('CSV options and datetime resolution', () => {
  test('resolveOptions converts delimiters and datetime offsets', () => {
    const resolved = resolveOptions({
      delimiter: ';',
      quote: "'",
      datetime: {
        defaultZone: '+02:30',
        columns: {
          ts: { format: 'unix-s', zone: '-0530' },
          local: { format: 'sql', zone: 'bad-zone' },
        },
      },
    });

    expect(resolved.delimiter).toBe(';'.charCodeAt(0));
    expect(resolved.quote).toBe("'".charCodeAt(0));
    expect(resolved.datetime.defaultOffsetMinutes).toBe(150);

    const ts = resolved.datetime.columns.get('ts');
    expect(ts).toEqual({ format: 'unix-s', offsetMinutes: -330 });

    const local = resolved.datetime.columns.get('local');
    expect(local).toEqual({ format: 'sql', offsetMinutes: 150 });
  });

  test('datetime parsers apply formats and offsets', () => {
    const unixSeconds = createDateTimeParser('unix-s', 0);
    expect(unixSeconds('1614861000')).toBe(1_614_861_000_000);

    const sqlUtcPlus2 = createDateTimeParser('sql', 120);
    const parsed = sqlUtcPlus2('2021-03-04 00:00:00');
    expect(parsed).toBe(Date.UTC(2021, 2, 4, 0, 0, 0) - 120 * 60 * 1000);

    const dateOnly = createDateTimeParser('date', 0);
    expect(Number.isNaN(dateOnly('2021-01-01T05:00:00'))).toBe(true);
  });
});

describe('Parse failure tracking', () => {
  test('records failures and success rate', () => {
    const tracker = createParseFailures(3);
    recordFailure(tracker, 1, 'oops');

    expect(tracker.failureCount).toBe(1);
    expect(tracker.failures.get(1)).toBe('oops');
    expect(tracker.successRate).toBeCloseTo(2 / 3);
  });
});

describe('CsvParser strategies', () => {
  test('parses unquoted buffers with unquoted path', () => {
    const parser = new CsvParser();
    const buffer = Buffer.from('a,b\n1,2\n3,4');
    const rows = parser.parse(buffer);

    expect(rows).toEqual([
      ['a', 'b'],
      ['1', '2'],
      ['3', '4'],
    ]);
  });

  test('parses quoted buffers with escaped quotes and newlines', () => {
    const parser = new CsvParser();
    const content = 'name,comment\n"Alice","Line1\nLine2"\n"Bob","He said ""Hi"""';
    const buffer = Buffer.from(content);

    const { headers, rows } = parser.parseWithHeader(buffer, true);
    expect(headers).toEqual(['name', 'comment']);
    expect(rows[0]).toEqual(['Alice', 'Line1\nLine2']);
    expect(rows[1]).toEqual(['Bob', 'He said "Hi"']);
  });
});

describe('Reader shared helpers', () => {
  test('computeLogicalRowStarts ignores newlines in quotes', () => {
    const content = 'a,"b\nc"\nd,e\n';
    const buffer = Buffer.from(content);
    const bytes = new Uint8Array(buffer);

    const starts = computeLogicalRowStarts(bytes, buffer.length, '"'.charCodeAt(0));
    const expectedSecond = content.indexOf('d,e');
    expect(starts).toEqual([0, expectedSecond]);
  });

  test('decodeQuotedField handles escapes', () => {
    const content = '"He said ""Hi""",ok';
    const buffer = Buffer.from(content);
    const bytes = new Uint8Array(buffer);
    const end = content.indexOf(',');

    expect(hasEscapedQuotes(bytes, 0, end, '"'.charCodeAt(0))).toBe(true);
    const field = decodeQuotedField(buffer, bytes, 0, end, '"'.charCodeAt(0));
    expect(field).toBe('He said "Hi"');
  });
});

describe('Node CSV reader', () => {
  const path = './tests/fixtures/node-reader.csv';
  const content = 'num,str\n1,ok\nnot-a-number,bad';

  beforeAll(async () => {
    await Bun.write(path, content);
  });

  afterAll(async () => {
    const file = Bun.file(path);
    if (await file.exists()) {
      await file.delete().catch(() => {});
    }
  });

  test('tracks parse errors and schema inference without header option', async () => {
    const result = await readCsvNode(path, {
      hasHeader: true,
      schema: { num: m.float64(), str: m.string() },
    });
    expect(result.hasErrors).toBe(true);

    const errors = result.parseErrors?.get('num');
    expect(errors?.failureCount).toBe(1);
    expect(errors?.failures.get(1)).toBe('not-a-number');
    expect(errors?.successRate).toBeCloseTo(0.5);
  });
});

describe('Hybrid quoted reader', () => {
  test('parses quoted rows with datetime and escaped strings', async () => {
    const csvContent =
      'currency,ts,comment,value\n"USD","2021-03-04 00:00:00","He said ""Hi""",1.5\n"EUR","2021-03-04 01:00:00","Plain",2.25\n';
    const buffer = Buffer.from(csvContent);
    const bytes = new Uint8Array(buffer);
    const lineStarts = computeLineStarts(buffer, buffer.length);

    const opts = resolveOptions({
      datetime: { columns: { ts: { format: 'sql', zone: 'UTC' } } },
      maxRows: 5,
    });

    const result = await readCsvWithHybridParser(buffer, bytes, lineStarts, opts, undefined, true);
    expect(result.df.shape).toEqual([2, 4]);

    const ts = result.df.col('ts');
    expect(ts.at(0)).toBe(Date.UTC(2021, 2, 4, 0, 0, 0));

    const comments = result.df.col('comment');
    expect(comments.at(0)).toBe('He said "Hi"');
  });
});

describe('Unquoted reader and writer edge cases', () => {
  const path = './tests/fixtures/unquoted-bool.csv';

  beforeAll(async () => {
    const content = 'flag,value\nTRUE,1\nfalse,0\n';
    await Bun.write(path, content);
  });

  afterAll(async () => {
    const file = Bun.file(path);
    if (await file.exists()) {
      await file.delete().catch(() => {});
    }
  });

  test('reads uppercase booleans and respects maxRows', async () => {
    const { df } = await readCsv(path, { maxRows: 1 });
    expect(df.shape[0]).toBe(1);
    const flag = df.col('flag');
    expect(flag.at(0)).toBe(true);
  });

  test('toCsv formats NaN as empty string without header', () => {
    const df = DataFrame.fromColumns({ a: [Number.NaN] });
    const csv = toCsv(df, { includeHeader: false });
    expect(csv).toBe('\n');
  });
});

describe('scanCsv parsing', () => {
  const path = './tests/fixtures/scan.csv';
  const content = 'name,"complex,header",value\nAlice,hello,1\nBob,goodbye,2\n';

  beforeAll(async () => {
    await Bun.write(path, content);
  });

  afterAll(async () => {
    const file = Bun.file(path);
    if (await file.exists()) {
      await file.delete().catch(() => {});
    }
  });

  test('handles quoted headers and produces lazy frame', async () => {
    const lf = await scanCsv(path);
    expect(lf.columns()).toEqual(['name', 'complex,header', 'value']);

    const head = await lf.head(1);
    expect(head.col('complex,header').at(0)).toBe('hello');
  });
});
