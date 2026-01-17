import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { m } from '../../src/core/types';
import { readCsv } from '../../src/io/csv';

describe('CSV Reader', () => {
  const testCsvPath = './tests/fixtures/test.csv';
  const testCsvContent = `name,age,score,active
Alice,25,95.5,true
Bob,30,87.2,false
Carol,22,91.8,true
`;

  beforeAll(async () => {
    // Create test fixture
    await Bun.write(testCsvPath, testCsvContent);
  });

  afterAll(async () => {
    // Cleanup
    const file = Bun.file(testCsvPath);
    if (await file.exists()) {
      await file.delete().catch(() => {});
    }
  });

  describe('basic parsing', () => {
    test('reads CSV with auto-inferred types', async () => {
      const { df } = await readCsv(testCsvPath);

      expect(df.shape).toEqual([3, 4]);
      expect(df.columns()).toEqual(['name', 'age', 'score', 'active']);
    });

    test('infers string type for names', async () => {
      const { df } = await readCsv(testCsvPath);
      const names = df.col('name');

      expect(names.dtype.kind).toBe('string');
      expect([...names]).toEqual(['Alice', 'Bob', 'Carol']);
    });

    test('infers float64 type for integers', async () => {
      const { df } = await readCsv(testCsvPath);
      const ages = df.col('age');

      // All numeric columns default to float64 for consistency
      expect(ages.dtype.kind).toBe('float64');
      expect([...ages]).toEqual([25, 30, 22]);
    });

    test('infers float64 type for decimals', async () => {
      const { df } = await readCsv(testCsvPath);
      const scores = df.col('score');

      expect(scores.dtype.kind).toBe('float64');
      expect([...scores]).toEqual([95.5, 87.2, 91.8]);
    });

    test('infers bool type for true/false', async () => {
      const { df } = await readCsv(testCsvPath);
      const active = df.col('active');

      expect(active.dtype.kind).toBe('bool');
      expect([...active]).toEqual([true, false, true]);
    });
  });

  describe('explicit schema', () => {
    test('uses provided schema', async () => {
      const schema = {
        name: m.string(),
        age: m.float64(), // Force float instead of int
        score: m.float64(),
        active: m.bool(),
      } as const;

      const { df } = await readCsv(testCsvPath, { schema });
      const ages = df.col('age');

      expect(ages.dtype.kind).toBe('float64');
    });
  });

  describe('options', () => {
    test('respects maxRows', async () => {
      const { df } = await readCsv(testCsvPath, { maxRows: 2 });
      expect(df.shape[0]).toBe(2);
    });
  });
});

describe('CSV Parser Edge Cases', () => {
  const quotedCsvPath = './tests/fixtures/quoted.csv';
  const quotedContent = `name,description
"Alice","Hello, World"
"Bob","He said ""Hi"""
"Carol","Line1
Line2"
`;

  beforeAll(async () => {
    await Bun.write(quotedCsvPath, quotedContent);
  });

  afterAll(async () => {
    const file = Bun.file(quotedCsvPath);
    if (await file.exists()) {
      await file.delete().catch(() => {});
    }
  });

  test('handles quoted fields with commas', async () => {
    const { df } = await readCsv(quotedCsvPath);
    const descriptions = df.col('description');

    expect(descriptions.at(0)).toBe('Hello, World');
  });

  test('handles escaped quotes', async () => {
    const { df } = await readCsv(quotedCsvPath);
    const descriptions = df.col('description');

    expect(descriptions.at(1)).toBe('He said "Hi"');
  });

  test('handles newlines in quoted fields', async () => {
    const { df } = await readCsv(quotedCsvPath);
    const descriptions = df.col('description');

    expect(descriptions.at(2)).toBe('Line1\nLine2');
  });
});

describe('CSV datetime parsing', () => {
  const datetimeCsvPath = './tests/fixtures/datetime.csv';
  const datetimeContent = `id,ts_local,ts_iso,ts_unix_s,ts_unix_ms
1,2021-03-04 12:30:00,2021-03-04T12:30:00Z,1614861000,1614861000000
2,2021-03-04 00:00:00,2021-03-04T00:00:00+02:00,1614816000,1614816000000
`;

  beforeAll(async () => {
    await Bun.write(datetimeCsvPath, datetimeContent);
  });

  afterAll(async () => {
    const file = Bun.file(datetimeCsvPath);
    if (await file.exists()) {
      await file.delete().catch(() => {});
    }
  });

  test('parses multiple datetime formats to epoch ms', async () => {
    const { df } = await readCsv(datetimeCsvPath, {
      datetime: {
        defaultZone: 'UTC',
        columns: {
          ts_local: { format: 'sql', zone: 'UTC' },
          ts_iso: { format: 'iso' },
          ts_unix_s: { format: 'unix-s' },
          ts_unix_ms: { format: 'unix-ms' },
        },
      },
    });

    const tsLocal = df.col('ts_local');
    const tsIso = df.col('ts_iso');
    const tsUnixS = df.col('ts_unix_s');
    const tsUnixMs = df.col('ts_unix_ms');

    expect(tsLocal.dtype.kind).toBe('float64');
    expect(tsIso.dtype.kind).toBe('float64');

    expect(tsLocal.at(0)).toBe(Date.UTC(2021, 2, 4, 12, 30, 0));
    expect(tsIso.at(0)).toBe(Date.UTC(2021, 2, 4, 12, 30, 0));

    // Second row ISO has +02:00 offset → UTC should be 22:00 previous day
    expect(tsIso.at(1)).toBe(Date.UTC(2021, 2, 3, 22, 0, 0));

    expect(tsUnixS.at(0)).toBe(1614861000000);
    expect(tsUnixMs.at(0)).toBe(1614861000000);
  });
});

describe('Lazy string storage edge cases', () => {
  const lazyCsvPath = './tests/fixtures/lazy-edge.csv';
  const lazyContent = `name,comment
foo,"Ends with escaped quote """""
bar,"multibyte 😀 € and ""quote"" inside"
`;

  beforeAll(async () => {
    await Bun.write(lazyCsvPath, lazyContent);
  });

  afterAll(async () => {
    const file = Bun.file(lazyCsvPath);
    if (await file.exists()) {
      await file.delete().catch(() => {});
    }
  });

  test('handles escaped quotes at end-of-line', async () => {
    const { df } = await readCsv(lazyCsvPath);
    const comments = df.col('comment');

    expect(comments.at(0)).toBe('Ends with escaped quote ""');
  });

  test('handles multibyte UTF-8 with escapes', async () => {
    const { df } = await readCsv(lazyCsvPath);
    const comments = df.col('comment');

    expect(comments.at(1)).toBe('multibyte 😀 € and "quote" inside');
  });
});
