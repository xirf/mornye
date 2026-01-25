import { describe, expect, test } from 'bun:test';
import { setColumnValue } from '../../src/core/column';
import { addColumn, createDataFrame } from '../../src/dataframe/dataframe';
import { formatDataFrame, formatValue } from '../../src/dataframe/print';
import { createDictionary, internString } from '../../src/memory/dictionary';
import { DType } from '../../src/types/dtypes';

describe('formatValue', () => {
  test('formats Float64 values', () => {
    expect(formatValue(DType.Float64, 42.5)).toBe('42.5');
    expect(formatValue(DType.Float64, 0)).toBe('0');
    expect(formatValue(DType.Float64, -123.456)).toBe('-123.456');
  });

  test('formats Int32 values', () => {
    expect(formatValue(DType.Int32, 42)).toBe('42');
    expect(formatValue(DType.Int32, 0)).toBe('0');
    expect(formatValue(DType.Int32, -100)).toBe('-100');
  });

  test('formats Bool values', () => {
    expect(formatValue(DType.Bool, 1)).toBe('true');
    expect(formatValue(DType.Bool, 0)).toBe('false');
  });

  test('formats String values (dictionary IDs)', () => {
    const dict = createDictionary();
    const id = internString(dict, 'hello');

    expect(formatValue(DType.String, id, dict)).toBe('hello');
  });

  test('handles missing string in dictionary', () => {
    const dict = createDictionary();
    expect(formatValue(DType.String, 999, dict)).toBe('<unknown>');
  });

  test('handles string without dictionary', () => {
    expect(formatValue(DType.String, 5)).toBe('<id:5>');
  });

  test('formats DateTime values', () => {
    const timestamp = 1706140800000n; // 2024-01-25 00:00:00 UTC
    const formatted = formatValue(DType.DateTime, timestamp);
    expect(formatted).toContain('2024');
  });

  test('formats Date values', () => {
    const timestamp = 1706140800000n;
    const formatted = formatValue(DType.Date, timestamp);
    expect(formatted).toContain('2024');
  });

  test('handles null/undefined', () => {
    expect(formatValue(DType.Float64, null)).toBe('null');
    expect(formatValue(DType.Float64, undefined)).toBe('null');
  });
});

describe('formatDataFrame', () => {
  test('formats empty DataFrame', () => {
    const df = createDataFrame();
    const output = formatDataFrame(df);
    expect(output).toContain('0 rows');
    expect(output).toContain('0 columns');
  });

  test('formats DataFrame with data', () => {
    const df = createDataFrame();
    addColumn(df, 'id', DType.Int32, 3);
    addColumn(df, 'price', DType.Float64, 3);

    const result1 = df.columns.get('id');
    const result2 = df.columns.get('price');

    if (result1 && result2) {
      setColumnValue(result1, 0, 1);
      setColumnValue(result1, 1, 2);
      setColumnValue(result1, 2, 3);

      setColumnValue(result2, 0, 10.5);
      setColumnValue(result2, 1, 20.75);
      setColumnValue(result2, 2, 30.25);
    }

    const output = formatDataFrame(df);

    expect(output).toContain('3 rows');
    expect(output).toContain('2 columns');
    expect(output).toContain('id');
    expect(output).toContain('price');
    expect(output).toContain('1');
    expect(output).toContain('10.5');
  });

  test('truncates large DataFrames', () => {
    const df = createDataFrame();
    addColumn(df, 'data', DType.Int32, 100);

    const col = df.columns.get('data');
    if (col) {
      for (let i = 0; i < 100; i++) {
        setColumnValue(col, i, i);
      }
    }

    const output = formatDataFrame(df, { maxRows: 5 });
    expect(output).toContain('100 rows');
    expect(output).toContain('...'); // Truncation indicator
  });

  test('respects maxRows option', () => {
    const df = createDataFrame();
    addColumn(df, 'num', DType.Int32, 20);

    const output = formatDataFrame(df, { maxRows: 3 });
    const lines = output.split('\n');
    // Should show header + 3 rows + truncation message
    const dataLines = lines.filter(
      (l) => l.trim() && !l.includes('rows') && !l.includes('columns'),
    );
    expect(dataLines.length).toBeLessThanOrEqual(5);
  });

  test('handles all data types', () => {
    const df = createDataFrame();
    addColumn(df, 'int', DType.Int32, 2);
    addColumn(df, 'float', DType.Float64, 2);
    addColumn(df, 'bool', DType.Bool, 2);
    addColumn(df, 'str', DType.String, 2);

    const output = formatDataFrame(df);
    expect(output).toContain('int');
    expect(output).toContain('float');
    expect(output).toContain('bool');
    expect(output).toContain('str');
  });

  test('aligns columns properly', () => {
    const df = createDataFrame();
    addColumn(df, 'a', DType.Int32, 2);
    addColumn(df, 'verylongname', DType.Int32, 2);

    const output = formatDataFrame(df);
    const lines = output.split('\n');

    // Find header line
    const headerLine = lines.find((l) => l.includes('a') && l.includes('verylongname'));
    expect(headerLine).toBeDefined();
  });
});

describe('Print options', () => {
  test('custom maxRows limits output', () => {
    const df = createDataFrame();
    addColumn(df, 'x', DType.Int32, 50);

    const output10 = formatDataFrame(df, { maxRows: 10 });
    const output5 = formatDataFrame(df, { maxRows: 5 });

    expect(output10.length).toBeGreaterThan(output5.length);
  });

  test('maxRows=0 shows only header', () => {
    const df = createDataFrame();
    addColumn(df, 'x', DType.Int32, 10);

    const output = formatDataFrame(df, { maxRows: 0 });
    expect(output).toContain('10 rows');
    expect(output).toContain('x');
  });
});
