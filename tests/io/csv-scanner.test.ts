import { describe, expect, test } from 'bun:test';
import { getColumnValue } from '../../src/core/column';
import { getColumnNames, getRowCount } from '../../src/dataframe/dataframe';
import { type CsvScanOptions, scanCsvFromString } from '../../src/io/csv-scanner';
import { DType } from '../../src/types/dtypes';

describe('scanCsv', () => {
  test('scans simple CSV file', async () => {
    const csv = `name,age,city
Alice,30,NYC
Bob,25,LA`;

    const schema = {
      name: DType.String,
      age: DType.Int32,
      city: DType.String,
    };

    const result = await scanCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);
    expect(getColumnNames(df)).toEqual(['name', 'age', 'city']);
  });

  test('handles custom chunk size', async () => {
    const lines = ['name,age'];
    for (let i = 0; i < 100; i++) {
      lines.push(`Person${i},${20 + i}`);
    }
    const csv = lines.join('\n');

    const schema = {
      name: DType.String,
      age: DType.Int32,
    };

    const result = await scanCsvFromString(csv, { schema, chunkSize: 20 });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(100);
  });

  test('handles large CSV efficiently', async () => {
    const rows = 5000;
    const lines = ['id,value,category'];
    for (let i = 0; i < rows; i++) {
      lines.push(`${i},${i * 10},cat${i % 10}`);
    }
    const csv = lines.join('\n');

    const schema = {
      id: DType.Int32,
      value: DType.Int32,
      category: DType.String,
    };

    const result = await scanCsvFromString(csv, { schema, chunkSize: 1000 });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(rows);
  });

  test('processes all chunks correctly', async () => {
    const csv = `value
1
2
3
4
5`;

    const schema = {
      value: DType.Int32,
    };

    const result = await scanCsvFromString(csv, { schema, chunkSize: 2 });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const col = df.columns.get('value');
    expect(getColumnValue(col!, 0)).toBe(1);
    expect(getColumnValue(col!, 4)).toBe(5);
  });

  test('handles quoted fields in chunks', async () => {
    const csv = `name,description
"Product A","High quality, top rated"
"Product B","Budget option"
"Product C","Premium choice"`;

    const schema = {
      name: DType.String,
      description: DType.String,
    };

    const result = await scanCsvFromString(csv, { schema, chunkSize: 1 });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(3);
  });

  test('handles custom delimiter', async () => {
    const csv = `name|age|city
Alice|30|NYC
Bob|25|LA`;

    const schema = {
      name: DType.String,
      age: DType.Int32,
      city: DType.String,
    };

    const result = await scanCsvFromString(csv, { schema, delimiter: '|' });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);
  });

  test('handles null values across chunks', async () => {
    const csv = `value
1
NA
3
null
5`;

    const schema = {
      value: DType.Int32,
    };

    const result = await scanCsvFromString(csv, { schema, chunkSize: 2 });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const col = df.columns.get('value');
    expect(getColumnValue(col!, 0)).toBe(1);
    expect(getColumnValue(col!, 1)).toBe(0); // NA -> 0 for now
    expect(getColumnValue(col!, 3)).toBe(0); // null -> 0 for now
  });

  test('handles empty CSV', async () => {
    const csv = 'name,age';

    const schema = {
      name: DType.String,
      age: DType.Int32,
    };

    const result = await scanCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(0);
  });

  test('rejects invalid data in any chunk', async () => {
    const csv = `value
1
2
invalid
4`;

    const schema = {
      value: DType.Int32,
    };

    const result = await scanCsvFromString(csv, { schema, chunkSize: 2 });
    expect(result.ok).toBe(false);
  });

  test('handles hasHeader=false', async () => {
    const csv = `Alice,30
Bob,25`;

    const schema = {
      col0: DType.String,
      col1: DType.Int32,
    };

    const result = await scanCsvFromString(csv, { schema, hasHeader: false, chunkSize: 1 });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);
  });

  test('maintains data integrity across chunks', async () => {
    const rows = 100;
    const lines = ['value'];
    for (let i = 0; i < rows; i++) {
      lines.push(String(i));
    }
    const csv = lines.join('\n');

    const schema = {
      value: DType.Int32,
    };

    const result = await scanCsvFromString(csv, { schema, chunkSize: 7 });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const col = df.columns.get('value');

    // Verify all values are correct
    for (let i = 0; i < rows; i++) {
      expect(getColumnValue(col!, i)).toBe(i);
    }
  });

  test('handles string interning across chunks', async () => {
    const csv = `category
A
B
A
B
A`;

    const schema = {
      category: DType.String,
    };

    const result = await scanCsvFromString(csv, { schema, chunkSize: 2 });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(df.dictionary).toBeDefined();
    if (df.dictionary) {
      expect(df.dictionary.stringToId.size).toBe(2);
    }
  });
});
