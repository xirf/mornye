import { describe, expect, test } from 'bun:test';
import { getColumnValue } from '../../src/core/column';
import { getColumnNames, getRowCount } from '../../src/dataframe/dataframe';
import { type CsvOptions, readCsvFromString } from '../../src/io/csv-reader';
import { DType } from '../../src/types/dtypes';

describe('readCsv', () => {
  test('reads simple CSV file', async () => {
    const csv = `name,age,city
Alice,30,NYC
Bob,25,LA`;

    const schema = {
      name: DType.String,
      age: DType.Int32,
      city: DType.String,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);
    expect(getColumnNames(df)).toEqual(['name', 'age', 'city']);
  });

  test('parses numeric values correctly', async () => {
    const csv = `price,quantity
10.5,100
20.75,200`;

    const schema = {
      price: DType.Float64,
      quantity: DType.Int32,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const priceCol = df.columns.get('price');
    const qtyCol = df.columns.get('quantity');

    expect(getColumnValue(priceCol!, 0)).toBe(10.5);
    expect(getColumnValue(qtyCol!, 0)).toBe(100);
  });

  test('handles quoted fields', async () => {
    const csv = `name,description
Product A,"High quality, top rated"
Product B,"Budget option"`;

    const schema = {
      name: DType.String,
      description: DType.String,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);
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

    const result = await readCsvFromString(csv, { schema, delimiter: '|' });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);
  });

  test('handles null values', async () => {
    const csv = `name,age,city
Alice,30,NYC
Bob,NA,LA
Charlie,-,SF`;

    const schema = {
      name: DType.String,
      age: DType.Int32,
      city: DType.String,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const ageCol = df.columns.get('age');

    expect(getColumnValue(ageCol!, 0)).toBe(30);
    // TODO: When null bitmaps are added to Column, these should be null
    expect(getColumnValue(ageCol!, 1)).toBe(0); // "NA" -> 0 for now
    expect(getColumnValue(ageCol!, 2)).toBe(0); // "-" -> 0 for now
  });

  test('handles custom null values', async () => {
    const csv = `name,age
Alice,30
Bob,MISSING`;

    const schema = {
      name: DType.String,
      age: DType.Int32,
    };

    const result = await readCsvFromString(csv, { schema, nullValues: ['MISSING'] });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const ageCol = df.columns.get('age');
    // TODO: When null bitmaps are added, this should be null
    expect(getColumnValue(ageCol!, 1)).toBe(0); // "MISSING" -> 0 for now
  });

  test('handles boolean values', async () => {
    const csv = `name,active
Alice,true
Bob,false
Charlie,1`;

    const schema = {
      name: DType.String,
      active: DType.Bool,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const activeCol = df.columns.get('active');

    expect(getColumnValue(activeCol!, 0)).toBe(1);
    expect(getColumnValue(activeCol!, 1)).toBe(0);
    expect(getColumnValue(activeCol!, 2)).toBe(1);
  });

  test('handles datetime values', async () => {
    const csv = `timestamp,value
2024-01-25T10:30:00.000Z,100
2024-01-25T11:30:00.000Z,200`;

    const schema = {
      timestamp: DType.DateTime,
      value: DType.Int32,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const tsCol = df.columns.get('timestamp');

    expect(typeof getColumnValue(tsCol!, 0)).toBe('bigint');
  });

  test('handles date values', async () => {
    const csv = `date,value
2024-01-25,100
2024-01-26,200`;

    const schema = {
      date: DType.Date,
      value: DType.Int32,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);
  });

  test('rejects CSV without header when hasHeader=true', async () => {
    const csv = `Alice,30,NYC
Bob,25,LA`;

    const schema = {
      name: DType.String,
      age: DType.Int32,
      city: DType.String,
    };

    const result = await readCsvFromString(csv, { schema, hasHeader: true });
    expect(result.ok).toBe(false);
  });

  test('handles CSV without header when hasHeader=false', async () => {
    const csv = `Alice,30,NYC
Bob,25,LA`;

    const schema = {
      col0: DType.String,
      col1: DType.Int32,
      col2: DType.String,
    };

    const result = await readCsvFromString(csv, { schema, hasHeader: false });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);
    expect(getColumnNames(df)).toEqual(['col0', 'col1', 'col2']);
  });

  test('rejects mismatched schema columns', async () => {
    const csv = `name,age,city
Alice,30,NYC`;

    const schema = {
      name: DType.String,
      age: DType.Int32,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(false);
  });

  test('rejects invalid type conversions', async () => {
    const csv = `name,age
Alice,not-a-number`;

    const schema = {
      name: DType.String,
      age: DType.Int32,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(false);
  });

  test('handles empty CSV', async () => {
    const csv = 'name,age,city';

    const schema = {
      name: DType.String,
      age: DType.Int32,
      city: DType.String,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(0);
  });

  test('handles large CSV efficiently', async () => {
    const rows = 1000;
    const lines = ['name,age,city'];
    for (let i = 0; i < rows; i++) {
      lines.push(`Person${i},${20 + (i % 50)},City${i % 10}`);
    }
    const csv = lines.join('\n');

    const schema = {
      name: DType.String,
      age: DType.Int32,
      city: DType.String,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(rows);
  });

  test('handles string interning', async () => {
    const csv = `category,value
A,1
B,2
A,3
B,4
A,5`;

    const schema = {
      category: DType.String,
      value: DType.Int32,
    };

    const result = await readCsvFromString(csv, { schema });
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(5);
    // Dictionary should only have 2 unique strings
    expect(df.dictionary).toBeDefined();
    if (df.dictionary) {
      expect(df.dictionary.stringToId.size).toBe(2);
    }
  });
});
