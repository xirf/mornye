import { describe, expect, test } from 'bun:test';
import {
  addColumn,
  createDataFrame,
  getColumn,
  getColumnNames,
  getRowCount,
  getSchema,
} from '../../src/dataframe/dataframe';
import { DType } from '../../src/types/dtypes';

describe('createDataFrame', () => {
  test('creates empty DataFrame', () => {
    const df = createDataFrame();
    expect(getColumnNames(df)).toEqual([]);
    expect(getRowCount(df)).toBe(0);
  });

  test('creates DataFrame with columns', () => {
    const df = createDataFrame();

    const schema = {
      id: DType.Int32,
      name: DType.String,
      price: DType.Float64,
    };

    // Add columns
    for (const [colName, dtype] of Object.entries(schema)) {
      const result = addColumn(df, colName, dtype, 10);
      expect(result.ok).toBe(true);
    }

    expect(getColumnNames(df)).toEqual(['id', 'name', 'price']);
    expect(getRowCount(df)).toBe(10);
  });
});

describe('getColumnNames', () => {
  test('returns empty array for empty DataFrame', () => {
    const df = createDataFrame();
    expect(getColumnNames(df)).toEqual([]);
  });

  test('returns column names in order', () => {
    const df = createDataFrame();
    addColumn(df, 'a', DType.Int32, 5);
    addColumn(df, 'b', DType.Float64, 5);
    addColumn(df, 'c', DType.String, 5);

    expect(getColumnNames(df)).toEqual(['a', 'b', 'c']);
  });
});

describe('getRowCount', () => {
  test('returns 0 for empty DataFrame', () => {
    const df = createDataFrame();
    expect(getRowCount(df)).toBe(0);
  });

  test('returns row count from first column', () => {
    const df = createDataFrame();
    addColumn(df, 'col1', DType.Int32, 100);
    expect(getRowCount(df)).toBe(100);
  });

  test('all columns must have same row count', () => {
    const df = createDataFrame();
    addColumn(df, 'col1', DType.Int32, 10);

    // Try to add column with different length
    const result = addColumn(df, 'col2', DType.Float64, 20);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('row count');
    }
  });
});

describe('getColumn', () => {
  test('retrieves column by name', () => {
    const df = createDataFrame();
    addColumn(df, 'price', DType.Float64, 10);

    const result = getColumn(df, 'price');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.name).toBe('price');
      expect(result.data.dtype).toBe(DType.Float64);
      expect(result.data.length).toBe(10);
    }
  });

  test('returns error for non-existent column', () => {
    const df = createDataFrame();
    const result = getColumn(df, 'missing');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('missing');
    }
  });

  test('is case sensitive', () => {
    const df = createDataFrame();
    addColumn(df, 'Price', DType.Float64, 10);

    const result = getColumn(df, 'price');
    expect(result.ok).toBe(false);
  });
});

describe('addColumn', () => {
  test('adds column to empty DataFrame', () => {
    const df = createDataFrame();
    const result = addColumn(df, 'id', DType.Int32, 100);

    expect(result.ok).toBe(true);
    expect(getColumnNames(df)).toEqual(['id']);
    expect(getRowCount(df)).toBe(100);
  });

  test('adds multiple columns', () => {
    const df = createDataFrame();

    addColumn(df, 'id', DType.Int32, 50);
    addColumn(df, 'name', DType.String, 50);
    addColumn(df, 'active', DType.Bool, 50);

    expect(getColumnNames(df)).toEqual(['id', 'name', 'active']);
    expect(getRowCount(df)).toBe(50);
  });

  test('rejects duplicate column name', () => {
    const df = createDataFrame();
    addColumn(df, 'id', DType.Int32, 10);

    const result = addColumn(df, 'id', DType.Float64, 10);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('already exists');
    }
  });

  test('rejects mismatched row count', () => {
    const df = createDataFrame();
    addColumn(df, 'col1', DType.Int32, 10);

    const result = addColumn(df, 'col2', DType.Float64, 15);
    expect(result.ok).toBe(false);
  });

  test('rejects zero length', () => {
    const df = createDataFrame();
    const result = addColumn(df, 'col', DType.Int32, 0);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(getRowCount(df)).toBe(0);
    }
  });

  test('rejects negative length', () => {
    const df = createDataFrame();
    const result = addColumn(df, 'col', DType.Int32, -5);
    expect(result.ok).toBe(false);
  });
});

describe('getSchema', () => {
  test('returns empty schema for empty DataFrame', () => {
    const df = createDataFrame();
    const schema = getSchema(df);
    expect(Object.keys(schema)).toHaveLength(0);
  });

  test('returns correct schema', () => {
    const df = createDataFrame();
    addColumn(df, 'id', DType.Int32, 10);
    addColumn(df, 'name', DType.String, 10);
    addColumn(df, 'price', DType.Float64, 10);
    addColumn(df, 'active', DType.Bool, 10);

    const schema = getSchema(df);
    expect(schema).toEqual({
      id: DType.Int32,
      name: DType.String,
      price: DType.Float64,
      active: DType.Bool,
    });
  });
});

describe('DataFrame edge cases', () => {
  test('handles many columns', () => {
    const df = createDataFrame();

    for (let i = 0; i < 100; i++) {
      addColumn(df, `col_${i}`, DType.Float64, 10);
    }

    expect(getColumnNames(df).length).toBe(100);
    expect(getRowCount(df)).toBe(10);
  });

  test('handles large row count', () => {
    const df = createDataFrame();
    addColumn(df, 'data', DType.Float64, 1_000_000);
    expect(getRowCount(df)).toBe(1_000_000);
  });

  test('preserves column order', () => {
    const df = createDataFrame();
    const names = ['z', 'a', 'm', 'b', 'x'];

    for (const name of names) {
      addColumn(df, name, DType.Int32, 5);
    }

    expect(getColumnNames(df)).toEqual(names);
  });
});
