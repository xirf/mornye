import { describe, expect, test } from 'bun:test';
import { type Schema, getColumnDType, validateSchema } from '../../src/core/schema';
import { DType } from '../../src/types/dtypes';

describe('Schema', () => {
  test('creates valid schema object', () => {
    const schema: Schema = {
      timestamp: DType.DateTime,
      price: DType.Float64,
      volume: DType.Int32,
      symbol: DType.String,
      active: DType.Bool,
    };

    expect(Object.keys(schema)).toHaveLength(5);
    expect(schema.timestamp).toBe(DType.DateTime);
    expect(schema.price).toBe(DType.Float64);
  });

  test('empty schema is valid', () => {
    const schema: Schema = {};
    expect(Object.keys(schema)).toHaveLength(0);
  });
});

describe('validateSchema', () => {
  test('validates correct schema', () => {
    const schema: Schema = {
      id: DType.Int32,
      name: DType.String,
      price: DType.Float64,
    };

    const result = validateSchema(schema);
    expect(result.ok).toBe(true);
  });

  test('rejects empty schema', () => {
    const schema: Schema = {};
    const result = validateSchema(schema);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('empty');
    }
  });

  test('rejects invalid column names', () => {
    const schema: Schema = {
      '': DType.String,
    };
    const result = validateSchema(schema);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('empty column name');
    }
  });

  test('rejects invalid dtypes', () => {
    const schema = {
      name: 'invalid_type',
    } as unknown as Schema;

    const result = validateSchema(schema);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('invalid dtype');
    }
  });

  test('rejects duplicate column names (case insensitive)', () => {
    const schema: Schema = {
      Name: DType.String,
      name: DType.String,
    };
    const result = validateSchema(schema);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('duplicate');
    }
  });

  test('accepts valid column names with special characters', () => {
    const schema: Schema = {
      user_id: DType.Int32,
      'first-name': DType.String,
      'price.usd': DType.Float64,
    };
    const result = validateSchema(schema);
    expect(result.ok).toBe(true);
  });
});

describe('getColumnDType', () => {
  const schema: Schema = {
    id: DType.Int32,
    name: DType.String,
    price: DType.Float64,
  };

  test('returns dtype for existing column', () => {
    const result = getColumnDType(schema, 'name');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(DType.String);
    }
  });

  test('returns error for missing column', () => {
    const result = getColumnDType(schema, 'nonexistent');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('nonexistent');
    }
  });

  test('is case sensitive', () => {
    const result = getColumnDType(schema, 'Name');
    expect(result.ok).toBe(false);
  });

  test('works with all dtypes', () => {
    const fullSchema: Schema = {
      f64: DType.Float64,
      i32: DType.Int32,
      str: DType.String,
      bool: DType.Bool,
      dt: DType.DateTime,
      date: DType.Date,
    };

    expect(getColumnDType(fullSchema, 'f64').ok).toBe(true);
    expect(getColumnDType(fullSchema, 'i32').ok).toBe(true);
    expect(getColumnDType(fullSchema, 'str').ok).toBe(true);
    expect(getColumnDType(fullSchema, 'bool').ok).toBe(true);
    expect(getColumnDType(fullSchema, 'dt').ok).toBe(true);
    expect(getColumnDType(fullSchema, 'date').ok).toBe(true);
  });
});

describe('Schema edge cases', () => {
  test('handles many columns', () => {
    const schema: Schema = {};
    for (let i = 0; i < 1000; i++) {
      schema[`col_${i}`] = DType.Float64;
    }

    const result = validateSchema(schema);
    expect(result.ok).toBe(true);
    expect(Object.keys(schema)).toHaveLength(1000);
  });

  test('handles long column names', () => {
    const longName = 'a'.repeat(1000);
    const schema: Schema = {
      [longName]: DType.String,
    };

    const result = validateSchema(schema);
    expect(result.ok).toBe(true);
    const dtypeResult = getColumnDType(schema, longName);
    expect(dtypeResult.ok).toBe(true);
  });
});
