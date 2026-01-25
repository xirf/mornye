import { describe, expect, test } from 'bun:test';
import {
  isNullValue,
  parseBool,
  parseFloat64,
  parseInt32,
  parseValue,
} from '../../src/types/conversions';
import { DType } from '../../src/types/dtypes';

describe('isNullValue', () => {
  test('recognizes default null values', () => {
    expect(isNullValue('NA')).toBe(true);
    expect(isNullValue('null')).toBe(true);
    expect(isNullValue('-')).toBe(true);
    expect(isNullValue('')).toBe(true);
  });

  test('recognizes custom null values', () => {
    expect(isNullValue('N/A', ['N/A', 'missing'])).toBe(true);
    expect(isNullValue('missing', ['N/A', 'missing'])).toBe(true);
  });

  test('rejects non-null values', () => {
    expect(isNullValue('0')).toBe(false);
    expect(isNullValue('false')).toBe(false);
    expect(isNullValue('abc')).toBe(false);
  });

  test('is case sensitive', () => {
    expect(isNullValue('NA')).toBe(true);
    expect(isNullValue('na')).toBe(false);
    expect(isNullValue('NULL')).toBe(false);
  });
});

describe('parseFloat64', () => {
  test('parses valid floats', () => {
    const result = parseFloat64('42.5');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(42.5);
    }
  });

  test('parses integers as floats', () => {
    const result = parseFloat64('100');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(100);
    }
  });

  test('parses negative numbers', () => {
    const result = parseFloat64('-123.456');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(-123.456);
    }
  });

  test('parses scientific notation', () => {
    const result = parseFloat64('1.23e10');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(1.23e10);
    }
  });

  test('returns null for null values', () => {
    expect(parseFloat64('NA').ok).toBe(true);
    expect(parseFloat64('null').ok).toBe(true);
    if (parseFloat64('NA').ok) {
      expect(parseFloat64('NA').data).toBe(null);
    }
  });

  test('rejects invalid floats', () => {
    const result = parseFloat64('not-a-number');
    expect(result.ok).toBe(false);
  });

  test('rejects empty strings (unless in null list)', () => {
    const result = parseFloat64('', ['NA']);
    expect(result.ok).toBe(false);
  });
});

describe('parseInt32', () => {
  test('parses valid integers', () => {
    const result = parseInt32('42');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(42);
    }
  });

  test('parses negative integers', () => {
    const result = parseInt32('-100');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(-100);
    }
  });

  test('parses zero', () => {
    const result = parseInt32('0');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(0);
    }
  });

  test('returns null for null values', () => {
    const result = parseInt32('NA');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(null);
    }
  });

  test('rejects floats', () => {
    const result = parseInt32('42.5');
    expect(result.ok).toBe(false);
  });

  test('rejects invalid integers', () => {
    const result = parseInt32('not-a-number');
    expect(result.ok).toBe(false);
  });

  test('rejects numbers outside Int32 range', () => {
    const result = parseInt32('9999999999999');
    expect(result.ok).toBe(false);
  });
});

describe('parseBool', () => {
  test('parses true values', () => {
    expect(parseBool('true').data).toBe(1);
    expect(parseBool('True').data).toBe(1);
    expect(parseBool('TRUE').data).toBe(1);
    expect(parseBool('1').data).toBe(1);
  });

  test('parses false values', () => {
    expect(parseBool('false').data).toBe(0);
    expect(parseBool('False').data).toBe(0);
    expect(parseBool('FALSE').data).toBe(0);
    expect(parseBool('0').data).toBe(0);
  });

  test('returns null for null values', () => {
    const result = parseBool('NA');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(null);
    }
  });

  test('rejects invalid bool strings', () => {
    expect(parseBool('yes').ok).toBe(false);
    expect(parseBool('no').ok).toBe(false);
    expect(parseBool('2').ok).toBe(false);
  });
});

describe('parseValue', () => {
  test('parses Float64 values', () => {
    const result = parseValue('42.5', DType.Float64);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(42.5);
    }
  });

  test('parses Int32 values', () => {
    const result = parseValue('100', DType.Int32);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(100);
    }
  });

  test('parses Bool values', () => {
    const result = parseValue('true', DType.Bool);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(1);
    }
  });

  test('returns string ID for String dtype', () => {
    // String values return the raw string (to be interned later)
    const result = parseValue('hello', DType.String);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe('hello');
    }
  });

  test('parses DateTime values', () => {
    const result = parseValue('2024-01-25T10:30:00.000Z', DType.DateTime);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(typeof result.data).toBe('bigint');
    }
  });

  test('parses Date values', () => {
    const result = parseValue('2024-01-25', DType.Date);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(typeof result.data).toBe('bigint');
    }
  });

  test('handles null values for all types', () => {
    expect(parseValue('NA', DType.Float64).data).toBe(null);
    expect(parseValue('null', DType.Int32).data).toBe(null);
    expect(parseValue('-', DType.Bool).data).toBe(null);
    expect(parseValue('', DType.String).data).toBe(null);
  });

  test('propagates type-specific errors', () => {
    expect(parseValue('invalid', DType.Float64).ok).toBe(false);
    expect(parseValue('42.5', DType.Int32).ok).toBe(false);
    expect(parseValue('maybe', DType.Bool).ok).toBe(false);
  });
});

describe('Edge cases', () => {
  test('handles whitespace-only strings', () => {
    expect(parseFloat64('   ').ok).toBe(false);
    expect(parseInt32('   ').ok).toBe(false);
  });

  test('handles very large floats', () => {
    const result = parseFloat64('1.7976931348623157e+308');
    expect(result.ok).toBe(true);
  });

  test('handles very small floats', () => {
    const result = parseFloat64('5e-324');
    expect(result.ok).toBe(true);
  });

  test('handles Int32 boundaries', () => {
    expect(parseInt32('2147483647').ok).toBe(true); // Max Int32
    expect(parseInt32('-2147483648').ok).toBe(true); // Min Int32
    expect(parseInt32('2147483648').ok).toBe(false); // Overflow
  });

  test('custom null values work across all types', () => {
    const nullValues = ['MISSING', 'N/A'];
    expect(parseFloat64('MISSING', nullValues).data).toBe(null);
    expect(parseInt32('N/A', nullValues).data).toBe(null);
    expect(parseBool('MISSING', nullValues).data).toBe(null);
  });
});
