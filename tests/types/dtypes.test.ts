import { describe, expect, test } from 'bun:test';
import {
  DType,
  getDTypeSize,
  getTypedArrayConstructor,
  isNumericDType,
} from '../../src/types/dtypes';

describe('DType', () => {
  test('exports all required types', () => {
    expect(DType.Float64).toBe('float64');
    expect(DType.Int32).toBe('int32');
    expect(DType.String).toBe('string');
    expect(DType.Bool).toBe('bool');
    expect(DType.DateTime).toBe('datetime');
    expect(DType.Date).toBe('date');
  });
});

describe('getDTypeSize', () => {
  test('returns correct byte size for numeric types', () => {
    expect(getDTypeSize(DType.Float64)).toBe(8);
    expect(getDTypeSize(DType.Int32)).toBe(4);
    expect(getDTypeSize(DType.DateTime)).toBe(8); // stored as int64
    expect(getDTypeSize(DType.Date)).toBe(8); // stored as int64
  });

  test('returns 1 for bool (bitmap storage)', () => {
    expect(getDTypeSize(DType.Bool)).toBe(1);
  });

  test('returns 4 for string (dictionary index as int32)', () => {
    expect(getDTypeSize(DType.String)).toBe(4);
  });

  test('handles invalid dtype', () => {
    expect(getDTypeSize('invalid' as DType)).toBe(0);
  });
});

describe('getTypedArrayConstructor', () => {
  test('returns Float64Array for float64', () => {
    const Constructor = getTypedArrayConstructor(DType.Float64);
    const arr = new Constructor(10);
    expect(arr).toBeInstanceOf(Float64Array);
    expect(arr.length).toBe(10);
  });

  test('returns Int32Array for int32', () => {
    const Constructor = getTypedArrayConstructor(DType.Int32);
    const arr = new Constructor(10);
    expect(arr).toBeInstanceOf(Int32Array);
    expect(arr.length).toBe(10);
  });

  test('returns Int32Array for string (dictionary indices)', () => {
    const Constructor = getTypedArrayConstructor(DType.String);
    const arr = new Constructor(10);
    expect(arr).toBeInstanceOf(Int32Array);
  });

  test('returns Uint8Array for bool', () => {
    const Constructor = getTypedArrayConstructor(DType.Bool);
    const arr = new Constructor(10);
    expect(arr).toBeInstanceOf(Uint8Array);
  });

  test('returns BigInt64Array for datetime', () => {
    const Constructor = getTypedArrayConstructor(DType.DateTime);
    const arr = new Constructor(10);
    expect(arr).toBeInstanceOf(BigInt64Array);
  });

  test('returns BigInt64Array for date', () => {
    const Constructor = getTypedArrayConstructor(DType.Date);
    const arr = new Constructor(10);
    expect(arr).toBeInstanceOf(BigInt64Array);
  });

  test('throws for invalid dtype', () => {
    expect(() => getTypedArrayConstructor('invalid' as DType)).toThrow();
  });
});

describe('isNumericDType', () => {
  test('returns true for numeric types', () => {
    expect(isNumericDType(DType.Float64)).toBe(true);
    expect(isNumericDType(DType.Int32)).toBe(true);
  });

  test('returns false for non-numeric types', () => {
    expect(isNumericDType(DType.String)).toBe(false);
    expect(isNumericDType(DType.Bool)).toBe(false);
    expect(isNumericDType(DType.DateTime)).toBe(false);
    expect(isNumericDType(DType.Date)).toBe(false);
  });

  test('returns false for invalid dtype', () => {
    expect(isNumericDType('invalid' as DType)).toBe(false);
  });
});

describe('TypedArray allocation', () => {
  test('creates properly sized arrays', () => {
    const float64 = new (getTypedArrayConstructor(DType.Float64))(100);
    expect(float64.byteLength).toBe(100 * 8); // 800 bytes

    const int32 = new (getTypedArrayConstructor(DType.Int32))(100);
    expect(int32.byteLength).toBe(100 * 4); // 400 bytes

    const bool = new (getTypedArrayConstructor(DType.Bool))(100);
    expect(bool.byteLength).toBe(100); // 100 bytes
  });

  test('arrays are zero-initialized', () => {
    const arr = new (getTypedArrayConstructor(DType.Float64))(10);
    for (let i = 0; i < arr.length; i++) {
      expect(arr[i]).toBe(0);
    }
  });
});
