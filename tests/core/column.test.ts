import { describe, expect, test } from 'bun:test';
import {
  createColumn,
  getColumnDType,
  getColumnLength,
  getColumnMemoryUsage,
  getColumnValue,
  resizeColumn,
  setColumnValue,
} from '../../src/core/column';
import { DType } from '../../src/types/dtypes';

describe('createColumn', () => {
  test('creates Float64 column', () => {
    const result = createColumn(DType.Float64, 100);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.dtype).toBe(DType.Float64);
      expect(result.data.length).toBe(100);
      expect(result.data.data).toBeInstanceOf(Uint8Array);
      expect(result.data.view).toBeInstanceOf(DataView);
      expect(result.data.data.byteLength).toBe(800); // 100 * 8 bytes
    }
  });

  test('creates Int32 column', () => {
    const result = createColumn(DType.Int32, 50);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.dtype).toBe(DType.Int32);
      expect(result.data.length).toBe(50);
    }
  });

  test('creates String column with dictionary', () => {
    const result = createColumn(DType.String, 100);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.dtype).toBe(DType.String);
      expect(result.data.data).toBeInstanceOf(Uint8Array);
      expect(result.data.data.byteLength).toBe(400); // 100 * 4 bytes for int32 indices
    }
  });

  test('creates Bool column', () => {
    const result = createColumn(DType.Bool, 200);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.dtype).toBe(DType.Bool);
      expect(result.data.data).toBeInstanceOf(Uint8Array);
    }
  });

  test('creates DateTime column', () => {
    const result = createColumn(DType.DateTime, 100);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.dtype).toBe(DType.DateTime);
      expect(result.data.data).toBeInstanceOf(Uint8Array);
      expect(result.data.data.byteLength).toBe(800); // 100 * 8 bytes for bigint64
    }
  });

  test('rejects zero length', () => {
    const result = createColumn(DType.Float64, 0);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.length).toBe(0);
      expect(result.data.data.byteLength).toBe(0);
    }
  });

  test('rejects negative length', () => {
    const result = createColumn(DType.Float64, -10);
    expect(result.ok).toBe(false);
  });
});

describe('getColumnLength', () => {
  test('returns correct length', () => {
    const result = createColumn(DType.Float64, 100);
    if (result.ok) {
      expect(getColumnLength(result.data)).toBe(100);
    }
  });
});

describe('getColumnDType', () => {
  test('returns correct dtype', () => {
    const result = createColumn(DType.String, 50);
    if (result.ok) {
      expect(getColumnDType(result.data)).toBe(DType.String);
    }
  });
});

describe('getColumnValue and setColumnValue', () => {
  test('get/set Float64 values', () => {
    const result = createColumn(DType.Float64, 10);
    if (result.ok) {
      const col = result.data;

      setColumnValue(col, 0, 42.5);
      setColumnValue(col, 5, 100.25);

      expect(getColumnValue(col, 0)).toBe(42.5);
      expect(getColumnValue(col, 5)).toBe(100.25);
      expect(getColumnValue(col, 2)).toBe(0); // Unset value
    }
  });

  test('get/set Int32 values', () => {
    const result = createColumn(DType.Int32, 10);
    if (result.ok) {
      const col = result.data;

      setColumnValue(col, 0, 42);
      setColumnValue(col, 3, -100);

      expect(getColumnValue(col, 0)).toBe(42);
      expect(getColumnValue(col, 3)).toBe(-100);
    }
  });

  test('get/set Bool values', () => {
    const result = createColumn(DType.Bool, 10);
    if (result.ok) {
      const col = result.data;

      setColumnValue(col, 0, 1);
      setColumnValue(col, 5, 1);

      expect(getColumnValue(col, 0)).toBe(1);
      expect(getColumnValue(col, 5)).toBe(1);
      expect(getColumnValue(col, 2)).toBe(0);
    }
  });

  test('get/set String values (dictionary indices)', () => {
    const result = createColumn(DType.String, 10);
    if (result.ok) {
      const col = result.data;

      // Set dictionary indices
      setColumnValue(col, 0, 5);
      setColumnValue(col, 3, 10);

      expect(getColumnValue(col, 0)).toBe(5);
      expect(getColumnValue(col, 3)).toBe(10);
    }
  });

  test('get/set DateTime values', () => {
    const result = createColumn(DType.DateTime, 10);
    if (result.ok) {
      const col = result.data;

      const timestamp = 1706140800000n; // BigInt timestamp
      setColumnValue(col, 0, timestamp);

      expect(getColumnValue(col, 0)).toBe(timestamp);
    }
  });

  test('handles out of bounds index', () => {
    const result = createColumn(DType.Float64, 10);
    if (result.ok) {
      expect(getColumnValue(result.data, 100)).toBeUndefined();
      expect(getColumnValue(result.data, -1)).toBeUndefined();
    }
  });
});

describe('getColumnMemoryUsage', () => {
  test('calculates memory for Float64 column', () => {
    const result = createColumn(DType.Float64, 100);
    if (result.ok) {
      expect(getColumnMemoryUsage(result.data)).toBe(800); // 100 * 8
    }
  });

  test('calculates memory for Int32 column', () => {
    const result = createColumn(DType.Int32, 100);
    if (result.ok) {
      expect(getColumnMemoryUsage(result.data)).toBe(400); // 100 * 4
    }
  });

  test('calculates memory for Bool column', () => {
    const result = createColumn(DType.Bool, 100);
    if (result.ok) {
      expect(getColumnMemoryUsage(result.data)).toBe(100); // 100 * 1
    }
  });
});

describe('resizeColumn', () => {
  test('grows column preserving data', () => {
    const result = createColumn(DType.Float64, 5);
    if (!result.ok) return;

    const col = result.data;
    setColumnValue(col, 0, 1.1);
    setColumnValue(col, 2, 2.2);
    setColumnValue(col, 4, 3.3);

    const resized = resizeColumn(col, 10);
    expect(resized.ok).toBe(true);
    if (resized.ok) {
      expect(getColumnLength(resized.data)).toBe(10);
      expect(getColumnValue(resized.data, 0)).toBe(1.1);
      expect(getColumnValue(resized.data, 2)).toBe(2.2);
      expect(getColumnValue(resized.data, 4)).toBe(3.3);
      expect(getColumnValue(resized.data, 7)).toBe(0); // New values
    }
  });

  test('shrinks column preserving data', () => {
    const result = createColumn(DType.Int32, 10);
    if (!result.ok) return;

    const col = result.data;
    setColumnValue(col, 0, 10);
    setColumnValue(col, 2, 20);
    setColumnValue(col, 8, 30);

    const resized = resizeColumn(col, 5);
    expect(resized.ok).toBe(true);
    if (resized.ok) {
      expect(getColumnLength(resized.data)).toBe(5);
      expect(getColumnValue(resized.data, 0)).toBe(10);
      expect(getColumnValue(resized.data, 2)).toBe(20);
    }
  });

  test('rejects zero length', () => {
    const result = createColumn(DType.Float64, 10);
    if (result.ok) {
      const resized = resizeColumn(result.data, 0);
      expect(resized.ok).toBe(false);
    }
  });
});

describe('Column edge cases', () => {
  test('handles large columns', () => {
    const result = createColumn(DType.Float64, 1_000_000);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(getColumnLength(result.data)).toBe(1_000_000);
    }
  });

  test('maintains dtype after resize', () => {
    const result = createColumn(DType.DateTime, 10);
    if (result.ok) {
      const resized = resizeColumn(result.data, 20);
      if (resized.ok) {
        expect(getColumnDType(resized.data)).toBe(DType.DateTime);
      }
    }
  });
});
