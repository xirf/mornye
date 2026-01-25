import { describe, expect, test } from 'bun:test';
import {
  createNullBitmap,
  getBitmapMemoryUsage,
  getNullCount,
  isNull,
  resizeNullBitmap,
  setNotNull,
  setNull,
} from '../../src/utils/nulls';

describe('createNullBitmap', () => {
  test('creates bitmap for given length', () => {
    const bitmap = createNullBitmap(100);
    expect(bitmap.length).toBe(100);
    expect(bitmap.data).toBeInstanceOf(Uint8Array);
  });

  test('initializes all as not null', () => {
    const bitmap = createNullBitmap(10);
    for (let i = 0; i < 10; i++) {
      expect(isNull(bitmap, i)).toBe(false);
    }
  });

  test('calculates correct buffer size', () => {
    // 100 values needs 13 bytes (ceil(100/8))
    const bitmap = createNullBitmap(100);
    expect(bitmap.data.length).toBeGreaterThanOrEqual(Math.ceil(100 / 8));
  });

  test('rejects zero length', () => {
    expect(() => createNullBitmap(0)).toThrow();
  });

  test('rejects negative length', () => {
    expect(() => createNullBitmap(-5)).toThrow();
  });
});

describe('isNull and setNull', () => {
  test('sets and checks null values', () => {
    const bitmap = createNullBitmap(20);

    setNull(bitmap, 5);
    setNull(bitmap, 10);
    setNull(bitmap, 15);

    expect(isNull(bitmap, 5)).toBe(true);
    expect(isNull(bitmap, 10)).toBe(true);
    expect(isNull(bitmap, 15)).toBe(true);

    // Others remain not null
    expect(isNull(bitmap, 0)).toBe(false);
    expect(isNull(bitmap, 8)).toBe(false);
    expect(isNull(bitmap, 19)).toBe(false);
  });

  test('handles edge case at byte boundaries', () => {
    const bitmap = createNullBitmap(16); // Exactly 2 bytes

    setNull(bitmap, 7); // End of first byte
    setNull(bitmap, 8); // Start of second byte

    expect(isNull(bitmap, 7)).toBe(true);
    expect(isNull(bitmap, 8)).toBe(true);
    expect(isNull(bitmap, 6)).toBe(false);
    expect(isNull(bitmap, 9)).toBe(false);
  });

  test('handles all positions in a byte', () => {
    const bitmap = createNullBitmap(8);

    for (let i = 0; i < 8; i++) {
      setNull(bitmap, i);
    }

    for (let i = 0; i < 8; i++) {
      expect(isNull(bitmap, i)).toBe(true);
    }
  });
});

describe('setNotNull', () => {
  test('clears null flag', () => {
    const bitmap = createNullBitmap(10);

    setNull(bitmap, 5);
    expect(isNull(bitmap, 5)).toBe(true);

    setNotNull(bitmap, 5);
    expect(isNull(bitmap, 5)).toBe(false);
  });

  test('multiple set/clear operations', () => {
    const bitmap = createNullBitmap(10);

    setNull(bitmap, 3);
    setNull(bitmap, 3); // Set again
    expect(isNull(bitmap, 3)).toBe(true);

    setNotNull(bitmap, 3);
    setNotNull(bitmap, 3); // Clear again
    expect(isNull(bitmap, 3)).toBe(false);
  });
});

describe('getNullCount', () => {
  test('returns 0 for all non-null', () => {
    const bitmap = createNullBitmap(100);
    expect(getNullCount(bitmap)).toBe(0);
  });

  test('counts null values correctly', () => {
    const bitmap = createNullBitmap(20);

    setNull(bitmap, 0);
    setNull(bitmap, 5);
    setNull(bitmap, 10);
    setNull(bitmap, 15);

    expect(getNullCount(bitmap)).toBe(4);
  });

  test('counts all nulls', () => {
    const bitmap = createNullBitmap(10);

    for (let i = 0; i < 10; i++) {
      setNull(bitmap, i);
    }

    expect(getNullCount(bitmap)).toBe(10);
  });

  test('updates count after clearing nulls', () => {
    const bitmap = createNullBitmap(10);

    setNull(bitmap, 2);
    setNull(bitmap, 5);
    setNull(bitmap, 8);
    expect(getNullCount(bitmap)).toBe(3);

    setNotNull(bitmap, 5);
    expect(getNullCount(bitmap)).toBe(2);
  });
});

describe('getBitmapMemoryUsage', () => {
  test('calculates memory for small bitmap', () => {
    const bitmap = createNullBitmap(10);
    // 10 bits needs 2 bytes
    expect(getBitmapMemoryUsage(bitmap)).toBe(2);
  });

  test('calculates memory for exact byte boundary', () => {
    const bitmap = createNullBitmap(64); // Exactly 8 bytes
    expect(getBitmapMemoryUsage(bitmap)).toBe(8);
  });

  test('calculates memory for large bitmap', () => {
    const bitmap = createNullBitmap(1000);
    // 1000 bits needs 125 bytes
    expect(getBitmapMemoryUsage(bitmap)).toBe(125);
  });
});

describe('resizeNullBitmap', () => {
  test('grows bitmap preserving null flags', () => {
    const bitmap = createNullBitmap(10);
    setNull(bitmap, 2);
    setNull(bitmap, 5);
    setNull(bitmap, 8);

    const resized = resizeNullBitmap(bitmap, 20);
    expect(resized.length).toBe(20);

    // Original nulls preserved
    expect(isNull(resized, 2)).toBe(true);
    expect(isNull(resized, 5)).toBe(true);
    expect(isNull(resized, 8)).toBe(true);

    // New values are not null
    expect(isNull(resized, 15)).toBe(false);
    expect(isNull(resized, 19)).toBe(false);
  });

  test('shrinks bitmap preserving null flags', () => {
    const bitmap = createNullBitmap(20);
    setNull(bitmap, 2);
    setNull(bitmap, 5);
    setNull(bitmap, 15);

    const resized = resizeNullBitmap(bitmap, 10);
    expect(resized.length).toBe(10);

    // Nulls within new range preserved
    expect(isNull(resized, 2)).toBe(true);
    expect(isNull(resized, 5)).toBe(true);
  });

  test('rejects zero length', () => {
    const bitmap = createNullBitmap(10);
    expect(() => resizeNullBitmap(bitmap, 0)).toThrow();
  });

  test('rejects negative length', () => {
    const bitmap = createNullBitmap(10);
    expect(() => resizeNullBitmap(bitmap, -5)).toThrow();
  });
});

describe('Edge cases', () => {
  test('handles index out of bounds gracefully', () => {
    const bitmap = createNullBitmap(10);

    // Should not crash, just return false or do nothing
    expect(isNull(bitmap, 100)).toBe(false);
    expect(isNull(bitmap, -1)).toBe(false);

    setNull(bitmap, 100); // Should not crash
    setNotNull(bitmap, -1); // Should not crash
  });

  test('handles large bitmaps', () => {
    const bitmap = createNullBitmap(1_000_000);
    expect(bitmap.length).toBe(1_000_000);

    setNull(bitmap, 0);
    setNull(bitmap, 999_999);

    expect(isNull(bitmap, 0)).toBe(true);
    expect(isNull(bitmap, 999_999)).toBe(true);
    expect(getNullCount(bitmap)).toBe(2);
  });

  test('bit packing is correct', () => {
    const bitmap = createNullBitmap(16);

    // Set every other bit
    for (let i = 0; i < 16; i += 2) {
      setNull(bitmap, i);
    }

    // Check pattern
    for (let i = 0; i < 16; i++) {
      expect(isNull(bitmap, i)).toBe(i % 2 === 0);
    }

    expect(getNullCount(bitmap)).toBe(8);
  });
});
