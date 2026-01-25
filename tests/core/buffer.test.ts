import { describe, expect, test } from 'bun:test';
import {
  type RawBuffer,
  allocateBuffer,
  getBufferMemoryUsage,
  resizeBuffer,
} from '../../src/core/buffer';
import { DType } from '../../src/types/dtypes';

describe('allocateBuffer', () => {
  test('allocates Uint8Array buffer for Float64', () => {
    const result = allocateBuffer(DType.Float64, 100);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBeInstanceOf(Uint8Array);
      expect(result.data.byteLength).toBe(800); // 100 * 8 bytes
    }
  });

  test('allocates Uint8Array buffer for Int32', () => {
    const result = allocateBuffer(DType.Int32, 50);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBeInstanceOf(Uint8Array);
      expect(result.data.byteLength).toBe(200); // 50 * 4 bytes
    }
  });

  test('allocates Uint8Array for bool', () => {
    const result = allocateBuffer(DType.Bool, 200);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBeInstanceOf(Uint8Array);
      expect(result.data.byteLength).toBe(200); // 200 * 1 byte
    }
  });

  test('allocates Uint8Array for string (dictionary indices)', () => {
    const result = allocateBuffer(DType.String, 100);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBeInstanceOf(Uint8Array);
      expect(result.data.byteLength).toBe(400); // 100 * 4 bytes (int32)
    }
  });

  test('allocates Uint8Array for datetime', () => {
    const result = allocateBuffer(DType.DateTime, 100);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBeInstanceOf(Uint8Array);
      expect(result.data.byteLength).toBe(800); // 100 * 8 bytes (bigint64)
    }
  });

  test('rejects zero length', () => {
    const result = allocateBuffer(DType.Float64, 0);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.byteLength).toBe(0);
    }
  });

  test('rejects negative length', () => {
    const result = allocateBuffer(DType.Float64, -10);
    expect(result.ok).toBe(false);
  });

  test('buffer is zero-initialized', () => {
    const result = allocateBuffer(DType.Float64, 10);
    if (result.ok) {
      for (let i = 0; i < result.data.length; i++) {
        expect(result.data[i]).toBe(0);
      }
    }
  });
});

describe('resizeBuffer', () => {
  test('grows buffer preserving data', () => {
    const result = allocateBuffer(DType.Float64, 5);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    // Write some values using DataView
    const view = new DataView(result.data.buffer, result.data.byteOffset, result.data.byteLength);
    view.setFloat64(0, 1.5, true);
    view.setFloat64(8, 2.5, true);
    view.setFloat64(16, 3.5, true);

    const resized = resizeBuffer(result.data, 10, DType.Float64);
    expect(resized.ok).toBe(true);
    if (!resized.ok) return;

    expect(resized.data.byteLength).toBe(80); // 10 * 8 bytes
    const newView = new DataView(
      resized.data.buffer,
      resized.data.byteOffset,
      resized.data.byteLength,
    );
    expect(newView.getFloat64(0, true)).toBe(1.5);
    expect(newView.getFloat64(8, true)).toBe(2.5);
    expect(newView.getFloat64(16, true)).toBe(3.5);
  });

  test('shrinks buffer preserving data', () => {
    const result = allocateBuffer(DType.Float64, 5);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const view = new DataView(result.data.buffer, result.data.byteOffset, result.data.byteLength);
    view.setFloat64(0, 1.5, true);
    view.setFloat64(8, 2.5, true);

    const resized = resizeBuffer(result.data, 2, DType.Float64);
    expect(resized.ok).toBe(true);
    if (!resized.ok) return;

    expect(resized.data.byteLength).toBe(16); // 2 * 8 bytes
    const newView = new DataView(
      resized.data.buffer,
      resized.data.byteOffset,
      resized.data.byteLength,
    );
    expect(newView.getFloat64(0, true)).toBe(1.5);
    expect(newView.getFloat64(8, true)).toBe(2.5);
  });

  test('works with Int32', () => {
    const result = allocateBuffer(DType.Int32, 3);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const view = new DataView(result.data.buffer, result.data.byteOffset, result.data.byteLength);
    view.setInt32(0, 10, true);
    view.setInt32(4, 20, true);

    const resized = resizeBuffer(result.data, 5, DType.Int32);
    expect(resized.ok).toBe(true);
    if (!resized.ok) return;

    expect(resized.data).toBeInstanceOf(Uint8Array);
    expect(resized.data.byteLength).toBe(20); // 5 * 4 bytes
    const newView = new DataView(
      resized.data.buffer,
      resized.data.byteOffset,
      resized.data.byteLength,
    );
    expect(newView.getInt32(0, true)).toBe(10);
    expect(newView.getInt32(4, true)).toBe(20);
  });

  test('works with BigInt64', () => {
    const result = allocateBuffer(DType.DateTime, 3);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const view = new DataView(result.data.buffer, result.data.byteOffset, result.data.byteLength);
    view.setBigInt64(0, 10n, true);
    view.setBigInt64(8, 20n, true);

    const resized = resizeBuffer(result.data, 5, DType.DateTime);
    expect(resized.ok).toBe(true);
    if (!resized.ok) return;

    expect(resized.data).toBeInstanceOf(Uint8Array);
    expect(resized.data.byteLength).toBe(40); // 5 * 8 bytes
    const newView = new DataView(
      resized.data.buffer,
      resized.data.byteOffset,
      resized.data.byteLength,
    );
    expect(newView.getBigInt64(0, true)).toBe(10n);
    expect(newView.getBigInt64(8, true)).toBe(20n);
  });

  test('rejects zero length', () => {
    const result = allocateBuffer(DType.Float64, 3);
    if (!result.ok) return;
    const resized = resizeBuffer(result.data, 0, DType.Float64);
    expect(resized.ok).toBe(false);
  });

  test('rejects negative length', () => {
    const result = allocateBuffer(DType.Float64, 3);
    if (!result.ok) return;
    const resized = resizeBuffer(result.data, -5, DType.Float64);
    expect(resized.ok).toBe(false);
  });

  test('no-op if same size', () => {
    const result = allocateBuffer(DType.Float64, 3);
    if (!result.ok) return;

    const view = new DataView(result.data.buffer, result.data.byteOffset, result.data.byteLength);
    view.setFloat64(0, 1.5, true);

    const resized = resizeBuffer(result.data, 3, DType.Float64);
    expect(resized.ok).toBe(true);
    if (!resized.ok) return;

    expect(resized.data.byteLength).toBe(24); // 3 * 8 bytes
    const newView = new DataView(
      resized.data.buffer,
      resized.data.byteOffset,
      resized.data.byteLength,
    );
    expect(newView.getFloat64(0, true)).toBe(1.5);
  });
});

describe('getBufferMemoryUsage', () => {
  test('calculates Float64Array memory', () => {
    const buffer = new Float64Array(100);
    expect(getBufferMemoryUsage(buffer)).toBe(800); // 100 * 8
  });

  test('calculates Int32Array memory', () => {
    const buffer = new Int32Array(100);
    expect(getBufferMemoryUsage(buffer)).toBe(400); // 100 * 4
  });

  test('calculates Uint8Array memory', () => {
    const buffer = new Uint8Array(100);
    expect(getBufferMemoryUsage(buffer)).toBe(100); // 100 * 1
  });

  test('calculates BigInt64Array memory', () => {
    const buffer = new BigInt64Array(100);
    expect(getBufferMemoryUsage(buffer)).toBe(800); // 100 * 8
  });

  test('returns 0 for empty buffer', () => {
    const buffer = new Float64Array(0);
    expect(getBufferMemoryUsage(buffer)).toBe(0);
  });
});

describe('Buffer edge cases', () => {
  test('handles large allocations', () => {
    const size = 1_000_000; // 1M elements
    const result = allocateBuffer(DType.Float64, size);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.byteLength).toBe(size * 8);
      expect(getBufferMemoryUsage(result.data)).toBe(size * 8);
    }
  });

  test('resize with large growth factor', () => {
    const result = allocateBuffer(DType.Int32, 3);
    if (!result.ok) return;

    const view = new DataView(result.data.buffer, result.data.byteOffset, result.data.byteLength);
    view.setInt32(0, 1, true);
    view.setInt32(4, 2, true);
    view.setInt32(8, 3, true);

    const resized = resizeBuffer(result.data, 100_000, DType.Int32);
    expect(resized.ok).toBe(true);
    if (!resized.ok) return;

    const newView = new DataView(
      resized.data.buffer,
      resized.data.byteOffset,
      resized.data.byteLength,
    );
    expect(newView.getInt32(0, true)).toBe(1);
    expect(resized.data.byteLength).toBe(100_000 * 4);
  });
});
