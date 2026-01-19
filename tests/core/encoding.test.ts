import { describe, expect, test } from 'bun:test';
import { DataFrame, Series, m } from '../../src';

describe('Categorical Encoding', () => {
  describe('Ordinal Encoding', () => {
    test('Series.toOrdinal', () => {
      const s = Series.string(['cat', 'dog', 'cat', 'bird', null as unknown as string]);
      const encoded = s.toOrdinal();

      expect(encoded.dtype.kind).toBe('int32');
      expect(encoded.toArray()).toEqual([0, 1, 0, 2, -1]);
    });

    test('DataFrame.toOrdinal', () => {
      const df = DataFrame.fromColumns({
        cat: ['A', 'B', 'A'],
        val: [1, 2, 3],
      });
      const encoded = df.toOrdinal('cat');

      expect(encoded.col('cat').toArray()).toEqual([0, 1, 0]);
      expect(encoded.col('val').toArray()).toEqual([1, 2, 3]);
      expect(encoded.schema.cat.kind).toBe('int32');
    });
  });

  describe('One-Hot Encoding (getDummies)', () => {
    test('Basic getDummies', () => {
      const df = DataFrame.fromColumns({
        brand: ['apple', 'samsung', 'apple'],
        price: [1000, 800, 900],
      });
      const dummies = df.getDummies(['brand']);

      expect(dummies.columns()).toContain('brand_apple');
      expect(dummies.columns()).toContain('brand_samsung');
      expect(dummies.columns()).not.toContain('brand');
      expect(dummies.col('brand_apple').toArray()).toEqual([true, false, true]);
      expect(dummies.col('brand_samsung').toArray()).toEqual([false, true, false]);
      expect(dummies.col('price').toArray()).toEqual([1000, 800, 900]);
    });

    test('getDummies with prefix=false and dropOriginal=false', () => {
      const df = DataFrame.fromColumns({
        type: ['X', 'Y'],
      });
      const dummies = df.getDummies(['type'], { prefix: false, dropOriginal: false });

      expect(dummies.columns()).toEqual(['type', 'X', 'Y']);
      expect(dummies.col('X').toArray()).toEqual([true, false]);
    });
  });

  describe('Binary Encoding', () => {
    test('Basic toBinary', () => {
      // 3 categories -> 2 bits needed (00, 01, 10)
      const df = DataFrame.fromColumns({
        label: ['A', 'B', 'C', 'A'],
      });
      const encoded = df.toBinary(['label']);

      // A -> 0 (bin 00)
      // B -> 1 (bin 01)
      // C -> 2 (bin 10)

      expect(encoded.columns()).toContain('label_0');
      expect(encoded.columns()).toContain('label_1');

      // Bit 0 (LSB)
      expect(encoded.col('label_0').toArray()).toEqual([false, true, false, false]);
      // Bit 1
      expect(encoded.col('label_1').toArray()).toEqual([false, false, true, false]);
    });
  });
});
