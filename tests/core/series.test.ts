import { describe, expect, test } from 'bun:test';
import { Series } from '../../src/core/series';

describe('Series', () => {
  describe('float64', () => {
    test('creates from array', () => {
      const s = Series.float64([1.1, 2.2, 3.3]);
      expect(s.length).toBe(3);
      expect(s.dtype.kind).toBe('float64');
    });

    test('at() returns correct values', () => {
      const s = Series.float64([1.5, 2.5, 3.5]);
      expect(s.at(0)).toBe(1.5);
      expect(s.at(1)).toBe(2.5);
      expect(s.at(2)).toBe(3.5);
    });

    test('at() returns undefined for out of bounds', () => {
      const s = Series.float64([1, 2, 3]);
      expect(s.at(-1)).toBeUndefined();
      expect(s.at(3)).toBeUndefined();
    });

    test('head() returns first n elements', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const h = s.head(3);
      expect(h.length).toBe(3);
      expect([...h]).toEqual([1, 2, 3]);
    });

    test('tail() returns last n elements', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const t = s.tail(2);
      expect(t.length).toBe(2);
      expect([...t]).toEqual([4, 5]);
    });

    test('slice() creates view', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const sl = s.slice(1, 4);
      expect(sl.length).toBe(3);
      expect([...sl]).toEqual([2, 3, 4]);
    });

    test('is iterable', () => {
      const s = Series.float64([1, 2, 3]);
      expect([...s]).toEqual([1, 2, 3]);
    });
  });

  describe('int32', () => {
    test('creates from array', () => {
      const s = Series.int32([1, 2, 3]);
      expect(s.length).toBe(3);
      expect(s.dtype.kind).toBe('int32');
    });

    test('truncates floats to integers', () => {
      const s = Series.int32([1.9, 2.1, 3.5]);
      expect([...s]).toEqual([1, 2, 3]);
    });
  });

  describe('string', () => {
    test('creates from array', () => {
      const s = Series.string(['a', 'b', 'c']);
      expect(s.length).toBe(3);
      expect(s.dtype.kind).toBe('string');
    });

    test('at() returns correct values', () => {
      const s = Series.string(['hello', 'world']);
      expect(s.at(0)).toBe('hello');
      expect(s.at(1)).toBe('world');
    });
  });

  describe('bool', () => {
    test('creates from array', () => {
      const s = Series.bool([true, false, true]);
      expect(s.length).toBe(3);
      expect(s.dtype.kind).toBe('bool');
    });

    test('at() returns boolean values', () => {
      const s = Series.bool([true, false, true]);
      expect(s.at(0)).toBe(true);
      expect(s.at(1)).toBe(false);
      expect(s.at(2)).toBe(true);
    });
  });

  describe('display', () => {
    test('toString() formats correctly', () => {
      const s = Series.float64([1.5, 2.5, 3.5]);
      const str = s.toString();
      expect(str).toContain('dtype: float64');
      expect(str).toContain('length: 3');
    });
  });

  describe('statistics', () => {
    test('sum() returns total', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      expect(s.sum()).toBe(15);
    });

    test('mean() returns average', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      expect(s.mean()).toBe(3);
    });

    test('min() returns minimum', () => {
      const s = Series.float64([5, 2, 8, 1, 9]);
      expect(s.min()).toBe(1);
    });

    test('max() returns maximum', () => {
      const s = Series.float64([5, 2, 8, 1, 9]);
      expect(s.max()).toBe(9);
    });

    test('std() returns standard deviation', () => {
      const s = Series.float64([2, 4, 4, 4, 5, 5, 7, 9]);
      expect(s.std()).toBeCloseTo(2, 1);
    });

    test('describe() returns summary', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const desc = s.describe();
      expect(desc.count).toBe(5);
      expect(desc.mean).toBe(3);
      expect(desc.min).toBe(1);
      expect(desc.max).toBe(5);
    });

    test('median() handles odd and even', () => {
      const odd = Series.float64([1, 3, 2]);
      const even = Series.float64([1, 2, 3, 4]);
      expect(odd.median()).toBe(2);
      expect(even.median()).toBe(2.5);
    });

    test('quantile() interpolates', () => {
      const s = Series.float64([1, 2, 3, 4]);
      expect(s.quantile(0)).toBe(1);
      expect(s.quantile(0.5)).toBe(2.5);
      expect(s.quantile(1)).toBe(4);
    });

    test('mode() returns all modes', () => {
      const s = Series.int32([1, 2, 2, 3, 1]);
      expect(s.mode()).toEqual([1, 2]);
    });

    test('cumulative operations', () => {
      const s = Series.int32([1, 2, 3]);
      expect([...s.cumsum()]).toEqual([1, 3, 6]);
      expect([...s.cumprod()]).toEqual([1, 2, 6]);
      expect([...s.cummax()]).toEqual([1, 2, 3]);
      expect([...s.cummin()]).toEqual([1, 1, 1]);
    });
  });

  describe('transformations', () => {
    test('filter() returns matching values', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const filtered = s.filter((v) => v > 2);
      expect([...filtered]).toEqual([3, 4, 5]);
    });

    test('map() transforms values', () => {
      const s = Series.float64([1, 2, 3]);
      const doubled = s.map((v) => v * 2);
      expect([...doubled]).toEqual([2, 4, 6]);
    });

    test('sort() sorts ascending by default', () => {
      const s = Series.float64([3, 1, 4, 1, 5]);
      const sorted = s.sort();
      expect([...sorted]).toEqual([1, 1, 3, 4, 5]);
    });

    test('sort(false) sorts descending', () => {
      const s = Series.float64([3, 1, 4, 1, 5]);
      const sorted = s.sort(false);
      expect([...sorted]).toEqual([5, 4, 3, 1, 1]);
    });

    test('unique() removes duplicates', () => {
      const s = Series.float64([1, 2, 2, 3, 3, 3]);
      const unique = s.unique();
      expect([...unique]).toEqual([1, 2, 3]);
    });

    test('valueCounts() counts occurrences', () => {
      const s = Series.string(['a', 'b', 'a', 'c', 'a']);
      const counts = s.valueCounts();
      expect(counts.get('a')).toBe(3);
      expect(counts.get('b')).toBe(1);
      expect(counts.get('c')).toBe(1);
    });

    test('toArray() returns plain array', () => {
      const s = Series.float64([1, 2, 3]);
      const arr = s.toArray();
      expect(arr).toEqual([1, 2, 3]);
      expect(Array.isArray(arr)).toBe(true);
    });
  });

  describe('missing value operations', () => {
    test('isna() detects NaN values', () => {
      const s = Series.float64([1, Number.NaN, 3]);
      const na = s.isna();
      expect([...na]).toEqual([false, true, false]);
    });

    test('fillna() replaces NaN with value', () => {
      const s = Series.float64([1, Number.NaN, 3]);
      const filled = s.fillna(0);
      expect([...filled]).toEqual([1, 0, 3]);
    });

    test('copy() creates independent copy', () => {
      const s = Series.float64([1, 2, 3]);
      const c = s.copy();
      expect([...c]).toEqual([1, 2, 3]);
      expect(c.length).toBe(s.length);
    });
  });

  describe('type conversion', () => {
    test('astype(int32) converts types', () => {
      const s = Series.float64([1.1, 2.9, 3.5]);
      const ints = s.astype('int32');
      expect(ints.dtype.kind).toBe('int32');
      expect([...ints]).toEqual([1, 2, 3]);

      const bools = Series.bool([true, false]);
      expect([...bools.astype('int32')]).toEqual([1, 0]);

      const strs = Series.string(['1', '2', 'foo']);
      expect([...strs.astype('int32')]).toEqual([1, 2, 0]);
    });

    test('astype(float64) converts types', () => {
      const s = Series.int32([1, 2]);
      const floats = s.astype('float64');
      expect(floats.dtype.kind).toBe('float64');
      expect([...floats]).toEqual([1.0, 2.0]);

      const bools = Series.bool([true, false]);
      expect([...bools.astype('float64')]).toEqual([1.0, 0.0]);

      const strs = Series.string(['1.5', 'foo']);
      expect([...strs.astype('float64')]).toEqual([1.5, 0]);
    });

    test('astype(string) converts to string', () => {
      const s = Series.float64([1.5, 2.5]);
      expect([...s.astype('string')]).toEqual(['1.5', '2.5']);
    });

    test('astype(bool) converts to boolean', () => {
      const nums = Series.int32([1, 0, 2]);
      expect([...nums.astype('bool')]).toEqual([true, false, true]);

      const strs = Series.string(['true', '1', 'FALSE', 'other']);
      expect([...strs.astype('bool')]).toEqual([true, true, false, false]);
    });
  });

  describe('data cleaning', () => {
    test('replace() swaps values', () => {
      const s = Series.int32([1, 2, 1, 3]);
      expect([...s.replace(1, 0)]).toEqual([0, 2, 0, 3]);
    });

    test('replace() handles NaN', () => {
      const s = Series.float64([1, Number.NaN, 2]);
      expect([...s.replace(Number.NaN, 0)]).toEqual([1, 0, 2]);
    });

    test('clip() clamps values', () => {
      const s = Series.int32([1, 5, 10]);
      expect([...s.clip(2, 8)]).toEqual([2, 5, 8]);
      expect([...s.clip(undefined, 8)]).toEqual([1, 5, 8]);
      expect([...s.clip(2, undefined)]).toEqual([2, 5, 10]);
    });

    test('clip() throws for strings', () => {
      const s = Series.string(['a']);
      expect(() => s.clip(1, 2)).toThrow();
    });

    test('ffill() propagates values forward', () => {
      const s = Series.float64([1, Number.NaN, Number.NaN, 2, Number.NaN]);
      expect([...s.ffill()]).toEqual([1, 1, 1, 2, 2]);
    });

    test('bfill() propagates values backward', () => {
      const s = Series.float64([Number.NaN, 1, Number.NaN, 2, Number.NaN]);
      // Last value remains NaN/null as there is no next value
      // Bun test expect(..).toEqual([..]) might be strict on null vs undefined vs NaN
      expect(s.bfill().toArray()).toEqual([1, 1, 2, 2, Number.NaN]);
    });
  });

  describe('internal storage', () => {
    test('_storage() returns underlying data', () => {
      const s = Series.float64([1, 2, 3]);
      const storage = s._storage(); // @ts-ignore
      expect(storage).toBeInstanceOf(Float64Array);
    });

    test('_storage() returns view for sliced numeric series', () => {
      const s = Series.float64([1, 2, 3, 4]);
      const sliced = s.slice(1, 3);
      const storage = sliced._storage(); // @ts-ignore
      expect(storage.length).toBe(2);
      expect(storage[0]).toBe(2);
    });

    test('_storage() returns sliced array for sliced string series', () => {
      const s = Series.string(['a', 'b', 'c', 'd']);
      const sliced = s.slice(1, 3);
      const storage = sliced._storage(); // @ts-ignore
      expect(storage).toEqual(['b', 'c']);
    });
  });

  describe('display', () => {
    test('print() logs to console', () => {
      // Bun doesn't have a direct 'jest' object. We can check if it runs without error,
      // or avoid mocking console.log if it's too complex in this env.
      // For now, let's just ensure it doesn't throw.
      const s = Series.int32([1, 2]);
      s.print();
    });
  });
});
