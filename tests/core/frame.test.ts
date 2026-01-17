import { describe, expect, test } from 'bun:test';
import { DataFrame } from '../../src/core/dataframe';
import { m } from '../../src/core/types';

describe('DataFrame Edge Cases', () => {
  describe('Creation', () => {
    test('from() with empty data', () => {
      const df = DataFrame.from({ A: m.int32() }, []);
      expect(df.shape).toEqual([0, 1]);
      expect(df.columns()).toEqual(['A']);
    });

    test('fromColumns() with mismatched lengths throws', () => {
      expect(() => {
        DataFrame.fromColumns({
          A: [1, 2],
          B: [1], // Mismatched length
        });
      }).toThrow();
    });

    test('fromColumns() infers boolean type', () => {
      const df = DataFrame.fromColumns({
        A: [true, false, true],
      });
      expect(df.col('A').dtype.kind).toBe('bool');
    });

    test('fromColumns() infers string type', () => {
      const df = DataFrame.fromColumns({
        A: ['a', 'b'],
      });
      expect(df.col('A').dtype.kind).toBe('string');
    });
  });

  describe('Properties', () => {
    test('shape is correct', () => {
      const df = DataFrame.fromColumns({ A: [1], B: [2] });
      expect(df.shape).toEqual([1, 2]);
    });
  });

  describe('Aggregation', () => {
    test('cumulative operations only affect numeric columns', () => {
      const df = DataFrame.fromColumns({ A: [1, 2, 3], B: ['x', 'y', 'z'] });
      const csum = df.cumsum();
      expect([...csum.col('A')]).toEqual([1, 3, 6]);
      expect([...csum.col('B')]).toEqual(['x', 'y', 'z']);
    });

    test('median/quantile/mode return per-column results', () => {
      const df = DataFrame.fromColumns({ A: [1, 2, 3, 4], B: ['a', 'a', 'b', 'b'] });
      expect(df.median().A).toBe(2.5);
      expect(df.quantile(0.25).A).toBe(1.75);
      expect(df.mode().B).toEqual(['a', 'b']);
    });
  });

  describe('Head/Tail Edge Cases', () => {
    test('head(n) where n > length', () => {
      const df = DataFrame.fromColumns({ A: [1, 2] });
      const head = df.head(10);
      expect(head.shape).toEqual([2, 1]); // Returns all rows
    });

    test('tail(n) where n > length', () => {
      const df = DataFrame.fromColumns({ A: [1, 2] });
      const tail = df.tail(10);
      expect(tail.shape).toEqual([2, 1]); // Returns all rows
    });

    test('head(0) returns empty', () => {
      const df = DataFrame.fromColumns({ A: [1, 2] });
      const head = df.head(0);
      expect(head.shape).toEqual([0, 1]);
    });
  });
});
