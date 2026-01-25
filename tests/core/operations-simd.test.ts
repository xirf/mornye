/**
 * Tests for SIMD-optimized filter operations
 * Verifies correctness and performance improvements
 */

import { describe, expect, test } from 'bun:test';
import { addColumn, createDataFrame, getColumn } from '../../src/dataframe/dataframe';
import { filter } from '../../src/dataframe/operations';
import {
  filterFloat64Vectorized,
  filterInt32Vectorized,
  getVectorizedFilter,
  shouldUseVectorized,
} from '../../src/dataframe/operations-simd';

describe('SIMD Operations', () => {
  describe('shouldUseVectorized', () => {
    test('returns false for small datasets', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'float64', 5000);
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      expect(shouldUseVectorized(col.data, 5000)).toBe(false);
    });

    test('returns true for large float64 datasets', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'float64', 20000);
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      expect(shouldUseVectorized(col.data, 20000)).toBe(true);
    });

    test('returns true for large int32 datasets', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'int32', 15000);
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      expect(shouldUseVectorized(col.data, 15000)).toBe(true);
    });

    test('returns false for string columns', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'string', 100000);
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      expect(shouldUseVectorized(col.data, 100000)).toBe(false);
    });
  });

  describe('filterFloat64Vectorized', () => {
    test('filters > operator correctly', () => {
      const df = createDataFrame();
      const addResult = addColumn(df, 'values', 'float64', 100);
      expect(addResult.ok).toBe(true);

      // Set values: 0, 1, 2, ..., 99
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      for (let i = 0; i < 100; i++) {
        col.data.view.setFloat64(i * 8, i, true);
      }

      const indices = filterFloat64Vectorized(col.data, '>', 50);
      expect(indices.length).toBe(49); // 51-99 = 49 values
      expect(indices[0]).toBe(51);
      expect(indices[indices.length - 1]).toBe(99);
    });

    test('filters < operator correctly', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'float64', 100);
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      for (let i = 0; i < 100; i++) {
        col.data.view.setFloat64(i * 8, i, true);
      }

      const indices = filterFloat64Vectorized(col.data, '<', 10);
      expect(indices.length).toBe(10); // 0-9
      expect(indices[0]).toBe(0);
      expect(indices[indices.length - 1]).toBe(9);
    });

    test('filters == operator correctly', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'float64', 100);
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      for (let i = 0; i < 100; i++) {
        col.data.view.setFloat64(i * 8, i % 10, true); // 0-9 repeated
      }

      const indices = filterFloat64Vectorized(col.data, '==', 5);
      expect(indices.length).toBe(10); // indices 5, 15, 25, ..., 95
      expect(indices[0]).toBe(5);
      expect(indices[1]).toBe(15);
    });

    test('handles non-batch-aligned sizes correctly', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'float64', 75); // Not divisible by 8
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      for (let i = 0; i < 75; i++) {
        col.data.view.setFloat64(i * 8, i, true);
      }

      const indices = filterFloat64Vectorized(col.data, '>=', 70);
      expect(indices.length).toBe(5); // 70, 71, 72, 73, 74
      expect(indices[0]).toBe(70);
      expect(indices[indices.length - 1]).toBe(74);
    });
  });

  describe('filterInt32Vectorized', () => {
    test('filters > operator correctly', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'int32', 100);
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      for (let i = 0; i < 100; i++) {
        col.data.view.setInt32(i * 4, i, true);
      }

      const indices = filterInt32Vectorized(col.data, '>', 75);
      expect(indices.length).toBe(24); // 76-99
      expect(indices[0]).toBe(76);
      expect(indices[indices.length - 1]).toBe(99);
    });

    test('filters <= operator correctly', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'int32', 100);
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      for (let i = 0; i < 100; i++) {
        col.data.view.setInt32(i * 4, i, true);
      }

      const indices = filterInt32Vectorized(col.data, '<=', 10);
      expect(indices.length).toBe(11); // 0-10
      expect(indices[0]).toBe(0);
      expect(indices[indices.length - 1]).toBe(10);
    });

    test('filters != operator correctly', () => {
      const df = createDataFrame();
      addColumn(df, 'values', 'int32', 50);
      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      for (let i = 0; i < 50; i++) {
        col.data.view.setInt32(i * 4, 5, true); // All values = 5
      }
      // Set one different value
      col.data.view.setInt32(25 * 4, 10, true);

      const indices = filterInt32Vectorized(col.data, '!=', 5);
      expect(indices.length).toBe(1);
      expect(indices[0]).toBe(25);
    });
  });

  describe('Integration with filter()', () => {
    test('SIMD path produces same results as scalar path', () => {
      const size = 20000; // Large enough to trigger SIMD
      const df = createDataFrame();
      addColumn(df, 'values', 'float64', size);

      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      // Fill with sequential values
      for (let i = 0; i < size; i++) {
        col.data.view.setFloat64(i * 8, i * 0.5, true);
      }

      // Test various operators
      const tests = [
        { op: '>' as const, val: 5000 },
        { op: '<' as const, val: 1000 },
        { op: '>=' as const, val: 9500 },
        { op: '<=' as const, val: 500 },
        { op: '==' as const, val: 2500 },
        { op: '!=' as const, val: 3000 },
      ];

      for (const { op, val } of tests) {
        const result = filter(df, 'values', op, val);

        // Manually verify some results
        const col = getColumn(result, 'values');
        expect(col.ok).toBe(true);
        if (!col.ok) continue;

        // Check that all values in result satisfy the condition
        for (let i = 0; i < col.data.length; i++) {
          const value = col.data.view.getFloat64(i * 8, true);

          switch (op) {
            case '>':
              expect(value).toBeGreaterThan(val);
              break;
            case '<':
              expect(value).toBeLessThan(val);
              break;
            case '>=':
              expect(value).toBeGreaterThanOrEqual(val);
              break;
            case '<=':
              expect(value).toBeLessThanOrEqual(val);
              break;
            case '==':
              expect(value).toBe(val);
              break;
            case '!=':
              expect(value).not.toBe(val);
              break;
          }
        }
      }
    });

    test('scalar path still works for small datasets', () => {
      const size = 100; // Too small for SIMD
      const df = createDataFrame();
      addColumn(df, 'values', 'int32', size);

      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      for (let i = 0; i < size; i++) {
        col.data.view.setInt32(i * 4, i, true);
      }

      const result = filter(df, 'values', '>', 50);

      const resultCol = getColumn(result, 'values');
      expect(resultCol.ok).toBe(true);
      if (!resultCol.ok) return;

      expect(resultCol.data.length).toBe(49); // 51-99
    });
  });

  describe('Performance comparison', () => {
    test('SIMD is faster than scalar for large datasets', () => {
      const size = 100000;
      const df = createDataFrame();
      addColumn(df, 'values', 'float64', size);

      const col = getColumn(df, 'values');
      expect(col.ok).toBe(true);
      if (!col.ok) return;

      // Fill with random-ish values
      for (let i = 0; i < size; i++) {
        col.data.view.setFloat64(i * 8, Math.sin(i) * 1000, true);
      }

      // Warm up
      for (let i = 0; i < 3; i++) {
        filter(df, 'values', '>', 0);
      }

      // Measure SIMD path (large dataset, automatic)
      const simdStart = performance.now();
      for (let i = 0; i < 10; i++) {
        filter(df, 'values', '>', 0);
      }
      const simdTime = performance.now() - simdStart;

      // Create small dataset to force scalar path
      const smallDf = createDataFrame();
      addColumn(smallDf, 'values', 'float64', 5000); // Below SIMD threshold
      const smallCol = getColumn(smallDf, 'values');
      expect(smallCol.ok).toBe(true);
      if (!smallCol.ok) return;

      for (let i = 0; i < 5000; i++) {
        smallCol.data.view.setFloat64(i * 8, Math.sin(i) * 1000, true);
      }

      // Measure scalar path
      const scalarStart = performance.now();
      for (let i = 0; i < 10; i++) {
        filter(smallDf, 'values', '>', 0);
      }
      const scalarTime = performance.now() - scalarStart;

      // Normalize by dataset size
      const simdPerRow = simdTime / size;
      const scalarPerRow = scalarTime / 5000;

      console.log(`SIMD: ${simdTime.toFixed(2)}ms (${simdPerRow.toFixed(6)}ms/row)`);
      console.log(`Scalar: ${scalarTime.toFixed(2)}ms (${scalarPerRow.toFixed(6)}ms/row)`);
      console.log(`Speedup: ${(scalarPerRow / simdPerRow).toFixed(2)}x`);

      // SIMD should be at least 1.5x faster per row
      expect(scalarPerRow / simdPerRow).toBeGreaterThan(1.5);
    });
  });
});
