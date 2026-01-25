import { describe, expect, test } from 'bun:test';
import { setColumnValue } from '../../src/core/column';
import {
  addColumn,
  createDataFrame,
  getColumn,
  getColumnNames,
  getRowCount,
} from '../../src/dataframe/dataframe';
import { drop, rename } from '../../src/dataframe/manipulation';
import { DType } from '../../src/types/dtypes';

describe('DataFrame Manipulation Operations', () => {
  describe('drop()', () => {
    describe('dropping columns', () => {
      test('drops specified columns', () => {
        const df = createDataFrame();
        addColumn(df, 'a', DType.Int32, 3);
        addColumn(df, 'b', DType.Float64, 3);
        addColumn(df, 'c', DType.String, 3);

        const result = drop(df, { columns: ['b'] });
        expect(result.ok).toBe(true);

        if (!result.ok) return;
        const droppedDf = result.data;

        expect(getColumnNames(droppedDf)).toEqual(['a', 'c']);
        expect(getRowCount(droppedDf)).toBe(3);
      });

      test('drops multiple columns', () => {
        const df = createDataFrame();
        addColumn(df, 'a', DType.Int32, 2);
        addColumn(df, 'b', DType.Float64, 2);
        addColumn(df, 'c', DType.String, 2);
        addColumn(df, 'd', DType.Bool, 2);

        const result = drop(df, { columns: ['a', 'c'] });
        expect(result.ok).toBe(true);

        if (!result.ok) return;
        const droppedDf = result.data;

        expect(getColumnNames(droppedDf)).toEqual(['b', 'd']);
      });

      test('preserves data in remaining columns', () => {
        const df = createDataFrame();
        addColumn(df, 'a', DType.Int32, 2);
        addColumn(df, 'b', DType.Float64, 2);

        const colA = getColumn(df, 'a');
        const colB = getColumn(df, 'b');
        if (!colA.ok || !colB.ok) throw new Error('Failed to get columns');

        setColumnValue(colA.data, 0, 10);
        setColumnValue(colA.data, 1, 20);
        setColumnValue(colB.data, 0, 1.5);
        setColumnValue(colB.data, 1, 2.5);

        const result = drop(df, { columns: ['b'] });
        expect(result.ok).toBe(true);

        if (!result.ok) return;
        const droppedDf = result.data;

        const resultA = getColumn(droppedDf, 'a');
        if (!resultA.ok) throw new Error('Failed to get result column');

        expect(resultA.data.view.getInt32(0, true)).toBe(10);
        expect(resultA.data.view.getInt32(4, true)).toBe(20);
      });

      test('returns error when dropping all columns', () => {
        const df = createDataFrame();
        addColumn(df, 'a', DType.Int32, 2);
        addColumn(df, 'b', DType.Float64, 2);

        const result = drop(df, { columns: ['a', 'b'] });
        expect(result.ok).toBe(false);

        if (result.ok) return;
        expect(result.error.message).toContain('Cannot drop all columns');
      });
    });

    describe('dropping rows', () => {
      test('drops specified row indices', () => {
        const df = createDataFrame();
        addColumn(df, 'a', DType.Int32, 4);

        const colA = getColumn(df, 'a');
        if (!colA.ok) throw new Error('Failed to get column');

        setColumnValue(colA.data, 0, 10);
        setColumnValue(colA.data, 1, 20);
        setColumnValue(colA.data, 2, 30);
        setColumnValue(colA.data, 3, 40);

        const result = drop(df, { index: [1, 3] });
        expect(result.ok).toBe(true);

        if (!result.ok) return;
        const droppedDf = result.data;

        expect(getRowCount(droppedDf)).toBe(2);

        const resultA = getColumn(droppedDf, 'a');
        if (!resultA.ok) throw new Error('Failed to get result column');

        expect(resultA.data.view.getInt32(0, true)).toBe(10);
        expect(resultA.data.view.getInt32(4, true)).toBe(30);
      });

      test('drops multiple rows and preserves column structure', () => {
        const df = createDataFrame();
        addColumn(df, 'a', DType.Int32, 5);
        addColumn(df, 'b', DType.Float64, 5);

        const colA = getColumn(df, 'a');
        const colB = getColumn(df, 'b');
        if (!colA.ok || !colB.ok) throw new Error('Failed to get columns');

        for (let i = 0; i < 5; i++) {
          setColumnValue(colA.data, i, (i + 1) * 10);
          setColumnValue(colB.data, i, (i + 1) * 1.5);
        }

        const result = drop(df, { index: [0, 2, 4] });
        expect(result.ok).toBe(true);

        if (!result.ok) return;
        const droppedDf = result.data;

        expect(getRowCount(droppedDf)).toBe(2);
        expect(getColumnNames(droppedDf)).toEqual(['a', 'b']);

        const resultA = getColumn(droppedDf, 'a');
        const resultB = getColumn(droppedDf, 'b');
        if (!resultA.ok || !resultB.ok) throw new Error('Failed to get result columns');

        expect(resultA.data.view.getInt32(0, true)).toBe(20);
        expect(resultA.data.view.getInt32(4, true)).toBe(40);
        expect(resultB.data.view.getFloat64(0, true)).toBe(3.0);
        expect(resultB.data.view.getFloat64(8, true)).toBe(6.0);
      });

      test('returns error for out-of-bounds indices', () => {
        const df = createDataFrame();
        addColumn(df, 'a', DType.Int32, 3);

        const result = drop(df, { index: [5] });
        expect(result.ok).toBe(false);

        if (result.ok) return;
        expect(result.error.message).toContain('out of bounds');
      });

      test('returns empty DataFrame when dropping all rows', () => {
        const df = createDataFrame();
        addColumn(df, 'a', DType.Int32, 2);
        addColumn(df, 'b', DType.Float64, 2);

        const result = drop(df, { index: [0, 1] });
        expect(result.ok).toBe(true);

        if (!result.ok) return;
        const droppedDf = result.data;

        expect(getRowCount(droppedDf)).toBe(0);
        expect(getColumnNames(droppedDf)).toEqual(['a', 'b']);
      });
    });

    describe('dropping both columns and rows', () => {
      test('drops both columns and rows', () => {
        const df = createDataFrame();
        addColumn(df, 'a', DType.Int32, 3);
        addColumn(df, 'b', DType.Float64, 3);
        addColumn(df, 'c', DType.String, 3);

        const colA = getColumn(df, 'a');
        if (!colA.ok) throw new Error('Failed to get column');

        setColumnValue(colA.data, 0, 10);
        setColumnValue(colA.data, 1, 20);
        setColumnValue(colA.data, 2, 30);

        const result = drop(df, { columns: ['c'], index: [1] });
        expect(result.ok).toBe(true);

        if (!result.ok) return;
        const droppedDf = result.data;

        expect(getRowCount(droppedDf)).toBe(2);
        expect(getColumnNames(droppedDf)).toEqual(['a', 'b']);

        const resultA = getColumn(droppedDf, 'a');
        if (!resultA.ok) throw new Error('Failed to get result column');

        expect(resultA.data.view.getInt32(0, true)).toBe(10);
        expect(resultA.data.view.getInt32(4, true)).toBe(30);
      });
    });

    test('returns error when neither columns nor index specified', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);

      const result = drop(df, {});
      expect(result.ok).toBe(false);

      if (result.ok) return;
      expect(result.error.message).toContain('Must specify either columns or index');
    });
  });

  describe('rename()', () => {
    test('renames single column', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);
      addColumn(df, 'b', DType.Float64, 2);

      const result = rename(df, { a: 'x' });
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const renamedDf = result.data;

      expect(getColumnNames(renamedDf)).toEqual(['x', 'b']);
    });

    test('renames multiple columns', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);
      addColumn(df, 'b', DType.Float64, 2);
      addColumn(df, 'c', DType.String, 2);

      const result = rename(df, { a: 'alpha', c: 'charlie' });
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const renamedDf = result.data;

      expect(getColumnNames(renamedDf)).toEqual(['alpha', 'b', 'charlie']);
    });

    test('preserves data after rename', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);

      const colA = getColumn(df, 'a');
      if (!colA.ok) throw new Error('Failed to get column');

      setColumnValue(colA.data, 0, 100);
      setColumnValue(colA.data, 1, 200);

      const result = rename(df, { a: 'new_name' });
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const renamedDf = result.data;

      const renamedCol = getColumn(renamedDf, 'new_name');
      if (!renamedCol.ok) throw new Error('Failed to get renamed column');

      expect(renamedCol.data.view.getInt32(0, true)).toBe(100);
      expect(renamedCol.data.view.getInt32(4, true)).toBe(200);
    });

    test('returns error for duplicate column names', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);
      addColumn(df, 'b', DType.Float64, 2);

      const result = rename(df, { a: 'b' }); // Rename 'a' to 'b' but 'b' already exists
      expect(result.ok).toBe(false);

      if (result.ok) return;
      expect(result.error.message).toContain('Duplicate column name');
    });

    test('handles empty rename mapping', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);
      addColumn(df, 'b', DType.Float64, 2);

      const result = rename(df, {});
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const renamedDf = result.data;

      // Should preserve original names
      expect(getColumnNames(renamedDf)).toEqual(['a', 'b']);
    });

    test('preserves column order', () => {
      const df = createDataFrame();
      addColumn(df, 'first', DType.Int32, 2);
      addColumn(df, 'second', DType.Float64, 2);
      addColumn(df, 'third', DType.String, 2);

      const result = rename(df, { second: 'middle' });
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const renamedDf = result.data;

      expect(getColumnNames(renamedDf)).toEqual(['first', 'middle', 'third']);
    });
  });
});
