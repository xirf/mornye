import { describe, expect, it } from 'bun:test';
import { concat, from, getColumn, getColumnNames, getRowCount, merge } from '../../src';
import { DType } from '../../src/types/dtypes';

// Helper to unwrap Result or throw
function unwrap<T>(result: { ok: boolean; data?: T; error?: Error }): T {
  if (!result.ok) throw new Error(result.error?.message || 'Result failed');
  return result.data!;
}

describe('Join Operations', () => {
  describe('merge()', () => {
    it('should perform inner join on single column', () => {
      const left = unwrap(
        from({
          id: { data: [1, 2, 3], dtype: DType.Int32 },
          name: { data: ['Alice', 'Bob', 'Charlie'], dtype: DType.String },
        }),
      );

      const right = unwrap(
        from({
          id: { data: [2, 3, 4], dtype: DType.Int32 },
          age: { data: [25, 30, 35], dtype: DType.Int32 },
        }),
      );

      const result = merge(left, right, { on: 'id', how: 'inner' });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(2); // Only rows with id 2 and 3
      expect(getColumnNames(df)).toEqual(['id', 'name', 'age']);

      const idCol = getColumn(df, 'id');
      expect(idCol.ok).toBe(true);
      if (!idCol.ok) return;

      const view = new Int32Array(
        idCol.data.data.buffer,
        idCol.data.data.byteOffset,
        idCol.data.length,
      );
      expect([...view]).toEqual([2, 3]);
    });

    it('should perform left join preserving all left rows', () => {
      const left = unwrap(
        from({
          id: { data: [1, 2, 3], dtype: DType.Int32 },
          name: { data: ['Alice', 'Bob', 'Charlie'], dtype: DType.String },
        }),
      );

      const right = unwrap(
        from({
          id: { data: [2, 4], dtype: DType.Int32 },
          age: { data: [25, 35], dtype: DType.Int32 },
        }),
      );

      const result = merge(left, right, { on: 'id', how: 'left' });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(3); // All left rows
      expect(getColumnNames(df)).toEqual(['id', 'name', 'age']);
    });

    it('should perform right join preserving all right rows', () => {
      const left = unwrap(
        from({
          id: { data: [1, 2], dtype: DType.Int32 },
          name: { data: ['Alice', 'Bob'], dtype: DType.String },
        }),
      );

      const right = unwrap(
        from({
          id: { data: [2, 3, 4], dtype: DType.Int32 },
          age: { data: [25, 30, 35], dtype: DType.Int32 },
        }),
      );

      const result = merge(left, right, { on: 'id', how: 'right' });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(3); // All right rows
      expect(getColumnNames(df)).toEqual(['id', 'name', 'age']);
    });

    it('should perform outer join including all rows', () => {
      const left = unwrap(
        from({
          id: { data: [1, 2], dtype: DType.Int32 },
          name: { data: ['Alice', 'Bob'], dtype: DType.String },
        }),
      );

      const right = unwrap(
        from({
          id: { data: [2, 3], dtype: DType.Int32 },
          age: { data: [25, 30], dtype: DType.Int32 },
        }),
      );

      const result = merge(left, right, { on: 'id', how: 'outer' });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(3); // All unique ids: 1, 2, 3
      expect(getColumnNames(df)).toEqual(['id', 'name', 'age']);
    });

    it('should join on multiple columns', () => {
      const left = unwrap(
        from({
          year: { data: [2020, 2020, 2021], dtype: DType.Int32 },
          month: { data: [1, 2, 1], dtype: DType.Int32 },
          sales: { data: [100, 200, 150], dtype: DType.Int32 },
        }),
      );

      const right = unwrap(
        from({
          year: { data: [2020, 2021], dtype: DType.Int32 },
          month: { data: [1, 1], dtype: DType.Int32 },
          target: { data: [120, 160], dtype: DType.Int32 },
        }),
      );

      const result = merge(left, right, {
        on: ['year', 'month'],
        how: 'inner',
      });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(2); // (2020,1) and (2021,1)
      expect(getColumnNames(df)).toEqual(['year', 'month', 'sales', 'target']);
    });

    it('should use leftOn and rightOn for different column names', () => {
      const left = unwrap(
        from({
          user_id: { data: [1, 2, 3], dtype: DType.Int32 },
          name: { data: ['Alice', 'Bob', 'Charlie'], dtype: DType.String },
        }),
      );

      const right = unwrap(
        from({
          id: { data: [2, 3, 4], dtype: DType.Int32 },
          age: { data: [25, 30, 35], dtype: DType.Int32 },
        }),
      );

      const result = merge(left, right, {
        leftOn: 'user_id',
        rightOn: 'id',
        how: 'inner',
      });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(2);
      expect(getColumnNames(df)).toContain('user_id');
      expect(getColumnNames(df)).toContain('name');
      expect(getColumnNames(df)).toContain('age');
    });

    it('should handle suffixes for overlapping columns', () => {
      const left = unwrap(
        from({
          id: { data: [1, 2], dtype: DType.Int32 },
          value: { data: [10, 20], dtype: DType.Int32 },
        }),
      );

      const right = unwrap(
        from({
          id: { data: [1, 2], dtype: DType.Int32 },
          value: { data: [100, 200], dtype: DType.Int32 },
        }),
      );

      const result = merge(left, right, {
        on: 'id',
        how: 'inner',
        suffixes: ['_left', '_right'],
      });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getColumnNames(df)).toContain('value_left');
      expect(getColumnNames(df)).toContain('value_right');
    });

    it('should return error when key column missing', () => {
      const left = unwrap(
        from({
          id: { data: [1, 2], dtype: DType.Int32 },
        }),
      );

      const right = unwrap(
        from({
          name: { data: ['Alice', 'Bob'], dtype: DType.String },
        }),
      );

      const result = merge(left, right, { on: 'id', how: 'inner' });

      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.error.message).toContain('missing');
    });

    it('should handle empty DataFrames', () => {
      const left = unwrap(
        from({
          id: { data: [], dtype: DType.Int32 },
          name: { data: [], dtype: DType.String },
        }),
      );

      const right = unwrap(
        from({
          id: { data: [1], dtype: DType.Int32 },
          age: { data: [25], dtype: DType.Int32 },
        }),
      );

      const result = merge(left, right, { on: 'id', how: 'inner' });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(0);
    });

    it('should handle joins with Float64 keys', () => {
      const left = unwrap(
        from({
          price: { data: [1.5, 2.5, 3.5], dtype: DType.Float64 },
          item: { data: ['A', 'B', 'C'], dtype: DType.String },
        }),
      );

      const right = unwrap(
        from({
          price: { data: [2.5, 3.5], dtype: DType.Float64 },
          quantity: { data: [10, 20], dtype: DType.Int32 },
        }),
      );

      const result = merge(left, right, { on: 'price', how: 'inner' });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(2);
    });
  });

  describe('concat()', () => {
    it('should concatenate DataFrames vertically (axis=0)', () => {
      const df1 = unwrap(
        from({
          id: { data: [1, 2], dtype: DType.Int32 },
          name: { data: ['Alice', 'Bob'], dtype: DType.String },
        }),
      );

      const df2 = unwrap(
        from({
          id: { data: [3, 4], dtype: DType.Int32 },
          name: { data: ['Charlie', 'David'], dtype: DType.String },
        }),
      );

      const result = concat([df1, df2], { axis: 0 });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(4);
      expect(getColumnNames(df)).toEqual(['id', 'name']);

      const idCol = getColumn(df, 'id');
      expect(idCol.ok).toBe(true);
      if (!idCol.ok) return;

      const view = new Int32Array(
        idCol.data.data.buffer,
        idCol.data.data.byteOffset,
        idCol.data.length,
      );
      expect([...view]).toEqual([1, 2, 3, 4]);
    });

    it('should concatenate DataFrames horizontally (axis=1)', () => {
      const df1 = unwrap(
        from({
          id: { data: [1, 2], dtype: DType.Int32 },
          name: { data: ['Alice', 'Bob'], dtype: DType.String },
        }),
      );

      const df2 = unwrap(
        from({
          age: { data: [25, 30], dtype: DType.Int32 },
          city: { data: ['NYC', 'LA'], dtype: DType.String },
        }),
      );

      const result = concat([df1, df2], { axis: 1 });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(2);
      expect(getColumnNames(df)).toEqual(['id', 'name', 'age', 'city']);
    });

    it('should concatenate multiple DataFrames vertically', () => {
      const df1 = unwrap(
        from({
          value: { data: [1], dtype: DType.Int32 },
        }),
      );

      const df2 = unwrap(
        from({
          value: { data: [2], dtype: DType.Int32 },
        }),
      );

      const df3 = unwrap(
        from({
          value: { data: [3], dtype: DType.Int32 },
        }),
      );

      const result = concat([df1, df2, df3], { axis: 0 });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(3);
    });

    it('should return error when vertical concat has mismatched schemas', () => {
      const df1 = unwrap(
        from({
          id: { data: [1], dtype: DType.Int32 },
        }),
      );

      const df2 = unwrap(
        from({
          name: { data: ['Alice'], dtype: DType.String },
        }),
      );

      const result = concat([df1, df2], { axis: 0 });

      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.error.message).toContain('different columns');
    });

    it('should return error when horizontal concat has mismatched row counts', () => {
      const df1 = unwrap(
        from({
          id: { data: [1, 2], dtype: DType.Int32 },
        }),
      );

      const df2 = unwrap(
        from({
          name: { data: ['Alice'], dtype: DType.String },
        }),
      );

      const result = concat([df1, df2], { axis: 1 });

      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.error.message).toContain('rows');
    });

    it('should return error when horizontal concat has duplicate columns', () => {
      const df1 = unwrap(
        from({
          id: { data: [1], dtype: DType.Int32 },
        }),
      );

      const df2 = unwrap(
        from({
          id: { data: [2], dtype: DType.Int32 },
        }),
      );

      const result = concat([df1, df2], { axis: 1 });

      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.error.message).toContain('Duplicate');
    });

    it('should handle empty array of DataFrames', () => {
      const result = concat([]);

      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.error.message).toContain('empty');
    });

    it('should handle single DataFrame', () => {
      const df = unwrap(
        from({
          id: { data: [1, 2], dtype: DType.Int32 },
        }),
      );

      const result = concat([df]);

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(2);
    });

    it('should concatenate with different dtypes per column', () => {
      const df1 = unwrap(
        from({
          int_col: { data: [1, 2], dtype: DType.Int32 },
          float_col: { data: [1.5, 2.5], dtype: DType.Float64 },
          str_col: { data: ['a', 'b'], dtype: DType.String },
        }),
      );

      const df2 = unwrap(
        from({
          int_col: { data: [3, 4], dtype: DType.Int32 },
          float_col: { data: [3.5, 4.5], dtype: DType.Float64 },
          str_col: { data: ['c', 'd'], dtype: DType.String },
        }),
      );

      const result = concat([df1, df2], { axis: 0 });

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(4);

      const intCol = getColumn(df, 'int_col');
      expect(intCol.ok).toBe(true);
      if (!intCol.ok) return;
      expect(intCol.data.dtype).toBe(DType.Int32);

      const floatCol = getColumn(df, 'float_col');
      expect(floatCol.ok).toBe(true);
      if (!floatCol.ok) return;
      expect(floatCol.data.dtype).toBe(DType.Float64);
    });

    it('should default to vertical concatenation (axis=0)', () => {
      const df1 = unwrap(
        from({
          id: { data: [1], dtype: DType.Int32 },
        }),
      );

      const df2 = unwrap(
        from({
          id: { data: [2], dtype: DType.Int32 },
        }),
      );

      const result = concat([df1, df2]); // No axis specified

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(2);
    });
  });
});
