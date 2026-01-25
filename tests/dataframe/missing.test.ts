import { describe, expect, test } from 'bun:test';
import { setColumnValue } from '../../src/core/column';
import { enableNullTracking } from '../../src/core/column';
import { addColumn, createDataFrame, getColumn, getRowCount } from '../../src/dataframe/dataframe';
import { dropna, fillna, isna, notna } from '../../src/dataframe/missing';
import { DType } from '../../src/types/dtypes';
import { isNull, setNull } from '../../src/utils/nulls';

describe('Missing Data Operations', () => {
  describe('isna()', () => {
    test('returns boolean DataFrame indicating null positions', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 3);
      addColumn(df, 'b', DType.Float64, 3);

      // Get columns and set values
      const colA = getColumn(df, 'a');
      const colB = getColumn(df, 'b');

      if (!colA.ok || !colB.ok) throw new Error('Failed to get columns');

      // Enable null tracking and mark some values as null
      enableNullTracking(colA.data);
      enableNullTracking(colB.data);

      setColumnValue(colA.data, 0, 1);
      setColumnValue(colA.data, 1, 2);
      setNull(colA.data.nullBitmap!, 2); // Mark index 2 as null

      setColumnValue(colB.data, 0, 1.5);
      setNull(colB.data.nullBitmap!, 1); // Mark index 1 as null
      setColumnValue(colB.data, 2, 3.5);

      const result = isna(df);
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const nullDf = result.data;

      expect(getRowCount(nullDf)).toBe(3);

      const nullA = getColumn(nullDf, 'a');
      const nullB = getColumn(nullDf, 'b');

      if (!nullA.ok || !nullB.ok) throw new Error('Failed to get null columns');

      // Check null indicators
      expect(nullA.data.view.getUint8(0)).toBe(0); // Not null
      expect(nullA.data.view.getUint8(1)).toBe(0); // Not null
      expect(nullA.data.view.getUint8(2)).toBe(1); // Null

      expect(nullB.data.view.getUint8(0)).toBe(0); // Not null
      expect(nullB.data.view.getUint8(1)).toBe(1); // Null
      expect(nullB.data.view.getUint8(2)).toBe(0); // Not null
    });

    test('handles DataFrame without null bitmaps', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 3);

      const result = isna(df);
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const nullDf = result.data;

      const nullA = getColumn(nullDf, 'a');
      if (!nullA.ok) throw new Error('Failed to get column');

      // All values should be not-null (0)
      expect(nullA.data.view.getUint8(0)).toBe(0);
      expect(nullA.data.view.getUint8(1)).toBe(0);
      expect(nullA.data.view.getUint8(2)).toBe(0);
    });
  });

  describe('notna()', () => {
    test('returns boolean DataFrame indicating non-null positions', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 3);

      const colA = getColumn(df, 'a');
      if (!colA.ok) throw new Error('Failed to get column');

      enableNullTracking(colA.data);
      setColumnValue(colA.data, 0, 1);
      setNull(colA.data.nullBitmap!, 1); // Mark as null
      setColumnValue(colA.data, 2, 3);

      const result = notna(df);
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const notNullDf = result.data;

      const notNullA = getColumn(notNullDf, 'a');
      if (!notNullA.ok) throw new Error('Failed to get column');

      expect(notNullA.data.view.getUint8(0)).toBe(1); // Not null
      expect(notNullA.data.view.getUint8(1)).toBe(0); // Null
      expect(notNullA.data.view.getUint8(2)).toBe(1); // Not null
    });
  });

  describe('dropna()', () => {
    test('drops rows with any null values by default', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 4);
      addColumn(df, 'b', DType.Float64, 4);

      const colA = getColumn(df, 'a');
      const colB = getColumn(df, 'b');
      if (!colA.ok || !colB.ok) throw new Error('Failed to get columns');

      enableNullTracking(colA.data);
      enableNullTracking(colB.data);

      // Row 0: a=1, b=1.0 (no nulls)
      setColumnValue(colA.data, 0, 1);
      setColumnValue(colB.data, 0, 1.0);

      // Row 1: a=null, b=2.0 (has null)
      setNull(colA.data.nullBitmap!, 1);
      setColumnValue(colB.data, 1, 2.0);

      // Row 2: a=3, b=3.0 (no nulls)
      setColumnValue(colA.data, 2, 3);
      setColumnValue(colB.data, 2, 3.0);

      // Row 3: a=4, b=null (has null)
      setColumnValue(colA.data, 3, 4);
      setNull(colB.data.nullBitmap!, 3);

      const result = dropna(df);
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const cleanDf = result.data;

      // Should keep only rows 0 and 2
      expect(getRowCount(cleanDf)).toBe(2);

      const resultA = getColumn(cleanDf, 'a');
      const resultB = getColumn(cleanDf, 'b');
      if (!resultA.ok || !resultB.ok) throw new Error('Failed to get result columns');

      expect(resultA.data.view.getInt32(0, true)).toBe(1);
      expect(resultA.data.view.getInt32(4, true)).toBe(3);

      expect(resultB.data.view.getFloat64(0, true)).toBe(1.0);
      expect(resultB.data.view.getFloat64(8, true)).toBe(3.0);
    });

    test('drops rows where all values are null with how="all"', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 3);
      addColumn(df, 'b', DType.Float64, 3);

      const colA = getColumn(df, 'a');
      const colB = getColumn(df, 'b');
      if (!colA.ok || !colB.ok) throw new Error('Failed to get columns');

      enableNullTracking(colA.data);
      enableNullTracking(colB.data);

      // Row 0: a=1, b=null (not all null)
      setColumnValue(colA.data, 0, 1);
      setNull(colB.data.nullBitmap!, 0);

      // Row 1: a=null, b=null (all null)
      setNull(colA.data.nullBitmap!, 1);
      setNull(colB.data.nullBitmap!, 1);

      // Row 2: a=3, b=3.0 (no nulls)
      setColumnValue(colA.data, 2, 3);
      setColumnValue(colB.data, 2, 3.0);

      const result = dropna(df, { how: 'all' });
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const cleanDf = result.data;

      // Should keep rows 0 and 2 (drop only row 1)
      expect(getRowCount(cleanDf)).toBe(2);
    });

    test('drops rows based on subset of columns', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);
      addColumn(df, 'b', DType.Float64, 2);

      const colA = getColumn(df, 'a');
      const colB = getColumn(df, 'b');
      if (!colA.ok || !colB.ok) throw new Error('Failed to get columns');

      enableNullTracking(colA.data);
      enableNullTracking(colB.data);

      // Row 0: a=1, b=null
      setColumnValue(colA.data, 0, 1);
      setNull(colB.data.nullBitmap!, 0);

      // Row 1: a=null, b=2.0
      setNull(colA.data.nullBitmap!, 1);
      setColumnValue(colB.data, 1, 2.0);

      // Only check column 'a' for nulls
      const result = dropna(df, { subset: ['a'] });
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const cleanDf = result.data;

      // Should keep only row 0 (where 'a' is not null)
      expect(getRowCount(cleanDf)).toBe(1);
    });

    test('returns empty DataFrame when all rows dropped', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);

      const colA = getColumn(df, 'a');
      if (!colA.ok) throw new Error('Failed to get column');

      enableNullTracking(colA.data);
      setNull(colA.data.nullBitmap!, 0);
      setNull(colA.data.nullBitmap!, 1);

      const result = dropna(df);
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const cleanDf = result.data;

      expect(getRowCount(cleanDf)).toBe(0);
      expect(cleanDf.columnOrder).toEqual(['a']);
    });
  });

  describe('fillna()', () => {
    test('fills null values with single value', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 3);

      const colA = getColumn(df, 'a');
      if (!colA.ok) throw new Error('Failed to get column');

      enableNullTracking(colA.data);

      setColumnValue(colA.data, 0, 1);
      setNull(colA.data.nullBitmap!, 1);
      setColumnValue(colA.data, 2, 3);

      const result = fillna(df, 999);
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const filledDf = result.data;

      const resultA = getColumn(filledDf, 'a');
      if (!resultA.ok) throw new Error('Failed to get result column');

      expect(resultA.data.view.getInt32(0, true)).toBe(1);
      expect(resultA.data.view.getInt32(4, true)).toBe(999); // Filled
      expect(resultA.data.view.getInt32(8, true)).toBe(3);

      // Check that null bit is cleared
      if (resultA.data.nullBitmap) {
        expect(isNull(resultA.data.nullBitmap, 1)).toBe(false);
      }
    });

    test('fills null values with column-specific values', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);
      addColumn(df, 'b', DType.Float64, 2);

      const colA = getColumn(df, 'a');
      const colB = getColumn(df, 'b');
      if (!colA.ok || !colB.ok) throw new Error('Failed to get columns');

      enableNullTracking(colA.data);
      enableNullTracking(colB.data);

      setNull(colA.data.nullBitmap!, 0);
      setColumnValue(colA.data, 1, 2);

      setColumnValue(colB.data, 0, 1.5);
      setNull(colB.data.nullBitmap!, 1);

      const result = fillna(df, { a: 100, b: 99.9 });
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const filledDf = result.data;

      const resultA = getColumn(filledDf, 'a');
      const resultB = getColumn(filledDf, 'b');
      if (!resultA.ok || !resultB.ok) throw new Error('Failed to get result columns');

      expect(resultA.data.view.getInt32(0, true)).toBe(100);
      expect(resultA.data.view.getInt32(4, true)).toBe(2);

      expect(resultB.data.view.getFloat64(0, true)).toBe(1.5);
      expect(resultB.data.view.getFloat64(8, true)).toBe(99.9);
    });

    test('fills null values only in subset columns', () => {
      const df = createDataFrame();
      addColumn(df, 'a', DType.Int32, 2);
      addColumn(df, 'b', DType.Float64, 2);

      const colA = getColumn(df, 'a');
      const colB = getColumn(df, 'b');
      if (!colA.ok || !colB.ok) throw new Error('Failed to get columns');

      enableNullTracking(colA.data);
      enableNullTracking(colB.data);

      setNull(colA.data.nullBitmap!, 0);
      setNull(colB.data.nullBitmap!, 0);

      const result = fillna(df, 999, { subset: ['a'] });
      expect(result.ok).toBe(true);

      if (!result.ok) return;
      const filledDf = result.data;

      const resultA = getColumn(filledDf, 'a');
      const resultB = getColumn(filledDf, 'b');
      if (!resultA.ok || !resultB.ok) throw new Error('Failed to get result columns');

      // Column 'a' should be filled
      expect(resultA.data.view.getInt32(0, true)).toBe(999);

      // Column 'b' should remain null
      if (resultB.data.nullBitmap) {
        expect(isNull(resultB.data.nullBitmap, 0)).toBe(true);
      }
    });
  });
});
