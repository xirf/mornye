import { describe, expect, it } from 'bun:test';
import {
  DType,
  append,
  dropDuplicates,
  duplicate,
  formatDataFrame,
  from,
  getColumn,
  getColumnNames,
  getColumnValue,
  getRowCount,
  isNull,
  join,
  unique,
} from '../../src';

describe('Row Operations', () => {
  describe('append()', () => {
    it('should append rows to DataFrame', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Bob'], dtype: DType.String },
        age: { data: [25, 30], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = append(dfResult.data, [
        { name: 2, age: 35 }, // Charlie (dict ID 2)
        { name: 3, age: 28 }, // Diana (dict ID 3)
      ]);

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(4);
      expect(getColumnNames(df)).toEqual(['name', 'age']);

      const ageCol = getColumn(df, 'age');
      expect(ageCol.ok).toBe(true);
      if (!ageCol.ok) return;

      expect(getColumnValue(ageCol.data, 0)).toBe(25);
      expect(getColumnValue(ageCol.data, 1)).toBe(30);
      expect(getColumnValue(ageCol.data, 2)).toBe(35);
      expect(getColumnValue(ageCol.data, 3)).toBe(28);
    });

    it('should handle null values in appended rows', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Bob'], dtype: DType.String },
        age: { data: [25, 30], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = append(dfResult.data, [
        { name: 2, age: null }, // Charlie with null age
      ]);

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const ageCol = getColumn(result.data, 'age');
      expect(ageCol.ok).toBe(true);
      if (!ageCol.ok) return;

      expect(ageCol.data.nullBitmap).toBeDefined();
      expect(isNull(ageCol.data.nullBitmap!, 2)).toBe(true);
    });

    it('should handle missing columns in appended rows', () => {
      const dfResult = from({
        name: { data: ['Alice'], dtype: DType.String },
        age: { data: [25], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = append(dfResult.data, [
        { name: 1 }, // Missing age column
      ]);

      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const ageCol = getColumn(result.data, 'age');
      expect(ageCol.ok).toBe(true);
      if (!ageCol.ok) return;

      expect(isNull(ageCol.data.nullBitmap!, 1)).toBe(true);
    });

    it('should return original DataFrame when appending empty array', () => {
      const dfResult = from({
        name: { data: ['Alice'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = append(dfResult.data, []);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(result.data).toBe(dfResult.data);
    });
  });

  describe('duplicate()', () => {
    it('should create deep copy of DataFrame', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Bob'], dtype: DType.String },
        age: { data: [25, 30], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = duplicate(dfResult.data);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const copy = result.data;

      // Same structure
      expect(getRowCount(copy)).toBe(getRowCount(dfResult.data));
      expect(getColumnNames(copy)).toEqual(getColumnNames(dfResult.data));

      // Same values
      const ageCol = getColumn(copy, 'age');
      expect(ageCol.ok).toBe(true);
      if (!ageCol.ok) return;

      expect(getColumnValue(ageCol.data, 0)).toBe(25);
      expect(getColumnValue(ageCol.data, 1)).toBe(30);

      // Different DataFrame object
      expect(copy).not.toBe(dfResult.data);
    });

    it('should share dictionary by default', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Bob'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = duplicate(dfResult.data, true);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(result.data.dictionary).toBe(dfResult.data.dictionary);
    });

    it('should deep copy dictionary when requested', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Bob'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = duplicate(dfResult.data, false);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(result.data.dictionary).not.toBe(dfResult.data.dictionary);
      expect(result.data.dictionary!.idToString.size).toBe(
        dfResult.data.dictionary!.idToString.size,
      );
    });

    it('should preserve null bitmaps', () => {
      const dfResult = from({
        name: { data: ['Alice', null, 'Charlie'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = duplicate(dfResult.data);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const nameCol = getColumn(result.data, 'name');
      expect(nameCol.ok).toBe(true);
      if (!nameCol.ok) return;

      expect(nameCol.data.nullBitmap).toBeDefined();
      expect(isNull(nameCol.data.nullBitmap!, 1)).toBe(true);
    });
  });

  describe('dropDuplicates()', () => {
    it('should drop duplicate rows (keep first)', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Bob', 'Alice', 'Charlie'], dtype: DType.String },
        age: { data: [25, 30, 25, 35], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = dropDuplicates(dfResult.data);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(3); // Alice, Bob, Charlie

      const nameCol = getColumn(result.data, 'name');
      expect(nameCol.ok).toBe(true);
      if (!nameCol.ok) return;

      // Should keep first Alice (index 0) and drop second (index 2)
      expect(getColumnValue(nameCol.data, 0)).toBe(0); // Alice dict ID
      expect(getColumnValue(nameCol.data, 1)).toBe(1); // Bob dict ID
      expect(getColumnValue(nameCol.data, 2)).toBe(2); // Charlie dict ID
    });

    it('should drop duplicate rows (keep last)', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Bob', 'Alice'], dtype: DType.String },
        age: { data: [25, 30, 25], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = dropDuplicates(dfResult.data, { keep: 'last' });
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(2); // Bob and second Alice

      const ageCol = getColumn(result.data, 'age');
      expect(ageCol.ok).toBe(true);
      if (!ageCol.ok) return;

      // Should have second Alice (25) then Bob (30) based on keepIndices order
      expect(getColumnValue(ageCol.data, 0)).toBe(25); // Second Alice
      expect(getColumnValue(ageCol.data, 1)).toBe(30); // Bob
    });

    it('should drop duplicates based on subset of columns', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Alice', 'Bob'], dtype: DType.String },
        age: { data: [25, 30, 30], dtype: DType.Int32 }, // Different ages
        city: { data: ['NYC', 'LA', 'NYC'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      // Drop duplicates based only on 'name'
      const result = dropDuplicates(dfResult.data, { subset: ['name'] });
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(2); // Alice (first), Bob

      const ageCol = getColumn(result.data, 'age');
      expect(ageCol.ok).toBe(true);
      if (!ageCol.ok) return;

      expect(getColumnValue(ageCol.data, 0)).toBe(25); // First Alice with age 25
      expect(getColumnValue(ageCol.data, 1)).toBe(30); // Bob
    });

    it('should handle DataFrame with no duplicates', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Bob', 'Charlie'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = dropDuplicates(dfResult.data);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(3); // All rows kept
    });

    it('should handle null values in deduplication', () => {
      const dfResult = from({
        name: { data: ['Alice', null, null, 'Bob'], dtype: DType.String },
        age: { data: [25, 30, 30, 35], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = dropDuplicates(dfResult.data);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      // Two nulls with same age should deduplicate
      expect(getRowCount(result.data)).toBe(3); // Alice, one null, Bob
    });

    it('should return error for invalid subset column', () => {
      const dfResult = from({
        name: { data: ['Alice'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = dropDuplicates(dfResult.data, { subset: ['invalid'] });
      expect(result.ok).toBe(false);
      if (result.ok) return;

      expect(result.error.message).toContain('not found');
    });
  });

  describe('unique()', () => {
    it('should return unique rows', () => {
      const dfResult = from({
        name: { data: ['Alice', 'Bob', 'Alice', 'Charlie', 'Bob'], dtype: DType.String },
        age: { data: [25, 30, 25, 35, 30], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = unique(dfResult.data);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(3); // Alice, Bob, Charlie
    });

    it('should be equivalent to dropDuplicates with all columns', () => {
      const dfResult = from({
        x: { data: [1, 2, 1, 3], dtype: DType.Int32 },
        y: { data: [10, 20, 10, 30], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const uniqueResult = unique(dfResult.data);
      const dropDupResult = dropDuplicates(dfResult.data);

      expect(uniqueResult.ok).toBe(true);
      expect(dropDupResult.ok).toBe(true);
      if (!uniqueResult.ok || !dropDupResult.ok) return;

      expect(getRowCount(uniqueResult.data)).toBe(getRowCount(dropDupResult.data));
    });
  });

  describe('join()', () => {
    it('should join DataFrames on index (left join)', () => {
      const leftResult = from({
        name: { data: ['Alice', 'Bob', 'Charlie'], dtype: DType.String },
        age: { data: [25, 30, 35], dtype: DType.Int32 },
      });
      const rightResult = from({
        city: { data: ['NYC', 'LA'], dtype: DType.String },
        salary: { data: [100000, 120000], dtype: DType.Int32 },
      });

      expect(leftResult.ok && rightResult.ok).toBe(true);
      if (!leftResult.ok || !rightResult.ok) return;

      const result = join(leftResult.data, rightResult.data, { how: 'left' });
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getRowCount(df)).toBe(3); // Left join keeps all left rows

      const cols = getColumnNames(df);
      expect(cols).toContain('name');
      expect(cols).toContain('city');
      expect(cols).not.toContain('__index__'); // Index column removed

      // Third row should have nulls from right DataFrame
      const cityCol = getColumn(df, 'city');
      expect(cityCol.ok).toBe(true);
      if (!cityCol.ok) return;

      expect(cityCol.data.nullBitmap).toBeDefined();
      expect(isNull(cityCol.data.nullBitmap!, 2)).toBe(true); // Charlie has no match
    });

    it('should join DataFrames on index (inner join)', () => {
      const leftResult = from({
        name: { data: ['Alice', 'Bob', 'Charlie'], dtype: DType.String },
      });
      const rightResult = from({
        city: { data: ['NYC', 'LA'], dtype: DType.String },
      });

      expect(leftResult.ok && rightResult.ok).toBe(true);
      if (!leftResult.ok || !rightResult.ok) return;

      const result = join(leftResult.data, rightResult.data, { how: 'inner' });
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(2); // Only matching rows (0, 1)
    });

    it('should handle suffix conflicts in join', () => {
      const leftResult = from({
        value: { data: [1, 2, 3], dtype: DType.Int32 },
      });
      const rightResult = from({
        value: { data: [10, 20], dtype: DType.Int32 },
      });

      expect(leftResult.ok && rightResult.ok).toBe(true);
      if (!leftResult.ok || !rightResult.ok) return;

      const result = join(leftResult.data, rightResult.data, {
        how: 'inner',
        suffixes: ['_left', '_right'],
      });
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const cols = getColumnNames(result.data);
      expect(cols).toContain('value_left');
      expect(cols).toContain('value_right');
    });

    it('should default to left join', () => {
      const leftResult = from({
        x: { data: [1, 2, 3], dtype: DType.Int32 },
      });
      const rightResult = from({
        y: { data: [10, 20], dtype: DType.Int32 },
      });

      expect(leftResult.ok && rightResult.ok).toBe(true);
      if (!leftResult.ok || !rightResult.ok) return;

      const result = join(leftResult.data, rightResult.data); // No options
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getRowCount(result.data)).toBe(3); // Left join behavior
    });
  });
});
