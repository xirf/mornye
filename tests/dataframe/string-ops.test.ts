import { describe, expect, it } from 'bun:test';
import {
  from,
  getColumn,
  getColumnNames,
  getColumnValue,
  getString,
  isNull,
  strContains,
  strEndsWith,
  strLen,
  strLower,
  strReplace,
  strStartsWith,
  strStrip,
  strUpper,
} from '../../src';
import { DType } from '../../src/types/dtypes';

describe('String Operations', () => {
  describe('strLower()', () => {
    it('should convert strings to lowercase', () => {
      const dfResult = from({
        name: { data: ['ALICE', 'Bob', 'CHARLIE'], dtype: DType.String },
        value: { data: [1, 2, 3], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strLower(dfResult.data, 'name');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const df = result.data;
      expect(getColumnNames(df)).toEqual(['name', 'value']);

      const nameCol = getColumn(df, 'name');
      expect(nameCol.ok).toBe(true);
      if (!nameCol.ok) return;

      const id0 = getColumnValue(nameCol.data, 0);
      const id1 = getColumnValue(nameCol.data, 1);
      const id2 = getColumnValue(nameCol.data, 2);

      expect(getString(df.dictionary!, Number(id0))).toBe('alice');
      expect(getString(df.dictionary!, Number(id1))).toBe('bob');
      expect(getString(df.dictionary!, Number(id2))).toBe('charlie');
    });

    it('should preserve nulls', () => {
      const dfResult = from({
        name: { data: ['ALICE', null, 'CHARLIE'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strLower(dfResult.data, 'name');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const nameCol = getColumn(result.data, 'name');
      expect(nameCol.ok).toBe(true);
      if (!nameCol.ok) return;

      // Check null bitmap
      expect(nameCol.data.nullBitmap).toBeDefined();
      if (!nameCol.data.nullBitmap) return;

      expect(isNull(nameCol.data.nullBitmap, 1)).toBe(true);
    });

    it('should return error for non-string column', () => {
      const dfResult = from({
        value: { data: [1, 2, 3], dtype: DType.Int32 },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strLower(dfResult.data, 'value');
      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.error.message).toContain('String type');
    });
  });

  describe('strUpper()', () => {
    it('should convert strings to uppercase', () => {
      const dfResult = from({
        name: { data: ['alice', 'Bob', 'charlie'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strUpper(dfResult.data, 'name');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const nameCol = getColumn(result.data, 'name');
      expect(nameCol.ok).toBe(true);
      if (!nameCol.ok) return;

      const id0 = getColumnValue(nameCol.data, 0);
      const id1 = getColumnValue(nameCol.data, 1);
      const id2 = getColumnValue(nameCol.data, 2);

      expect(getString(result.data.dictionary!, Number(id0))).toBe('ALICE');
      expect(getString(result.data.dictionary!, Number(id1))).toBe('BOB');
      expect(getString(result.data.dictionary!, Number(id2))).toBe('CHARLIE');
    });
  });

  describe('strStrip()', () => {
    it('should trim whitespace from strings', () => {
      const dfResult = from({
        name: { data: ['  alice  ', 'bob', ' charlie '], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strStrip(dfResult.data, 'name');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const nameCol = getColumn(result.data, 'name');
      expect(nameCol.ok).toBe(true);
      if (!nameCol.ok) return;

      const id0 = getColumnValue(nameCol.data, 0);
      const id1 = getColumnValue(nameCol.data, 1);
      const id2 = getColumnValue(nameCol.data, 2);

      expect(getString(result.data.dictionary!, Number(id0))).toBe('alice');
      expect(getString(result.data.dictionary!, Number(id1))).toBe('bob');
      expect(getString(result.data.dictionary!, Number(id2))).toBe('charlie');
    });
  });

  describe('strContains()', () => {
    it('should check if strings contain substring', () => {
      const dfResult = from({
        email: {
          data: ['alice@example.com', 'bob@test.com', 'charlie@example.com'],
          dtype: DType.String,
        },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strContains(dfResult.data, 'email', 'example');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getColumnNames(result.data)).toContain('email_contains');

      const containsCol = getColumn(result.data, 'email_contains');
      expect(containsCol.ok).toBe(true);
      if (!containsCol.ok) return;

      expect(getColumnValue(containsCol.data, 0)).toBe(1); // true
      expect(getColumnValue(containsCol.data, 1)).toBe(0); // false
      expect(getColumnValue(containsCol.data, 2)).toBe(1); // true
    });

    it('should support case-insensitive search', () => {
      const dfResult = from({
        name: { data: ['ALICE', 'bob', 'Charlie'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strContains(dfResult.data, 'name', 'alice', false);
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const containsCol = getColumn(result.data, 'name_contains');
      expect(containsCol.ok).toBe(true);
      if (!containsCol.ok) return;

      expect(getColumnValue(containsCol.data, 0)).toBe(1); // ALICE matches
    });
  });

  describe('strStartsWith()', () => {
    it('should check if strings start with prefix', () => {
      const dfResult = from({
        name: { data: ['alice', 'bob', 'alex'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strStartsWith(dfResult.data, 'name', 'al');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const startsCol = getColumn(result.data, 'name_startswith');
      expect(startsCol.ok).toBe(true);
      if (!startsCol.ok) return;

      expect(getColumnValue(startsCol.data, 0)).toBe(1); // alice
      expect(getColumnValue(startsCol.data, 1)).toBe(0); // bob
      expect(getColumnValue(startsCol.data, 2)).toBe(1); // alex
    });
  });

  describe('strEndsWith()', () => {
    it('should check if strings end with suffix', () => {
      const dfResult = from({
        filename: { data: ['doc.pdf', 'image.png', 'text.pdf'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strEndsWith(dfResult.data, 'filename', '.pdf');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const endsCol = getColumn(result.data, 'filename_endswith');
      expect(endsCol.ok).toBe(true);
      if (!endsCol.ok) return;

      expect(getColumnValue(endsCol.data, 0)).toBe(1); // doc.pdf
      expect(getColumnValue(endsCol.data, 1)).toBe(0); // image.png
      expect(getColumnValue(endsCol.data, 2)).toBe(1); // text.pdf
    });
  });

  describe('strReplace()', () => {
    it('should replace substrings', () => {
      const dfResult = from({
        text: { data: ['hello world', 'world peace', 'hello'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strReplace(dfResult.data, 'text', 'world', 'universe');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const textCol = getColumn(result.data, 'text');
      expect(textCol.ok).toBe(true);
      if (!textCol.ok) return;

      const id0 = getColumnValue(textCol.data, 0);
      const id1 = getColumnValue(textCol.data, 1);
      const id2 = getColumnValue(textCol.data, 2);

      expect(getString(result.data.dictionary!, Number(id0))).toBe('hello universe');
      expect(getString(result.data.dictionary!, Number(id1))).toBe('universe peace');
      expect(getString(result.data.dictionary!, Number(id2))).toBe('hello');
    });
  });

  describe('strLen()', () => {
    it('should get string lengths', () => {
      const dfResult = from({
        name: { data: ['alice', 'bob', 'charlie'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strLen(dfResult.data, 'name');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      expect(getColumnNames(result.data)).toContain('name_len');

      const lenCol = getColumn(result.data, 'name_len');
      expect(lenCol.ok).toBe(true);
      if (!lenCol.ok) return;

      expect(getColumnValue(lenCol.data, 0)).toBe(5); // alice
      expect(getColumnValue(lenCol.data, 1)).toBe(3); // bob
      expect(getColumnValue(lenCol.data, 2)).toBe(7); // charlie
    });

    it('should handle nulls', () => {
      const dfResult = from({
        name: { data: ['alice', null, 'charlie'], dtype: DType.String },
      });
      expect(dfResult.ok).toBe(true);
      if (!dfResult.ok) return;

      const result = strLen(dfResult.data, 'name');
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      const lenCol = getColumn(result.data, 'name_len');
      expect(lenCol.ok).toBe(true);
      if (!lenCol.data) return;

      // Check null bitmap
      expect(lenCol.data.nullBitmap).toBeDefined();
      if (!lenCol.data.nullBitmap) return;

      expect(isNull(lenCol.data.nullBitmap, 1)).toBe(true);
    });
  });
});
