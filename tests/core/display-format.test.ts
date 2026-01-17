import { describe, expect, test } from 'bun:test';
import { DataFrame } from '../../src/core/dataframe';
import {
  formatDataFrame,
  formatRow,
  formatValue,
  padCenter,
} from '../../src/core/dataframe/display';
import { m } from '../../src/core/types';

const df = DataFrame.from({ a: m.int32(), b: m.float64(), c: m.string() }, [
  { a: 1, b: 1.234567, c: 'long-string-value' },
  { a: 2, b: Number.NaN, c: 'short' },
]);

describe('display helpers', () => {
  test('formatValue handles nulls, NaN, integers, floats', () => {
    expect(formatValue(null)).toBe('null');
    expect(formatValue(Number.NaN)).toBe('NaN');
    expect(formatValue(42)).toBe('42');
    expect(formatValue(3.5)).toBe('3.5000');
  });

  test('padCenter centers text within width', () => {
    expect(padCenter('ab', 6)).toBe('  ab  ');
  });

  test('formatRow truncates wide values with ellipsis', () => {
    const ctx = df as unknown as Parameters<typeof formatRow>[0];
    const widths = [3, 4, 5];
    const rowStr = formatRow(ctx, 0, widths);

    expect(rowStr).toContain('…');
  });

  test('formatDataFrame limits rows and shows shape', () => {
    const formatted = formatDataFrame(df as unknown as Parameters<typeof formatDataFrame>[0]);
    expect(formatted).toContain('[2 rows × 3 columns]');
  });
});
