import { describe, expect, test } from 'bun:test';
import { setColumnValue } from '../../src/core/column';
import { addColumn, createDataFrame } from '../../src/dataframe/dataframe';
import { DType } from '../../src/types/dtypes';
import {
  type SortSpec,
  createRowIndices,
  findGroupBoundaries,
  isSorted,
  sortByColumn,
  sortByColumns,
} from '../../src/utils/sort';

describe('createRowIndices', () => {
  test('creates sequential indices', () => {
    const indices = createRowIndices(5);
    expect(indices).toEqual([0, 1, 2, 3, 4]);
  });

  test('handles zero rows', () => {
    const indices = createRowIndices(0);
    expect(indices).toEqual([]);
  });

  test('creates correct length array', () => {
    const indices = createRowIndices(100);
    expect(indices.length).toBe(100);
    expect(indices[0]).toBe(0);
    expect(indices[99]).toBe(99);
  });
});

describe('sortByColumn', () => {
  test('sorts Int32 column ascending', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 5);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 50);
    setColumnValue(col, 1, 10);
    setColumnValue(col, 2, 30);
    setColumnValue(col, 3, 40);
    setColumnValue(col, 4, 20);

    const indices = createRowIndices(5);
    sortByColumn(indices, col, 'asc');

    expect(indices).toEqual([1, 4, 2, 3, 0]); // Indices sorted by value
  });

  test('sorts Int32 column descending', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 5);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 50);
    setColumnValue(col, 1, 10);
    setColumnValue(col, 2, 30);
    setColumnValue(col, 3, 40);
    setColumnValue(col, 4, 20);

    const indices = createRowIndices(5);
    sortByColumn(indices, col, 'desc');

    expect(indices).toEqual([0, 3, 2, 4, 1]); // Indices sorted by value descending
  });

  test('sorts Float64 column ascending', () => {
    const df = createDataFrame();
    addColumn(df, 'prices', DType.Float64, 4);

    const col = df.columns.get('prices')!;
    setColumnValue(col, 0, 100.5);
    setColumnValue(col, 1, 50.2);
    setColumnValue(col, 2, 200.0);
    setColumnValue(col, 3, 75.8);

    const indices = createRowIndices(4);
    sortByColumn(indices, col, 'asc');

    expect(indices).toEqual([1, 3, 0, 2]);
  });

  test('sorts DateTime column ascending', () => {
    const df = createDataFrame();
    addColumn(df, 'timestamps', DType.DateTime, 3);

    const col = df.columns.get('timestamps')!;
    setColumnValue(col, 0, 3000n);
    setColumnValue(col, 1, 1000n);
    setColumnValue(col, 2, 2000n);

    const indices = createRowIndices(3);
    sortByColumn(indices, col, 'asc');

    expect(indices).toEqual([1, 2, 0]);
  });

  test('handles duplicate values', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 5);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 10);
    setColumnValue(col, 1, 20);
    setColumnValue(col, 2, 10);
    setColumnValue(col, 3, 20);
    setColumnValue(col, 4, 10);

    const indices = createRowIndices(5);
    sortByColumn(indices, col, 'asc');

    // All 10s should come before all 20s (stable sort preserves original order within ties)
    const values = indices.map((i) => col.view.getInt32(i * 4, true));
    expect(values).toEqual([10, 10, 10, 20, 20]);
  });

  test('handles single element', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 1);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 42);

    const indices = createRowIndices(1);
    sortByColumn(indices, col, 'asc');

    expect(indices).toEqual([0]);
  });

  test('handles empty array', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 0);

    const col = df.columns.get('values')!;
    const indices = createRowIndices(0);
    sortByColumn(indices, col, 'asc');

    expect(indices).toEqual([]);
  });
});

describe('sortByColumns', () => {
  test('sorts by single column', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 3);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 30);
    setColumnValue(col, 1, 10);
    setColumnValue(col, 2, 20);

    const indices = createRowIndices(3);
    const sortSpecs: SortSpec[] = [{ column: col, direction: 'asc' }];
    sortByColumns(indices, sortSpecs);

    expect(indices).toEqual([1, 2, 0]);
  });

  test('sorts by multiple columns (two-level)', () => {
    const df = createDataFrame();
    addColumn(df, 'category', DType.Int32, 6);
    addColumn(df, 'value', DType.Int32, 6);

    const catCol = df.columns.get('category')!;
    const valCol = df.columns.get('value')!;

    // category, value
    setColumnValue(catCol, 0, 2);
    setColumnValue(valCol, 0, 10);

    setColumnValue(catCol, 1, 1);
    setColumnValue(valCol, 1, 30);

    setColumnValue(catCol, 2, 2);
    setColumnValue(valCol, 2, 5);

    setColumnValue(catCol, 3, 1);
    setColumnValue(valCol, 3, 20);

    setColumnValue(catCol, 4, 2);
    setColumnValue(valCol, 4, 15);

    setColumnValue(catCol, 5, 1);
    setColumnValue(valCol, 5, 25);

    const indices = createRowIndices(6);
    const sortSpecs: SortSpec[] = [
      { column: catCol, direction: 'asc' },
      { column: valCol, direction: 'asc' },
    ];
    sortByColumns(indices, sortSpecs);

    // Should sort by category first, then by value within each category
    // Category 1: rows 3(20), 5(25), 1(30)
    // Category 2: rows 2(5), 0(10), 4(15)
    expect(indices).toEqual([3, 5, 1, 2, 0, 4]);
  });

  test('sorts by multiple columns with mixed directions', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 4);
    addColumn(df, 'score', DType.Int32, 4);

    const groupCol = df.columns.get('group')!;
    const scoreCol = df.columns.get('score')!;

    setColumnValue(groupCol, 0, 1);
    setColumnValue(scoreCol, 0, 100);

    setColumnValue(groupCol, 1, 2);
    setColumnValue(scoreCol, 1, 80);

    setColumnValue(groupCol, 2, 1);
    setColumnValue(scoreCol, 2, 90);

    setColumnValue(groupCol, 3, 2);
    setColumnValue(scoreCol, 3, 95);

    const indices = createRowIndices(4);
    const sortSpecs: SortSpec[] = [
      { column: groupCol, direction: 'asc' },
      { column: scoreCol, direction: 'desc' }, // Descending within group
    ];
    sortByColumns(indices, sortSpecs);

    // Group 1 (desc by score): 0(100), 2(90)
    // Group 2 (desc by score): 3(95), 1(80)
    expect(indices).toEqual([0, 2, 3, 1]);
  });

  test('handles empty sort specs', () => {
    const indices = createRowIndices(5);
    const originalIndices = [...indices];
    sortByColumns(indices, []);

    expect(indices).toEqual(originalIndices);
  });

  test('handles three-level sort', () => {
    const df = createDataFrame();
    addColumn(df, 'a', DType.Int32, 4);
    addColumn(df, 'b', DType.Int32, 4);
    addColumn(df, 'c', DType.Int32, 4);

    const aCol = df.columns.get('a')!;
    const bCol = df.columns.get('b')!;
    const cCol = df.columns.get('c')!;

    // a, b, c
    setColumnValue(aCol, 0, 1);
    setColumnValue(bCol, 0, 1);
    setColumnValue(cCol, 0, 2);

    setColumnValue(aCol, 1, 1);
    setColumnValue(bCol, 1, 1);
    setColumnValue(cCol, 1, 1);

    setColumnValue(aCol, 2, 1);
    setColumnValue(bCol, 2, 2);
    setColumnValue(cCol, 2, 1);

    setColumnValue(aCol, 3, 2);
    setColumnValue(bCol, 3, 1);
    setColumnValue(cCol, 3, 1);

    const indices = createRowIndices(4);
    const sortSpecs: SortSpec[] = [{ column: aCol }, { column: bCol }, { column: cCol }];
    sortByColumns(indices, sortSpecs);

    // a=1, b=1: c=1(idx 1), c=2(idx 0)
    // a=1, b=2: c=1(idx 2)
    // a=2, b=1: c=1(idx 3)
    expect(indices).toEqual([1, 0, 2, 3]);
  });
});

describe('findGroupBoundaries', () => {
  test('finds single group', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 4);

    const col = df.columns.get('group')!;
    setColumnValue(col, 0, 1);
    setColumnValue(col, 1, 1);
    setColumnValue(col, 2, 1);
    setColumnValue(col, 3, 1);

    const indices = createRowIndices(4);
    const boundaries = findGroupBoundaries(indices, [col]);

    expect(boundaries).toEqual([[0, 4]]);
  });

  test('finds multiple groups', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 6);

    const col = df.columns.get('group')!;
    setColumnValue(col, 0, 1);
    setColumnValue(col, 1, 1);
    setColumnValue(col, 2, 2);
    setColumnValue(col, 3, 2);
    setColumnValue(col, 4, 2);
    setColumnValue(col, 5, 3);

    const indices = createRowIndices(6);
    sortByColumn(indices, col, 'asc');

    const boundaries = findGroupBoundaries(indices, [col]);

    expect(boundaries).toEqual([
      [0, 2], // Group 1: indices 0-1
      [2, 5], // Group 2: indices 2-4
      [5, 6], // Group 3: index 5
    ]);
  });

  test('finds groups with multi-column keys', () => {
    const df = createDataFrame();
    addColumn(df, 'a', DType.Int32, 5);
    addColumn(df, 'b', DType.Int32, 5);

    const aCol = df.columns.get('a')!;
    const bCol = df.columns.get('b')!;

    setColumnValue(aCol, 0, 1);
    setColumnValue(bCol, 0, 10);

    setColumnValue(aCol, 1, 1);
    setColumnValue(bCol, 1, 10);

    setColumnValue(aCol, 2, 1);
    setColumnValue(bCol, 2, 20);

    setColumnValue(aCol, 3, 2);
    setColumnValue(bCol, 3, 10);

    setColumnValue(aCol, 4, 2);
    setColumnValue(bCol, 4, 10);

    const indices = createRowIndices(5);
    sortByColumns(indices, [{ column: aCol }, { column: bCol }]);

    const boundaries = findGroupBoundaries(indices, [aCol, bCol]);

    expect(boundaries).toEqual([
      [0, 2], // a=1, b=10
      [2, 3], // a=1, b=20
      [3, 5], // a=2, b=10
    ]);
  });

  test('handles empty indices', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 0);

    const col = df.columns.get('group')!;
    const indices: number[] = [];
    const boundaries = findGroupBoundaries(indices, [col]);

    expect(boundaries).toEqual([]);
  });

  test('handles no group columns', () => {
    const indices = createRowIndices(5);
    const boundaries = findGroupBoundaries(indices, []);

    expect(boundaries).toEqual([[0, 5]]);
  });

  test('handles single-row groups', () => {
    const df = createDataFrame();
    addColumn(df, 'id', DType.Int32, 5);

    const col = df.columns.get('id')!;
    setColumnValue(col, 0, 1);
    setColumnValue(col, 1, 2);
    setColumnValue(col, 2, 3);
    setColumnValue(col, 3, 4);
    setColumnValue(col, 4, 5);

    const indices = createRowIndices(5);
    const boundaries = findGroupBoundaries(indices, [col]);

    expect(boundaries).toEqual([
      [0, 1],
      [1, 2],
      [2, 3],
      [3, 4],
      [4, 5],
    ]);
  });
});

describe('isSorted', () => {
  test('returns true for sorted ascending', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 4);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 10);
    setColumnValue(col, 1, 20);
    setColumnValue(col, 2, 30);
    setColumnValue(col, 3, 40);

    const indices = createRowIndices(4);
    expect(isSorted(indices, [col], ['asc'])).toBe(true);
  });

  test('returns true for sorted descending', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 4);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 40);
    setColumnValue(col, 1, 30);
    setColumnValue(col, 2, 20);
    setColumnValue(col, 3, 10);

    const indices = createRowIndices(4);
    expect(isSorted(indices, [col], ['desc'])).toBe(true);
  });

  test('returns false for unsorted', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 4);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 10);
    setColumnValue(col, 1, 30);
    setColumnValue(col, 2, 20); // Out of order
    setColumnValue(col, 3, 40);

    const indices = createRowIndices(4);
    expect(isSorted(indices, [col], ['asc'])).toBe(false);
  });

  test('handles duplicate values as sorted', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 4);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 10);
    setColumnValue(col, 1, 20);
    setColumnValue(col, 2, 20);
    setColumnValue(col, 3, 30);

    const indices = createRowIndices(4);
    expect(isSorted(indices, [col], ['asc'])).toBe(true);
  });

  test('validates multi-column sort', () => {
    const df = createDataFrame();
    addColumn(df, 'a', DType.Int32, 4);
    addColumn(df, 'b', DType.Int32, 4);

    const aCol = df.columns.get('a')!;
    const bCol = df.columns.get('b')!;

    setColumnValue(aCol, 0, 1);
    setColumnValue(bCol, 0, 10);

    setColumnValue(aCol, 1, 1);
    setColumnValue(bCol, 1, 20);

    setColumnValue(aCol, 2, 2);
    setColumnValue(bCol, 2, 5);

    setColumnValue(aCol, 3, 2);
    setColumnValue(bCol, 3, 15);

    const indices = createRowIndices(4);
    expect(isSorted(indices, [aCol, bCol], ['asc', 'asc'])).toBe(true);
  });

  test('returns true for single element', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 1);

    const col = df.columns.get('values')!;
    setColumnValue(col, 0, 42);

    const indices = createRowIndices(1);
    expect(isSorted(indices, [col], ['asc'])).toBe(true);
  });

  test('returns true for empty array', () => {
    const df = createDataFrame();
    addColumn(df, 'values', DType.Int32, 0);

    const col = df.columns.get('values')!;
    const indices: number[] = [];
    expect(isSorted(indices, [col], ['asc'])).toBe(true);
  });
});
