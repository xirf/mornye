import { describe, expect, test } from 'bun:test';
import { getColumnValue, setColumnValue } from '../../src/core/column';
import { addColumn, createDataFrame, getColumn, getRowCount } from '../../src/dataframe/dataframe';
import { filter, select } from '../../src/dataframe/operations';
import { DType } from '../../src/types/dtypes';

describe('filter operation', () => {
  test('filters with == operator on Float64', () => {
    const df = createDataFrame();
    addColumn(df, 'price', DType.Float64, 5);
    addColumn(df, 'volume', DType.Int32, 5);

    const priceCol = df.columns.get('price')!;
    const volumeCol = df.columns.get('volume')!;

    setColumnValue(priceCol, 0, 100.5);
    setColumnValue(priceCol, 1, 200.0);
    setColumnValue(priceCol, 2, 100.5);
    setColumnValue(priceCol, 3, 300.0);
    setColumnValue(priceCol, 4, 100.5);

    setColumnValue(volumeCol, 0, 10);
    setColumnValue(volumeCol, 1, 20);
    setColumnValue(volumeCol, 2, 30);
    setColumnValue(volumeCol, 3, 40);
    setColumnValue(volumeCol, 4, 50);

    const filtered = filter(df, 'price', '==', 100.5);
    expect(getRowCount(filtered)).toBe(3);

    const filteredPrice = filtered.columns.get('price')!;
    const filteredVolume = filtered.columns.get('volume')!;

    expect(getColumnValue(filteredPrice, 0)).toBe(100.5);
    expect(getColumnValue(filteredPrice, 1)).toBe(100.5);
    expect(getColumnValue(filteredPrice, 2)).toBe(100.5);

    expect(getColumnValue(filteredVolume, 0)).toBe(10);
    expect(getColumnValue(filteredVolume, 1)).toBe(30);
    expect(getColumnValue(filteredVolume, 2)).toBe(50);
  });

  test('filters with != operator', () => {
    const df = createDataFrame();
    addColumn(df, 'status', DType.Int32, 4);

    const col = df.columns.get('status')!;
    setColumnValue(col, 0, 1);
    setColumnValue(col, 1, 0);
    setColumnValue(col, 2, 1);
    setColumnValue(col, 3, 0);

    const filtered = filter(df, 'status', '!=', 0);
    expect(getRowCount(filtered)).toBe(2);

    const statusCol = filtered.columns.get('status')!;
    expect(getColumnValue(statusCol, 0)).toBe(1);
    expect(getColumnValue(statusCol, 1)).toBe(1);
  });

  test('filters with > operator', () => {
    const df = createDataFrame();
    addColumn(df, 'value', DType.Float64, 5);

    const col = df.columns.get('value')!;
    setColumnValue(col, 0, 10.0);
    setColumnValue(col, 1, 50.0);
    setColumnValue(col, 2, 100.0);
    setColumnValue(col, 3, 25.0);
    setColumnValue(col, 4, 75.0);

    const filtered = filter(df, 'value', '>', 50);
    expect(getRowCount(filtered)).toBe(2);

    const valueCol = filtered.columns.get('value')!;
    expect(getColumnValue(valueCol, 0)).toBe(100.0);
    expect(getColumnValue(valueCol, 1)).toBe(75.0);
  });

  test('filters with < operator', () => {
    const df = createDataFrame();
    addColumn(df, 'value', DType.Float64, 3);

    const col = df.columns.get('value')!;
    setColumnValue(col, 0, 10.0);
    setColumnValue(col, 1, 50.0);
    setColumnValue(col, 2, 30.0);

    const filtered = filter(df, 'value', '<', 40);
    expect(getRowCount(filtered)).toBe(2);

    const valueCol = filtered.columns.get('value')!;
    expect(getColumnValue(valueCol, 0)).toBe(10.0);
    expect(getColumnValue(valueCol, 1)).toBe(30.0);
  });

  test('filters with >= operator', () => {
    const df = createDataFrame();
    addColumn(df, 'score', DType.Int32, 5);

    const col = df.columns.get('score')!;
    setColumnValue(col, 0, 80);
    setColumnValue(col, 1, 90);
    setColumnValue(col, 2, 70);
    setColumnValue(col, 3, 85);
    setColumnValue(col, 4, 90);

    const filtered = filter(df, 'score', '>=', 85);
    expect(getRowCount(filtered)).toBe(3);

    const scoreCol = filtered.columns.get('score')!;
    expect(getColumnValue(scoreCol, 0)).toBe(90);
    expect(getColumnValue(scoreCol, 1)).toBe(85);
    expect(getColumnValue(scoreCol, 2)).toBe(90);
  });

  test('filters with <= operator', () => {
    const df = createDataFrame();
    addColumn(df, 'age', DType.Int32, 4);

    const col = df.columns.get('age')!;
    setColumnValue(col, 0, 25);
    setColumnValue(col, 1, 30);
    setColumnValue(col, 2, 20);
    setColumnValue(col, 3, 35);

    const filtered = filter(df, 'age', '<=', 25);
    expect(getRowCount(filtered)).toBe(2);

    const ageCol = filtered.columns.get('age')!;
    expect(getColumnValue(ageCol, 0)).toBe(25);
    expect(getColumnValue(ageCol, 1)).toBe(20);
  });

  test('filters with in operator', () => {
    const df = createDataFrame();
    addColumn(df, 'id', DType.Int32, 6);

    const col = df.columns.get('id')!;
    setColumnValue(col, 0, 1);
    setColumnValue(col, 1, 2);
    setColumnValue(col, 2, 3);
    setColumnValue(col, 3, 4);
    setColumnValue(col, 4, 5);
    setColumnValue(col, 5, 6);

    const filtered = filter(df, 'id', 'in', [2, 4, 6]);
    expect(getRowCount(filtered)).toBe(3);

    const idCol = filtered.columns.get('id')!;
    expect(getColumnValue(idCol, 0)).toBe(2);
    expect(getColumnValue(idCol, 1)).toBe(4);
    expect(getColumnValue(idCol, 2)).toBe(6);
  });

  test('filters with not-in operator', () => {
    const df = createDataFrame();
    addColumn(df, 'category', DType.Int32, 5);

    const col = df.columns.get('category')!;
    setColumnValue(col, 0, 1);
    setColumnValue(col, 1, 2);
    setColumnValue(col, 2, 3);
    setColumnValue(col, 3, 1);
    setColumnValue(col, 4, 2);

    const filtered = filter(df, 'category', 'not-in', [1, 3]);
    expect(getRowCount(filtered)).toBe(2);

    const categoryCol = filtered.columns.get('category')!;
    expect(getColumnValue(categoryCol, 0)).toBe(2);
    expect(getColumnValue(categoryCol, 1)).toBe(2);
  });

  test('chained filters work correctly', () => {
    const df = createDataFrame();
    addColumn(df, 'price', DType.Float64, 5);
    addColumn(df, 'volume', DType.Int32, 5);

    const priceCol = df.columns.get('price')!;
    const volumeCol = df.columns.get('volume')!;

    setColumnValue(priceCol, 0, 100.0);
    setColumnValue(priceCol, 1, 200.0);
    setColumnValue(priceCol, 2, 150.0);
    setColumnValue(priceCol, 3, 50.0);
    setColumnValue(priceCol, 4, 250.0);

    setColumnValue(volumeCol, 0, 10);
    setColumnValue(volumeCol, 1, 5);
    setColumnValue(volumeCol, 2, 20);
    setColumnValue(volumeCol, 3, 15);
    setColumnValue(volumeCol, 4, 8);

    // Filter: price > 100 AND volume >= 10
    const step1 = filter(df, 'price', '>', 100);
    const filtered = filter(step1, 'volume', '>=', 10);
    expect(getRowCount(filtered)).toBe(1);

    const finalPrice = filtered.columns.get('price')!;
    const finalVolume = filtered.columns.get('volume')!;

    expect(getColumnValue(finalPrice, 0)).toBe(150.0);
    expect(getColumnValue(finalVolume, 0)).toBe(20);
  });

  test('returns empty DataFrame when no rows match', () => {
    const df = createDataFrame();
    addColumn(df, 'value', DType.Float64, 3);

    const col = df.columns.get('value')!;
    setColumnValue(col, 0, 10.0);
    setColumnValue(col, 1, 20.0);
    setColumnValue(col, 2, 30.0);

    const filtered = filter(df, 'value', '>', 100);
    expect(getRowCount(filtered)).toBe(0);
  });

  test('rejects filter on non-existent column', () => {
    const df = createDataFrame();
    addColumn(df, 'value', DType.Float64, 3);

    expect(() => filter(df, 'nonexistent', '>', 10)).toThrow("Column 'nonexistent' not found");
  });

  test('rejects array value for non-array operator', () => {
    const df = createDataFrame();
    addColumn(df, 'value', DType.Int32, 3);

    expect(() => filter(df, 'value', '==', [1, 2, 3])).toThrow('requires a single value');
  });

  test('rejects single value for array operator', () => {
    const df = createDataFrame();
    addColumn(df, 'value', DType.Int32, 3);

    expect(() => filter(df, 'value', 'in', 1)).toThrow('requires an array value');
  });

  test('handles DateTime comparison', () => {
    const df = createDataFrame();
    addColumn(df, 'timestamp', DType.DateTime, 3);

    const col = df.columns.get('timestamp')!;
    setColumnValue(col, 0, 1000n);
    setColumnValue(col, 1, 2000n);
    setColumnValue(col, 2, 1500n);

    const filtered = filter(df, 'timestamp', '>', 1500n);
    expect(getRowCount(filtered)).toBe(1);

    const tsCol = filtered.columns.get('timestamp')!;
    expect(getColumnValue(tsCol, 0)).toBe(2000n);
  });
});

describe('select operation', () => {
  test('selects single column', () => {
    const df = createDataFrame();
    addColumn(df, 'a', DType.Int32, 3);
    addColumn(df, 'b', DType.Float64, 3);
    addColumn(df, 'c', DType.Int32, 3);

    const aCol = df.columns.get('a')!;
    setColumnValue(aCol, 0, 1);
    setColumnValue(aCol, 1, 2);
    setColumnValue(aCol, 2, 3);

    const selected = select(df, ['a']);
    expect(getRowCount(selected)).toBe(3);
    expect(selected.columnOrder).toEqual(['a']);

    const selCol = selected.columns.get('a')!;
    expect(getColumnValue(selCol, 0)).toBe(1);
    expect(getColumnValue(selCol, 1)).toBe(2);
    expect(getColumnValue(selCol, 2)).toBe(3);
  });

  test('selects multiple columns in order', () => {
    const df = createDataFrame();
    addColumn(df, 'a', DType.Int32, 2);
    addColumn(df, 'b', DType.Float64, 2);
    addColumn(df, 'c', DType.Int32, 2);

    const selected = select(df, ['c', 'a']);
    expect(selected.columnOrder).toEqual(['c', 'a']);
    expect(selected.columns.has('b')).toBe(false);
  });

  test('selects all columns', () => {
    const df = createDataFrame();
    addColumn(df, 'x', DType.Int32, 1);
    addColumn(df, 'y', DType.Float64, 1);

    const selected = select(df, ['x', 'y']);
    expect(getRowCount(selected)).toBe(1);
    expect(selected.columnOrder).toEqual(['x', 'y']);
  });

  test('rejects select with non-existent column', () => {
    const df = createDataFrame();
    addColumn(df, 'a', DType.Int32, 1);

    expect(() => select(df, ['a', 'nonexistent'])).toThrow("Column 'nonexistent' not found");
  });

  test('select preserves data correctly', () => {
    const df = createDataFrame();
    addColumn(df, 'id', DType.Int32, 3);
    addColumn(df, 'value', DType.Float64, 3);

    const idCol = df.columns.get('id')!;
    const valueCol = df.columns.get('value')!;

    setColumnValue(idCol, 0, 10);
    setColumnValue(idCol, 1, 20);
    setColumnValue(idCol, 2, 30);

    setColumnValue(valueCol, 0, 1.5);
    setColumnValue(valueCol, 1, 2.5);
    setColumnValue(valueCol, 2, 3.5);

    const selected = select(df, ['value', 'id']);
    const selValue = selected.columns.get('value')!;
    const selId = selected.columns.get('id')!;

    expect(getColumnValue(selValue, 0)).toBe(1.5);
    expect(getColumnValue(selValue, 1)).toBe(2.5);
    expect(getColumnValue(selValue, 2)).toBe(3.5);

    expect(getColumnValue(selId, 0)).toBe(10);
    expect(getColumnValue(selId, 1)).toBe(20);
    expect(getColumnValue(selId, 2)).toBe(30);
  });

  test('select on empty DataFrame returns empty DataFrame', () => {
    const df = createDataFrame();

    const selected = select(df, []);
    expect(getRowCount(selected)).toBe(0);
  });
});
