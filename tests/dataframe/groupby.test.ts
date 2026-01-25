import { describe, expect, test } from 'bun:test';
import { setColumnValue } from '../../src/core/column';
import { addColumn, createDataFrame, getRowCount } from '../../src/dataframe/dataframe';
import { type AggSpec, groupby } from '../../src/dataframe/groupby';
import { DType } from '../../src/types/dtypes';

describe('groupby operation', () => {
  test('groups by single column and counts', () => {
    const df = createDataFrame();
    addColumn(df, 'category', DType.Int32, 6);
    addColumn(df, 'value', DType.Float64, 6);

    const catCol = df.columns.get('category')!;
    const valCol = df.columns.get('value')!;

    setColumnValue(catCol, 0, 1);
    setColumnValue(valCol, 0, 10.0);

    setColumnValue(catCol, 1, 2);
    setColumnValue(valCol, 1, 20.0);

    setColumnValue(catCol, 2, 1);
    setColumnValue(valCol, 2, 15.0);

    setColumnValue(catCol, 3, 2);
    setColumnValue(valCol, 3, 25.0);

    setColumnValue(catCol, 4, 1);
    setColumnValue(valCol, 4, 12.0);

    setColumnValue(catCol, 5, 2);
    setColumnValue(valCol, 5, 30.0);

    const aggs: AggSpec[] = [{ col: 'category', func: 'count', outName: 'count' }];

    const result = groupby(df, ['category'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    expect(getRowCount(grouped)).toBe(2);

    const groupCat = grouped.columns.get('category')!;
    const groupCount = grouped.columns.get('count')!;

    expect(groupCat.view.getInt32(0, true)).toBe(1);
    expect(groupCount.view.getInt32(0, true)).toBe(3);

    expect(groupCat.view.getInt32(4, true)).toBe(2);
    expect(groupCount.view.getInt32(4, true)).toBe(3);
  });

  test('groups and sums', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 5);
    addColumn(df, 'amount', DType.Float64, 5);

    const groupCol = df.columns.get('group')!;
    const amountCol = df.columns.get('amount')!;

    setColumnValue(groupCol, 0, 1);
    setColumnValue(amountCol, 0, 100.5);

    setColumnValue(groupCol, 1, 2);
    setColumnValue(amountCol, 1, 50.0);

    setColumnValue(groupCol, 2, 1);
    setColumnValue(amountCol, 2, 200.0);

    setColumnValue(groupCol, 3, 2);
    setColumnValue(amountCol, 3, 75.5);

    setColumnValue(groupCol, 4, 1);
    setColumnValue(amountCol, 4, 50.5);

    const aggs: AggSpec[] = [{ col: 'amount', func: 'sum', outName: 'total' }];

    const result = groupby(df, ['group'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    expect(getRowCount(grouped)).toBe(2);

    const totalCol = grouped.columns.get('total')!;
    expect(totalCol.view.getFloat64(0, true)).toBeCloseTo(351.0); // 100.5 + 200 + 50.5
    expect(totalCol.view.getFloat64(8, true)).toBeCloseTo(125.5); // 50 + 75.5
  });

  test('groups and calculates mean', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 6);
    addColumn(df, 'score', DType.Float64, 6);

    const groupCol = df.columns.get('group')!;
    const scoreCol = df.columns.get('score')!;

    setColumnValue(groupCol, 0, 1);
    setColumnValue(scoreCol, 0, 80.0);

    setColumnValue(groupCol, 1, 1);
    setColumnValue(scoreCol, 1, 90.0);

    setColumnValue(groupCol, 2, 1);
    setColumnValue(scoreCol, 2, 100.0);

    setColumnValue(groupCol, 3, 2);
    setColumnValue(scoreCol, 3, 70.0);

    setColumnValue(groupCol, 4, 2);
    setColumnValue(scoreCol, 4, 80.0);

    setColumnValue(groupCol, 5, 2);
    setColumnValue(scoreCol, 5, 90.0);

    const aggs: AggSpec[] = [{ col: 'score', func: 'mean', outName: 'avg_score' }];

    const result = groupby(df, ['group'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    const avgCol = grouped.columns.get('avg_score')!;

    expect(avgCol.view.getFloat64(0, true)).toBeCloseTo(90.0); // (80+90+100)/3
    expect(avgCol.view.getFloat64(8, true)).toBeCloseTo(80.0); // (70+80+90)/3
  });

  test('groups and finds min/max', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 6);
    addColumn(df, 'value', DType.Float64, 6);

    const groupCol = df.columns.get('group')!;
    const valueCol = df.columns.get('value')!;

    setColumnValue(groupCol, 0, 1);
    setColumnValue(valueCol, 0, 50.0);

    setColumnValue(groupCol, 1, 1);
    setColumnValue(valueCol, 1, 30.0);

    setColumnValue(groupCol, 2, 1);
    setColumnValue(valueCol, 2, 40.0);

    setColumnValue(groupCol, 3, 2);
    setColumnValue(valueCol, 3, 100.0);

    setColumnValue(groupCol, 4, 2);
    setColumnValue(valueCol, 4, 80.0);

    setColumnValue(groupCol, 5, 2);
    setColumnValue(valueCol, 5, 120.0);

    const aggs: AggSpec[] = [
      { col: 'value', func: 'min', outName: 'min_val' },
      { col: 'value', func: 'max', outName: 'max_val' },
    ];

    const result = groupby(df, ['group'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    const minCol = grouped.columns.get('min_val')!;
    const maxCol = grouped.columns.get('max_val')!;

    expect(minCol.view.getFloat64(0, true)).toBe(30.0);
    expect(maxCol.view.getFloat64(0, true)).toBe(50.0);

    expect(minCol.view.getFloat64(8, true)).toBe(80.0);
    expect(maxCol.view.getFloat64(8, true)).toBe(120.0);
  });

  test('groups and gets first/last', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 4);
    addColumn(df, 'value', DType.Int32, 4);

    const groupCol = df.columns.get('group')!;
    const valueCol = df.columns.get('value')!;

    setColumnValue(groupCol, 0, 1);
    setColumnValue(valueCol, 0, 10);

    setColumnValue(groupCol, 1, 1);
    setColumnValue(valueCol, 1, 20);

    setColumnValue(groupCol, 2, 2);
    setColumnValue(valueCol, 2, 30);

    setColumnValue(groupCol, 3, 2);
    setColumnValue(valueCol, 3, 40);

    const aggs: AggSpec[] = [
      { col: 'value', func: 'first', outName: 'first_val' },
      { col: 'value', func: 'last', outName: 'last_val' },
    ];

    const result = groupby(df, ['group'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    const firstCol = grouped.columns.get('first_val')!;
    const lastCol = grouped.columns.get('last_val')!;

    expect(firstCol.view.getInt32(0, true)).toBe(10);
    expect(lastCol.view.getInt32(0, true)).toBe(20);

    expect(firstCol.view.getInt32(4, true)).toBe(30);
    expect(lastCol.view.getInt32(4, true)).toBe(40);
  });

  test('groups by multiple columns', () => {
    const df = createDataFrame();
    addColumn(df, 'cat1', DType.Int32, 6);
    addColumn(df, 'cat2', DType.Int32, 6);
    addColumn(df, 'value', DType.Float64, 6);

    const cat1Col = df.columns.get('cat1')!;
    const cat2Col = df.columns.get('cat2')!;
    const valueCol = df.columns.get('value')!;

    // cat1, cat2, value
    setColumnValue(cat1Col, 0, 1);
    setColumnValue(cat2Col, 0, 10);
    setColumnValue(valueCol, 0, 5.0);

    setColumnValue(cat1Col, 1, 1);
    setColumnValue(cat2Col, 1, 20);
    setColumnValue(valueCol, 1, 10.0);

    setColumnValue(cat1Col, 2, 1);
    setColumnValue(cat2Col, 2, 10);
    setColumnValue(valueCol, 2, 7.0);

    setColumnValue(cat1Col, 3, 2);
    setColumnValue(cat2Col, 3, 10);
    setColumnValue(valueCol, 3, 15.0);

    setColumnValue(cat1Col, 4, 2);
    setColumnValue(cat2Col, 4, 20);
    setColumnValue(valueCol, 4, 20.0);

    setColumnValue(cat1Col, 5, 1);
    setColumnValue(cat2Col, 5, 20);
    setColumnValue(valueCol, 5, 12.0);

    const aggs: AggSpec[] = [{ col: 'value', func: 'sum', outName: 'total' }];

    const result = groupby(df, ['cat1', 'cat2'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    expect(getRowCount(grouped)).toBe(4); // (1,10), (1,20), (2,10), (2,20)

    const resCat1 = grouped.columns.get('cat1')!;
    const resCat2 = grouped.columns.get('cat2')!;
    const resTotal = grouped.columns.get('total')!;

    // Group 1: cat1=1, cat2=10 -> sum = 5 + 7 = 12
    expect(resCat1.view.getInt32(0, true)).toBe(1);
    expect(resCat2.view.getInt32(0, true)).toBe(10);
    expect(resTotal.view.getFloat64(0, true)).toBeCloseTo(12.0);

    // Group 2: cat1=1, cat2=20 -> sum = 10 + 12 = 22
    expect(resCat1.view.getInt32(4, true)).toBe(1);
    expect(resCat2.view.getInt32(4, true)).toBe(20);
    expect(resTotal.view.getFloat64(8, true)).toBeCloseTo(22.0);

    // Group 3: cat1=2, cat2=10 -> sum = 15
    expect(resCat1.view.getInt32(8, true)).toBe(2);
    expect(resCat2.view.getInt32(8, true)).toBe(10);
    expect(resTotal.view.getFloat64(16, true)).toBeCloseTo(15.0);

    // Group 4: cat1=2, cat2=20 -> sum = 20
    expect(resCat1.view.getInt32(12, true)).toBe(2);
    expect(resCat2.view.getInt32(12, true)).toBe(20);
    expect(resTotal.view.getFloat64(24, true)).toBeCloseTo(20.0);
  });

  test('supports multiple aggregations', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 4);
    addColumn(df, 'value', DType.Float64, 4);

    const groupCol = df.columns.get('group')!;
    const valueCol = df.columns.get('value')!;

    setColumnValue(groupCol, 0, 1);
    setColumnValue(valueCol, 0, 10.0);

    setColumnValue(groupCol, 1, 1);
    setColumnValue(valueCol, 1, 20.0);

    setColumnValue(groupCol, 2, 2);
    setColumnValue(valueCol, 2, 30.0);

    setColumnValue(groupCol, 3, 2);
    setColumnValue(valueCol, 3, 40.0);

    const aggs: AggSpec[] = [
      { col: 'value', func: 'count', outName: 'count' },
      { col: 'value', func: 'sum', outName: 'total' },
      { col: 'value', func: 'mean', outName: 'average' },
      { col: 'value', func: 'min', outName: 'minimum' },
      { col: 'value', func: 'max', outName: 'maximum' },
    ];

    const result = groupby(df, ['group'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    expect(grouped.columnOrder).toEqual([
      'group',
      'count',
      'total',
      'average',
      'minimum',
      'maximum',
    ]);

    const countCol = grouped.columns.get('count')!;
    const totalCol = grouped.columns.get('total')!;
    const avgCol = grouped.columns.get('average')!;
    const minCol = grouped.columns.get('minimum')!;
    const maxCol = grouped.columns.get('maximum')!;

    // Group 1
    expect(countCol.view.getInt32(0, true)).toBe(2);
    expect(totalCol.view.getFloat64(0, true)).toBeCloseTo(30.0);
    expect(avgCol.view.getFloat64(0, true)).toBeCloseTo(15.0);
    expect(minCol.view.getFloat64(0, true)).toBe(10.0);
    expect(maxCol.view.getFloat64(0, true)).toBe(20.0);

    // Group 2
    expect(countCol.view.getInt32(4, true)).toBe(2);
    expect(totalCol.view.getFloat64(8, true)).toBeCloseTo(70.0);
    expect(avgCol.view.getFloat64(8, true)).toBeCloseTo(35.0);
    expect(minCol.view.getFloat64(8, true)).toBe(30.0);
    expect(maxCol.view.getFloat64(8, true)).toBe(40.0);
  });

  test('handles single group', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 3);
    addColumn(df, 'value', DType.Int32, 3);

    const groupCol = df.columns.get('group')!;
    const valueCol = df.columns.get('value')!;

    setColumnValue(groupCol, 0, 1);
    setColumnValue(valueCol, 0, 10);

    setColumnValue(groupCol, 1, 1);
    setColumnValue(valueCol, 1, 20);

    setColumnValue(groupCol, 2, 1);
    setColumnValue(valueCol, 2, 30);

    const aggs: AggSpec[] = [{ col: 'value', func: 'sum', outName: 'total' }];

    const result = groupby(df, ['group'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    expect(getRowCount(grouped)).toBe(1);

    const totalCol = grouped.columns.get('total')!;
    expect(totalCol.view.getInt32(0, true)).toBe(60);
  });

  test('handles each row as separate group', () => {
    const df = createDataFrame();
    addColumn(df, 'id', DType.Int32, 3);
    addColumn(df, 'value', DType.Int32, 3);

    const idCol = df.columns.get('id')!;
    const valueCol = df.columns.get('value')!;

    setColumnValue(idCol, 0, 1);
    setColumnValue(valueCol, 0, 10);

    setColumnValue(idCol, 1, 2);
    setColumnValue(valueCol, 1, 20);

    setColumnValue(idCol, 2, 3);
    setColumnValue(valueCol, 2, 30);

    const aggs: AggSpec[] = [{ col: 'value', func: 'sum', outName: 'total' }];

    const result = groupby(df, ['id'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    expect(getRowCount(grouped)).toBe(3);

    const totalCol = grouped.columns.get('total')!;
    expect(totalCol.view.getInt32(0, true)).toBe(10);
    expect(totalCol.view.getInt32(4, true)).toBe(20);
    expect(totalCol.view.getInt32(8, true)).toBe(30);
  });

  test('handles empty DataFrame', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 0);
    addColumn(df, 'value', DType.Int32, 0);

    const aggs: AggSpec[] = [{ col: 'value', func: 'sum', outName: 'total' }];

    const result = groupby(df, ['group'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    expect(getRowCount(grouped)).toBe(0);
  });

  test('rejects invalid group key column', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 3);

    const aggs: AggSpec[] = [{ col: 'group', func: 'count', outName: 'count' }];

    const result = groupby(df, ['nonexistent'], aggs);
    expect(result.ok).toBe(false);
    if (result.ok) return;

    expect(result.error.message).toContain("Group key column 'nonexistent' not found");
  });

  test('rejects invalid aggregation column', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 3);

    const aggs: AggSpec[] = [{ col: 'nonexistent', func: 'sum', outName: 'total' }];

    const result = groupby(df, ['group'], aggs);
    expect(result.ok).toBe(false);
    if (result.ok) return;

    expect(result.error.message).toContain("Aggregation column 'nonexistent' not found");
  });

  test('works with DateTime columns', () => {
    const df = createDataFrame();
    addColumn(df, 'group', DType.Int32, 4);
    addColumn(df, 'timestamp', DType.DateTime, 4);

    const groupCol = df.columns.get('group')!;
    const tsCol = df.columns.get('timestamp')!;

    setColumnValue(groupCol, 0, 1);
    setColumnValue(tsCol, 0, 1000n);

    setColumnValue(groupCol, 1, 1);
    setColumnValue(tsCol, 1, 2000n);

    setColumnValue(groupCol, 2, 2);
    setColumnValue(tsCol, 2, 3000n);

    setColumnValue(groupCol, 3, 2);
    setColumnValue(tsCol, 3, 4000n);

    const aggs: AggSpec[] = [
      { col: 'timestamp', func: 'min', outName: 'earliest' },
      { col: 'timestamp', func: 'max', outName: 'latest' },
    ];

    const result = groupby(df, ['group'], aggs);
    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const grouped = result.data;
    const earliestCol = grouped.columns.get('earliest')!;
    const latestCol = grouped.columns.get('latest')!;

    expect(earliestCol.view.getBigInt64(0, true)).toBe(1000n);
    expect(latestCol.view.getBigInt64(0, true)).toBe(2000n);

    expect(earliestCol.view.getBigInt64(8, true)).toBe(3000n);
    expect(latestCol.view.getBigInt64(8, true)).toBe(4000n);
  });
});
