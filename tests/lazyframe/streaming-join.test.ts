import { describe, expect, test } from 'bun:test';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { getColumnValue } from '../../src/core/column';
import { getColumn, getRowCount } from '../../src/dataframe/dataframe';
import { LazyFrame } from '../../src/lazyframe/lazyframe';
import { DType } from '../../src/types/dtypes';

const TEST_DIR = path.join(process.cwd(), '.test_streaming_join');

function ensureDir(): void {
  if (!fs.existsSync(TEST_DIR)) {
    fs.mkdirSync(TEST_DIR, { recursive: true });
  }
}

describe('Streaming joins', () => {
  test('inner join merges matching rows', async () => {
    ensureDir();

    const leftPath = path.join(TEST_DIR, 'left.csv');
    const rightPath = path.join(TEST_DIR, 'right.csv');

    fs.writeFileSync(leftPath, 'id,left_val\n1,10\n2,20\n3,30\n');
    fs.writeFileSync(rightPath, 'id,right_val\n2,200\n3,300\n4,400\n');

    const left = LazyFrame.scanCsv(leftPath, { id: DType.Int32, left_val: DType.Int32 });
    const right = LazyFrame.scanCsv(rightPath, { id: DType.Int32, right_val: DType.Int32 });

    const result = await left.merge(right, { on: 'id', how: 'inner' }).collect();

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);

    const idCol = getColumn(df, 'id');
    const leftCol = getColumn(df, 'left_val');
    const rightCol = getColumn(df, 'right_val');

    expect(idCol.ok).toBe(true);
    expect(leftCol.ok).toBe(true);
    expect(rightCol.ok).toBe(true);

    if (!idCol.ok || !leftCol.ok || !rightCol.ok) return;

    expect(getColumnValue(idCol.data, 0)).toBe(2);
    expect(getColumnValue(idCol.data, 1)).toBe(3);
    expect(getColumnValue(leftCol.data, 0)).toBe(20);
    expect(getColumnValue(leftCol.data, 1)).toBe(30);
    expect(getColumnValue(rightCol.data, 0)).toBe(200);
    expect(getColumnValue(rightCol.data, 1)).toBe(300);
  });

  test('inner join applies suffixes for overlapping columns', async () => {
    ensureDir();

    const leftPath = path.join(TEST_DIR, 'left-overlap.csv');
    const rightPath = path.join(TEST_DIR, 'right-overlap.csv');

    fs.writeFileSync(leftPath, 'id,value\n1,10\n2,20\n');
    fs.writeFileSync(rightPath, 'id,value\n2,200\n3,300\n');

    const left = LazyFrame.scanCsv(leftPath, { id: DType.Int32, value: DType.Int32 });
    const right = LazyFrame.scanCsv(rightPath, { id: DType.Int32, value: DType.Int32 });

    const result = await left
      .merge(right, { on: 'id', how: 'inner', suffixes: ['_l', '_r'] })
      .collect();

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(1);

    const leftVal = getColumn(df, 'value_l');
    const rightVal = getColumn(df, 'value_r');

    expect(leftVal.ok).toBe(true);
    expect(rightVal.ok).toBe(true);

    if (!leftVal.ok || !rightVal.ok) return;

    expect(getColumnValue(leftVal.data, 0)).toBe(20);
    expect(getColumnValue(rightVal.data, 0)).toBe(200);
  });

  test('left join includes unmatched left rows', async () => {
    ensureDir();

    const leftPath = path.join(TEST_DIR, 'left-left.csv');
    const rightPath = path.join(TEST_DIR, 'right-left.csv');

    fs.writeFileSync(leftPath, 'id,value\n1,10\n2,20\n3,30\n');
    fs.writeFileSync(rightPath, 'id,score\n2,200\n');

    const left = LazyFrame.scanCsv(leftPath, { id: DType.Int32, value: DType.Int32 });
    const right = LazyFrame.scanCsv(rightPath, { id: DType.Int32, score: DType.Int32 });

    const result = await left.merge(right, { on: 'id', how: 'left' }).collect();

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(3);

    const idCol = getColumn(df, 'id');
    const valueCol = getColumn(df, 'value');
    const scoreCol = getColumn(df, 'score');

    expect(idCol.ok).toBe(true);
    expect(valueCol.ok).toBe(true);
    expect(scoreCol.ok).toBe(true);

    if (!idCol.ok || !valueCol.ok || !scoreCol.ok) return;

    expect(getColumnValue(idCol.data, 0)).toBe(1);
    expect(getColumnValue(idCol.data, 1)).toBe(2);
    expect(getColumnValue(idCol.data, 2)).toBe(3);
    expect(getColumnValue(valueCol.data, 0)).toBe(10);
    expect(getColumnValue(valueCol.data, 1)).toBe(20);
    expect(getColumnValue(valueCol.data, 2)).toBe(30);
    expect(getColumnValue(scoreCol.data, 1)).toBe(200);
  });

  test('right join includes unmatched right rows', async () => {
    ensureDir();

    const leftPath = path.join(TEST_DIR, 'left-right.csv');
    const rightPath = path.join(TEST_DIR, 'right-right.csv');

    fs.writeFileSync(leftPath, 'id,value\n1,10\n2,20\n');
    fs.writeFileSync(rightPath, 'id,score\n2,200\n3,300\n');

    const left = LazyFrame.scanCsv(leftPath, { id: DType.Int32, value: DType.Int32 });
    const right = LazyFrame.scanCsv(rightPath, { id: DType.Int32, score: DType.Int32 });

    const result = await left.merge(right, { on: 'id', how: 'right' }).collect();

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(2);

    const idCol = getColumn(df, 'id');
    const valueCol = getColumn(df, 'value');
    const scoreCol = getColumn(df, 'score');

    expect(idCol.ok).toBe(true);
    expect(valueCol.ok).toBe(true);
    expect(scoreCol.ok).toBe(true);

    if (!idCol.ok || !valueCol.ok || !scoreCol.ok) return;

    expect(getColumnValue(idCol.data, 0)).toBe(2);
    expect(getColumnValue(scoreCol.data, 0)).toBe(200);
    expect(getColumnValue(scoreCol.data, 1)).toBe(300);
  });

  test('outer join includes unmatched rows from both sides', async () => {
    ensureDir();

    const leftPath = path.join(TEST_DIR, 'left-outer.csv');
    const rightPath = path.join(TEST_DIR, 'right-outer.csv');

    fs.writeFileSync(leftPath, 'id,value\n1,10\n2,20\n');
    fs.writeFileSync(rightPath, 'id,score\n2,200\n3,300\n');

    const left = LazyFrame.scanCsv(leftPath, { id: DType.Int32, value: DType.Int32 });
    const right = LazyFrame.scanCsv(rightPath, { id: DType.Int32, score: DType.Int32 });

    const result = await left.merge(right, { on: 'id', how: 'outer' }).collect();

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(3);

    const idCol = getColumn(df, 'id');
    const valueCol = getColumn(df, 'value');
    const scoreCol = getColumn(df, 'score');

    expect(idCol.ok).toBe(true);
    expect(valueCol.ok).toBe(true);
    expect(scoreCol.ok).toBe(true);

    if (!idCol.ok || !valueCol.ok || !scoreCol.ok) return;

    expect(getColumnValue(idCol.data, 0)).toBe(1);
    expect(getColumnValue(idCol.data, 1)).toBe(2);
    expect(getColumnValue(valueCol.data, 0)).toBe(10);
    expect(getColumnValue(valueCol.data, 1)).toBe(20);
    expect(getColumnValue(scoreCol.data, 1)).toBe(200);
    expect(getColumnValue(scoreCol.data, 2)).toBe(300);
  });
});
