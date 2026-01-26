import { describe, expect, test } from 'bun:test';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { getColumnValue } from '../../src/core/column';
import { getColumn, getRowCount } from '../../src/dataframe/dataframe';
import { streamCsvBatches } from '../../src/io/csv-streamer';
import { aggregateBatchesToDataFrame } from '../../src/lazyframe/streaming';
import { getString } from '../../src/memory/dictionary';
import { DType } from '../../src/types/dtypes';

const TEST_DIR = path.join(process.cwd(), '.test_streaming_agg');

function ensureDir(): void {
  if (!fs.existsSync(TEST_DIR)) {
    fs.mkdirSync(TEST_DIR, { recursive: true });
  }
}

describe('Streaming aggregations', () => {
  test('aggregates globally with SIMD helpers', async () => {
    ensureDir();

    const filePath = path.join(TEST_DIR, 'global-agg.csv');
    const csv = 'id,value\n1,2.5\n2,3.5\n3,4.0\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      id: DType.Int32,
      value: DType.Float64,
    };

    const streamResult = await streamCsvBatches(filePath, {
      schema,
      batchSizeBytes: 32,
    });

    expect(streamResult.ok).toBe(true);
    if (!streamResult.ok) return;

    const aggResult = await aggregateBatchesToDataFrame(
      streamResult.data,
      [],
      [
        { col: 'value', func: 'sum', outName: 'value_sum' },
        { col: 'value', func: 'mean', outName: 'value_mean' },
        { col: 'value', func: 'min', outName: 'value_min' },
        { col: 'value', func: 'max', outName: 'value_max' },
        { col: 'value', func: 'count', outName: 'value_count' },
      ],
    );

    expect(aggResult.ok).toBe(true);
    if (!aggResult.ok) return;

    const df = aggResult.data;

    const sumCol = getColumn(df, 'value_sum');
    const meanCol = getColumn(df, 'value_mean');
    const minCol = getColumn(df, 'value_min');
    const maxCol = getColumn(df, 'value_max');
    const countCol = getColumn(df, 'value_count');

    expect(sumCol.ok).toBe(true);
    expect(meanCol.ok).toBe(true);
    expect(minCol.ok).toBe(true);
    expect(maxCol.ok).toBe(true);
    expect(countCol.ok).toBe(true);

    if (!sumCol.ok || !meanCol.ok || !minCol.ok || !maxCol.ok || !countCol.ok) return;

    expect(getColumnValue(sumCol.data, 0)).toBeCloseTo(10.0, 6);
    expect(getColumnValue(meanCol.data, 0)).toBeCloseTo(10.0 / 3.0, 6);
    expect(getColumnValue(minCol.data, 0)).toBeCloseTo(2.5, 6);
    expect(getColumnValue(maxCol.data, 0)).toBeCloseTo(4.0, 6);
    expect(getColumnValue(countCol.data, 0)).toBe(3);
  });

  test('aggregates by group keys with streaming batches', async () => {
    ensureDir();

    const filePath = path.join(TEST_DIR, 'group-agg.csv');
    const csv = 'group,value\nA,1\nB,2\nA,3\nB,4\nA,5\n';
    fs.writeFileSync(filePath, csv);

    const schema = {
      group: DType.String,
      value: DType.Float64,
    };

    const streamResult = await streamCsvBatches(filePath, {
      schema,
      batchSizeBytes: 32,
    });

    expect(streamResult.ok).toBe(true);
    if (!streamResult.ok) return;

    const aggResult = await aggregateBatchesToDataFrame(
      streamResult.data,
      ['group'],
      [
        { col: 'value', func: 'sum', outName: 'value_sum' },
        { col: 'value', func: 'mean', outName: 'value_mean' },
        { col: 'value', func: 'count', outName: 'value_count' },
      ],
    );

    expect(aggResult.ok).toBe(true);
    if (!aggResult.ok) return;

    const df = aggResult.data;
    const groupCol = getColumn(df, 'group');
    const sumCol = getColumn(df, 'value_sum');
    const meanCol = getColumn(df, 'value_mean');
    const countCol = getColumn(df, 'value_count');

    expect(groupCol.ok).toBe(true);
    expect(sumCol.ok).toBe(true);
    expect(meanCol.ok).toBe(true);
    expect(countCol.ok).toBe(true);

    if (!groupCol.ok || !sumCol.ok || !meanCol.ok || !countCol.ok) return;

    const rowCount = getRowCount(df);
    const results = new Map<string, { sum: number; mean: number; count: number }>();
    const dictionary = df.dictionary;

    for (let i = 0; i < rowCount; i++) {
      const groupId = getColumnValue(groupCol.data, i);
      const groupName =
        dictionary && typeof groupId === 'number'
          ? (getString(dictionary, groupId) ?? '')
          : String(groupId ?? '');
      results.set(groupName, {
        sum: Number(getColumnValue(sumCol.data, i) ?? 0),
        mean: Number(getColumnValue(meanCol.data, i) ?? 0),
        count: Number(getColumnValue(countCol.data, i) ?? 0),
      });
    }

    const groupA = results.get('A');
    const groupB = results.get('B');

    expect(groupA).toBeDefined();
    expect(groupB).toBeDefined();
    if (!groupA || !groupB) return;

    expect(groupA.sum).toBeCloseTo(9, 6);
    expect(groupA.mean).toBeCloseTo(3, 6);
    expect(groupA.count).toBe(3);

    expect(groupB.sum).toBeCloseTo(6, 6);
    expect(groupB.mean).toBeCloseTo(3, 6);
    expect(groupB.count).toBe(2);
  });
});
