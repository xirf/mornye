import { describe, expect, test } from 'bun:test';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { getColumnValue } from '../../src/core/column';
import { getColumn, getRowCount } from '../../src/dataframe/dataframe';
import { LazyFrame } from '../../src/lazyframe/lazyframe';
import { DType } from '../../src/types/dtypes';

const TEST_DIR = path.join(process.cwd(), '.test_streaming_sort');

function ensureDir(): void {
  if (!fs.existsSync(TEST_DIR)) {
    fs.mkdirSync(TEST_DIR, { recursive: true });
  }
}

describe('External merge sort', () => {
  test('sorts rows across multiple runs', async () => {
    ensureDir();

    const filePath = path.join(TEST_DIR, 'sort.csv');
    const csv = 'id,value\n1,30\n2,10\n3,20\n4,40\n5,15\n';
    fs.writeFileSync(filePath, csv);

    const lf = LazyFrame.scanCsv(filePath, { id: DType.Int32, value: DType.Int32 });
    const result = await lf.sort('value', 'asc', { runBytes: 64 }).collect();

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(5);

    const valueCol = getColumn(df, 'value');
    expect(valueCol.ok).toBe(true);
    if (!valueCol.ok) return;

    expect(getColumnValue(valueCol.data, 0)).toBe(10);
    expect(getColumnValue(valueCol.data, 1)).toBe(15);
    expect(getColumnValue(valueCol.data, 2)).toBe(20);
    expect(getColumnValue(valueCol.data, 3)).toBe(30);
    expect(getColumnValue(valueCol.data, 4)).toBe(40);
  });
});
