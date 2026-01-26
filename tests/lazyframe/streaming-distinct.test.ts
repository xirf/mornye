import { describe, expect, test } from 'bun:test';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { getColumnValue } from '../../src/core/column';
import { getColumn, getRowCount } from '../../src/dataframe/dataframe';
import { LazyFrame } from '../../src/lazyframe/lazyframe';
import { DType } from '../../src/types/dtypes';

const TEST_DIR = path.join(process.cwd(), '.test_streaming_distinct');

function ensureDir(): void {
  if (!fs.existsSync(TEST_DIR)) {
    fs.mkdirSync(TEST_DIR, { recursive: true });
  }
}

describe('Streaming distinct', () => {
  test('keeps first occurrence of duplicate rows', async () => {
    ensureDir();

    const filePath = path.join(TEST_DIR, 'distinct.csv');
    const csv = 'a,b\n1,10\n1,10\n2,20\n1,10\n3,30\n';
    fs.writeFileSync(filePath, csv);

    const lf = LazyFrame.scanCsv(filePath, { a: DType.Int32, b: DType.Int32 });
    const result = await lf.unique().collect();

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    expect(getRowCount(df)).toBe(3);

    const aCol = getColumn(df, 'a');
    const bCol = getColumn(df, 'b');

    expect(aCol.ok).toBe(true);
    expect(bCol.ok).toBe(true);

    if (!aCol.ok || !bCol.ok) return;

    expect(getColumnValue(aCol.data, 0)).toBe(1);
    expect(getColumnValue(bCol.data, 0)).toBe(10);
    expect(getColumnValue(aCol.data, 1)).toBe(2);
    expect(getColumnValue(bCol.data, 1)).toBe(20);
    expect(getColumnValue(aCol.data, 2)).toBe(3);
    expect(getColumnValue(bCol.data, 2)).toBe(30);
  });
});
