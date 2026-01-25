import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { getColumn, getRowCount } from '../../src/dataframe/dataframe';
import { LazyFrame } from '../../src/lazyframe/lazyframe';
import { DType } from '../../src/types/dtypes';

describe('LazyFrame Executor', () => {
  let tmpDir: string;

  // Create temp directory before each test
  function setupTest() {
    tmpDir = mkdtempSync(join(tmpdir(), 'molniya-test-'));
  }

  // Clean up temp directory after each test
  afterEach(() => {
    if (tmpDir) {
      rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  test('executes simple scan', async () => {
    setupTest();
    const csvPath = join(tmpDir, 'data.csv');
    writeFileSync(
      csvPath,
      `price,volume
100.5,50
200.5,100
150.0,75`,
    );

    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv(csvPath, schema);

    const result = await lf.collect();
    expect(result.ok).toBe(true);

    if (!result.ok) return;
    const df = result.data;

    expect(getRowCount(df)).toBe(3);
    expect(df.columnOrder).toEqual(['price', 'volume']);
  });

  test('executes filter operation', async () => {
    setupTest();
    const csvPath = join(tmpDir, 'data.csv');
    writeFileSync(
      csvPath,
      `price,volume
100.5,50
200.5,100
150.0,75
250.0,120`,
    );

    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv(csvPath, schema).filter('price', '>', 150);

    const result = await lf.collect();
    expect(result.ok).toBe(true);

    if (!result.ok) return;
    const df = result.data;

    expect(getRowCount(df)).toBe(2); // 200.5 and 250.0
  });

  test('executes select operation', async () => {
    setupTest();
    const csvPath = join(tmpDir, 'data.csv');
    writeFileSync(
      csvPath,
      `price,volume,side
100.5,50,buy
200.5,100,sell`,
    );

    const schema = { price: DType.Float64, volume: DType.Int32, side: DType.String };
    const lf = LazyFrame.scanCsv(csvPath, schema).select(['price', 'side']);

    const result = await lf.collect();
    expect(result.ok).toBe(true);

    if (!result.ok) return;
    const df = result.data;

    expect(df.columnOrder).toEqual(['price', 'side']);
    expect(df.columns.has('volume')).toBe(false);
  });

  test('executes groupby operation', async () => {
    setupTest();
    const csvPath = join(tmpDir, 'data.csv');
    writeFileSync(
      csvPath,
      `symbol,price,volume
BTC,50000,10
BTC,50100,20
ETH,3000,100
ETH,3010,150`,
    );

    const schema = { symbol: DType.String, price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv(csvPath, schema).groupby(
      ['symbol'],
      [
        { col: 'volume', func: 'sum', outName: 'total_volume' },
        { col: 'price', func: 'mean', outName: 'avg_price' },
      ],
    );

    const result = await lf.collect();
    expect(result.ok).toBe(true);

    if (!result.ok) return;
    const df = result.data;

    expect(getRowCount(df)).toBe(2);
    expect(df.columnOrder).toEqual(['symbol', 'total_volume', 'avg_price']);
  });

  test('executes chained operations', async () => {
    setupTest();
    const csvPath = join(tmpDir, 'data.csv');
    writeFileSync(
      csvPath,
      `symbol,side,price,volume
BTC,buy,50000,10
BTC,buy,50100,20
BTC,sell,50200,15
ETH,buy,3000,100
ETH,sell,3010,150`,
    );

    const schema = {
      symbol: DType.String,
      side: DType.String,
      price: DType.Float64,
      volume: DType.Int32,
    };

    const lf = LazyFrame.scanCsv(csvPath, schema)
      .filter('side', '==', 'buy')
      .select(['symbol', 'price', 'volume'])
      .groupby(['symbol'], [{ col: 'volume', func: 'sum', outName: 'total_volume' }]);

    const result = await lf.collect();
    expect(result.ok).toBe(true);

    if (!result.ok) return;
    const df = result.data;

    expect(getRowCount(df)).toBe(2);
    expect(df.columnOrder).toEqual(['symbol', 'total_volume']);

    const totalVolumeCol = df.columns.get('total_volume')!;
    // BTC buy: 10 + 20 = 30
    // ETH buy: 100
    expect(totalVolumeCol.view.getInt32(0, true)).toBe(30);
    expect(totalVolumeCol.view.getInt32(4, true)).toBe(100);
  });

  test('handles multiple filters', async () => {
    setupTest();
    const csvPath = join(tmpDir, 'data.csv');
    writeFileSync(
      csvPath,
      `price,volume
100,50
200,100
150,75
250,120
300,200`,
    );

    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv(csvPath, schema)
      .filter('price', '>', 100)
      .filter('price', '<', 300)
      .filter('volume', '>', 70);

    const result = await lf.collect();
    expect(result.ok).toBe(true);

    if (!result.ok) return;
    const df = result.data;

    // Should keep: 200,100; 150,75; and 250,120
    expect(getRowCount(df)).toBe(3);
  });

  test('handles empty result from filters', async () => {
    setupTest();
    const csvPath = join(tmpDir, 'data.csv');
    writeFileSync(
      csvPath,
      `price,volume
100,50
200,100`,
    );

    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv(csvPath, schema).filter('price', '>', 1000);

    const result = await lf.collect();
    expect(result.ok).toBe(true);

    if (!result.ok) return;
    const df = result.data;

    expect(getRowCount(df)).toBe(0);
  });

  test('returns error for invalid CSV', async () => {
    const lf = LazyFrame.scanCsv('nonexistent.csv', { price: DType.Float64 });

    const result = await lf.collect();
    expect(result.ok).toBe(false);
  });

  test('propagates error from filter on non-existent column', async () => {
    setupTest();
    const csvPath = join(tmpDir, 'data.csv');
    writeFileSync(
      csvPath,
      `price,volume
100,50`,
    );

    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv(csvPath, schema).filter('nonexistent', '>', 100);

    const result = await lf.collect();
    expect(result.ok).toBe(false);
  });

  test('propagates error from groupby on non-existent column', async () => {
    setupTest();
    const csvPath = join(tmpDir, 'data.csv');
    writeFileSync(
      csvPath,
      `price,volume
100,50`,
    );

    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv(csvPath, schema).groupby(
      ['nonexistent'],
      [{ col: 'price', func: 'sum', outName: 'total' }],
    );

    const result = await lf.collect();
    expect(result.ok).toBe(false);
  });
});
