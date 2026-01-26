import { describe, expect, test } from 'bun:test';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { getColumnValue } from '../../src/core/column';
import { getColumn, getRowCount } from '../../src/dataframe/dataframe';
import { readCsv } from '../../src/io/csv-reader';
import { scanCsv } from '../../src/io/csv-scanner';
import { DType } from '../../src/types/dtypes';
import { formatDateTime } from '../../src/utils/datetime';

describe('Bitcoin CSV Integration Test', () => {
  const csvPath = resolve(process.cwd(), 'artifac', 'btcusd_1-min_data.csv');
  const csvExists = existsSync(csvPath);

  if (!csvExists) {
    test.skip('requires Bitcoin CSV dataset', () => {
      // dataset missing; skip
    });
    return;
  }

  test('loads Bitcoin 1-minute OHLCV data using scanCsv', async () => {
    // Define schema matching the CSV structure
    const schema = {
      Timestamp: DType.Float64,
      Open: DType.Float64,
      High: DType.Float64,
      Low: DType.Float64,
      Close: DType.Float64,
      Volume: DType.Float64,
    };

    // Scan with chunking (default 50K rows per chunk) - scanCsv now handles file reading
    const result = await scanCsv(csvPath, { schema, chunkSize: 100000 });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;

    // Verify row count (should be ~7.3M rows)
    const rowCount = getRowCount(df);
    console.log(`Loaded ${rowCount.toLocaleString()} rows`);
    expect(rowCount).toBeGreaterThan(7_000_000);
    expect(rowCount).toBeLessThan(8_000_000);

    // Verify all columns exist
    const timestampCol = getColumn(df, 'Timestamp');
    expect(timestampCol.ok).toBe(true);

    const openCol = getColumn(df, 'Open');
    expect(openCol.ok).toBe(true);

    const closeCol = getColumn(df, 'Close');
    expect(closeCol.ok).toBe(true);

    const volumeCol = getColumn(df, 'Volume');
    expect(volumeCol.ok).toBe(true);

    // Verify first row values
    if (timestampCol.ok && openCol.ok && closeCol.ok && volumeCol.ok) {
      const firstTimestamp = getColumnValue(timestampCol.data, 0);
      const firstOpen = getColumnValue(openCol.data, 0);
      const firstClose = getColumnValue(closeCol.data, 0);
      const firstVolume = getColumnValue(volumeCol.data, 0);

      expect(firstTimestamp).toBe(1325412060.0);
      expect(firstOpen).toBe(4.58);
      expect(firstClose).toBe(4.58);
      expect(firstVolume).toBe(0.0);

      console.log(
        `First row: ${firstTimestamp}, Open=${firstOpen}, Close=${firstClose}, Volume=${firstVolume}`,
      );
    }
  }, 60000); // 60 second timeout for large file

  test('loads smaller sample with eager readCsv', async () => {
    // Read the file to get a sample
    const file = Bun.file(csvPath);
    const fullData = await file.text();

    // Take only first 1000 lines for eager loading test
    const lines = fullData.split('\n');
    const sampleData = lines.slice(0, 1001).join('\n'); // header + 1000 rows

    // Define schema
    const schema = {
      Timestamp: DType.Float64,
      Open: DType.Float64,
      High: DType.Float64,
      Low: DType.Float64,
      Close: DType.Float64,
      Volume: DType.Float64,
    };

    // Use eager loading for small sample (readCsvFromString since we have data in memory)
    const { readCsvFromString } = await import('../../src/io/csv-reader');
    const result = await readCsvFromString(sampleData, { schema });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const rowCount = getRowCount(df);

    expect(rowCount).toBe(1000);
    console.log(`Sample loaded: ${rowCount} rows`);
  }, 10000);

  test('verifies data integrity across chunks', async () => {
    // Read the file
    // File reading now handled by readCsv/scanCsv

    // Define schema
    const schema = {
      Timestamp: DType.Float64,
      Open: DType.Float64,
      High: DType.Float64,
      Low: DType.Float64,
      Close: DType.Float64,
      Volume: DType.Float64,
    };

    // Scan with small chunks to test chunk boundaries
    const result = await scanCsv(csvPath, { schema, chunkSize: 10000 });

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    const df = result.data;
    const rowCount = getRowCount(df);

    // Verify row count
    expect(rowCount).toBeGreaterThan(7_000_000);

    // Get columns
    const timestampCol = getColumn(df, 'Timestamp');
    const highCol = getColumn(df, 'High');
    const lowCol = getColumn(df, 'Low');

    if (timestampCol.ok && highCol.ok && lowCol.ok) {
      // Verify timestamps are monotonically increasing
      let prevTimestamp = getColumnValue(timestampCol.data, 0);
      let violations = 0;
      const checkInterval = 10000; // Check every 10K rows

      for (let i = checkInterval; i < rowCount; i += checkInterval) {
        const currentTimestamp = getColumnValue(timestampCol.data, i);
        if (typeof currentTimestamp === 'number' && typeof prevTimestamp === 'number') {
          if (currentTimestamp < prevTimestamp) {
            violations++;
          }
          prevTimestamp = currentTimestamp;
        }
      }

      console.log(`Timestamp ordering violations: ${violations}`);

      // Verify High >= Low for some samples
      let invalidPrices = 0;
      for (let i = 0; i < rowCount; i += checkInterval) {
        const high = getColumnValue(highCol.data, i);
        const low = getColumnValue(lowCol.data, i);
        if (typeof high === 'number' && typeof low === 'number') {
          if (high < low) {
            invalidPrices++;
          }
        }
      }

      expect(invalidPrices).toBe(0);
      console.log(
        `Verified High >= Low invariant for ${Math.floor(rowCount / checkInterval)} samples`,
      );
    }
  }, 60000);

  test('handles different chunk sizes', async () => {
    // Read the file
    // File reading now handled by readCsv/scanCsv

    const schema = {
      Timestamp: DType.Float64,
      Open: DType.Float64,
      High: DType.Float64,
      Low: DType.Float64,
      Close: DType.Float64,
      Volume: DType.Float64,
    };

    // Test with various chunk sizes
    const chunkSizes = [5000, 50000, 200000];

    for (const chunkSize of chunkSizes) {
      const result = await scanCsv(csvPath, { schema, chunkSize });

      expect(result.ok).toBe(true);
      if (!result.ok) continue;

      const df = result.data;
      const rowCount = getRowCount(df);

      console.log(
        `Chunk size ${chunkSize.toLocaleString()}: Loaded ${rowCount.toLocaleString()} rows`,
      );
      expect(rowCount).toBeGreaterThan(7_000_000);
    }
  }, 120000);
});
