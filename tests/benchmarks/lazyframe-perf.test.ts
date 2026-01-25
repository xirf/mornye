import { describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { DType, LazyFrame, getRowCount } from '../../src';

describe('LazyFrame Performance', () => {
  let tmpDir: string;

  function setupTest() {
    tmpDir = mkdtempSync(join(tmpdir(), 'molniya-perf-'));
  }

  function cleanup() {
    if (tmpDir) {
      rmSync(tmpDir, { recursive: true, force: true });
    }
  }

  test(
    'LazyFrame filter chain performance',
    async () => {
      setupTest();
      try {
        // Create a CSV with 100K rows
        const csvPath = join(tmpDir, 'data.csv');
        const rows = 100_000;
        let csvContent = 'id,value,category\n';
        for (let i = 0; i < rows; i++) {
          csvContent += `${i},${Math.random() * 1000},cat${i % 10}\n`;
        }
        writeFileSync(csvPath, csvContent);

        const schema = {
          id: DType.Int32,
          value: DType.Float64,
          category: DType.String,
        };

        // Test LazyFrame execution time
        const start = performance.now();

        const lf = LazyFrame.scanCsv(csvPath, schema)
          .filter('value', '>', 500)
          .filter('id', '<', 90000)
          .select(['id', 'category']);

        const result = await lf.collect();
        const duration = performance.now() - start;

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        const df = result.data;
        const rowCount = getRowCount(df);

        console.log('\n📊 LazyFrame Filter Chain (100K rows)');
        console.log('Filters: value > 500 AND id < 90000');
        console.log(`Result: ${rowCount} rows in ${duration.toFixed(2)}ms`);
        console.log(`Rate: ${((rows / duration) * 1000).toFixed(0)} rows/sec`);

        // Should complete in reasonable time
        expect(duration).toBeLessThan(1000); // 1 second for 100K rows
        expect(rowCount).toBeGreaterThan(0);
      } finally {
        cleanup();
      }
    },
    { timeout: 30000 },
  );

  test(
    'LazyFrame groupby performance',
    async () => {
      setupTest();
      try {
        // Create a CSV with 50K rows
        const csvPath = join(tmpDir, 'data.csv');
        const rows = 50_000;
        let csvContent = 'category,value\n';
        for (let i = 0; i < rows; i++) {
          csvContent += `cat${i % 100},${i}\n`;
        }
        writeFileSync(csvPath, csvContent);

        const schema = {
          category: DType.String,
          value: DType.Int32,
        };

        const start = performance.now();

        const lf = LazyFrame.scanCsv(csvPath, schema).groupby(
          ['category'],
          [
            { col: 'value', func: 'sum', outName: 'total' },
            { col: 'value', func: 'count', outName: 'count' },
          ],
        );

        const result = await lf.collect();
        const duration = performance.now() - start;

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        const df = result.data;
        const rowCount = getRowCount(df);

        console.log('\n📊 LazyFrame GroupBy (50K rows, 100 groups)');
        console.log('Aggregations: sum, count');
        console.log(`Result: ${rowCount} groups in ${duration.toFixed(2)}ms`);
        console.log(`Rate: ${((rows / duration) * 1000).toFixed(0)} rows/sec`);

        expect(duration).toBeLessThan(500); // 500ms for 50K rows
        expect(rowCount).toBe(100); // 100 unique categories
      } finally {
        cleanup();
      }
    },
    { timeout: 30000 },
  );

  test(
    'LazyFrame complex chain performance',
    async () => {
      setupTest();
      try {
        // Create a CSV with 100K rows
        const csvPath = join(tmpDir, 'data.csv');
        const rows = 100_000;
        let csvContent = 'id,price,volume,side\n';
        for (let i = 0; i < rows; i++) {
          const side = i % 2 === 0 ? 'buy' : 'sell';
          csvContent += `${i},${50000 + Math.random() * 10000},${10 + (i % 100)},${side}\n`;
        }
        writeFileSync(csvPath, csvContent);

        const schema = {
          id: DType.Int32,
          price: DType.Float64,
          volume: DType.Int32,
          side: DType.String,
        };

        const start = performance.now();

        const lf = LazyFrame.scanCsv(csvPath, schema)
          .filter('side', '==', 'buy')
          .filter('price', '>', 52000)
          .select(['id', 'volume'])
          .groupby(['id'], [{ col: 'volume', func: 'sum', outName: 'total_volume' }]);

        const result = await lf.collect();
        const duration = performance.now() - start;

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        const df = result.data;
        const rowCount = getRowCount(df);

        console.log('\n📊 LazyFrame Complex Chain (100K rows)');
        console.log('Operations: filter -> filter -> select -> groupby');
        console.log(`Result: ${rowCount} groups in ${duration.toFixed(2)}ms`);
        console.log(`Rate: ${((rows / duration) * 1000).toFixed(0)} rows/sec`);

        expect(duration).toBeLessThan(1000); // 1 second for complex chain on 100K rows
        expect(rowCount).toBeGreaterThan(0);
      } finally {
        cleanup();
      }
    },
    { timeout: 30000 },
  );

  test(
    'String filter performance (dictionary lookup)',
    async () => {
      setupTest();
      try {
        // Create CSV with many string values
        const csvPath = join(tmpDir, 'data.csv');
        const rows = 50_000;
        const categories = ['cat1', 'cat2', 'cat3', 'cat4', 'cat5'];
        let csvContent = 'id,category\n';
        for (let i = 0; i < rows; i++) {
          csvContent += `${i},${categories[i % categories.length]}\n`;
        }
        writeFileSync(csvPath, csvContent);

        const schema = {
          id: DType.Int32,
          category: DType.String,
        };

        const start = performance.now();

        const lf = LazyFrame.scanCsv(csvPath, schema).filter('category', '==', 'cat3');

        const result = await lf.collect();
        const duration = performance.now() - start;

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        const df = result.data;
        const rowCount = getRowCount(df);

        console.log('\n📊 String Filter Performance (50K rows, 5 unique strings)');
        console.log(`Filter: category == 'cat3'`);
        console.log(`Result: ${rowCount} rows in ${duration.toFixed(2)}ms`);
        console.log(`Rate: ${((rows / duration) * 1000).toFixed(0)} rows/sec`);

        expect(duration).toBeLessThan(500); // 500ms for string filter on 50K rows
        expect(rowCount).toBe(10000); // 1/5 of rows
      } finally {
        cleanup();
      }
    },
    { timeout: 30000 },
  );
});
