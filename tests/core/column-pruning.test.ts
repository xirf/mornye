/**
 * Tests for column pruning optimization
 */

import { describe, expect, test } from 'bun:test';
import { unlinkSync, writeFileSync } from 'node:fs';
import { getColumn } from '../../src/dataframe/dataframe';
import {
  analyzeRequiredColumns,
  getRequiredColumnIndices,
  shouldPruneColumns,
} from '../../src/lazyframe/column-analyzer';
import { estimatePruningSavings, scanCsvWithPruning } from '../../src/lazyframe/csv-pruning';
import { QueryPlan } from '../../src/lazyframe/plan';

describe('Column Pruning', () => {
  describe('analyzeRequiredColumns', () => {
    test('scan alone requires no columns (data source)', async () => {
      const plan = QueryPlan.scan('test.csv', {}, []);
      const required = analyzeRequiredColumns(plan);
      expect(required.size).toBe(0);
    });

    test('filter requires its column', async () => {
      const plan = QueryPlan.filter(QueryPlan.scan('test.csv', {}, []), 'price', '>', 100);
      const required = analyzeRequiredColumns(plan);
      expect(required.has('price')).toBe(true);
    });

    test('select explicitly specifies required columns', async () => {
      const plan = QueryPlan.select(QueryPlan.scan('test.csv', {}, []), ['name', 'price']);
      const required = analyzeRequiredColumns(plan);
      expect(required.size).toBe(2);
      expect(required.has('name')).toBe(true);
      expect(required.has('price')).toBe(true);
    });

    test('filter + select requires both sets of columns', async () => {
      const scan = QueryPlan.scan('test.csv', {}, []);
      const filtered = QueryPlan.filter(scan, 'quantity', '>', 10);
      const selected = QueryPlan.select(filtered, ['name', 'price']);

      const required = analyzeRequiredColumns(selected);
      expect(required.size).toBe(3); // Select columns + filter column
      expect(required.has('name')).toBe(true);
      expect(required.has('price')).toBe(true);
      expect(required.has('quantity')).toBe(true); // Filter column is required too
    });

    test('groupby requires group and aggregation columns', async () => {
      const plan = QueryPlan.groupby(
        QueryPlan.scan('test.csv', {}, []),
        ['category'],
        [{ col: 'sales', func: 'sum', outName: 'total_sales' }],
      );
      const required = analyzeRequiredColumns(plan);
      expect(required.has('category')).toBe(true);
      expect(required.has('sales')).toBe(true);
    });

    test('complex plan: filter -> select -> groupby', async () => {
      const scan = QueryPlan.scan('test.csv', {}, []);
      const filtered = QueryPlan.filter(scan, 'status', '==', 'active');
      const selected = QueryPlan.select(filtered, ['category', 'sales', 'status']);
      const grouped = QueryPlan.groupby(
        selected,
        ['category'],
        [{ col: 'sales', func: 'sum', outName: 'total' }],
      );

      const required = analyzeRequiredColumns(grouped);
      // Should need: category (group), sales (agg), status (filter)
      // But select cuts it off to just category, sales, status
      expect(required.has('category')).toBe(true);
      expect(required.has('sales')).toBe(true);
      expect(required.has('status')).toBe(true);
    });
  });

  describe('shouldPruneColumns', () => {
    test('returns false when no select in plan', async () => {
      const plan = QueryPlan.filter(QueryPlan.scan('test.csv', {}, []), 'price', '>', 100);
      expect(shouldPruneColumns(plan, 10)).toBe(false);
    });

    test('returns true when select uses few columns from many', async () => {
      const plan = QueryPlan.select(QueryPlan.scan('test.csv', {}, []), ['col1', 'col2']);
      expect(shouldPruneColumns(plan, 10)).toBe(true); // 2/10 = 20% < 70%
    });

    test('returns false when selecting most columns', async () => {
      const plan = QueryPlan.select(QueryPlan.scan('test.csv', {}, []), [
        'col1',
        'col2',
        'col3',
        'col4',
        'col5',
        'col6',
        'col7',
        'col8',
      ]);
      expect(shouldPruneColumns(plan, 10)).toBe(false); // 8/10 = 80% > 70%
    });
  });

  describe('getRequiredColumnIndices', () => {
    test('maps column names to indices', async () => {
      const headers = ['id', 'name', 'price', 'quantity', 'category'];
      const required = new Set(['name', 'price']);

      const indices = getRequiredColumnIndices(headers, required);
      expect(indices.get('name')).toBe(1);
      expect(indices.get('price')).toBe(2);
      expect(indices.has('quantity')).toBe(false);
    });

    test('handles columns in different order', async () => {
      const headers = ['z', 'a', 'm', 'b'];
      const required = new Set(['m', 'z']);

      const indices = getRequiredColumnIndices(headers, required);
      expect(indices.get('z')).toBe(0);
      expect(indices.get('m')).toBe(2);
    });
  });

  describe('scanCsvWithPruning', () => {
    const testFile = './test-pruning.csv';

    test('loads only required columns', async () => {
      // Create test CSV
      const csv =
        'id,name,price,quantity,category\n1,Apple,1.50,100,Fruit\n2,Banana,0.80,150,Fruit\n3,Carrot,0.60,200,Vegetable';
      writeFileSync(testFile, csv);

      try {
        const result = await scanCsvWithPruning(testFile, {
          requiredColumns: new Set(['name', 'price']),
          schema: new Map([
            ['name', 'string'],
            ['price', 'float64'],
          ]),
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        // Should only have 2 columns
        expect(result.data.columns.size).toBe(2);
        expect(result.data.columns.has('name')).toBe(true);
        expect(result.data.columns.has('price')).toBe(true);
        expect(result.data.columns.has('quantity')).toBe(false);

        // Check data
        const nameCol = getColumn(result.data, 'name');
        const priceCol = getColumn(result.data, 'price');

        expect(nameCol.ok).toBe(true);
        expect(priceCol.ok).toBe(true);

        if (nameCol.ok && priceCol.ok) {
          expect(nameCol.data.length).toBe(3);
          expect(priceCol.data.length).toBe(3);
        }
      } finally {
        unlinkSync(testFile);
      }
    });

    test('returns error for missing required column', async () => {
      const csv = 'id,name,price\n1,Apple,1.50';
      writeFileSync(testFile, csv);

      try {
        const result = await scanCsvWithPruning(testFile, {
          requiredColumns: new Set(['name', 'price', 'quantity']),
        });

        expect(result.ok).toBe(false);
        if (!result.ok) {
          expect(result.error.message).toContain('quantity');
        }
      } finally {
        unlinkSync(testFile);
      }
    });

    test('handles wide table efficiently', async () => {
      // Create CSV with 20 columns
      const headers = Array.from({ length: 20 }, (_, i) => `col${i}`).join(',');
      const row = Array.from({ length: 20 }, (_, i) => i * 10).join(',');
      const csv = `${headers}\n${row}\n${row}\n${row}`;

      writeFileSync(testFile, csv);

      try {
        // Only select 3 columns
        const result = await scanCsvWithPruning(testFile, {
          requiredColumns: new Set(['col0', 'col5', 'col10']),
          schema: new Map([
            ['col0', 'int32'],
            ['col5', 'int32'],
            ['col10', 'int32'],
          ]),
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        // Should only have 3 columns (15% of 20)
        expect(result.data.columns.size).toBe(3);

        const col0 = getColumn(result.data, 'col0');
        const col5 = getColumn(result.data, 'col5');
        const col10 = getColumn(result.data, 'col10');

        expect(col0.ok).toBe(true);
        expect(col5.ok).toBe(true);
        expect(col10.ok).toBe(true);

        if (col0.ok && col5.ok && col10.ok) {
          expect(col0.data.view.getInt32(0, true)).toBe(0);
          expect(col5.data.view.getInt32(0, true)).toBe(50);
          expect(col10.data.view.getInt32(0, true)).toBe(100);
        }
      } finally {
        unlinkSync(testFile);
      }
    });
  });

  describe('estimatePruningSavings', () => {
    test('calculates memory savings correctly', async () => {
      const result = estimatePruningSavings(10, 3, 1000, 8);

      expect(result.totalBytes).toBe(80000); // 10 * 1000 * 8
      expect(result.requiredBytes).toBe(24000); // 3 * 1000 * 8
      expect(result.savingsPercent).toBe(70);
    });

    test('handles 50% pruning', async () => {
      const result = estimatePruningSavings(20, 10, 5000, 8);

      expect(result.savingsPercent).toBe(50);
    });

    test('handles minimal pruning', async () => {
      const result = estimatePruningSavings(10, 9, 1000, 8);

      expect(result.savingsPercent).toBe(10);
    });
  });
});
