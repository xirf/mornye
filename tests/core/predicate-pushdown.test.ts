/**
 * Tests for predicate pushdown optimization
 */

import { describe, expect, test } from 'bun:test';
import { unlinkSync, writeFileSync } from 'node:fs';
import { getColumn } from '../../src/dataframe/dataframe';
import { QueryPlan } from '../../src/lazyframe/plan';
import {
  estimatePushdownSavings,
  extractPushdownPredicates,
  scanCsvWithPredicates,
} from '../../src/lazyframe/predicate-pushdown';

describe('Predicate Pushdown', () => {
  describe('extractPushdownPredicates', () => {
    test('extracts filter from simple plan', async () => {
      const plan = QueryPlan.filter(QueryPlan.scan('test.csv', {}, []), 'price', '>', 100);

      const predicates = extractPushdownPredicates(plan);
      expect(predicates.length).toBe(1);
      expect(predicates[0].columnName).toBe('price');
      expect(predicates[0].operator).toBe('>');
      expect(predicates[0].value).toBe(100);
    });

    test('extracts multiple filters from chain', async () => {
      const scan = QueryPlan.scan('test.csv', {}, []);
      const filter1 = QueryPlan.filter(scan, 'price', '>', 100);
      const filter2 = QueryPlan.filter(filter1, 'quantity', '<', 50);

      const predicates = extractPushdownPredicates(filter2);
      expect(predicates.length).toBe(2);
      expect(predicates[0].columnName).toBe('quantity');
      expect(predicates[1].columnName).toBe('price');
    });

    test('only extracts supported operators', async () => {
      const scan = QueryPlan.scan('test.csv', {}, []);
      const filter1 = QueryPlan.filter(scan, 'status', '==', 'active');
      const filter2 = QueryPlan.filter(filter1, 'tags', 'in', ['urgent', 'important']);

      const predicates = extractPushdownPredicates(filter2);
      // 'in' operator not supported in pushdown
      expect(predicates.length).toBe(1);
      expect(predicates[0].columnName).toBe('status');
    });

    test('works with complex plans', async () => {
      const scan = QueryPlan.scan('test.csv', {}, []);
      const filter1 = QueryPlan.filter(scan, 'price', '>', 50);
      const select = QueryPlan.select(filter1, ['id', 'price']);
      const filter2 = QueryPlan.filter(select, 'price', '<', 200);

      const predicates = extractPushdownPredicates(filter2);
      expect(predicates.length).toBe(2);
    });
  });

  describe('scanCsvWithPredicates', () => {
    const testFile = './test-predicate-pushdown.csv';

    test('loads only matching rows (numeric filter)', async () => {
      const csv = 'id,price,quantity\n1,50,100\n2,150,200\n3,75,50\n4,200,300\n5,25,75';
      writeFileSync(testFile, csv);

      try {
        const result = await scanCsvWithPredicates(testFile, {
          predicates: [{ columnName: 'price', operator: '>', value: 100 }],
          schema: new Map([
            ['id', 'int32'],
            ['price', 'int32'],
            ['quantity', 'int32'],
          ]),
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        // Should only load rows where price > 100 (rows 2 and 4)
        const priceCol = getColumn(result.data, 'price');
        expect(priceCol.ok).toBe(true);
        if (!priceCol.ok) return;

        expect(priceCol.data.length).toBe(2);
        expect(priceCol.data.view.getInt32(0, true)).toBe(150);
        expect(priceCol.data.view.getInt32(4, true)).toBe(200);
      } finally {
        unlinkSync(testFile);
      }
    });

    test('loads only matching rows (string filter)', async () => {
      const csv = 'id,status,value\n1,active,10\n2,inactive,20\n3,active,30\n4,pending,40';
      writeFileSync(testFile, csv);

      try {
        const result = await scanCsvWithPredicates(testFile, {
          predicates: [{ columnName: 'status', operator: '==', value: 'active' }],
          schema: new Map([
            ['id', 'int32'],
            ['status', 'string'],
            ['value', 'int32'],
          ]),
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        // Should only load rows where status == 'active' (rows 1 and 3)
        const idCol = getColumn(result.data, 'id');
        expect(idCol.ok).toBe(true);
        if (!idCol.ok) return;

        expect(idCol.data.length).toBe(2);
        expect(idCol.data.view.getInt32(0, true)).toBe(1);
        expect(idCol.data.view.getInt32(4, true)).toBe(3);
      } finally {
        unlinkSync(testFile);
      }
    });

    test('applies multiple predicates (AND logic)', async () => {
      const csv = 'id,price,quantity\n1,50,100\n2,150,200\n3,75,150\n4,200,50\n5,125,175';
      writeFileSync(testFile, csv);

      try {
        const result = await scanCsvWithPredicates(testFile, {
          predicates: [
            { columnName: 'price', operator: '>', value: 100 },
            { columnName: 'quantity', operator: '>', value: 100 },
          ],
          schema: new Map([
            ['id', 'int32'],
            ['price', 'int32'],
            ['quantity', 'int32'],
          ]),
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        // Should only load rows where price > 100 AND quantity > 100 (rows 2 and 5)
        const idCol = getColumn(result.data, 'id');
        expect(idCol.ok).toBe(true);
        if (!idCol.ok) return;

        expect(idCol.data.length).toBe(2);
        expect(idCol.data.view.getInt32(0, true)).toBe(2);
        expect(idCol.data.view.getInt32(4, true)).toBe(5);
      } finally {
        unlinkSync(testFile);
      }
    });

    test('combines with column pruning', async () => {
      const csv =
        'id,name,price,quantity,category\n1,A,50,100,X\n2,B,150,200,Y\n3,C,75,50,X\n4,D,200,300,Z';
      writeFileSync(testFile, csv);

      try {
        const result = await scanCsvWithPredicates(testFile, {
          predicates: [{ columnName: 'price', operator: '>', value: 100 }],
          requiredColumns: new Set(['name', 'price']),
          schema: new Map([
            ['name', 'string'],
            ['price', 'int32'],
          ]),
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        // Should only have 2 columns (name, price)
        expect(result.data.columns.size).toBe(2);
        expect(result.data.columns.has('name')).toBe(true);
        expect(result.data.columns.has('price')).toBe(true);
        expect(result.data.columns.has('quantity')).toBe(false);

        // Should only have 2 rows (price > 100)
        const priceCol = getColumn(result.data, 'price');
        expect(priceCol.ok).toBe(true);
        if (!priceCol.ok) return;

        expect(priceCol.data.length).toBe(2);
      } finally {
        unlinkSync(testFile);
      }
    });

    test('returns empty dataframe when no rows match', async () => {
      const csv = 'id,price\n1,10\n2,20\n3,30';
      writeFileSync(testFile, csv);

      try {
        const result = await scanCsvWithPredicates(testFile, {
          predicates: [{ columnName: 'price', operator: '>', value: 100 }],
          schema: new Map([
            ['id', 'int32'],
            ['price', 'int32'],
          ]),
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        const priceCol = getColumn(result.data, 'price');
        expect(priceCol.ok).toBe(true);
        if (!priceCol.ok) return;

        expect(priceCol.data.length).toBe(0);
      } finally {
        unlinkSync(testFile);
      }
    });

    test('handles all comparison operators', async () => {
      const csv = 'id,value\n1,10\n2,20\n3,30\n4,20\n5,40';
      writeFileSync(testFile, csv);

      const operators = [
        { op: '>' as const, val: 25, expected: [3, 5] },
        { op: '<' as const, val: 25, expected: [1, 2, 4] },
        { op: '>=' as const, val: 20, expected: [2, 3, 4, 5] },
        { op: '<=' as const, val: 20, expected: [1, 2, 4] },
        { op: '==' as const, val: 20, expected: [2, 4] },
        { op: '!=' as const, val: 20, expected: [1, 3, 5] },
      ];

      try {
        for (const { op, val, expected } of operators) {
          const result = await scanCsvWithPredicates(testFile, {
            predicates: [{ columnName: 'value', operator: op, value: val }],
            schema: new Map([
              ['id', 'int32'],
              ['value', 'int32'],
            ]),
          });

          expect(result.ok).toBe(true);
          if (!result.ok) continue;

          const idCol = getColumn(result.data, 'id');
          expect(idCol.ok).toBe(true);
          if (!idCol.ok) continue;

          expect(idCol.data.length).toBe(expected.length);
          for (let i = 0; i < expected.length; i++) {
            expect(idCol.data.view.getInt32(i * 4, true)).toBe(expected[i]);
          }
        }
      } finally {
        unlinkSync(testFile);
      }
    });

    test('handles large selective query', async () => {
      // Create CSV with 1000 rows, only 50 match
      const rows = ['id,value'];
      for (let i = 0; i < 1000; i++) {
        rows.push(`${i},${i % 20}`); // Values 0-19 repeating
      }
      const csv = rows.join('\n');
      writeFileSync(testFile, csv);

      try {
        const result = await scanCsvWithPredicates(testFile, {
          predicates: [{ columnName: 'value', operator: '==', value: 5 }],
          schema: new Map([
            ['id', 'int32'],
            ['value', 'int32'],
          ]),
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;

        const valueCol = getColumn(result.data, 'value');
        expect(valueCol.ok).toBe(true);
        if (!valueCol.ok) return;

        // Should have 50 rows (5% selectivity)
        expect(valueCol.data.length).toBe(50);

        // All values should be 5
        for (let i = 0; i < valueCol.data.length; i++) {
          expect(valueCol.data.view.getInt32(i * 4, true)).toBe(5);
        }
      } finally {
        unlinkSync(testFile);
      }
    });
  });

  describe('estimatePushdownSavings', () => {
    test('calculates savings for 10% selectivity', async () => {
      const result = estimatePushdownSavings(1000, 100, 10, 8);

      expect(result.totalBytes).toBe(80000); // 1000 * 10 * 8
      expect(result.loadedBytes).toBe(8000); // 100 * 10 * 8
      expect(result.savingsPercent).toBe(90);
    });

    test('calculates savings for 50% selectivity', async () => {
      const result = estimatePushdownSavings(10000, 5000, 5, 8);

      expect(result.savingsPercent).toBe(50);
    });

    test('calculates savings for very selective query (1%)', async () => {
      const result = estimatePushdownSavings(100000, 1000, 20, 8);

      expect(result.savingsPercent).toBe(99);
    });
  });
});
