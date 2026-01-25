/**
 * Tests for Query Plan Optimizer
 */

import { describe, expect, it } from 'bun:test';
import {
  type OptimizedPlan,
  deduplicateFilters,
  explainPlan,
  optimizePlan,
} from '../../src/lazyframe/optimizer';
import { QueryPlan } from '../../src/lazyframe/plan';
import type {
  FilterPlan,
  GroupByPlan,
  PlanNode,
  ScanPlan,
  SelectPlan,
} from '../../src/lazyframe/plan';

describe('Query Optimizer', () => {
  // Test data
  const testSchema = {
    id: 'i32' as const,
    name: 'str' as const,
    age: 'i32' as const,
    salary: 'f64' as const,
    department: 'str' as const,
  };

  const scanPlan: ScanPlan = {
    type: 'scan',
    id: 1,
    path: 'test.csv',
    schema: testSchema,
    columnOrder: Object.keys(testSchema),
  };

  describe('Filter Reordering', () => {
    it('should reorder filters by selectivity (most selective first)', () => {
      // Create plan: scan → age > 30 (50%) → department == "Engineering" (5%)
      let plan = QueryPlan.filter(scanPlan, 'age', '>', 30);
      plan = QueryPlan.filter(plan, 'department', '==', 'Engineering');

      const optimized = optimizePlan(plan, 10000);

      // Extract filters in order from top to bottom
      const filters: string[] = [];
      let current: PlanNode = optimized.plan;
      while (current.type === 'filter') {
        filters.push(current.column);
        current = current.input;
      }

      // After optimization: age (top) → department (closer to scan, more selective)
      // Most selective should be closer to scan (later in top-to-bottom traversal)
      expect(filters[filters.length - 1]).toBe('department'); // Closest to scan
      expect(filters[0]).toBe('age'); // Furthest from scan
    });

    it('should keep equality filters before range filters', () => {
      // Create: scan → salary > 50000 → name == "John"
      let plan = QueryPlan.filter(scanPlan, 'salary', '>', 50000);
      plan = QueryPlan.filter(plan, 'name', '==', 'John');

      const optimized = optimizePlan(plan, 10000);

      const filters: string[] = [];
      let current: PlanNode = optimized.plan;
      while (current.type === 'filter') {
        filters.push(current.column);
        current = current.input;
      }

      // Equality (name ==) is more selective, should be closer to scan
      expect(filters[filters.length - 1]).toBe('name'); // Closest to scan
      expect(filters[0]).toBe('salary'); // Furthest from scan
    });

    it('should report filter combination', () => {
      let plan = QueryPlan.filter(scanPlan, 'age', '>', 30);
      plan = QueryPlan.filter(plan, 'department', '==', 'Engineering');

      const optimized = optimizePlan(plan, 10000);

      expect(optimized.optimizationsApplied).toContain('Combined 2 consecutive filters');
    });
  });

  describe('Operation Reordering', () => {
    it('should move filters before selects', () => {
      // Create: scan → select → filter (suboptimal)
      let plan = QueryPlan.select(scanPlan, ['name', 'salary']);
      plan = QueryPlan.filter(plan, 'salary', '>', 50000);

      const optimized = optimizePlan(plan, 10000);

      // Check order: should be filter first, then select
      expect(optimized.plan.type).toBe('select');
      expect((optimized.plan as SelectPlan).input.type).toBe('filter');

      expect(optimized.optimizationsApplied).toContain('Moved select operations after filters');
    });

    it('should handle multiple operations correctly', () => {
      // Create: scan → select → filter1 → filter2
      let plan = QueryPlan.select(scanPlan, ['name', 'salary', 'department']);
      plan = QueryPlan.filter(plan, 'salary', '>', 50000);
      plan = QueryPlan.filter(plan, 'department', '==', 'Engineering');

      const optimized = optimizePlan(plan, 10000);

      // Traverse and collect operation types
      const ops: string[] = [];
      let current: PlanNode = optimized.plan;
      while (current.type !== 'scan') {
        ops.push(current.type);
        current = (current as FilterPlan | SelectPlan | GroupByPlan).input;
      }

      // Should be: select → filter → filter → scan
      expect(ops[0]).toBe('select');
      expect(ops[1]).toBe('filter');
      expect(ops[2]).toBe('filter');
    });
  });

  describe('Pushdown Detection', () => {
    it('should detect predicate pushdown opportunities', () => {
      const plan = QueryPlan.filter(scanPlan, 'age', '>=', 25);
      const optimized = optimizePlan(plan, 10000);

      expect(optimized.stats.pushdownEligible).toBe(true);
      expect(optimized.optimizationsApplied.some((opt) => opt.includes('pushdown'))).toBe(true);
    });

    it('should detect column pruning opportunities', () => {
      const plan = QueryPlan.select(scanPlan, ['name', 'salary']);
      const optimized = optimizePlan(plan, 10000);

      expect(optimized.optimizationsApplied.some((opt) => opt.includes('column pruning'))).toBe(
        true,
      );
    });

    it('should detect combined optimizations', () => {
      let plan = QueryPlan.filter(scanPlan, 'age', '>', 30);
      plan = QueryPlan.select(plan, ['name', 'salary']);

      const optimized = optimizePlan(plan, 10000);

      // Should detect both pushdown and pruning
      const hasPredicatePushdown = optimized.optimizationsApplied.some((opt) =>
        opt.includes('pushdown'),
      );
      const hasColumnPruning = optimized.optimizationsApplied.some((opt) =>
        opt.includes('column pruning'),
      );

      expect(hasPredicatePushdown).toBe(true);
      expect(hasColumnPruning).toBe(true);
    });
  });

  describe('Statistics Calculation', () => {
    it('should estimate rows after filters', () => {
      // Equality filter: ~10% selectivity
      const plan = QueryPlan.filter(scanPlan, 'department', '==', 'Engineering');
      const optimized = optimizePlan(plan, 10000);

      expect(optimized.stats.estimatedRows).toBeLessThan(10000);
      expect(optimized.stats.estimatedRows).toBeGreaterThan(0);
      expect(optimized.stats.selectivity).toBeLessThan(1.0);
    });

    it('should calculate selectivity for multiple filters', () => {
      let plan = QueryPlan.filter(scanPlan, 'age', '>', 30); // ~50%
      plan = QueryPlan.filter(plan, 'department', '==', 'Engineering'); // ~10%

      const optimized = optimizePlan(plan, 10000);

      // Combined selectivity should be ~5% (0.5 * 0.1)
      expect(optimized.stats.selectivity).toBeLessThan(0.1);
      expect(optimized.stats.estimatedRows).toBeLessThan(1000);
    });

    it('should calculate cost estimates', () => {
      const simplePlan = QueryPlan.filter(scanPlan, 'age', '>', 30);
      const complexPlan = QueryPlan.filter(
        QueryPlan.filter(QueryPlan.filter(scanPlan, 'age', '>', 30), 'salary', '>', 50000),
        'department',
        '==',
        'Engineering',
      );

      const simpleOptimized = optimizePlan(simplePlan, 10000);
      const complexOptimized = optimizePlan(complexPlan, 10000);

      // Complex plan should have higher cost
      expect(complexOptimized.stats.cost).toBeGreaterThan(simpleOptimized.stats.cost);
    });
  });

  describe('Filter Deduplication', () => {
    it('should remove duplicate consecutive filters', () => {
      // Create duplicate filters manually
      let plan = QueryPlan.filter(scanPlan, 'age', '>', 30);
      plan = QueryPlan.filter(plan, 'age', '>', 30); // Duplicate!

      const deduplicated = deduplicateFilters(plan);

      // Count filters
      let filterCount = 0;
      let current: PlanNode = deduplicated;
      while (current.type === 'filter') {
        filterCount++;
        current = current.input;
      }

      expect(filterCount).toBe(1); // Duplicate should be removed
    });

    it('should keep different filters', () => {
      let plan = QueryPlan.filter(scanPlan, 'age', '>', 30);
      plan = QueryPlan.filter(plan, 'salary', '>', 50000);

      const deduplicated = deduplicateFilters(plan);

      let filterCount = 0;
      let current: PlanNode = deduplicated;
      while (current.type === 'filter') {
        filterCount++;
        current = current.input;
      }

      expect(filterCount).toBe(2); // Both should remain
    });
  });

  describe('Plan Explanation', () => {
    it('should generate human-readable plan explanation', () => {
      let plan = QueryPlan.filter(scanPlan, 'age', '>', 30);
      plan = QueryPlan.select(plan, ['name', 'salary']);

      const optimized = optimizePlan(plan, 10000);
      const explanation = explainPlan(optimized);

      expect(explanation).toContain('Query Plan Optimization');
      expect(explanation).toContain('Optimizations Applied');
      expect(explanation).toContain('Statistics');
      expect(explanation).toContain('Optimized Plan');
    });

    it('should show applied optimizations', () => {
      let plan = QueryPlan.filter(scanPlan, 'salary', '>', 50000);
      plan = QueryPlan.filter(plan, 'department', '==', 'Engineering');

      const optimized = optimizePlan(plan, 10000);
      const explanation = explainPlan(optimized);

      // Should mention filter reordering
      expect(explanation.toLowerCase()).toContain('filter');
    });

    it('should show statistics', () => {
      const plan = QueryPlan.filter(scanPlan, 'age', '>', 30);
      const optimized = optimizePlan(plan, 10000);
      const explanation = explainPlan(optimized);

      expect(explanation).toContain('Estimated output rows');
      expect(explanation).toContain('Selectivity');
      expect(explanation).toContain('Pushdown eligible');
      expect(explanation).toContain('Estimated cost');
    });
  });

  describe('Edge Cases', () => {
    it('should handle plan with no optimizations needed', () => {
      const optimized = optimizePlan(scanPlan, 10000);

      expect(optimized.plan).toEqual(scanPlan);
      expect(optimized.optimizationsApplied.length).toBe(0);
    });

    it('should handle single filter', () => {
      const plan = QueryPlan.filter(scanPlan, 'age', '>', 30);
      const optimized = optimizePlan(plan, 10000);

      expect(optimized.plan.type).toBe('filter');
      expect((optimized.plan as FilterPlan).column).toBe('age');
    });

    it('should handle single select', () => {
      const plan = QueryPlan.select(scanPlan, ['name', 'age']);
      const optimized = optimizePlan(plan, 10000);

      expect(optimized.plan.type).toBe('select');
      expect((optimized.plan as SelectPlan).columns).toEqual(['name', 'age']);
    });

    it('should handle empty statistics gracefully', () => {
      const optimized = optimizePlan(scanPlan, 0);

      expect(optimized.stats.estimatedRows).toBe(0);
      // With 0 input rows, selectivity might be NaN, which is ok
      expect(optimized.stats.selectivity === 0 || Number.isNaN(optimized.stats.selectivity)).toBe(
        true,
      );
    });
  });

  describe('Real-World Scenarios', () => {
    it('should optimize typical analytics query', () => {
      // Query: Find high-earning engineers, show name and salary
      let plan = QueryPlan.select(scanPlan, ['name', 'salary', 'age']);
      plan = QueryPlan.filter(plan, 'department', '==', 'Engineering');
      plan = QueryPlan.filter(plan, 'salary', '>=', 100000);
      plan = QueryPlan.select(plan, ['name', 'salary']);

      const optimized = optimizePlan(plan, 100000);

      // Should detect multiple optimizations
      expect(optimized.optimizationsApplied.length).toBeGreaterThan(0);

      // Should have low selectivity (very selective query)
      expect(optimized.stats.selectivity).toBeLessThan(0.1);

      // Should be pushdown eligible
      expect(optimized.stats.pushdownEligible).toBe(true);
    });

    it('should optimize complex multi-filter query', () => {
      // Multiple filters with varying selectivity
      let plan = QueryPlan.filter(scanPlan, 'age', '>=', 25); // ~75% pass
      plan = QueryPlan.filter(plan, 'age', '<=', 65); // ~90% of remaining pass
      plan = QueryPlan.filter(plan, 'salary', '>', 60000); // ~50% pass
      plan = QueryPlan.filter(plan, 'department', '==', 'Sales'); // ~10% pass

      const optimized = optimizePlan(plan, 50000);

      // Department == should be closest to scan (most selective)
      let current: PlanNode = optimized.plan;
      // Skip to the last filter before scan
      while (current.type === 'filter' && (current as FilterPlan).input.type !== 'scan') {
        current = (current as FilterPlan).input;
      }
      expect(current.type).toBe('filter');
      expect((current as FilterPlan).column).toBe('department');

      // Should show significant row reduction
      expect(optimized.stats.estimatedRows).toBeLessThan(5000);
    });

    it('should handle wide table column selection', () => {
      // Selecting 2 columns from 5 (40% - should trigger pruning)
      const plan = QueryPlan.select(scanPlan, ['name', 'salary']);
      const optimized = optimizePlan(plan, 100000);

      expect(optimized.optimizationsApplied.some((opt) => opt.includes('column pruning'))).toBe(
        true,
      );
    });
  });
});
