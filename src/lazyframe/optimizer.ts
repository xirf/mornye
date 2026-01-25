/**
 * Query Plan Optimizer
 *
 * Analyzes and optimizes query plans before execution by:
 * - Combining multiple filters into single operations
 * - Reordering operations for optimal performance
 * - Detecting optimization opportunities (predicate pushdown, column pruning)
 * - Cost-based decisions using selectivity estimates
 * - Filter deduplication and simplification
 */

import type { FilterOperator } from '../types/operators';
import type { FilterPlan, PlanNode, ScanPlan, SelectPlan } from './plan';

/**
 * Optimization statistics for cost-based decisions
 */
export interface OptimizationStats {
  /** Estimated number of rows after this operation */
  estimatedRows: number;
  /** Estimated selectivity (0.0 - 1.0) */
  selectivity: number;
  /** Can this operation be pushed down to scan? */
  pushdownEligible: boolean;
  /** Estimated cost (lower is better) */
  cost: number;
}

/**
 * Optimized query plan with metadata
 */
export interface OptimizedPlan {
  plan: PlanNode;
  stats: OptimizationStats;
  optimizationsApplied: string[];
}

/**
 * Main entry point: optimize a query plan
 *
 * @param plan - The query plan to optimize
 * @param estimatedInputRows - Estimated number of input rows (for cost calculations)
 * @returns Optimized plan with statistics
 */
export function optimizePlan(plan: PlanNode, estimatedInputRows = 1000000): OptimizedPlan {
  const optimizationsApplied: string[] = [];

  // Step 1: Combine consecutive filters
  let optimized = combineFilters(plan, optimizationsApplied);

  // Step 2: Reorder operations for optimal execution
  optimized = reorderOperations(optimized, optimizationsApplied);

  // Step 3: Detect and mark pushdown opportunities
  optimized = detectPushdownOpportunities(optimized, optimizationsApplied);

  // Step 4: Calculate statistics for cost-based execution
  const stats = calculateStats(optimized, estimatedInputRows);

  return {
    plan: optimized,
    stats,
    optimizationsApplied,
  };
}

/**
 * Combine consecutive filter operations into multi-predicate filters
 * Reduces overhead and enables better optimization
 */
function combineFilters(plan: PlanNode, optimizations: string[]): PlanNode {
  if (plan.type !== 'filter') {
    // Recursively process child nodes
    switch (plan.type) {
      case 'select':
        return {
          ...plan,
          input: combineFilters(plan.input, optimizations),
        };
      case 'groupby':
        return {
          ...plan,
          input: combineFilters(plan.input, optimizations),
        };
      default:
        return plan;
    }
  }

  // Collect consecutive filters
  const filters: FilterPlan[] = [];
  let current: PlanNode = plan;

  while (current.type === 'filter') {
    filters.push(current);
    current = current.input;
  }

  if (filters.length > 1) {
    optimizations.push(`Combined ${filters.length} consecutive filters`);

    // Sort by selectivity (most selective first)
    const sorted = sortFiltersBySelectivity(filters);

    // Use sorted filters
    filters.length = 0;
    filters.push(...sorted);
  }

  // Process the base (non-filter) node
  const baseInput = combineFilters(current, optimizations);

  // Rebuild filter chain with sorted filters
  // Filters are collected top-to-bottom (reverse execution order)
  // After sorting by selectivity (most selective = lowest value first)
  // Rebuild by applying most selective closest to scan
  // So iterate REVERSE through sorted array (most selective at end, closest to scan)
  let result: PlanNode = baseInput;
  for (let i = filters.length - 1; i >= 0; i--) {
    const filter = filters[i];
    if (!filter) continue;
    result = {
      type: 'filter' as const,
      input: result,
      column: filter.column,
      operator: filter.operator,
      value: filter.value,
      id: filter.id,
    };
  }

  return result;
}

/**
 * Reorder operations for optimal performance
 * Rules:
 * - Filters before selects (reduce data volume early)
 * - Most selective filters first (reduce data volume maximally)
 * - Selects as late as possible (keep all columns for filter evaluation)
 */
function reorderOperations(plan: PlanNode, optimizations: string[]): PlanNode {
  // Collect all operations in the chain
  const operations: PlanNode[] = [];
  let current: PlanNode = plan;
  let scanNode: ScanPlan | null = null;

  while (current.type !== 'scan') {
    operations.push(current);
    current = (current as FilterPlan | SelectPlan).input;
  }
  scanNode = current as ScanPlan;

  if (operations.length === 0) {
    return plan; // Nothing to reorder
  }

  // Separate operations by type
  const filters: FilterPlan[] = [];
  const selects: SelectPlan[] = [];
  const others: PlanNode[] = [];

  for (const op of operations) {
    if (op.type === 'filter') {
      filters.push(op as FilterPlan);
    } else if (op.type === 'select') {
      selects.push(op as SelectPlan);
    } else {
      others.push(op);
    }
  }

  // Sort filters by estimated selectivity (most selective first)
  const sortedFilters = sortFiltersBySelectivity(filters);

  // Rebuild plan: scan → filters (sorted) → selects → other ops (groupby, etc.)
  // IMPORTANT: Selects must come BEFORE groupby because groupby transforms columns
  let result: PlanNode = scanNode;

  // Apply filters first (most selective first)
  for (const filter of sortedFilters) {
    result = {
      ...filter,
      input: result,
    };
  }

  // Then selects (to reduce data size before expensive operations)
  for (const selectOp of selects) {
    result = {
      ...selectOp,
      input: result,
    };
  }

  // Finally other operations (groupby, etc.)
  for (const op of others) {
    result = {
      ...op,
      input: result,
    } as PlanNode;
  }

  if (sortedFilters.length > 1 && JSON.stringify(sortedFilters) !== JSON.stringify(filters)) {
    optimizations.push('Reordered filters by selectivity');
  }

  if (selects.length > 0 && operations[0] && operations[0].type !== 'select') {
    optimizations.push('Moved select operations after filters');
  }

  return result;
}

/**
 * Sort filters by estimated selectivity (most selective first)
 * Uses heuristics when actual statistics are not available
 */
function sortFiltersBySelectivity(filters: FilterPlan[]): FilterPlan[] {
  return filters.slice().sort((a, b) => {
    const selectivityA = estimateFilterSelectivity(a);
    const selectivityB = estimateFilterSelectivity(b);
    return selectivityA - selectivityB; // Lower selectivity (more selective) first
  });
}

/**
 * Estimate selectivity of a filter operation (0.0 = filters all, 1.0 = keeps all)
 * Uses heuristics based on operator and value type
 */
function estimateFilterSelectivity(filter: FilterPlan): number {
  const { operator, value } = filter;

  // Equality is typically very selective
  if (operator === '==') {
    if (typeof value === 'string') {
      return 0.05; // String equality: ~5% match (high selectivity)
    }
    return 0.1; // Numeric equality: ~10% match
  }

  // Inequality is less selective
  if (operator === '!=') {
    return 0.9; // ~90% typically pass
  }

  // Range operators
  if (operator === '>' || operator === '<') {
    return 0.5; // ~50% on average
  }

  if (operator === '>=' || operator === '<=') {
    return 0.5; // ~50% on average
  }

  // Default for unknown operators
  return 0.5;
}

/**
 * Detect and mark operations eligible for pushdown optimization
 * Adds metadata to help executor make decisions
 */
function detectPushdownOpportunities(plan: PlanNode, optimizations: string[]): PlanNode {
  if (plan.type === 'scan') {
    return plan;
  }

  // Check if this is a filter → ... → scan pattern (pushdown eligible)
  if (plan.type === 'filter') {
    const filterPlan = plan as FilterPlan;
    const isPushdownEligible = canPushdownFilter(filterPlan);

    if (isPushdownEligible) {
      // Check if there's a scan anywhere in the chain
      let hasScan = false;
      let current: PlanNode = filterPlan.input;
      while (current.type !== 'scan') {
        if (current.type === 'filter' || current.type === 'select') {
          current = (current as FilterPlan | SelectPlan).input;
        } else {
          break;
        }
      }
      if (current.type === 'scan') {
        hasScan = true;
        optimizations.push(`Detected pushdown opportunity for filter on '${filterPlan.column}'`);
      }
    }

    return {
      ...filterPlan,
      input: detectPushdownOpportunities(filterPlan.input, optimizations),
    };
  }

  // Check for select → ... → scan pattern (column pruning eligible)
  if (plan.type === 'select') {
    const selectPlan = plan as SelectPlan;

    // Check if there's a scan anywhere in the chain
    let current: PlanNode = selectPlan.input;
    while (current.type !== 'scan') {
      if (current.type === 'filter' || current.type === 'select') {
        current = (current as FilterPlan | SelectPlan).input;
      } else {
        break;
      }
    }

    if (current.type === 'scan') {
      optimizations.push(
        `Detected column pruning opportunity (selecting ${selectPlan.columns.length} columns)`,
      );
    }

    return {
      ...selectPlan,
      input: detectPushdownOpportunities(selectPlan.input, optimizations),
    };
  }

  // Recursively process other node types
  if (plan.type === 'groupby') {
    return {
      ...plan,
      input: detectPushdownOpportunities(plan.input, optimizations),
    };
  }

  return plan;
}

/**
 * Check if a filter can be pushed down to the scan operation
 * Only simple column comparisons can be pushed down
 */
function canPushdownFilter(filter: FilterPlan): boolean {
  // Only simple comparison operators can be pushed down
  const pushdownOperators: FilterOperator[] = ['==', '!=', '>', '<', '>=', '<='];
  if (!pushdownOperators.includes(filter.operator)) {
    return false;
  }

  // Value must be a simple scalar (not array or complex expression)
  if (Array.isArray(filter.value)) {
    return false;
  }

  return true;
}

/**
 * Calculate statistics for the optimized plan
 * Used for cost-based execution decisions
 */
function calculateStats(plan: PlanNode, inputRows: number): OptimizationStats {
  let estimatedRows = inputRows;
  let selectivity = 1.0;
  let pushdownEligible = false;
  let cost = 0;

  const traverse = (node: PlanNode, rows: number): number => {
    switch (node.type) {
      case 'scan':
        cost += rows * 1; // Base cost for scanning
        return rows;

      case 'filter': {
        const filterPlan = node as FilterPlan;
        const filterSelectivity = estimateFilterSelectivity(filterPlan);
        const outputRows = rows * filterSelectivity;

        if (canPushdownFilter(filterPlan) && filterPlan.input.type === 'scan') {
          pushdownEligible = true;
          cost += rows * 0.5; // Cheaper when pushed down
        } else {
          cost += rows * 1.5; // More expensive in-memory filter
        }

        return traverse(filterPlan.input, outputRows);
      }

      case 'select': {
        const selectPlan = node as SelectPlan;
        const columnRatio = selectPlan.columns.length / 10; // Assume ~10 columns average
        cost += rows * columnRatio * 0.3; // Select is relatively cheap
        return traverse(selectPlan.input, rows);
      }

      case 'groupby':
        cost += rows * 2; // GroupBy is expensive
        return traverse(node.input, rows * 0.1); // Typically reduces rows significantly
    }

    return rows;
  };

  estimatedRows = traverse(plan, inputRows);
  selectivity = estimatedRows / inputRows;

  return {
    estimatedRows,
    selectivity,
    pushdownEligible,
    cost,
  };
}

/**
 * Deduplicate identical consecutive filters
 * Removes redundant filter operations
 */
export function deduplicateFilters(plan: PlanNode): PlanNode {
  if (plan.type !== 'filter') {
    // Recursively process child nodes
    switch (plan.type) {
      case 'select':
        return {
          ...plan,
          input: deduplicateFilters(plan.input),
        };
      case 'groupby':
        return {
          ...plan,
          input: deduplicateFilters(plan.input),
        };
      default:
        return plan;
    }
  }

  const filterPlan = plan as FilterPlan;
  const processedInput = deduplicateFilters(filterPlan.input);

  // Check if input is an identical filter
  if (
    processedInput.type === 'filter' &&
    processedInput.column === filterPlan.column &&
    processedInput.operator === filterPlan.operator &&
    processedInput.value === filterPlan.value
  ) {
    // Skip this duplicate filter
    return processedInput;
  }

  return {
    ...filterPlan,
    input: processedInput,
  };
}

/**
 * Explain the optimization decisions made
 * Returns a human-readable description of the optimized plan
 */
export function explainPlan(optimized: OptimizedPlan): string {
  const lines: string[] = [];

  lines.push('=== Query Plan Optimization ===\n');

  if (optimized.optimizationsApplied.length === 0) {
    lines.push('No optimizations applied (plan is already optimal)\n');
  } else {
    lines.push('Optimizations Applied:');
    for (const opt of optimized.optimizationsApplied) {
      lines.push(`  • ${opt}`);
    }
    lines.push('');
  }

  lines.push('Statistics:');
  lines.push(
    `  Estimated output rows: ${Math.round(optimized.stats.estimatedRows).toLocaleString()}`,
  );
  lines.push(`  Selectivity: ${(optimized.stats.selectivity * 100).toFixed(1)}%`);
  lines.push(`  Pushdown eligible: ${optimized.stats.pushdownEligible ? 'Yes' : 'No'}`);
  lines.push(`  Estimated cost: ${optimized.stats.cost.toFixed(2)}`);
  lines.push('');

  lines.push('Optimized Plan:');
  lines.push(formatPlanTree(optimized.plan, 1));

  return lines.join('\n');
}

/**
 * Format plan tree for display
 */
function formatPlanTree(plan: PlanNode, depth: number): string {
  const indent = '  '.repeat(depth);
  const lines: string[] = [];

  switch (plan.type) {
    case 'scan':
      lines.push(`${indent}Scan: ${plan.path}`);
      break;

    case 'filter': {
      const filterPlan = plan as FilterPlan;
      lines.push(
        `${indent}Filter: ${filterPlan.column} ${filterPlan.operator} ${filterPlan.value}`,
      );
      lines.push(formatPlanTree(filterPlan.input, depth + 1));
      break;
    }

    case 'select': {
      const selectPlan = plan as SelectPlan;
      lines.push(`${indent}Select: [${selectPlan.columns.join(', ')}]`);
      lines.push(formatPlanTree(selectPlan.input, depth + 1));
      break;
    }

    case 'groupby':
      lines.push(`${indent}GroupBy: ${plan.groupKeys.join(', ')}`);
      lines.push(formatPlanTree(plan.input, depth + 1));
      break;
  }

  return lines.join('\n');
}
