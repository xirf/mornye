import type { DataFrame } from '../dataframe/dataframe';
import { groupby } from '../dataframe/groupby';
import { filter, select } from '../dataframe/operations';
import { readCsv } from '../io/csv-reader';
import type { Result } from '../types/result';
import { err, ok } from '../types/result';
import { analyzeRequiredColumns, shouldPruneColumns } from './column-analyzer';
import { scanCsvWithPruning } from './csv-pruning';
import { type OptimizedPlan, explainPlan, optimizePlan as optimizeQueryPlan } from './optimizer';
import type { FilterPlan, GroupByPlan, PlanNode, ScanPlan, SelectPlan } from './plan';
import {
  type ScanPredicate,
  extractPushdownPredicates,
  scanCsvWithPredicates,
} from './predicate-pushdown';

/**
 * Find the scan node in a plan tree
 */
function findScanNode(plan: PlanNode): ScanPlan | null {
  if (plan.type === 'scan') {
    return plan as ScanPlan;
  }

  switch (plan.type) {
    case 'filter':
    case 'select':
    case 'groupby':
      return findScanNode((plan as FilterPlan | SelectPlan | GroupByPlan).input);
    default:
      return null;
  }
}

/**
 * Execute a query plan and return a DataFrame
 * Automatically applies optimizations (query optimization, column pruning, predicate pushdown)
 */
export async function executePlan(
  plan: PlanNode,
  enableOptimizations = true,
): Promise<Result<DataFrame, Error>> {
  // Step 1: Optimize the query plan (reorder operations, combine filters, etc.)
  let optimizedPlan = plan;
  if (enableOptimizations) {
    const optimizationResult = optimizeQueryPlan(plan, 1000000);
    optimizedPlan = optimizationResult.plan;
  }

  // Step 2: Try specialized execution paths (predicate pushdown, column pruning)
  if (enableOptimizations) {
    // Find the scan node in the plan (might not be at the root after optimization)
    const scanNode = findScanNode(optimizedPlan);
    if (scanNode) {
      const pruningResult = await executeScanWithPruning(scanNode, optimizedPlan);
      if (pruningResult) {
        return pruningResult;
      }
    }
  }

  // Step 3: Execute the optimized plan using standard execution path
  switch (optimizedPlan.type) {
    case 'scan':
      return executeScan(optimizedPlan);

    case 'filter':
      return executeFilter(optimizedPlan);

    case 'select':
      return executeSelect(optimizedPlan);

    case 'groupby':
      return executeGroupBy(optimizedPlan);

    default: {
      // TypeScript's exhaustive checking ensures this never happens
      const _exhaustiveCheck: never = optimizedPlan;
      return err(new Error(`Unknown plan node type: ${(_exhaustiveCheck as PlanNode).type}`));
    }
  }
}

/**
 * Execute a scan operation (load CSV)
 */
async function executeScan(plan: ScanPlan): Promise<Result<DataFrame, Error>> {
  // Use eager loading (readCsv) - it now handles file reading internally
  // TODO: Implement streaming execution with scanCsv for large files
  const result = await readCsv(plan.path, {
    schema: plan.schema,
    delimiter: plan.delimiter ?? ',',
    hasHeader: plan.hasHeader ?? true,
    nullValues: plan.nullValues,
  });

  return result;
}

/**
 * Execute scan with column pruning if beneficial
 * Returns null if pruning is not applicable, otherwise returns the result
 */
async function executeScanWithPruning(
  scanPlan: ScanPlan,
  fullPlan: PlanNode,
): Promise<Result<DataFrame, Error> | null> {
  // First, try predicate pushdown (more impactful than column pruning)
  const predicates = extractPushdownPredicates(fullPlan);
  if (predicates.length > 0) {
    return executeScanWithPredicates(scanPlan, fullPlan, predicates);
  }

  // Analyze the full plan to see what columns are needed
  const requiredColumns = analyzeRequiredColumns(fullPlan);

  // If we need all columns or pruning won't help, skip
  if (requiredColumns.size === 0) {
    return null; // Can't determine columns, use normal path
  }

  // Read first line to get total column count
  try {
    const file = Bun.file(scanPlan.path);
    const fileContent = await file.text();
    const firstLine = fileContent.split('\n')[0];

    if (!firstLine) {
      return null; // Empty file
    }

    const headers = firstLine
      .split(scanPlan.delimiter ?? ',')
      .map((h) => h.trim().replace(/^"|"$/g, ''));

    // Check if pruning is beneficial
    if (!shouldPruneColumns(fullPlan, headers.length)) {
      return null; // Not worth pruning
    }

    // Use pruned scan
    const result = await scanCsvWithPruning(scanPlan.path, {
      requiredColumns,
      schema: new Map(Object.entries(scanPlan.schema)),
    });

    if (!result.ok) {
      return null; // Fall back to normal scan on error
    }

    // Now execute the remaining plan on the pruned data
    return await executePlanOnData(fullPlan, result.data);
  } catch (e) {
    return null; // Fall back to normal execution
  }
}

/**
 * Execute scan with predicate pushdown
 * Applies filters during CSV parsing to avoid loading filtered-out rows
 */
async function executeScanWithPredicates(
  scanPlan: ScanPlan,
  fullPlan: PlanNode,
  predicates: ScanPredicate[],
): Promise<Result<DataFrame, Error> | null> {
  try {
    // Also get required columns for combined optimization
    const requiredColumns = analyzeRequiredColumns(fullPlan);

    const result = await scanCsvWithPredicates(scanPlan.path, {
      predicates,
      schema: new Map(Object.entries(scanPlan.schema)),
      requiredColumns: requiredColumns.size > 0 ? requiredColumns : undefined,
    });

    if (!result.ok) {
      return null; // Fall back to normal scan
    }

    // Execute remaining plan (skip filters that were pushed down)
    return await executePlanOnDataSkippingPushedFilters(fullPlan, result.data, predicates);
  } catch (e) {
    return null;
  }
}

/**
 * Execute plan on data, skipping filters that were already applied during pushdown
 */
async function executePlanOnDataSkippingPushedFilters(
  plan: PlanNode,
  data: DataFrame,
  pushedPredicates: ScanPredicate[],
): Promise<Result<DataFrame, Error>> {
  switch (plan.type) {
    case 'scan':
      return ok(data);

    case 'filter': {
      // First execute the input plan
      const inputResult = await executePlanOnDataSkippingPushedFilters(
        plan.input,
        data,
        pushedPredicates,
      );
      if (!inputResult.ok) return inputResult;

      // Check if this filter was pushed down
      const wasPushed = pushedPredicates.some(
        (p) =>
          p.columnName === plan.column && p.operator === plan.operator && p.value === plan.value,
      );

      if (wasPushed) {
        // Skip this filter, already applied
        return inputResult;
      }

      // Apply filter normally
      try {
        // @ts-ignore - Dynamic execution
        const filtered = filter(inputResult.data, plan.column, plan.operator, plan.value);
        return ok(filtered);
      } catch (error) {
        return err(error instanceof Error ? error : new Error(String(error)));
      }
    }

    case 'select': {
      // First execute the input plan
      const inputResult = await executePlanOnDataSkippingPushedFilters(
        plan.input,
        data,
        pushedPredicates,
      );
      if (!inputResult.ok) return inputResult;

      // Apply select to the result
      try {
        // @ts-ignore - Dynamic execution
        const selected = select(inputResult.data, plan.columns);
        return ok(selected);
      } catch (error) {
        return err(error instanceof Error ? error : new Error(String(error)));
      }
    }

    case 'groupby': {
      // First execute the input plan
      const inputResult = await executePlanOnDataSkippingPushedFilters(
        plan.input,
        data,
        pushedPredicates,
      );
      if (!inputResult.ok) return inputResult;

      // Apply groupby to the result
      return groupby(inputResult.data, plan.groupKeys, plan.aggregations);
    }

    default: {
      // TypeScript's exhaustive checking ensures this never happens
      {
        const _exhaustiveCheck: never = plan;
        return err(new Error(`Unknown plan node type: ${(_exhaustiveCheck as PlanNode).type}`));
      }
    }
  }
}

/**
 * Execute filter operation
 */
async function executeFilter(plan: FilterPlan): Promise<Result<DataFrame, Error>> {
  // First execute the input plan
  const inputResult = await executePlan(plan.input);
  if (!inputResult.ok) {
    return inputResult;
  }

  // Apply filter
  try {
    // @ts-ignore - Dynamic execution
    const filtered = filter(inputResult.data, plan.column, plan.operator, plan.value);
    return ok(filtered);
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}

/**
 * Execute plan operations after the scan (used for pruned execution)
 */
async function executePlanOnData(
  plan: PlanNode,
  data: DataFrame,
): Promise<Result<DataFrame, Error>> {
  switch (plan.type) {
    case 'scan':
      // Already have data from pruned scan
      return ok(data);

    case 'filter': {
      // First execute the input plan
      const inputResult = await executePlanOnData(plan.input, data);
      if (!inputResult.ok) return inputResult;

      // Apply filter to the result
      try {
        // @ts-ignore - Dynamic execution
        const filtered = filter(inputResult.data, plan.column, plan.operator, plan.value);
        return ok(filtered);
      } catch (error) {
        return err(error instanceof Error ? error : new Error(String(error)));
      }
    }

    case 'select': {
      // First execute the input plan
      const inputResult = await executePlanOnData(plan.input, data);
      if (!inputResult.ok) return inputResult;

      // Apply select to the result
      try {
        // @ts-ignore - Dynamic execution
        const selected = select(inputResult.data, plan.columns);
        return ok(selected);
      } catch (error) {
        return err(error instanceof Error ? error : new Error(String(error)));
      }
    }

    case 'groupby': {
      // First execute the input plan
      const inputResult = await executePlanOnData(plan.input, data);
      if (!inputResult.ok) return inputResult;

      // Apply groupby to the result
      return groupby(inputResult.data, plan.groupKeys, plan.aggregations);
    }

    default: {
      // TypeScript's exhaustive checking ensures this never happens
      {
        const _exhaustiveCheck: never = plan;
        return err(new Error(`Unknown plan node type: ${(_exhaustiveCheck as PlanNode).type}`));
      }
    }
  }
}

/**
 * Execute a select operation
 */
async function executeSelect(plan: SelectPlan): Promise<Result<DataFrame, Error>> {
  // First execute the input plan
  const inputResult = await executePlan(plan.input);
  if (!inputResult.ok) {
    return inputResult;
  }

  // Apply select
  try {
    // @ts-ignore - Dynamic execution
    const selected = select(inputResult.data, plan.columns);
    return ok(selected);
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}

/**
 * Execute a groupby operation
 */
async function executeGroupBy(plan: GroupByPlan): Promise<Result<DataFrame, Error>> {
  // First execute the input plan
  const inputResult = await executePlan(plan.input);
  if (!inputResult.ok) {
    return inputResult;
  }

  const df = inputResult.data;

  // Perform groupby operation (use correct property names from plan)
  return groupby(df, plan.groupKeys, plan.aggregations);
}

/**
 * Optimize a query plan and return detailed optimization information
 *
 * @param plan - The query plan to optimize
 * @param estimatedRows - Estimated number of input rows (for cost calculations)
 * @returns Optimized plan with statistics and applied optimizations
 */
export function optimizePlan(plan: PlanNode, estimatedRows = 1000000): OptimizedPlan {
  return optimizeQueryPlan(plan, estimatedRows);
}

/**
 * Explain how a query plan will be executed with optimizations
 *
 * @param plan - The query plan to explain
 * @param estimatedRows - Estimated number of input rows
 * @returns Human-readable explanation of the optimized plan
 */
export function explainQueryPlan(plan: PlanNode, estimatedRows = 1000000): string {
  const optimized = optimizeQueryPlan(plan, estimatedRows);
  return explainPlan(optimized);
}
