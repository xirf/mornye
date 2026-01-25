import type { Column } from '../core/column';
import type { AggFunc, AggSpec } from '../dataframe/groupby';
import { DType } from '../types/dtypes';
import type { FilterOperator } from '../types/operators';

/**
 * Query plan node types for lazy evaluation
 */
export type PlanNode = ScanPlan | FilterPlan | SelectPlan | GroupByPlan;

/**
 * Base plan node
 */
interface BasePlan {
  id: number; // Unique node identifier for caching
}

/**
 * CSV scan operation (source node)
 */
export interface ScanPlan extends BasePlan {
  type: 'scan';
  path: string;
  schema: Record<string, DType>;
  columnOrder: string[];
  chunkSize?: number;
  delimiter?: string;
  hasHeader?: boolean;
  nullValues?: string[];
}

/**
 * Filter operation
 */
export interface FilterPlan extends BasePlan {
  type: 'filter';
  input: PlanNode;
  column: string;
  operator: FilterOperator;
  value: number | bigint | string | boolean | Array<number | bigint | string | boolean>;
}

/**
 * Select (column projection) operation
 */
export interface SelectPlan extends BasePlan {
  type: 'select';
  input: PlanNode;
  columns: string[];
}

/**
 * GroupBy aggregation operation
 */
export interface GroupByPlan extends BasePlan {
  type: 'groupby';
  input: PlanNode;
  groupKeys: string[];
  aggregations: AggSpec[];
}

/**
 * Plan builder for constructing query plans
 */
export class QueryPlan {
  private static nextId = 0;

  /**
   * Create a scan plan node
   */
  static scan(
    path: string,
    schema: Record<string, DType>,
    columnOrder: string[],
    options?: {
      chunkSize?: number;
      delimiter?: string;
      hasHeader?: boolean;
      nullValues?: string[];
    },
  ): ScanPlan {
    return {
      type: 'scan',
      id: QueryPlan.nextId++,
      path,
      schema,
      columnOrder,
      chunkSize: options?.chunkSize,
      delimiter: options?.delimiter,
      hasHeader: options?.hasHeader,
      nullValues: options?.nullValues,
    };
  }

  /**
   * Create a filter plan node
   */
  static filter(
    input: PlanNode,
    column: string,
    operator: FilterOperator,
    value: number | bigint | string | boolean | Array<number | bigint | string | boolean>,
  ): FilterPlan {
    return {
      type: 'filter',
      id: QueryPlan.nextId++,
      input,
      column,
      operator,
      value,
    };
  }

  /**
   * Create a select plan node
   */
  static select(input: PlanNode, columns: string[]): SelectPlan {
    return {
      type: 'select',
      id: QueryPlan.nextId++,
      input,
      columns,
    };
  }

  /**
   * Create a groupby plan node
   */
  static groupby(input: PlanNode, groupKeys: string[], aggregations: AggSpec[]): GroupByPlan {
    return {
      type: 'groupby',
      id: QueryPlan.nextId++,
      input,
      groupKeys,
      aggregations,
    };
  }

  /**
   * Get the output schema for a plan node
   */
  static getOutputSchema(node: PlanNode): Record<string, DType> {
    switch (node.type) {
      case 'scan':
        return node.schema;

      case 'filter':
        return QueryPlan.getOutputSchema(node.input);

      case 'select': {
        const inputSchema = QueryPlan.getOutputSchema(node.input);
        const outputSchema: Record<string, DType> = {};
        for (const col of node.columns) {
          if (inputSchema[col]) {
            outputSchema[col] = inputSchema[col];
          }
        }
        return outputSchema;
      }

      case 'groupby': {
        const inputSchema = QueryPlan.getOutputSchema(node.input);
        const outputSchema: Record<string, DType> = {};

        // Add group key columns
        for (const key of node.groupKeys) {
          if (inputSchema[key]) {
            outputSchema[key] = inputSchema[key];
          }
        }

        // Add aggregation columns with their output dtypes
        for (const agg of node.aggregations) {
          const srcDtype = inputSchema[agg.col];
          if (!srcDtype) continue;

          // Determine output dtype based on aggregation function
          let outDtype: DType;
          if (agg.func === 'count') {
            outDtype = DType.Int32;
          } else if (agg.func === 'mean') {
            outDtype = DType.Float64;
          } else {
            outDtype = srcDtype; // Preserve source dtype for sum/min/max/first/last
          }

          outputSchema[agg.outName] = outDtype;
        }

        return outputSchema;
      }
    }
  }

  /**
   * Get column order for a plan node
   */
  static getColumnOrder(node: PlanNode): string[] {
    switch (node.type) {
      case 'scan':
        return node.columnOrder;

      case 'filter':
        return QueryPlan.getColumnOrder(node.input);

      case 'select':
        return node.columns;

      case 'groupby': {
        const cols: string[] = [];
        cols.push(...node.groupKeys);
        for (const agg of node.aggregations) {
          cols.push(agg.outName);
        }
        return cols;
      }
    }
  }

  /**
   * Pretty-print a plan for debugging
   */
  static explain(node: PlanNode, indent = 0): string {
    const prefix = '  '.repeat(indent);
    let result = '';

    switch (node.type) {
      case 'scan':
        result += `${prefix}Scan: ${node.path}\n`;
        result += `${prefix}  Schema: ${Object.keys(node.schema).join(', ')}\n`;
        result += `${prefix}  Rows: unknown (streaming)\n`;
        break;

      case 'filter':
        result += `${prefix}Filter: ${node.column} ${node.operator} ${JSON.stringify(node.value)}\n`;
        result += QueryPlan.explain(node.input, indent + 1);
        break;

      case 'select':
        result += `${prefix}Select: ${node.columns.join(', ')}\n`;
        result += QueryPlan.explain(node.input, indent + 1);
        break;

      case 'groupby':
        result += `${prefix}GroupBy: ${node.groupKeys.join(', ')}\n`;
        result += `${prefix}  Aggregations:\n`;
        for (const agg of node.aggregations) {
          result += `${prefix}    ${agg.func}(${agg.col}) AS ${agg.outName}\n`;
        }
        result += QueryPlan.explain(node.input, indent + 1);
        break;
    }

    return result;
  }
}
