import { getColumnValue } from '../core/column';
import type { Column } from '../core/column';
import { DType } from '../types/dtypes';

/**
 * Sort direction
 */
export type SortDirection = 'asc' | 'desc';

/**
 * Column sort specification
 */
export interface SortSpec {
  /** Column to sort by */
  column: Column;
  /** Sort direction (default: 'asc') */
  direction?: SortDirection;
}

/**
 * Creates an array of row indices [0, 1, 2, ..., n-1]
 * @param rowCount - Number of rows
 * @returns Array of indices
 */
export function createRowIndices(rowCount: number): number[] {
  const indices = new Array(rowCount);
  for (let i = 0; i < rowCount; i++) {
    indices[i] = i;
  }
  return indices;
}

/**
 * Compare two values of the same dtype
 * Returns: negative if a < b, 0 if a == b, positive if a > b
 */
function compareValues(a: number | bigint, b: number | bigint, dtype: DType): number {
  // Handle numeric comparisons
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }

  // Handle bigint comparisons (DateTime, Date)
  if (typeof a === 'bigint' && typeof b === 'bigint') {
    return a < b ? -1 : a > b ? 1 : 0;
  }

  // Handle string comparisons (stored as Int32 dictionary IDs)
  // Dictionary IDs are already ordered, so numeric comparison works
  if (dtype === DType.String && typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }

  // Default fallback
  return 0;
}

/**
 * Sorts row indices based on a single column
 * Uses in-place sort for efficiency (modifies indices array)
 *
 * @param indices - Array of row indices to sort
 * @param column - Column to sort by
 * @param direction - Sort direction ('asc' or 'desc')
 */
export function sortByColumn(
  indices: number[],
  column: Column,
  direction: SortDirection = 'asc',
): void {
  const multiplier = direction === 'asc' ? 1 : -1;

  // Use native sort with custom comparator
  // This is very efficient in modern JS engines (typically Timsort)
  indices.sort((aIdx, bIdx) => {
    const aVal = getColumnValue(column, aIdx);
    const bVal = getColumnValue(column, bIdx);
    if (aVal === undefined || bVal === undefined) return 0;

    const cmp = compareValues(aVal, bVal, column.dtype);
    return cmp * multiplier;
  });
}

/**
 * Sorts row indices based on multiple columns (multi-key sort)
 * Sorts by first column, then by second column for ties, etc.
 * Uses in-place sort for efficiency (modifies indices array)
 *
 * @param indices - Array of row indices to sort
 * @param sortSpecs - Array of column sort specifications (order matters)
 */
export function sortByColumns(indices: number[], sortSpecs: SortSpec[]): void {
  if (sortSpecs.length === 0) return;

  // Use native sort with multi-column comparator
  indices.sort((aIdx, bIdx) => {
    // Compare each sort key in order until we find a difference
    for (const spec of sortSpecs) {
      const aVal = getColumnValue(spec.column, aIdx);
      const bVal = getColumnValue(spec.column, bIdx);
      if (aVal === undefined || bVal === undefined) continue;

      const cmp = compareValues(aVal, bVal, spec.column.dtype);

      if (cmp !== 0) {
        const multiplier = (spec.direction ?? 'asc') === 'asc' ? 1 : -1;
        return cmp * multiplier;
      }

      // Values are equal, continue to next sort key
    }

    // All sort keys are equal
    return 0;
  });
}

/**
 * Finds group boundaries in a sorted array of indices
 * Returns array of [startIdx, endIdx) pairs for each group
 *
 * This is used after sorting to identify where groups start and end
 * for efficient aggregation in GroupBy operations.
 *
 * @param indices - Sorted array of row indices
 * @param columns - Columns that define groups (must be sorted by these)
 * @returns Array of [start, end) index pairs for each group
 */
export function findGroupBoundaries(indices: number[], columns: Column[]): [number, number][] {
  if (indices.length === 0) return [];
  if (columns.length === 0) return [[0, indices.length]];

  const boundaries: [number, number][] = [];
  let groupStart = 0;

  // Scan through sorted indices to find group boundaries
  for (let i = 1; i < indices.length; i++) {
    const prevIdx = indices[i - 1];
    const currIdx = indices[i];
    if (prevIdx === undefined || currIdx === undefined) continue;

    // Check if any group key changed
    let groupChanged = false;

    for (const col of columns) {
      const prevVal = getColumnValue(col, prevIdx);
      const currVal = getColumnValue(col, currIdx);

      if (prevVal !== currVal) {
        groupChanged = true;
        break;
      }
    }

    if (groupChanged) {
      // Found group boundary - save previous group
      boundaries.push([groupStart, i]);
      groupStart = i;
    }
  }

  // Add final group
  boundaries.push([groupStart, indices.length]);

  return boundaries;
}

/**
 * Checks if indices are sorted according to the given columns
 * Useful for testing and validation
 *
 * @param indices - Array of row indices
 * @param columns - Columns to check sort order
 * @param directions - Sort directions for each column
 * @returns true if sorted, false otherwise
 */
export function isSorted(
  indices: number[],
  columns: Column[],
  directions: SortDirection[] = [],
): boolean {
  if (indices.length <= 1) return true;

  for (let i = 1; i < indices.length; i++) {
    const prevIdx = indices[i - 1];
    const currIdx = indices[i];
    if (prevIdx === undefined || currIdx === undefined) continue;

    for (let colIdx = 0; colIdx < columns.length; colIdx++) {
      const col = columns[colIdx];
      if (!col) continue;
      const direction = directions[colIdx] ?? 'asc';
      const multiplier = direction === 'asc' ? 1 : -1;

      const prevVal = getColumnValue(col, prevIdx);
      const currVal = getColumnValue(col, currIdx);
      if (prevVal === undefined || currVal === undefined) continue;

      const cmp = compareValues(prevVal, currVal, col.dtype);

      if (cmp * multiplier > 0) {
        // Previous value is greater than current (ascending violation)
        // or less than current (descending violation)
        return false;
      }

      if (cmp !== 0) {
        // Found difference, move to next pair
        break;
      }

      // Equal, check next column
    }
  }

  return true;
}
