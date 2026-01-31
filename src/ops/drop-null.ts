/**
 * Drop null operations.
 *
 * Filters out rows containing null values in specified columns.
 * Returns a selection vector (no data copying).
 *
 * Strategy:
 * - Scan null bitmaps for specified columns
 * - Build selection vector with non-null row indices
 * - Uses AND logic: row is kept only if ALL specified columns are non-null
 */

import type { Chunk } from "../buffer/chunk.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/**
 * Build a selection vector that excludes rows with nulls.
 *
 * @param chunk Chunk to scan for nulls
 * @param columnIndices Column indices to check (empty = check all)
 * @returns Uint32Array selection vector and count of selected rows
 */
export function dropNullSelection(
	chunk: Chunk,
	columnIndices?: number[],
): Result<{ selection: Uint32Array; count: number }> {
	const rowCount = chunk.rowCount;

	if (rowCount === 0) {
		return ok({ selection: new Uint32Array(0), count: 0 });
	}

	// Determine which columns to check
	const columnsToCheck = columnIndices ?? getAllColumnIndices(chunk);

	if (columnsToCheck.length === 0) {
		// No columns to check - keep all rows
		const selection = new Uint32Array(rowCount);
		for (let i = 0; i < rowCount; i++) {
			selection[i] = i;
		}
		return ok({ selection, count: rowCount });
	}

	// Validate column indices
	for (const idx of columnsToCheck) {
		if (idx < 0 || idx >= chunk.columnCount) {
			return err(ErrorCode.UnknownColumn);
		}
	}

	// Pre-allocate selection vector (worst case: all rows pass)
	const selection = new Uint32Array(rowCount);
	let count = 0;

	// Check each row
	for (let row = 0; row < rowCount; row++) {
		let hasNull = false;

		for (const colIdx of columnsToCheck) {
			if (chunk.isNull(colIdx, row)) {
				hasNull = true;
				break;
			}
		}

		if (!hasNull) {
			selection[count] = row;
			count++;
		}
	}

	return ok({ selection, count });
}

/**
 * Count null values in specified columns.
 *
 * @param chunk Chunk to scan
 * @param columnIndex Column index to count nulls in
 * @returns Number of null values
 */
export function countNulls(chunk: Chunk, columnIndex: number): number {
	const column = chunk.getColumn(columnIndex);
	if (!column || !column.isNullable) {
		return 0;
	}

	let count = 0;
	const rowCount = chunk.rowCount;

	for (let i = 0; i < rowCount; i++) {
		if (chunk.isNull(columnIndex, i)) {
			count++;
		}
	}

	return count;
}

/**
 * Check if any row has a null in the specified columns.
 */
export function hasAnyNull(chunk: Chunk, columnIndices?: number[]): boolean {
	const columnsToCheck = columnIndices ?? getAllColumnIndices(chunk);
	const rowCount = chunk.rowCount;

	for (const colIdx of columnsToCheck) {
		const column = chunk.getColumn(colIdx);
		if (!column?.isNullable) continue;

		for (let row = 0; row < rowCount; row++) {
			if (chunk.isNull(colIdx, row)) {
				return true;
			}
		}
	}

	return false;
}

/**
 * Get indices of all nullable columns.
 */
function getAllColumnIndices(chunk: Chunk): number[] {
	const indices: number[] = [];
	for (let i = 0; i < chunk.columnCount; i++) {
		const column = chunk.getColumn(i);
		if (column?.isNullable) {
			indices.push(i);
		}
	}
	return indices;
}
