/**
 * Unique/deduplication operations.
 *
 * Removes duplicate rows based on specified columns using hash-based approach.
 * Returns a selection vector (no data copying).
 *
 * Strategy:
 * - Hash row values for specified columns using FNV-1a
 * - Build hash set of seen row hashes
 * - Output selection vector with first occurrence of each unique row
 */

import type { Chunk } from "../buffer/chunk.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/** FNV-1a hash constants */
const FNV_OFFSET_BASIS = 2166136261;
const FNV_PRIME = 16777619;

/**
 * Build a selection vector containing only unique rows.
 *
 * @param chunk Chunk to deduplicate
 * @param columnIndices Column indices to use for uniqueness (empty = all columns)
 * @param keepFirst If true, keep first occurrence; if false, keep last
 * @returns Selection vector and count
 */
export function uniqueSelection(
	chunk: Chunk,
	columnIndices?: number[],
	keepFirst: boolean = true,
): Result<{ selection: Uint32Array; count: number }> {
	const rowCount = chunk.rowCount;

	if (rowCount === 0) {
		return ok({ selection: new Uint32Array(0), count: 0 });
	}

	// Determine which columns to check
	const columnsToCheck = columnIndices ?? getAllColumnIndices(chunk);

	if (columnsToCheck.length === 0) {
		return err(ErrorCode.InvalidOperand);
	}

	// Validate column indices
	for (const idx of columnsToCheck) {
		if (idx < 0 || idx >= chunk.columnCount) {
			return err(ErrorCode.UnknownColumn);
		}
	}

	if (keepFirst) {
		return uniqueKeepFirst(chunk, columnsToCheck, rowCount);
	} else {
		return uniqueKeepLast(chunk, columnsToCheck, rowCount);
	}
}

/**
 * Keep first occurrence of each unique row.
 */
function uniqueKeepFirst(
	chunk: Chunk,
	columnsToCheck: number[],
	rowCount: number,
): Result<{ selection: Uint32Array; count: number }> {
	// Use Map for hash collision handling (hash -> list of row indices)
	const seen = new Map<number, number[]>();
	const selection = new Uint32Array(rowCount);
	let count = 0;

	for (let row = 0; row < rowCount; row++) {
		const hash = hashRow(chunk, row, columnsToCheck);

		const existing = seen.get(hash);
		if (existing === undefined) {
			// New hash, definitely new row
			seen.set(hash, [row]);
			selection[count] = row;
			count++;
		} else {
			// Hash collision - check if any existing row actually matches
			let isDuplicate = false;
			for (const existingRow of existing) {
				if (rowsEqual(chunk, row, existingRow, columnsToCheck)) {
					isDuplicate = true;
					break;
				}
			}
			if (!isDuplicate) {
				existing.push(row);
				selection[count] = row;
				count++;
			}
		}
	}

	return ok({ selection, count });
}

/**
 * Keep last occurrence of each unique row.
 */
function uniqueKeepLast(
	chunk: Chunk,
	columnsToCheck: number[],
	rowCount: number,
): Result<{ selection: Uint32Array; count: number }> {
	// Scan backwards, but reverse selection at the end
	const seen = new Map<number, number[]>();
	const selectionReversed: number[] = [];

	for (let row = rowCount - 1; row >= 0; row--) {
		const hash = hashRow(chunk, row, columnsToCheck);

		const existing = seen.get(hash);
		if (existing === undefined) {
			seen.set(hash, [row]);
			selectionReversed.push(row);
		} else {
			let isDuplicate = false;
			for (const existingRow of existing) {
				if (rowsEqual(chunk, row, existingRow, columnsToCheck)) {
					isDuplicate = true;
					break;
				}
			}
			if (!isDuplicate) {
				existing.push(row);
				selectionReversed.push(row);
			}
		}
	}

	// Reverse to maintain original order
	const count = selectionReversed.length;
	const selection = new Uint32Array(count);
	for (let i = 0; i < count; i++) {
		// biome-ignore lint/style/noNonNullAssertion: Logic guarantees indices exist
		selection[i] = selectionReversed[count - 1 - i]!;
	}

	return ok({ selection, count });
}

/**
 * Hash a row's values for the specified columns.
 */
function hashRow(chunk: Chunk, row: number, columnIndices: number[]): number {
	let hash = FNV_OFFSET_BASIS;

	for (const colIdx of columnIndices) {
		const column = chunk.getColumn(colIdx);
		if (!column) continue;

		// Include null status in hash
		if (chunk.isNull(colIdx, row)) {
			hash ^= 0xff;
			hash = Math.imul(hash, FNV_PRIME);
			continue;
		}

		const value = chunk.getValue(colIdx, row);
		if (value === undefined) continue;

		// Hash based on type
		if (typeof value === "number") {
			// Hash the float bits
			hash = hashNumber(hash, value);
		} else if (typeof value === "bigint") {
			hash = hashBigInt(hash, value);
		} else {
			// For strings (dictionary indices), hash the index
			hash ^= Number(value) & 0xffffffff;
			hash = Math.imul(hash, FNV_PRIME);
		}
	}

	return hash >>> 0;
}

/**
 * Hash a number value.
 */
function hashNumber(hash: number, value: number): number {
	// Convert to integer representation for hashing
	const intValue = value | 0;
	hash ^= intValue;
	hash = Math.imul(hash, FNV_PRIME);

	// Hash fractional part if not integer
	if (value !== intValue) {
		const frac = ((value - intValue) * 1e9) | 0;
		hash ^= frac;
		hash = Math.imul(hash, FNV_PRIME);
	}

	return hash;
}

/**
 * Hash a BigInt value.
 */
function hashBigInt(hash: number, value: bigint): number {
	// Hash low 32 bits and high 32 bits
	const low = Number(value & 0xffffffffn);
	const high = Number((value >> 32n) & 0xffffffffn);

	hash ^= low;
	hash = Math.imul(hash, FNV_PRIME);
	hash ^= high;
	hash = Math.imul(hash, FNV_PRIME);

	return hash;
}

/**
 * Check if two rows are equal for the specified columns.
 */
function rowsEqual(
	chunk: Chunk,
	row1: number,
	row2: number,
	columnIndices: number[],
): boolean {
	for (const colIdx of columnIndices) {
		const null1 = chunk.isNull(colIdx, row1);
		const null2 = chunk.isNull(colIdx, row2);

		if (null1 !== null2) return false;
		if (null1) continue; // Both null, considered equal

		const val1 = chunk.getValue(colIdx, row1);
		const val2 = chunk.getValue(colIdx, row2);

		if (val1 !== val2) return false;
	}

	return true;
}

/**
 * Get all column indices.
 */
function getAllColumnIndices(chunk: Chunk): number[] {
	const indices: number[] = [];
	for (let i = 0; i < chunk.columnCount; i++) {
		indices.push(i);
	}
	return indices;
}

/**
 * Count unique values in a single column.
 */
export function countUnique(chunk: Chunk, columnIndex: number): number {
	const rowCount = chunk.rowCount;
	if (rowCount === 0) return 0;

	const seen = new Set<number | bigint | null>();

	for (let row = 0; row < rowCount; row++) {
		if (chunk.isNull(columnIndex, row)) {
			seen.add(null);
		} else {
			const value = chunk.getValue(columnIndex, row);
			if (value !== undefined) {
				seen.add(value as number | bigint);
			}
		}
	}

	return seen.size;
}
