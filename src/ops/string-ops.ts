/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
/**
 * String operations for data cleaning.
 *
 * Operations work on the dictionary table directly where possible,
 * avoiding per-row string processing for low-cardinality data.
 *
 * Strategy:
 * - For trim/replace: Transform dictionary entries, remap column indices
 * - Creates new dictionary with transformed strings
 * - In-place column update (just remapping indices)
 */

import type { ColumnBuffer } from "../buffer/column-buffer.ts";
import { createDictionary, type Dictionary } from "../buffer/dictionary.ts";
import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/**
 * Trim whitespace from string column values.
 * Operates on dictionary, remaps column indices.
 *
 * @param column String column buffer (indices into dictionary)
 * @param dictionary Source dictionary
 * @returns New dictionary with trimmed strings
 */
export function trimColumn(
	column: ColumnBuffer,
	dictionary: Dictionary,
): Result<Dictionary> {
	if (column.kind !== DTypeKind.String) {
		return err(ErrorCode.TypeMismatch);
	}

	const length = column.length;
	const data = column.data as Uint32Array;

	// Build mapping from old index to new index
	const newDict = createDictionary();
	const indexMap = new Map<number, number>();

	// First pass: transform and intern all unique values
	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;

		const oldIdx = data[i]!;
		if (indexMap.has(oldIdx)) continue;

		const str = dictionary.getString(oldIdx);
		if (str === undefined) {
			indexMap.set(oldIdx, oldIdx);
			continue;
		}

		const trimmed = str.trim();
		const newIdx = newDict.internString(trimmed);
		indexMap.set(oldIdx, newIdx);
	}

	// Second pass: remap column indices
	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;

		const oldIdx = data[i]!;
		const newIdx = indexMap.get(oldIdx);
		if (newIdx !== undefined) {
			data[i] = newIdx;
		}
	}

	return ok(newDict);
}

/**
 * Replace substrings in string column values.
 * Operates on dictionary, remaps column indices.
 *
 * @param column String column buffer
 * @param dictionary Source dictionary
 * @param pattern Substring to find
 * @param replacement Replacement string
 * @param all If true, replace all occurrences; if false, only first
 * @returns New dictionary with replaced strings
 */
export function replaceColumn(
	column: ColumnBuffer,
	dictionary: Dictionary,
	pattern: string,
	replacement: string,
	all: boolean = true,
): Result<Dictionary> {
	if (column.kind !== DTypeKind.String) {
		return err(ErrorCode.TypeMismatch);
	}

	const length = column.length;
	const data = column.data as Uint32Array;

	const newDict = createDictionary();
	const indexMap = new Map<number, number>();

	// Transform and intern all unique values
	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;

		const oldIdx = data[i]!;
		if (indexMap.has(oldIdx)) continue;

		const str = dictionary.getString(oldIdx);
		if (str === undefined) {
			indexMap.set(oldIdx, oldIdx);
			continue;
		}

		let replaced: string;
		if (all) {
			replaced = str.replaceAll(pattern, replacement);
		} else {
			replaced = str.replace(pattern, replacement);
		}

		const newIdx = newDict.internString(replaced);
		indexMap.set(oldIdx, newIdx);
	}

	// Remap column indices
	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;

		const oldIdx = data[i]!;
		const newIdx = indexMap.get(oldIdx);
		if (newIdx !== undefined) {
			data[i] = newIdx;
		}
	}

	return ok(newDict);
}

/**
 * Convert string column to lowercase.
 */
export function toLowerColumn(
	column: ColumnBuffer,
	dictionary: Dictionary,
): Result<Dictionary> {
	if (column.kind !== DTypeKind.String) {
		return err(ErrorCode.TypeMismatch);
	}

	const length = column.length;
	const data = column.data as Uint32Array;

	const newDict = createDictionary();
	const indexMap = new Map<number, number>();

	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;

		const oldIdx = data[i]!;
		if (indexMap.has(oldIdx)) continue;

		const str = dictionary.getString(oldIdx);
		if (str === undefined) {
			indexMap.set(oldIdx, oldIdx);
			continue;
		}

		const lower = str.toLowerCase();
		const newIdx = newDict.internString(lower);
		indexMap.set(oldIdx, newIdx);
	}

	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;
		const oldIdx = data[i]!;
		const newIdx = indexMap.get(oldIdx);
		if (newIdx !== undefined) {
			data[i] = newIdx;
		}
	}

	return ok(newDict);
}

/**
 * Convert string column to uppercase.
 */
export function toUpperColumn(
	column: ColumnBuffer,
	dictionary: Dictionary,
): Result<Dictionary> {
	if (column.kind !== DTypeKind.String) {
		return err(ErrorCode.TypeMismatch);
	}

	const length = column.length;
	const data = column.data as Uint32Array;

	const newDict = createDictionary();
	const indexMap = new Map<number, number>();

	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;

		const oldIdx = data[i]!;
		if (indexMap.has(oldIdx)) continue;

		const str = dictionary.getString(oldIdx);
		if (str === undefined) {
			indexMap.set(oldIdx, oldIdx);
			continue;
		}

		const upper = str.toUpperCase();
		const newIdx = newDict.internString(upper);
		indexMap.set(oldIdx, newIdx);
	}

	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;
		const oldIdx = data[i]!;
		const newIdx = indexMap.get(oldIdx);
		if (newIdx !== undefined) {
			data[i] = newIdx;
		}
	}

	return ok(newDict);
}

/**
 * Pad string to specified length (left or right).
 */
export function padColumn(
	column: ColumnBuffer,
	dictionary: Dictionary,
	length: number,
	fillChar: string = " ",
	side: "left" | "right" = "left",
): Result<Dictionary> {
	if (column.kind !== DTypeKind.String) {
		return err(ErrorCode.TypeMismatch);
	}

	const colLength = column.length;
	const data = column.data as Uint32Array;

	const newDict = createDictionary();
	const indexMap = new Map<number, number>();

	for (let i = 0; i < colLength; i++) {
		if (column.isNull(i)) continue;

		const oldIdx = data[i]!;
		if (indexMap.has(oldIdx)) continue;

		const str = dictionary.getString(oldIdx);
		if (str === undefined) {
			indexMap.set(oldIdx, oldIdx);
			continue;
		}

		let padded: string;
		if (side === "left") {
			padded = str.padStart(length, fillChar);
		} else {
			padded = str.padEnd(length, fillChar);
		}

		const newIdx = newDict.internString(padded);
		indexMap.set(oldIdx, newIdx);
	}

	for (let i = 0; i < colLength; i++) {
		if (column.isNull(i)) continue;
		const oldIdx = data[i]!;
		const newIdx = indexMap.get(oldIdx);
		if (newIdx !== undefined) {
			data[i] = newIdx;
		}
	}

	return ok(newDict);
}

/**
 * Extract substring from string column.
 */
export function substringColumn(
	column: ColumnBuffer,
	dictionary: Dictionary,
	start: number,
	end?: number,
): Result<Dictionary> {
	if (column.kind !== DTypeKind.String) {
		return err(ErrorCode.TypeMismatch);
	}

	const length = column.length;
	const data = column.data as Uint32Array;

	const newDict = createDictionary();
	const indexMap = new Map<number, number>();

	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;

		const oldIdx = data[i]!;
		if (indexMap.has(oldIdx)) continue;

		const str = dictionary.getString(oldIdx);
		if (str === undefined) {
			indexMap.set(oldIdx, oldIdx);
			continue;
		}

		const substr =
			end !== undefined ? str.substring(start, end) : str.substring(start);
		const newIdx = newDict.internString(substr);
		indexMap.set(oldIdx, newIdx);
	}

	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) continue;
		const oldIdx = data[i]!;
		const newIdx = indexMap.get(oldIdx);
		if (newIdx !== undefined) {
			data[i] = newIdx;
		}
	}

	return ok(newDict);
}
