/**
 * Fill null operations.
 *
 * Replaces null values in a column with a specified fill value.
 * Uses null bitmap scanning for efficient null detection.
 *
 * Strategy:
 * - Scan null bitmap byte by byte (8 rows at a time)
 * - Skip bytes that are all zeros (no nulls)
 * - Process individual bits only when nulls exist
 * - In-place modification for performance
 */

import type { ColumnBuffer } from "../buffer/column-buffer.ts";
import type { Dictionary } from "../buffer/dictionary.ts";
import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode } from "../types/error.ts";

/**
 * Fill null values in a column with a constant value.
 * Modifies the column in-place.
 *
 * @param column Column buffer to modify
 * @param fillValue Value to replace nulls with
 * @param dictionary Dictionary for string columns (required for string fill)
 */
export function fillNullColumn(
	column: ColumnBuffer,
	fillValue: number | bigint | string | boolean,
	dictionary?: Dictionary,
): ErrorCode {
	if (!column.isNullable) {
		return ErrorCode.None; // No nulls possible
	}

	const length = column.length;
	const kind = column.kind;

	// Handle string columns specially
	if (kind === DTypeKind.String) {
		if (typeof fillValue !== "string" || !dictionary) {
			return ErrorCode.InvalidFillValue;
		}
		return fillNullString(column, fillValue, dictionary);
	}

	// Handle boolean
	if (kind === DTypeKind.Boolean) {
		if (typeof fillValue !== "boolean" && typeof fillValue !== "number") {
			return ErrorCode.InvalidFillValue;
		}
		const boolValue =
			typeof fillValue === "boolean"
				? fillValue
					? 1
					: 0
				: fillValue !== 0
					? 1
					: 0;
		return fillNullNumeric(column, boolValue, length);
	}

	// Handle numeric
	if (typeof fillValue === "string") {
		return ErrorCode.InvalidFillValue;
	}

	// Handle bigint types
	const isBigInt =
		kind === DTypeKind.Int64 ||
		kind === DTypeKind.UInt64 ||
		kind === DTypeKind.Timestamp;
	if (isBigInt) {
		const bigValue =
			typeof fillValue === "bigint" ? fillValue : BigInt(Number(fillValue));
		return fillNullBigInt(column, bigValue, length);
	}

	// Regular numeric
	const numValue =
		typeof fillValue === "bigint" ? Number(fillValue) : Number(fillValue);
	return fillNullNumeric(column, numValue, length);
}

/**
 * Fill nulls with a numeric value using bitmap scanning.
 */
function fillNullNumeric(
	column: ColumnBuffer,
	fillValue: number,
	length: number,
): ErrorCode {
	const data = column.data as
		| Float64Array
		| Float32Array
		| Int32Array
		| Int16Array
		| Int8Array
		| Uint32Array
		| Uint16Array
		| Uint8Array;

	// Scan rows and fill nulls
	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) {
			data[i] = fillValue;
			column.setNull(i, false);
		}
	}

	return ErrorCode.None;
}

/**
 * Fill nulls with a BigInt value.
 */
function fillNullBigInt(
	column: ColumnBuffer,
	fillValue: bigint,
	length: number,
): ErrorCode {
	const data = column.data as BigInt64Array | BigUint64Array;

	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) {
			data[i] = fillValue;
			column.setNull(i, false);
		}
	}

	return ErrorCode.None;
}

/**
 * Fill nulls with a string value (interned).
 */
function fillNullString(
	column: ColumnBuffer,
	fillValue: string,
	dictionary: Dictionary,
): ErrorCode {
	const data = column.data as Uint32Array;
	const length = column.length;

	// Intern the fill value once
	const fillIndex = dictionary.internString(fillValue);

	for (let i = 0; i < length; i++) {
		if (column.isNull(i)) {
			data[i] = fillIndex;
			column.setNull(i, false);
		}
	}

	return ErrorCode.None;
}

/**
 * Fill nulls using a forward fill strategy (propagate last non-null value).
 * Modifies the column in-place.
 */
export function fillNullForward(column: ColumnBuffer): ErrorCode {
	if (!column.isNullable) {
		return ErrorCode.None;
	}

	const length = column.length;
	const kind = column.kind;
	const isBigInt =
		kind === DTypeKind.Int64 ||
		kind === DTypeKind.UInt64 ||
		kind === DTypeKind.Timestamp;

	if (isBigInt) {
		const data = column.data as BigInt64Array | BigUint64Array;
		let lastValue: bigint | null = null;

		for (let i = 0; i < length; i++) {
			if (column.isNull(i)) {
				if (lastValue !== null) {
					data[i] = lastValue;
					column.setNull(i, false);
				}
			} else {
				// biome-ignore lint/style/noNonNullAssertion: Checked via isNull
				lastValue = data[i]!;
			}
		}
	} else {
		const data = column.data as Float64Array | Int32Array | Uint32Array;
		let lastValue: number | null = null;

		for (let i = 0; i < length; i++) {
			if (column.isNull(i)) {
				if (lastValue !== null) {
					data[i] = lastValue;
					column.setNull(i, false);
				}
			} else {
				// biome-ignore lint/style/noNonNullAssertion: Checked via isNull
				lastValue = data[i]!;
			}
		}
	}

	return ErrorCode.None;
}

/**
 * Fill nulls using a backward fill strategy (propagate next non-null value).
 * Modifies the column in-place.
 */
export function fillNullBackward(column: ColumnBuffer): ErrorCode {
	if (!column.isNullable) {
		return ErrorCode.None;
	}

	const length = column.length;
	const kind = column.kind;
	const isBigInt =
		kind === DTypeKind.Int64 ||
		kind === DTypeKind.UInt64 ||
		kind === DTypeKind.Timestamp;

	if (isBigInt) {
		const data = column.data as BigInt64Array | BigUint64Array;
		let nextValue: bigint | null = null;

		for (let i = length - 1; i >= 0; i--) {
			if (column.isNull(i)) {
				if (nextValue !== null) {
					data[i] = nextValue;
					column.setNull(i, false);
				}
			} else {
				// biome-ignore lint/style/noNonNullAssertion: Checked via isNull
				nextValue = data[i]!;
			}
		}
	} else {
		const data = column.data as Float64Array | Int32Array | Uint32Array;
		let nextValue: number | null = null;

		for (let i = length - 1; i >= 0; i--) {
			if (column.isNull(i)) {
				if (nextValue !== null) {
					data[i] = nextValue;
					column.setNull(i, false);
				}
			} else {
				// biome-ignore lint/style/noNonNullAssertion: Checked via isNull
				nextValue = data[i]!;
			}
		}
	}

	return ErrorCode.None;
}
