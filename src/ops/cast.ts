/**
 * Column type casting operations.
 *
 * Provides high-performance type conversion between DTypes.
 * Uses SIMD-friendly patterns via TypedArray.from() where possible.
 *
 * Cast strategies:
 * - Numeric→Numeric: TypedArray conversion (Bun SIMD optimized)
 * - String→Numeric: Parse dictionary entries once, map indices
 * - Numeric→String: Intern values into dictionary
 * - Boolean→Numeric: 0/1 mapping
 */

/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { createDictionary, type Dictionary } from "../buffer/dictionary.ts";
import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/** Result of a cast operation */
export interface CastResult {
	column: ColumnBuffer;
	dictionary?: Dictionary;
}

/**
 * Cast a column to a different DType.
 *
 * @param source Source column buffer
 * @param fromKind Source DType kind
 * @param toKind Target DType kind
 * @param dictionary Dictionary for string columns (required for string casts)
 * @param nullable Whether output should be nullable
 */
export function castColumn(
	source: ColumnBuffer,
	fromKind: DTypeKind,
	toKind: DTypeKind,
	dictionary?: Dictionary,
	nullable: boolean = true,
): Result<CastResult> {
	if (fromKind === toKind) {
		return ok({ column: source, dictionary });
	}

	// Dispatch to appropriate cast function
	if (isNumericKind(fromKind) && isNumericKind(toKind)) {
		return castNumericToNumeric(source, fromKind, toKind, nullable);
	}

	if (fromKind === DTypeKind.String && isNumericKind(toKind)) {
		if (!dictionary) {
			return err(ErrorCode.InvalidOperand);
		}
		return castStringToNumeric(source, toKind, dictionary, nullable);
	}

	if (isNumericKind(fromKind) && toKind === DTypeKind.String) {
		const dict = dictionary ?? createDictionary();
		return castNumericToString(source, fromKind, dict, nullable);
	}

	if (fromKind === DTypeKind.Boolean && isNumericKind(toKind)) {
		return castBooleanToNumeric(source, toKind, nullable);
	}

	if (isNumericKind(fromKind) && toKind === DTypeKind.Boolean) {
		return castNumericToBoolean(source, fromKind, nullable);
	}

	if (fromKind === DTypeKind.Boolean && toKind === DTypeKind.String) {
		const dict = dictionary ?? createDictionary();
		return castBooleanToString(source, dict, nullable);
	}

	if (fromKind === DTypeKind.String && toKind === DTypeKind.Boolean) {
		if (!dictionary) {
			return err(ErrorCode.InvalidOperand);
		}
		return castStringToBoolean(source, dictionary, nullable);
	}

	return err(ErrorCode.CastNotSupported);
}

/**
 * Cast numeric type to another numeric type.
 * Uses direct copy loops which Bun/V8 can SIMD-optimize.
 */
function castNumericToNumeric(
	source: ColumnBuffer,
	fromKind: DTypeKind,
	toKind: DTypeKind,
	nullable: boolean,
): Result<CastResult> {
	const length = source.length;
	const output = new ColumnBuffer(
		toKind,
		length,
		nullable || source.isNullable,
	);
	const srcData = source.data;

	const srcIsBigInt = isBigIntKind(fromKind);
	const dstIsBigInt = isBigIntKind(toKind);

	if (!srcIsBigInt && !dstIsBigInt) {
		// Both are regular numbers - direct loop copy
		const src = srcData as
			| Float64Array
			| Float32Array
			| Int32Array
			| Int16Array
			| Int8Array
			| Uint32Array
			| Uint16Array
			| Uint8Array;
		const dst = output.data as
			| Float64Array
			| Float32Array
			| Int32Array
			| Int16Array
			| Int8Array
			| Uint32Array
			| Uint16Array
			| Uint8Array;
		for (let i = 0; i < length; i++) {
			dst[i] = src[i]!;
		}
	} else if (srcIsBigInt && dstIsBigInt) {
		// Both are BigInt types - direct loop copy
		const src = srcData as BigInt64Array | BigUint64Array;
		const dst = output.data as BigInt64Array | BigUint64Array;
		for (let i = 0; i < length; i++) {
			dst[i] = src[i]!;
		}
	} else if (srcIsBigInt && !dstIsBigInt) {
		// BigInt → Number: manual conversion
		const src = srcData as BigInt64Array | BigUint64Array;
		const dst = output.data as Float64Array | Int32Array | Float32Array;
		for (let i = 0; i < length; i++) {
			dst[i] = Number(src[i]);
		}
	} else {
		// Number → BigInt: manual conversion
		const src = srcData as Int32Array | Float64Array;
		const dst = output.data as BigInt64Array | BigUint64Array;
		for (let i = 0; i < length; i++) {
			dst[i] = BigInt(Math.trunc(src[i]!));
		}
	}

	// Copy null bitmap
	copyNullBitmap(source, output, length);

	// Set length
	setColumnLength(output, length);

	return ok({ column: output });
}

/**
 * Cast string column to numeric type.
 * Parses each dictionary entry once, then maps column indices.
 */
function castStringToNumeric(
	source: ColumnBuffer,
	toKind: DTypeKind,
	dictionary: Dictionary,
	_nullable: boolean,
): Result<CastResult> {
	const length = source.length;
	const output = new ColumnBuffer(toKind, length, true); // Always nullable (parse may fail)
	const srcData = source.data as Uint32Array;
	const dstIsBigInt = isBigIntKind(toKind);

	// Build parse cache for dictionary entries
	const parseCache = new Map<number, number | bigint>();
	const failedIndices = new Set<number>();

	for (let i = 0; i < dictionary.size; i++) {
		const str = dictionary.getString(i);
		if (str === undefined) continue;

		const trimmed = str.trim();
		if (trimmed === "") {
			failedIndices.add(i);
			continue;
		}

		if (dstIsBigInt) {
			try {
				parseCache.set(i, BigInt(trimmed));
			} catch {
				failedIndices.add(i);
			}
		} else {
			const num = parseFloat(trimmed);
			if (Number.isNaN(num)) {
				failedIndices.add(i);
			} else {
				parseCache.set(i, num);
			}
		}
	}

	// Map column values through cache
	if (dstIsBigInt) {
		const dst = output.data as BigInt64Array | BigUint64Array;
		for (let i = 0; i < length; i++) {
			if (source.isNull(i)) {
				output.setNull(i, true);
				continue;
			}
			const dictIdx = srcData[i]!;
			const parsed = parseCache.get(dictIdx);
			if (parsed !== undefined) {
				dst[i] = parsed as bigint;
			} else {
				output.setNull(i, true);
			}
		}
	} else {
		const dst = output.data as Float64Array | Int32Array | Float32Array;
		for (let i = 0; i < length; i++) {
			if (source.isNull(i)) {
				output.setNull(i, true);
				continue;
			}
			const dictIdx = srcData[i]!;
			const parsed = parseCache.get(dictIdx);
			if (parsed !== undefined) {
				dst[i] = parsed as number;
			} else {
				output.setNull(i, true);
			}
		}
	}

	setColumnLength(output, length);

	return ok({ column: output });
}

/**
 * Cast numeric column to string.
 * Interns stringified values into dictionary.
 */
function castNumericToString(
	source: ColumnBuffer,
	fromKind: DTypeKind,
	dictionary: Dictionary,
	nullable: boolean,
): Result<CastResult> {
	const length = source.length;
	const output = new ColumnBuffer(
		DTypeKind.String,
		length,
		nullable || source.isNullable,
	);
	const srcData = source.data;
	const dstData = output.data as Uint32Array;
	const srcIsBigInt = isBigIntKind(fromKind);

	// Cache stringified values to avoid re-interning duplicates
	const stringCache = new Map<number | bigint, number>();

	if (srcIsBigInt) {
		const src = srcData as BigInt64Array | BigUint64Array;
		for (let i = 0; i < length; i++) {
			if (source.isNull(i)) {
				output.setNull(i, true);
				continue;
			}
			const value = src[i]!;
			let dictIdx = stringCache.get(value);
			if (dictIdx === undefined) {
				dictIdx = dictionary.internString(value.toString());
				stringCache.set(value, dictIdx);
			}
			dstData[i] = dictIdx;
		}
	} else {
		const src = srcData as Float64Array | Int32Array | Float32Array;
		for (let i = 0; i < length; i++) {
			if (source.isNull(i)) {
				output.setNull(i, true);
				continue;
			}
			const value = src[i]!;
			let dictIdx = stringCache.get(value);
			if (dictIdx === undefined) {
				dictIdx = dictionary.internString(value.toString());
				stringCache.set(value, dictIdx);
			}
			dstData[i] = dictIdx;
		}
	}

	setColumnLength(output, length);

	return ok({ column: output, dictionary });
}

/**
 * Cast boolean to numeric (0/1).
 */
function castBooleanToNumeric(
	source: ColumnBuffer,
	toKind: DTypeKind,
	nullable: boolean,
): Result<CastResult> {
	const length = source.length;
	const output = new ColumnBuffer(
		toKind,
		length,
		nullable || source.isNullable,
	);
	const srcData = source.data as Uint8Array;
	const dstIsBigInt = isBigIntKind(toKind);

	if (dstIsBigInt) {
		const dst = output.data as BigInt64Array | BigUint64Array;
		for (let i = 0; i < length; i++) {
			dst[i] = srcData[i] !== 0 ? 1n : 0n;
		}
	} else {
		const dst = output.data as Float64Array | Int32Array | Float32Array;
		for (let i = 0; i < length; i++) {
			dst[i] = srcData[i] !== 0 ? 1 : 0;
		}
	}

	copyNullBitmap(source, output, length);
	setColumnLength(output, length);

	return ok({ column: output });
}

/**
 * Cast numeric to boolean (non-zero = true).
 */
function castNumericToBoolean(
	source: ColumnBuffer,
	fromKind: DTypeKind,
	nullable: boolean,
): Result<CastResult> {
	const length = source.length;
	const output = new ColumnBuffer(
		DTypeKind.Boolean,
		length,
		nullable || source.isNullable,
	);
	const srcData = source.data;
	const dstData = output.data as Uint8Array;
	const srcIsBigInt = isBigIntKind(fromKind);

	if (srcIsBigInt) {
		const src = srcData as BigInt64Array | BigUint64Array;
		for (let i = 0; i < length; i++) {
			dstData[i] = src[i] !== 0n ? 1 : 0;
		}
	} else {
		const src = srcData as Float64Array | Int32Array | Float32Array;
		for (let i = 0; i < length; i++) {
			dstData[i] = src[i] !== 0 ? 1 : 0;
		}
	}

	copyNullBitmap(source, output, length);
	setColumnLength(output, length);

	return ok({ column: output });
}

/**
 * Cast boolean to string ("true"/"false").
 */
function castBooleanToString(
	source: ColumnBuffer,
	dictionary: Dictionary,
	nullable: boolean,
): Result<CastResult> {
	const length = source.length;
	const output = new ColumnBuffer(
		DTypeKind.String,
		length,
		nullable || source.isNullable,
	);
	const srcData = source.data as Uint8Array;
	const dstData = output.data as Uint32Array;

	// Intern true/false strings once
	const trueIdx = dictionary.internString("true");
	const falseIdx = dictionary.internString("false");

	for (let i = 0; i < length; i++) {
		if (source.isNull(i)) {
			output.setNull(i, true);
			continue;
		}
		dstData[i] = srcData[i] !== 0 ? trueIdx : falseIdx;
	}

	setColumnLength(output, length);

	return ok({ column: output, dictionary });
}

/**
 * Cast string to boolean ("true"/"1"/"yes" → true).
 */
function castStringToBoolean(
	source: ColumnBuffer,
	dictionary: Dictionary,
	_nullable: boolean,
): Result<CastResult> {
	const length = source.length;
	const output = new ColumnBuffer(DTypeKind.Boolean, length, true); // Always nullable
	const srcData = source.data as Uint32Array;
	const dstData = output.data as Uint8Array;

	// Build parse cache for dictionary entries
	const TRUTHY = new Set(["true", "1", "yes", "y", "on"]);
	const FALSY = new Set(["false", "0", "no", "n", "off"]);

	const boolCache = new Map<number, boolean | null>();
	for (let i = 0; i < dictionary.size; i++) {
		const str = dictionary.getString(i)?.toLowerCase().trim();
		if (str === undefined) {
			boolCache.set(i, null);
		} else if (TRUTHY.has(str)) {
			boolCache.set(i, true);
		} else if (FALSY.has(str)) {
			boolCache.set(i, false);
		} else {
			boolCache.set(i, null); // Unknown string → null
		}
	}

	for (let i = 0; i < length; i++) {
		if (source.isNull(i)) {
			output.setNull(i, true);
			continue;
		}
		const dictIdx = srcData[i]!;
		const value = boolCache.get(dictIdx);
		if (value === null || value === undefined) {
			output.setNull(i, true);
		} else {
			dstData[i] = value ? 1 : 0;
		}
	}

	setColumnLength(output, length);

	return ok({ column: output });
}

// ============ Helpers ============

function isNumericKind(kind: DTypeKind): boolean {
	return kind >= DTypeKind.Int8 && kind <= DTypeKind.Float64;
}

function isBigIntKind(kind: DTypeKind): boolean {
	return (
		kind === DTypeKind.Int64 ||
		kind === DTypeKind.UInt64 ||
		kind === DTypeKind.Timestamp
	);
}

function copyNullBitmap(
	source: ColumnBuffer,
	output: ColumnBuffer,
	length: number,
): void {
	if (!source.isNullable || !output.isNullable) return;

	for (let i = 0; i < length; i++) {
		if (source.isNull(i)) {
			output.setNull(i, true);
		}
	}
}

function setColumnLength(column: ColumnBuffer, length: number): void {
	// Access private _length through casting
	(column as unknown as { _length: number })._length = length;
}
