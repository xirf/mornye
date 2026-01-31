/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
/**
 * Join operator.
 *
 * Hash-based join implementation supporting inner, left, and right joins.
 */

import { Chunk } from "../buffer/chunk.ts";
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { createDictionary, type Dictionary } from "../buffer/dictionary.ts";
import { type DType, DTypeKind, toNullable } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { createSchema, getColumnIndex, type Schema } from "../types/schema.ts";

/** Join type */
export enum JoinType {
	Inner = "inner",
	Left = "left",
	Right = "right",
}

/** Join configuration */
export interface JoinConfig {
	leftKey: string;
	rightKey: string;
	joinType?: JoinType;
	suffix?: string; // Suffix for conflicting column names (default: "_right")
}

/**
 * Perform a hash join between two sets of chunks.
 *
 * For streaming, the right side (build side) is fully materialized.
 * The left side is streamed through.
 */
export function hashJoin(
	leftChunks: Chunk[],
	leftSchema: Schema,
	rightChunks: Chunk[],
	rightSchema: Schema,
	config: JoinConfig,
): Result<{ chunks: Chunk[]; schema: Schema }> {
	const joinType = config.joinType ?? JoinType.Inner;
	const suffix = config.suffix ?? "_right";

	// Validate key columns
	const leftKeyResult = getColumnIndex(leftSchema, config.leftKey);
	if (leftKeyResult.error !== ErrorCode.None) {
		return err(ErrorCode.UnknownColumn);
	}
	const leftKeyIdx = leftKeyResult.value;

	const rightKeyResult = getColumnIndex(rightSchema, config.rightKey);
	if (rightKeyResult.error !== ErrorCode.None) {
		return err(ErrorCode.UnknownColumn);
	}
	const rightKeyIdx = rightKeyResult.value;

	// Build output schema
	const outputSchemaResult = buildJoinSchema(
		leftSchema,
		rightSchema,
		config.leftKey,
		config.rightKey,
		suffix,
		joinType,
	);
	if (outputSchemaResult.error !== ErrorCode.None) {
		return err(outputSchemaResult.error);
	}
	const outputSchema = outputSchemaResult.value;

	// Build hash table from right side
	const hashTable = buildHashTable(rightChunks, rightKeyIdx);

	// Track matched right rows for right/outer joins
	const rightMatched = new Set<string>();

	// Process left side and produce output
	const outputChunks: Chunk[] = [];
	const leftDict =
		(leftChunks.length > 0 ? leftChunks[0]!.dictionary : null) ??
		createDictionary();

	for (const leftChunk of leftChunks) {
		const result = processLeftChunk(
			leftChunk,
			leftKeyIdx,
			rightChunks,
			rightKeyIdx,
			hashTable,
			outputSchema,
			leftSchema,
			rightSchema,
			joinType,
			rightMatched,
			config.leftKey,
		);

		if (result.rowCount > 0) {
			outputChunks.push(result);
		}
	}

	// For right join, add unmatched right rows
	if (joinType === JoinType.Right) {
		const unmatchedResult = addUnmatchedRight(
			rightChunks,
			rightKeyIdx,
			rightMatched,
			outputSchema,
			leftSchema,
			rightSchema,
			config.leftKey,
			leftDict,
		);

		if (unmatchedResult.rowCount > 0) {
			outputChunks.push(unmatchedResult);
		}
	}

	return ok({ chunks: outputChunks, schema: outputSchema });
}

/**
 * Build output schema for join.
 * For left joins, right columns become nullable.
 * For right joins, left columns become nullable.
 */
function buildJoinSchema(
	leftSchema: Schema,
	rightSchema: Schema,
	leftKey: string,
	rightKey: string,
	suffix: string,
	joinType: JoinType,
): Result<Schema> {
	const schemaSpec: Record<string, DType> = {};
	const usedNames = new Set<string>();

	// Add all left columns (nullable for right joins)
	for (const col of leftSchema.columns) {
		const dtype =
			joinType === JoinType.Right ? toNullable(col.dtype) : col.dtype;
		schemaSpec[col.name] = dtype;
		usedNames.add(col.name);
	}

	// Add right columns (except the key if it has the same name)
	// Nullable for left joins
	for (const col of rightSchema.columns) {
		// Skip the join key if same name
		if (col.name === rightKey && leftKey === rightKey) {
			continue;
		}

		let name = col.name;
		if (usedNames.has(name)) {
			name = col.name + suffix;
		}

		const dtype =
			joinType === JoinType.Left ? toNullable(col.dtype) : col.dtype;
		schemaSpec[name] = dtype;
	}

	return createSchema(schemaSpec);
}

/**
 * Build hash table from right chunks.
 * Maps key value (as string) to array of (chunkIdx, rowIdx) pairs.
 */
function buildHashTable(
	rightChunks: Chunk[],
	keyIdx: number,
): Map<string, Array<{ chunkIdx: number; rowIdx: number }>> {
	const table = new Map<string, Array<{ chunkIdx: number; rowIdx: number }>>();

	for (let c = 0; c < rightChunks.length; c++) {
		const chunk = rightChunks[c]!;
		for (let r = 0; r < chunk.rowCount; r++) {
			if (chunk.isNull(keyIdx, r)) continue;

			const keyValue = getKeyAsString(chunk, keyIdx, r);
			let entries = table.get(keyValue);
			if (!entries) {
				entries = [];
				table.set(keyValue, entries);
			}
			entries.push({ chunkIdx: c, rowIdx: r });
		}
	}

	return table;
}

/**
 * Get key value as string for hashing.
 */
function getKeyAsString(chunk: Chunk, colIdx: number, rowIdx: number): string {
	const dtype = chunk.schema.columns[colIdx]!.dtype;

	if (dtype.kind === DTypeKind.String) {
		return chunk.getStringValue(colIdx, rowIdx) ?? "";
	}

	return String(chunk.getValue(colIdx, rowIdx));
}

/**
 * Process left chunk and produce joined output.
 */
function processLeftChunk(
	leftChunk: Chunk,
	leftKeyIdx: number,
	rightChunks: Chunk[],
	rightKeyIdx: number,
	hashTable: Map<string, Array<{ chunkIdx: number; rowIdx: number }>>,
	outputSchema: Schema,
	leftSchema: Schema,
	rightSchema: Schema,
	joinType: JoinType,
	rightMatched: Set<string>,
	leftKey: string,
): Chunk {
	// Pre-allocate (may need to grow for multi-matches)
	const columns: ColumnBuffer[] = [];
	for (const col of outputSchema.columns) {
		columns.push(
			new ColumnBuffer(
				col.dtype.kind,
				leftChunk.rowCount * 2,
				col.dtype.nullable,
			),
		);
	}

	for (let r = 0; r < leftChunk.rowCount; r++) {
		if (leftChunk.isNull(leftKeyIdx, r)) {
			if (joinType === JoinType.Left) {
				// Add left row with nulls for right
				appendLeftRow(
					columns,
					leftChunk,
					r,
					leftSchema,
					rightSchema,
					null,
					null,
					rightKeyIdx,
					leftKey,
				);
			}
			continue;
		}

		const keyValue = getKeyAsString(leftChunk, leftKeyIdx, r);
		const matches = hashTable.get(keyValue);

		if (!matches || matches.length === 0) {
			if (joinType === JoinType.Left) {
				appendLeftRow(
					columns,
					leftChunk,
					r,
					leftSchema,
					rightSchema,
					null,
					null,
					rightKeyIdx,
					leftKey,
				);
			}
			continue;
		}

		// Add a row for each match
		for (const match of matches) {
			const rightChunk = rightChunks[match.chunkIdx]!;
			appendLeftRow(
				columns,
				leftChunk,
				r,
				leftSchema,
				rightSchema,
				rightChunk,
				match,
				rightKeyIdx,
				leftKey,
			);
			rightMatched.add(`${match.chunkIdx}:${match.rowIdx}`);
		}
	}

	return new Chunk(outputSchema, columns, leftChunk.dictionary);
}

/**
 * Append a joined row to output columns.
 */
function appendLeftRow(
	columns: ColumnBuffer[],
	leftChunk: Chunk,
	leftRow: number,
	leftSchema: Schema,
	rightSchema: Schema,
	rightChunk: Chunk | null,
	rightMatch: { chunkIdx: number; rowIdx: number } | null,
	rightKeyIdx: number,
	leftKey: string,
): void {
	let outIdx = 0;

	// Add left columns
	for (let c = 0; c < leftSchema.columnCount; c++) {
		const col = columns[outIdx++]!;
		if (leftChunk.isNull(c, leftRow)) {
			col.appendNull();
		} else {
			const value = leftChunk.getValue(c, leftRow);
			col.append(value!);
		}
	}

	// Add right columns (except duplicate key)
	for (let c = 0; c < rightSchema.columnCount; c++) {
		const colName = rightSchema.columns[c]!.name;
		// Skip if same key column name
		if (c === rightKeyIdx && colName === leftKey) {
			continue;
		}

		const col = columns[outIdx++]!;
		const rightColDef = rightSchema.columns[c]!;

		if (rightChunk && rightMatch) {
			if (rightChunk.isNull(c, rightMatch.rowIdx)) {
				col.appendNull();
			} else {
				let value = rightChunk.getValue(c, rightMatch.rowIdx);

				if (
					rightColDef.dtype.kind === DTypeKind.String &&
					rightChunk.dictionary &&
					leftChunk.dictionary
				) {
					const str = rightChunk.dictionary.getString(value as number);
					if (str !== undefined) {
						value = leftChunk.dictionary.internString(str);
					}
				}

				col.append(value!);
			}
		} else {
			col.appendNull();
		}
	}
}

/**
 * Add unmatched right rows for right join.
 */
function addUnmatchedRight(
	rightChunks: Chunk[],
	rightKeyIdx: number,
	rightMatched: Set<string>,
	outputSchema: Schema,
	leftSchema: Schema,
	rightSchema: Schema,
	leftKey: string,
	dictionary: Dictionary,
): Chunk {
	const columns: ColumnBuffer[] = [];
	for (const col of outputSchema.columns) {
		columns.push(new ColumnBuffer(col.dtype.kind, 1024, col.dtype.nullable));
	}

	for (let c = 0; c < rightChunks.length; c++) {
		const chunk = rightChunks[c]!;
		for (let r = 0; r < chunk.rowCount; r++) {
			const key = `${c}:${r}`;
			if (rightMatched.has(key)) continue;

			// Add null left columns + right row
			let outIdx = 0;

			// Left columns as null
			for (let lc = 0; lc < leftSchema.columnCount; lc++) {
				columns[outIdx++]!.appendNull();
			}

			// Right columns
			for (let rc = 0; rc < rightSchema.columnCount; rc++) {
				const colName = rightSchema.columns[rc]!.name;
				if (rc === rightKeyIdx && colName === leftKey) {
					continue;
				}

				const col = columns[outIdx++]!;
				if (chunk.isNull(rc, r)) {
					col.appendNull();
				} else {
					let value = chunk.getValue(rc, r);
					if (col.kind === DTypeKind.String && chunk.dictionary && dictionary) {
						const str = chunk.dictionary.getString(value as number);
						if (str !== undefined) {
							value = dictionary.internString(str);
						}
					}
					col.append(value!);
				}
			}
		}
	}

	return new Chunk(outputSchema, columns, dictionary);
}

/**
 * Convenience function for inner join.
 */
export function innerJoin(
	leftChunks: Chunk[],
	leftSchema: Schema,
	rightChunks: Chunk[],
	rightSchema: Schema,
	leftKey: string,
	rightKey: string,
): Result<{ chunks: Chunk[]; schema: Schema }> {
	return hashJoin(leftChunks, leftSchema, rightChunks, rightSchema, {
		leftKey,
		rightKey,
		joinType: JoinType.Inner,
	});
}

/**
 * Convenience function for left join.
 */
export function leftJoin(
	leftChunks: Chunk[],
	leftSchema: Schema,
	rightChunks: Chunk[],
	rightSchema: Schema,
	leftKey: string,
	rightKey: string,
): Result<{ chunks: Chunk[]; schema: Schema }> {
	return hashJoin(leftChunks, leftSchema, rightChunks, rightSchema, {
		leftKey,
		rightKey,
		joinType: JoinType.Left,
	});
}
