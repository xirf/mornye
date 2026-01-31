/**
 * Concat operator.
 *
 * Vertically concatenates multiple DataFrames/chunks.
 */

import { Chunk } from "../buffer/chunk.ts";
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";

/**
 * Concatenate multiple chunks vertically.
 * All chunks must have the same schema.
 */
export function concatChunks(chunks: Chunk[], schema: Schema): Result<Chunk> {
	if (chunks.length === 0) {
		return err(ErrorCode.SchemaMismatch);
	}

	if (chunks.length === 1) {
		// biome-ignore lint/style/noNonNullAssertion: Length checked
		return ok(chunks[0]!);
	}

	// Calculate total rows
	const totalRows = chunks.reduce((sum, c) => sum + c.rowCount, 0);
	if (totalRows === 0) {
		// biome-ignore lint/style/noNonNullAssertion: Length checked
		return ok(chunks[0]!);
	}

	// Use dictionary from first chunk
	// biome-ignore lint/style/noNonNullAssertion: Length checked
	const dictionary = chunks[0]!.dictionary;

	// Create output columns
	const columns: ColumnBuffer[] = [];
	for (const col of schema.columns) {
		columns.push(
			new ColumnBuffer(col.dtype.kind, totalRows, col.dtype.nullable),
		);
	}

	// Copy all rows
	for (const chunk of chunks) {
		for (let r = 0; r < chunk.rowCount; r++) {
			for (let c = 0; c < schema.columnCount; c++) {
				// biome-ignore lint/style/noNonNullAssertion: Columns initialized to match schema
				const destCol = columns[c]!;
				if (chunk.isNull(c, r)) {
					destCol.appendNull();
				} else {
					const value = chunk.getValue(c, r);
					// biome-ignore lint/style/noNonNullAssertion: Checked by isNull
					destCol.append(value!);
				}
			}
		}
	}

	return ok(new Chunk(schema, columns, dictionary));
}

/**
 * Validate that schemas are compatible for concatenation.
 */
export function validateConcatSchemas(schemas: Schema[]): Result<Schema> {
	if (schemas.length === 0) {
		return err(ErrorCode.SchemaMismatch);
	}

	// biome-ignore lint/style/noNonNullAssertion: Length checked
	const first = schemas[0]!;

	for (let i = 1; i < schemas.length; i++) {
		// biome-ignore lint/style/noNonNullAssertion: Loop bounds incorrect? No, length checked.
		const other = schemas[i]!;

		if (other.columnCount !== first.columnCount) {
			return err(ErrorCode.SchemaMismatch);
		}

		for (let c = 0; c < first.columnCount; c++) {
			// biome-ignore lint/style/noNonNullAssertion: Column count match checked
			const col1 = first.columns[c]!;
			// biome-ignore lint/style/noNonNullAssertion: Column count match checked
			const col2 = other.columns[c]!;

			if (col1.name !== col2.name || col1.dtype.kind !== col2.dtype.kind) {
				return err(ErrorCode.SchemaMismatch);
			}
		}
	}

	return ok(first);
}
