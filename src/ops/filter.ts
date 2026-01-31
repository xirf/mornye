/**
 * Filter operator.
 *
 * Applies a predicate to filter rows.
 * Uses selection vectors to avoid copying data.
 */

import type { Chunk } from "../buffer/chunk.ts";
import type { Expr } from "../expr/ast.ts";
import {
	applyPredicate,
	type CompiledPredicate,
	compilePredicate,
} from "../expr/compiler.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";
import {
	type OperatorResult,
	opEmpty,
	opResult,
	SimpleOperator,
} from "./operator.ts";

/**
 * Filter operator that applies a predicate expression.
 */
export class FilterOperator extends SimpleOperator {
	readonly name = "Filter";
	readonly outputSchema: Schema;

	private readonly predicate: CompiledPredicate;
	private readonly selectionBuffer: Uint32Array;

	private constructor(
		schema: Schema,
		predicate: CompiledPredicate,
		maxChunkSize: number,
	) {
		super();
		this.outputSchema = schema;
		this.predicate = predicate;
		this.selectionBuffer = new Uint32Array(maxChunkSize);
	}

	/**
	 * Create a filter operator from an expression.
	 */
	static create(
		schema: Schema,
		expr: Expr,
		maxChunkSize: number = 65536,
	): Result<FilterOperator> {
		const predicateResult = compilePredicate(expr, schema);
		if (predicateResult.error !== ErrorCode.None) {
			return err(predicateResult.error);
		}

		return ok(new FilterOperator(schema, predicateResult.value, maxChunkSize));
	}

	/**
	 * Create a filter operator from a pre-compiled predicate.
	 */
	static fromPredicate(
		schema: Schema,
		predicate: CompiledPredicate,
		maxChunkSize: number = 65536,
	): FilterOperator {
		return new FilterOperator(schema, predicate, maxChunkSize);
	}

	process(chunk: Chunk): Result<OperatorResult> {
		const rowCount = chunk.rowCount;

		if (rowCount === 0) {
			return ok(opEmpty());
		}

		// Apply predicate to generate selection vector
		const selectedCount = applyPredicate(
			this.predicate,
			chunk,
			this.selectionBuffer,
		);

		if (selectedCount === 0) {
			// No rows matched
			return ok(opEmpty());
		}

		if (selectedCount === rowCount && !chunk.hasSelection()) {
			// All rows matched and no existing selection - pass through
			return ok(opResult(chunk));
		}

		// Apply the new selection to the chunk
		// Create a copy of selection buffer since chunk may store reference
		const selection = this.selectionBuffer.slice(0, selectedCount);
		chunk.applySelection(selection, selectedCount);

		return ok(opResult(chunk));
	}
}

/**
 * Create a filter operator from an expression.
 */
export function filter(
	schema: Schema,
	expr: Expr,
	maxChunkSize?: number,
): Result<FilterOperator> {
	return FilterOperator.create(schema, expr, maxChunkSize);
}
