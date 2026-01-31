/**
 * Pipeline builder for fluent API.
 *
 * Provides a type-safe builder pattern for constructing pipelines.
 */

import type { Chunk } from "../buffer/chunk.ts";
import type { Expr } from "../expr/ast.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";
import { type AggSpec, aggregate } from "./aggregate.ts";
import { filter } from "./filter.ts";
import { groupBy } from "./groupby.ts";
import { limit } from "./limit.ts";
import type { Operator } from "./operator.ts";
import { Pipeline, type PipelineResult } from "./pipeline.ts";
import { type ProjectSpec, project, projectWithRename } from "./project.ts";
import { type ComputedColumn, transform, withColumn } from "./transform.ts";

/**
 * Pipeline builder with fluent API.
 */
export class PipelineBuilder {
	private currentSchema: Schema;
	private readonly operators: Operator[] = [];
	private buildError: ErrorCode = ErrorCode.None;

	constructor(schema: Schema) {
		this.currentSchema = schema;
	}

	/**
	 * Get the current output schema.
	 */
	get schema(): Schema {
		return this.currentSchema;
	}

	/**
	 * Filter rows by predicate expression.
	 */
	filter(expr: Expr): this {
		if (this.buildError !== ErrorCode.None) return this;

		const result = filter(this.currentSchema, expr);
		if (result.error !== ErrorCode.None) {
			this.buildError = result.error;
			return this;
		}

		this.operators.push(result.value);
		return this;
	}

	/**
	 * Select specific columns.
	 */
	select(...columns: string[]): this {
		if (this.buildError !== ErrorCode.None) return this;

		const result = project(this.currentSchema, columns);
		if (result.error !== ErrorCode.None) {
			this.buildError = result.error;
			return this;
		}

		this.operators.push(result.value);
		this.currentSchema = result.value.outputSchema;
		return this;
	}

	/**
	 * Select and rename columns.
	 */
	selectAs(specs: ProjectSpec[]): this {
		if (this.buildError !== ErrorCode.None) return this;

		const result = projectWithRename(this.currentSchema, specs);
		if (result.error !== ErrorCode.None) {
			this.buildError = result.error;
			return this;
		}

		this.operators.push(result.value);
		this.currentSchema = result.value.outputSchema;
		return this;
	}

	/**
	 * Add computed columns.
	 */
	withColumns(...columns: ComputedColumn[]): this {
		if (this.buildError !== ErrorCode.None) return this;

		const result = transform(this.currentSchema, columns);
		if (result.error !== ErrorCode.None) {
			this.buildError = result.error;
			return this;
		}

		this.operators.push(result.value);
		this.currentSchema = result.value.outputSchema;
		return this;
	}

	/**
	 * Add a single computed column.
	 */
	addColumn(name: string, expr: Expr): this {
		return this.withColumns(withColumn(name, expr));
	}

	/**
	 * Limit output rows.
	 */
	limit(count: number, offset?: number): this {
		if (this.buildError !== ErrorCode.None) return this;

		this.operators.push(limit(this.currentSchema, count, offset));
		return this;
	}

	/**
	 * Aggregate without grouping.
	 */
	aggregate(specs: AggSpec[]): this {
		if (this.buildError !== ErrorCode.None) return this;

		const result = aggregate(this.currentSchema, specs);
		if (result.error !== ErrorCode.None) {
			this.buildError = result.error;
			return this;
		}

		this.operators.push(result.value);
		this.currentSchema = result.value.outputSchema;
		return this;
	}

	/**
	 * Group by columns and aggregate.
	 */
	groupBy(keyColumns: string[], aggSpecs: AggSpec[]): this {
		if (this.buildError !== ErrorCode.None) return this;

		const result = groupBy(this.currentSchema, keyColumns, aggSpecs);
		if (result.error !== ErrorCode.None) {
			this.buildError = result.error;
			return this;
		}

		this.operators.push(result.value);
		this.currentSchema = result.value.outputSchema;
		return this;
	}

	/**
	 * Build the pipeline.
	 */
	build(): Result<Pipeline> {
		if (this.buildError !== ErrorCode.None) {
			return err(this.buildError);
		}

		if (this.operators.length === 0) {
			return err(ErrorCode.InvalidPipeline);
		}

		return ok(new Pipeline(this.operators));
	}

	/**
	 * Build and execute on chunks.
	 */
	execute(chunks: Iterable<Chunk>): Result<PipelineResult> {
		const pipelineResult = this.build();
		if (pipelineResult.error !== ErrorCode.None) {
			return err(pipelineResult.error);
		}

		return pipelineResult.value.execute(chunks);
	}

	/**
	 * Build and execute on async chunks.
	 */
	async executeAsync(
		chunks: AsyncIterable<Chunk>,
	): Promise<Result<PipelineResult>> {
		const pipelineResult = this.build();
		if (pipelineResult.error !== ErrorCode.None) {
			return err(pipelineResult.error);
		}

		return pipelineResult.value.executeAsync(chunks);
	}
}

/**
 * Start building a pipeline from a schema.
 */
export function from(schema: Schema): PipelineBuilder {
	return new PipelineBuilder(schema);
}
