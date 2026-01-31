/* DATAFRAME TYPE DECLARATIONS
/*-----------------------------------------------------
/* Type definitions for all DataFrame methods
/* ==================================================== */

import type { AggSpec, ComputedColumn, SortKey } from "../ops/index.ts";
import type { ColumnRef } from "../expr/builders.ts";
import type { Expr } from "../expr/ast.ts";
import type { DType } from "../types/dtypes.ts";

declare module "./core.ts" {
	interface DataFrame<T = Record<string, unknown>> {
		// Filtering
		filter(expr: Expr | ColumnRef): DataFrame<T>;
		where(expr: Expr): DataFrame<T>;

		// Projection
		select<K extends keyof T>(
			...columns: (K & string)[]
		): DataFrame<Pick<T, K>>;
		drop<K extends keyof T>(...columns: (K & string)[]): DataFrame<Omit<T, K>>;
		rename<K extends keyof T>(
			mapping: Partial<Record<K, string>>,
		): DataFrame<Record<string, unknown>>; // New type hard to infer perfectly without more complex types

		// Transformation
		withColumn<K extends string, V = unknown>(
			name: K,
			expr: Expr | ColumnRef,
		): DataFrame<T & Record<K, V>>;
		// ComputedColumn complex to type fully without recursively mapping
		withColumns(columns: ComputedColumn[]): DataFrame<Record<string, unknown>>;

		// Data Cleaning
		cast(column: keyof T, targetDType: DType): DataFrame<T>;
		fillNull(
			column: keyof T,
			fillValue: number | bigint | string | boolean,
		): DataFrame<T>;
		dropNull(columns?: keyof T | (keyof T)[]): DataFrame<T>;

		// Deduplication
		unique(columns?: keyof T | (keyof T)[], keepFirst?: boolean): DataFrame<T>;
		dropDuplicates(
			columns?: keyof T | (keyof T)[],
			keepFirst?: boolean,
		): DataFrame<T>;
		distinct(columns?: keyof T | (keyof T)[]): DataFrame<T>;

		// String Operations
		trim(column: keyof T): DataFrame<T>;
		replace(
			column: keyof T,
			pattern: string,
			replacement: string,
			all?: boolean,
		): DataFrame<T>;

		// Limiting
		limit(count: number): DataFrame<T>;
		head(count?: number): DataFrame<T>;
		slice(start: number, count: number): DataFrame<T>;

		// Aggregation
		agg(specs: AggSpec[]): DataFrame<Record<string, unknown>>; // Agg return type depends on spec
		groupBy<K extends keyof T>(
			keyColumns: (K & string) | (K & string)[],
			aggSpecs: AggSpec[],
		): DataFrame<Record<string, unknown>>; // GroupBy changes shape significantly

		// Sorting
		sort(keys: keyof T | (keyof T)[] | SortKey[]): DataFrame<T>;
		orderBy(keys: keyof T | (keyof T)[] | SortKey[]): DataFrame<T>;

		// Joins
		innerJoin<U>(
			other: DataFrame<U>,
			leftOn: keyof T,
			rightOn?: keyof U,
		): Promise<DataFrame<Record<string, unknown>>>;
		leftJoin<U>(
			other: DataFrame<U>,
			leftOn: keyof T,
			rightOn?: keyof U,
		): Promise<DataFrame<Record<string, unknown>>>;
		join<U>(
			other: DataFrame<U>,
			leftOn: keyof T,
			rightOn?: keyof U,
			how?: "inner" | "left",
		): Promise<DataFrame<Record<string, unknown>>>;

		// Concatenation
		concat(other: DataFrame<T>): Promise<DataFrame<T>>;

		// Execution
		collect(): Promise<DataFrame<T>>;
		toChunks(): Promise<import("../buffer/chunk.ts").Chunk[]>;
		count(): Promise<number>;
		toArray(): Promise<T[]>;
		show(maxRows?: number): Promise<void>;
	}
}
