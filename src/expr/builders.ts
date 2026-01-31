/**
 * Expression builders for fluent DSL.
 *
 * These functions create expression AST nodes with a chainable API.
 * The ColumnRef class provides method chaining for column operations.
 */

import type { DTypeKind } from "../types/dtypes.ts";
import {
	type AggExpr,
	type AliasExpr,
	type ArithmeticExpr,
	type BetweenExpr,
	type CastExpr,
	type CoalesceExpr,
	type ColumnExpr,
	type ComparisonExpr,
	type CountExpr,
	type Expr,
	ExprType,
	type LiteralExpr,
	type LogicalExpr,
	type NegExpr,
	type NotExpr,
	type NullCheckExpr,
	type StringOpExpr,
} from "./ast.ts";

/**
 * Wrapper class providing fluent API for column expressions.
 * Methods return Expr objects, not new ColumnRef instances.
 */
export class ColumnRef {
	private readonly expr: ColumnExpr;

	constructor(name: string) {
		this.expr = { type: ExprType.Column, name };
	}

	/** Get the underlying expression */
	toExpr(): ColumnExpr {
		return this.expr;
	}

	// Comparison operators
	eq(other: Expr | number | string | boolean): ComparisonExpr {
		return { type: ExprType.Eq, left: this.expr, right: toExpr(other) };
	}

	neq(other: Expr | number | string | boolean): ComparisonExpr {
		return { type: ExprType.Neq, left: this.expr, right: toExpr(other) };
	}

	lt(other: Expr | number): ComparisonExpr {
		return { type: ExprType.Lt, left: this.expr, right: toExpr(other) };
	}

	lte(other: Expr | number): ComparisonExpr {
		return { type: ExprType.Lte, left: this.expr, right: toExpr(other) };
	}

	gt(other: Expr | number): ComparisonExpr {
		return { type: ExprType.Gt, left: this.expr, right: toExpr(other) };
	}

	gte(other: Expr | number): ComparisonExpr {
		return { type: ExprType.Gte, left: this.expr, right: toExpr(other) };
	}

	between(low: Expr | number, high: Expr | number): BetweenExpr {
		return {
			type: ExprType.Between,
			expr: this.expr,
			low: toExpr(low),
			high: toExpr(high),
		};
	}

	isNull(): NullCheckExpr {
		return { type: ExprType.IsNull, expr: this.expr };
	}

	isNotNull(): NullCheckExpr {
		return { type: ExprType.IsNotNull, expr: this.expr };
	}

	// Arithmetic operators
	add(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Add, left: this.expr, right: toExpr(other) };
	}

	sub(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Sub, left: this.expr, right: toExpr(other) };
	}

	mul(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Mul, left: this.expr, right: toExpr(other) };
	}

	div(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Div, left: this.expr, right: toExpr(other) };
	}

	mod(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Mod, left: this.expr, right: toExpr(other) };
	}

	neg(): NegExpr {
		return { type: ExprType.Neg, expr: this.expr };
	}

	// String operations
	contains(pattern: string): StringOpExpr {
		return { type: ExprType.Contains, expr: this.expr, pattern };
	}

	startsWith(pattern: string): StringOpExpr {
		return { type: ExprType.StartsWith, expr: this.expr, pattern };
	}

	endsWith(pattern: string): StringOpExpr {
		return { type: ExprType.EndsWith, expr: this.expr, pattern };
	}

	// Alias
	alias(name: string): AliasExpr {
		return { type: ExprType.Alias, expr: this.expr, alias: name };
	}

	// Type casting
	cast(targetDType: DTypeKind): CastExpr {
		return { type: ExprType.Cast, expr: this.expr, targetDType };
	}
}

/**
 * Create a column reference expression.
 *
 * Usage:
 *   col("name")              // Returns ColumnRef with fluent methods
 *   col("age").gt(18)        // Returns ComparisonExpr
 *   col("price").mul(1.1)    // Returns ArithmeticExpr
 */
export function col(name: string): ColumnRef {
	return new ColumnRef(name);
}

/**
 * Create a literal expression.
 */
export function lit(
	value: number | bigint | string | boolean | null,
): LiteralExpr {
	return { type: ExprType.Literal, value };
}

/**
 * Create a typed literal (with explicit dtype hint).
 */
export function typedLit(
	value: number | bigint | string | boolean | null,
	dtype: DTypeKind,
): LiteralExpr {
	return { type: ExprType.Literal, value, dtype };
}

/**
 * Convert a value or ColumnRef to an Expr.
 */
function toExpr(
	value: Expr | ColumnRef | number | string | boolean | bigint | null,
): Expr {
	if (value === null) {
		return lit(null);
	}
	if (
		typeof value === "number" ||
		typeof value === "string" ||
		typeof value === "boolean" ||
		typeof value === "bigint"
	) {
		return lit(value);
	}
	if (value instanceof ColumnRef) {
		return value.toExpr();
	}
	return value;
}

// Logical combinators

/**
 * Logical AND of multiple expressions.
 */
export function and(...exprs: (Expr | ColumnRef)[]): LogicalExpr {
	return {
		type: ExprType.And,
		exprs: exprs.map((e) => (e instanceof ColumnRef ? e.toExpr() : e)),
	};
}

/**
 * Logical OR of multiple expressions.
 */
export function or(...exprs: (Expr | ColumnRef)[]): LogicalExpr {
	return {
		type: ExprType.Or,
		exprs: exprs.map((e) => (e instanceof ColumnRef ? e.toExpr() : e)),
	};
}

/**
 * Logical NOT of an expression.
 */
export function not(expr: Expr | ColumnRef): NotExpr {
	return {
		type: ExprType.Not,
		expr: expr instanceof ColumnRef ? expr.toExpr() : expr,
	};
}

// Aggregation functions

/**
 * Sum aggregation.
 */
export function sum(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Sum, expr };
}

/**
 * Average aggregation.
 */
export function avg(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Avg, expr };
}

/**
 * Minimum aggregation.
 */
export function min(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Min, expr };
}

/**
 * Maximum aggregation.
 */
export function max(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Max, expr };
}

/**
 * First value aggregation.
 */
export function first(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.First, expr };
}

/**
 * Last value aggregation.
 */
export function last(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Last, expr };
}

/**
 * Count aggregation.
 * count() - count all rows
 * count(column) - count non-null values in column
 */
export function count(column?: string | Expr | ColumnRef): CountExpr {
	if (column === undefined) {
		return { type: ExprType.Count, expr: null };
	}
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Count, expr };
}

// Arithmetic on expressions (not just columns)

/**
 * Add two expressions.
 */
export function add(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Add, left: toExpr(left), right: toExpr(right) };
}

/**
 * Subtract two expressions.
 */
export function sub(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Sub, left: toExpr(left), right: toExpr(right) };
}

/**
 * Multiply two expressions.
 */
export function mul(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Mul, left: toExpr(left), right: toExpr(right) };
}

/**
 * Divide two expressions.
 */
export function div(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Div, left: toExpr(left), right: toExpr(right) };
}

/**
 * Modulo two expressions.
 */
export function mod(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Mod, left: toExpr(left), right: toExpr(right) };
}

/**
 * Negate an expression.
 */
export function neg(expr: Expr | ColumnRef | number): NegExpr {
	return { type: ExprType.Neg, expr: toExpr(expr) };
}

// Null handling

/**
 * Coalesce multiple expressions (returns first non-null).
 */
export function coalesce(
	...exprs: (Expr | ColumnRef | number | string | boolean | null)[]
): CoalesceExpr {
	return {
		type: ExprType.Coalesce,
		exprs: exprs.map(toExpr),
	};
}
