/**
 * Public exports for the expr module.
 */

export {
	type AggExpr,
	type AliasExpr,
	type ArithmeticExpr,
	type BetweenExpr,
	type ColumnExpr,
	type ComparisonExpr,
	type CountExpr,
	type Expr,
	ExprType,
	formatExpr,
	isAggExpr,
	isArithmeticExpr,
	isColumnExpr,
	isComparisonExpr,
	isLiteralExpr,
	type LiteralExpr,
	type LogicalExpr,
	type NegExpr,
	type NotExpr,
	type NullCheckExpr,
	type StringOpExpr,
} from "./ast.ts";

export {
	add,
	and,
	avg,
	ColumnRef,
	col,
	count,
	div,
	first,
	last,
	lit,
	max,
	min,
	mod,
	mul,
	neg,
	not,
	or,
	sub,
	sum,
	typedLit,
} from "./builders.ts";
export {
	applyPredicate,
	applyValue,
	type CompiledPredicate,
	type CompiledValue,
	compilePredicate,
	compileValue,
	countMatching,
} from "./compiler.ts";
export {
	type InferredType,
	inferExprType,
	isPredicateExpr,
	validateExpr,
} from "./types.ts";
