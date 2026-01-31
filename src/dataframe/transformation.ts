/* TRANSFORMATION METHODS
/*-----------------------------------------------------
/* Add computed columns to DataFrame
/* ==================================================== */

import type { Expr } from "../expr/ast.ts";
import { ColumnRef } from "../expr/builders.ts";
import { type ComputedColumn, transform } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addTransformMethods(df: typeof DataFrame.prototype) {
	df.withColumn = function (name: string, expr: Expr | ColumnRef): DataFrame {
		const e = expr instanceof ColumnRef ? expr.toExpr() : expr;
		const result = transform(this.currentSchema(), [{ name, expr: e }]);
		if (result.error !== ErrorCode.None) {
			throw new Error(`WithColumn error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};

	df.withColumns = function (columns: ComputedColumn[]): DataFrame {
		const result = transform(this.currentSchema(), columns);
		if (result.error !== ErrorCode.None) {
			throw new Error(`WithColumns error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};
}
