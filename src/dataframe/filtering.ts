/* FILTERING METHODS
/*-----------------------------------------------------
/* Filter and where methods for DataFrame
/* ==================================================== */

import type { Expr } from "../expr/ast.ts";
import { ColumnRef } from "../expr/builders.ts";
import { filter } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addFilteringMethods(df: typeof DataFrame.prototype) {
	df.filter = function (expr: Expr | ColumnRef): DataFrame {
		const condition = expr instanceof ColumnRef ? expr.toExpr() : expr;
		const result = filter(this.currentSchema(), condition);
		if (result.error !== ErrorCode.None) {
			throw new Error(`Filter error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};

	df.where = function (expr: Expr): DataFrame {
		return this.filter(expr);
	};
}
