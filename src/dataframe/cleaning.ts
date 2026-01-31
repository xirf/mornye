/* DATA CLEANING METHODS
/*-----------------------------------------------------
/* Cast types, fill nulls, drop nulls
/* ==================================================== */

import { and, coalesce, col, lit } from "../expr/builders.ts";
import type { DType } from "../types/dtypes.ts";
import { getColumnNames } from "../types/schema.ts";
import type { DataFrame } from "./core.ts";

export function addCleaningMethods(df: typeof DataFrame.prototype) {
	df.cast = function (column: string, targetDType: DType): DataFrame {
		return this.withColumn(column, col(column).cast(targetDType.kind));
	};

	df.fillNull = function (
		column: string,
		fillValue: number | bigint | string | boolean,
	): DataFrame {
		return this.withColumn(
			column,
			coalesce(col(column), lit(fillValue as number | string | boolean | null)),
		);
	};

	df.dropNull = function (columns?: string | string[]): DataFrame {
		const cols = columns
			? Array.isArray(columns)
				? columns
				: [columns]
			: getColumnNames(this.schema);

		// biome-ignore lint/style/noNonNullAssertion: Accessing verified existing columns
		let predicate: import("../expr/ast.ts").Expr = col(cols[0]!).isNotNull();
		for (let i = 1; i < cols.length; i++) {
			// biome-ignore lint/style/noNonNullAssertion: Accessing verified existing columns
			predicate = and(predicate, col(cols[i]!).isNotNull());
		}

		return this.filter(predicate);
	};
}
