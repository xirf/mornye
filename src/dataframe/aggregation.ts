/* AGGREGATION METHODS
/*-----------------------------------------------------
/* Group and aggregate column values
/* ==================================================== */

import { type AggSpec, aggregate, groupBy } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addAggMethods(df: typeof DataFrame.prototype) {
	df.agg = function (specs: AggSpec[]): DataFrame {
		const result = aggregate(this.currentSchema(), specs);
		if (result.error !== ErrorCode.None) {
			throw new Error(`Agg error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};

	df.groupBy = function (
		keyColumns: string | string[],
		aggSpecs: AggSpec[],
	): DataFrame {
		const keys = Array.isArray(keyColumns) ? keyColumns : [keyColumns];
		const result = groupBy(this.currentSchema(), keys, aggSpecs);
		if (result.error !== ErrorCode.None) {
			throw new Error(`GroupBy error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};
}
