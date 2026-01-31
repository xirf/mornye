/* LIMITING METHODS
/*-----------------------------------------------------
/* Limit and slice row counts
/* ==================================================== */

import { limit } from "../ops/index.ts";
import type { DataFrame } from "./core.ts";

export function addLimitingMethods(df: typeof DataFrame.prototype) {
	df.limit = function (count: number): DataFrame {
		return this.withOperator(limit(this.currentSchema(), count));
	};

	df.head = function (count: number = 5): DataFrame {
		return this.limit(count);
	};

	df.slice = function (start: number, count: number): DataFrame {
		return this.withOperator(limit(this.currentSchema(), count, start));
	};
}
