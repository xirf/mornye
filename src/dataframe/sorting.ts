/* SORTING METHODS
/*-----------------------------------------------------
/* Order rows by column values
/* ==================================================== */

import { type SortKey, sort } from "../ops/index.ts";
import { unwrap } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addSortingMethods(df: typeof DataFrame.prototype) {
	df.sort = function (keys: string | string[] | SortKey[]): DataFrame {
		const keyArray = Array.isArray(keys) ? keys : [keys];
		const result = sort(this.currentSchema(), ...(keyArray as SortKey[]));
		return this.withOperator(unwrap(result));
	};

	df.orderBy = function (keys: string | string[] | SortKey[]): DataFrame {
		return this.sort(keys);
	};
}
