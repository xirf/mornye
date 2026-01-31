/* PROJECTION METHODS
/*-----------------------------------------------------
/* Select, drop, and rename column operations
/* ==================================================== */

import { type ProjectSpec, project, projectWithRename } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addProjectionMethods(df: typeof DataFrame.prototype) {
	df.select = function (...columns: string[]): DataFrame {
		const result = project(this.currentSchema(), columns);
		if (result.error !== ErrorCode.None) {
			throw new Error(`Select error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};

	df.drop = function (...columns: string[]): DataFrame {
		const remaining = this.columnNames.filter((n) => !columns.includes(n));
		return this.select(...remaining);
	};

	df.rename = function (mapping: Record<string, string>): DataFrame {
		const specs: ProjectSpec[] = this.columnNames.map((name) => ({
			source: name,
			target: mapping[name] ?? name,
		}));
		const result = projectWithRename(this.currentSchema(), specs);
		if (result.error !== ErrorCode.None) {
			throw new Error(`Rename error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};
}
