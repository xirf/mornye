/* DEDUPLICATION METHODS
/*-----------------------------------------------------
/* Remove duplicate rows from DataFrame
/* ==================================================== */

import type { Chunk } from "../buffer/chunk.ts";
import { uniqueSelection } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import { getColumnNames } from "../types/schema.ts";
import type { DataFrame } from "./core.ts";

export function addDedupMethods(df: typeof DataFrame.prototype) {
	df.unique = function (
		columns?: string | string[],
		keepFirst: boolean = true,
	): DataFrame {
		const collected = this.collect();

		const colNames = columns
			? Array.isArray(columns)
				? columns
				: [columns]
			: getColumnNames(collected._schema);

		const colIndices = colNames.map((name) => {
			const idx = collected._schema.columnMap.get(name);
			if (idx === undefined) {
				throw new Error(`Unique error: Column '${name}' not found`);
			}
			return idx;
		});

		const newChunks: Chunk[] = [];
		for (const chunk of collected.chunks) {
			const result = uniqueSelection(chunk, colIndices, keepFirst);
			if (result.error !== ErrorCode.None) {
				throw new Error(`Unique error: ${result.error}`);
			}

			chunk.applySelection(result.value.selection, result.value.count);
			newChunks.push(chunk);
		}

		return (this.constructor as typeof DataFrame).fromChunks(
			newChunks,
			collected._schema,
			collected._dictionary,
		);
	};

	df.dropDuplicates = function (
		columns?: string | string[],
		keepFirst: boolean = true,
	): DataFrame {
		return this.unique(columns, keepFirst);
	};

	df.distinct = function (columns?: string | string[]): DataFrame {
		return this.unique(columns, true);
	};
}
