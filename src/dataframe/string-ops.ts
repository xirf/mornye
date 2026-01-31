/* STRING OPERATIONS
/*-----------------------------------------------------
/* Transform string column values
/* ==================================================== */

import { replaceColumn, trimColumn } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addStringMethods(df: typeof DataFrame.prototype) {
	df.trim = function (column: string): DataFrame {
		const collected = this.collect();
		const colIdx = collected._schema.columnMap.get(column);
		if (colIdx === undefined) {
			throw new Error(`Trim error: Column '${column}' not found`);
		}

		let newDictionary = collected._dictionary;
		for (const chunk of collected.chunks) {
			const col = chunk.getColumn(colIdx);
			if (!col) continue;

			const result = trimColumn(col, newDictionary);
			if (result.error !== ErrorCode.None) {
				throw new Error(`Trim error: ${result.error}`);
			}

			newDictionary = result.value;
		}

		return (this.constructor as typeof DataFrame).fromChunks(
			collected.chunks,
			collected._schema,
			newDictionary,
		);
	};

	df.replace = function (
		column: string,
		pattern: string,
		replacement: string,
		all: boolean = true,
	): DataFrame {
		const collected = this.collect();
		const colIdx = collected._schema.columnMap.get(column);
		if (colIdx === undefined) {
			throw new Error(`Replace error: Column '${column}' not found`);
		}

		let newDictionary = collected._dictionary;
		for (const chunk of collected.chunks) {
			const col = chunk.getColumn(colIdx);
			if (!col) continue;

			const result = replaceColumn(
				col,
				newDictionary,
				pattern,
				replacement,
				all,
			);
			if (result.error !== ErrorCode.None) {
				throw new Error(`Replace error: ${result.error}`);
			}

			newDictionary = result.value;
		}

		return (this.constructor as typeof DataFrame).fromChunks(
			collected.chunks,
			collected._schema,
			newDictionary,
		);
	};
}
