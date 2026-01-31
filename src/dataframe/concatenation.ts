/* CONCATENATION METHODS
/*-----------------------------------------------------
/* Vertically combine DataFrames
/* ==================================================== */

import { concatChunks } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addConcatMethods(df: typeof DataFrame.prototype) {
	df.concat = async function (other: DataFrame): Promise<DataFrame> {
		const collected = await this.collect();
		const collectedOther = await other.collect();

		const allChunks = [
			...(collected.source as import("../buffer/chunk.ts").Chunk[]),
			...(collectedOther.source as import("../buffer/chunk.ts").Chunk[]),
		];

		if (allChunks.length === 0) {
			return collected;
		}

		const result = concatChunks(allChunks, collected._schema);
		if (result.error !== ErrorCode.None) {
			throw new Error(`Concat error: ${result.error}`);
		}

		return (this.constructor as typeof DataFrame).fromChunks(
			[result.value],
			collected._schema,
			collected._dictionary,
		);
	};
}
