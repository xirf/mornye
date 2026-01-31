/* JOIN METHODS
/*-----------------------------------------------------
/* Combine DataFrames by key columns
/* ==================================================== */

import { innerJoin, leftJoin } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addJoinMethods(df: typeof DataFrame.prototype) {
	df.innerJoin = async function (
		// biome-ignore lint/suspicious/noExplicitAny: compatibility with generic interface
		other: DataFrame<any>,
		leftOn: string,
		rightOn?: string,
	): Promise<DataFrame> {
		const collectedLeft = await this.collect();
		const collectedRight = await other.collect();
		const actualRightOn = rightOn ?? leftOn;

		const result = innerJoin(
			collectedLeft.source as import("../buffer/chunk.ts").Chunk[],
			collectedLeft._schema,
			collectedRight.source as import("../buffer/chunk.ts").Chunk[],
			collectedRight._schema,
			leftOn,
			actualRightOn,
		);

		if (result.error !== ErrorCode.None) {
			throw new Error(`InnerJoin error: ${result.error}`);
		}

		return (this.constructor as typeof DataFrame).fromChunks(
			result.value.chunks,
			result.value.schema,
			collectedLeft._dictionary,
		);
	};

	df.leftJoin = async function (
		// biome-ignore lint/suspicious/noExplicitAny: compatibility with generic interface
		other: DataFrame<any>,
		leftOn: string,
		rightOn?: string,
	): Promise<DataFrame> {
		const collectedLeft = await this.collect();
		const collectedRight = await other.collect();
		const actualRightOn = rightOn ?? leftOn;

		const result = leftJoin(
			collectedLeft.source as import("../buffer/chunk.ts").Chunk[],
			collectedLeft._schema,
			collectedRight.source as import("../buffer/chunk.ts").Chunk[],
			collectedRight._schema,
			leftOn,
			actualRightOn,
		);

		if (result.error !== ErrorCode.None) {
			throw new Error(`LeftJoin error: ${result.error}`);
		}

		return (this.constructor as typeof DataFrame).fromChunks(
			result.value.chunks,
			result.value.schema,
			collectedLeft._dictionary,
		);
	};

	df.join = function (
		// biome-ignore lint/suspicious/noExplicitAny: compatibility with generic interface
		other: DataFrame<any>,
		leftOn: string,
		rightOn?: string,
		how: "inner" | "left" = "inner",
	): Promise<DataFrame> {
		const actualRightOn = (rightOn ?? leftOn) as string;
		return how === "inner"
			? this.innerJoin(other, leftOn, actualRightOn)
			: this.leftJoin(other, leftOn, actualRightOn);
	};
}
