/**
 * Compiled expression types.
 *
 * These types are shared between compiler and apply modules.
 */

import type { Chunk } from "../buffer/chunk.ts";

/**
 * Compiled predicate function.
 * Returns true if the row at the given index matches the predicate.
 */
export type CompiledPredicate = (chunk: Chunk, rowIndex: number) => boolean;

/**
 * Compiled value function.
 * Returns the computed value for the row at the given index.
 */
export type CompiledValue = (
	chunk: Chunk,
	rowIndex: number,
) => number | bigint | boolean | string | null;
