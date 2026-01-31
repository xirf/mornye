/**
 * Buffer pool for reusing ColumnBuffers to reduce GC pressure.
 */

import type { DTypeKind } from "../types/dtypes.ts";
import type { Chunk } from "./chunk.ts";
import { ColumnBuffer } from "./column-buffer.ts";

export class ColumnBufferPool {
	private static instance: ColumnBufferPool;

	// Map<DTypeKind, Array<AvailableBuffers>>
	// We bucket by capacity? Or just keep "at least capacity"?
	// For simplicity, we assume fixed chunk size mostly.
	// If request capacity > buffer capacity, we discard and create new?
	// Or we just bucket by exact capacity or power of 2?
	// Let's assume standard chunk size (e.g. 8192) for now.
	// Map<Key, ColumnBuffer[]>
	// Key = kind + "_" + capacity + "_" + nullable
	private pools = new Map<string, ColumnBuffer[]>();

	private constructor() {}

	static getInstance(): ColumnBufferPool {
		if (!ColumnBufferPool.instance) {
			ColumnBufferPool.instance = new ColumnBufferPool();
		}
		return ColumnBufferPool.instance;
	}

	acquire(kind: DTypeKind, capacity: number, nullable: boolean): ColumnBuffer {
		const key = this.getKey(kind, capacity, nullable);
		const pool = this.pools.get(key);

		if (pool && pool.length > 0) {
			return pool.pop() as ColumnBuffer;
		}

		return new ColumnBuffer(kind, capacity, nullable);
	}

	release(buffer: ColumnBuffer): void {
		// Reset buffer before storing
		buffer.clear();

		const key = this.getKey(buffer.kind, buffer.capacity, buffer.isNullable);
		let pool = this.pools.get(key);
		if (!pool) {
			pool = [];
			this.pools.set(key, pool);
		}

		// Limit pool size?
		if (pool.length < 50) {
			// Arbitrary limit per type
			pool.push(buffer);
		}
	}

	private getKey(kind: DTypeKind, capacity: number, nullable: boolean): string {
		return `${kind}_${capacity}_${nullable}`;
	}
}

export const bufferPool = ColumnBufferPool.getInstance();

export function recycleChunk(chunk: Chunk): void {
	const cols = chunk.dispose();
	for (const col of cols) {
		bufferPool.release(col);
	}
}
