/**
 * LRU Chunk Cache - Memory-efficient caching for parsed row data.
 *
 * Caches parsed row chunks with configurable memory budget.
 * Automatically evicts least-recently-used chunks when budget exceeded.
 *
 * Integrates with global memory tracker for server-side safety.
 */

import { MemoryLimitError } from '../../errors';
import {
  generateTaskId,
  getConfig,
  releaseAllocation,
  requestAllocation,
  updateUsage,
} from '../config';

/** Parsed data for a chunk (Columnar) */
export interface BinaryChunk {
  /** Starting row index of this chunk */
  startRow: number;
  /** Parsed columns (arrays of values) */
  columns: Vector[];
  /** Number of rows in this chunk */
  rowCount: number;
  /** Approximate memory size in bytes */
  sizeBytes: number;
}

export type Vector =
  | { kind: 'float64'; data: Float64Array }
  | { kind: 'int32'; data: Int32Array }
  | { kind: 'bool'; data: Uint8Array } // 0 or 1
  | {
      kind: 'string';
      data: Uint8Array;
      offsets: Uint32Array;
      lengths: Uint32Array;
      needsUnescape: Uint8Array;
    };

/** Cache configuration */
export interface CacheConfig {
  /** Maximum memory budget in bytes (default: 100MB) */
  maxMemoryBytes: number;
  /** Number of rows per chunk (default: 10,000) */
  chunkSize: number;
}

const DEFAULT_CONFIG: CacheConfig = {
  maxMemoryBytes: 100 * 1024 * 1024, // 100MB
  chunkSize: 10_000,
};

/**
 * LRU cache for parsed chunks (Columnar Storage).
 */
export class ChunkCache {
  private readonly _config: CacheConfig;
  private readonly _cache: Map<number, BinaryChunk>;
  private readonly _lruOrder: number[];
  private _memoryUsed: number;
  private readonly _taskId: string;
  private _isRegistered: boolean;

  constructor(config: Partial<CacheConfig> = {}) {
    this._config = { ...DEFAULT_CONFIG, ...config };
    this._cache = new Map();
    this._lruOrder = [];
    this._memoryUsed = 0;
    this._taskId = generateTaskId();
    this._isRegistered = false;

    // Register with global memory tracker
    this._registerWithTracker();
  }

  /**
   * Register this cache's memory budget with the global tracker.
   */
  private _registerWithTracker(): void {
    const globalConfig = getConfig();
    if (!globalConfig.enabled) return;

    const allocation = requestAllocation(this._taskId, this._config.maxMemoryBytes);
    this._isRegistered = allocation.success;

    // If allocation partially succeeded, adjust our local limit
    if (allocation.success && allocation.allocatedBytes < this._config.maxMemoryBytes) {
      this._config.maxMemoryBytes = allocation.allocatedBytes;
    }
  }

  /**
   * Get chunk size configuration.
   */
  get chunkSize(): number {
    return this._config.chunkSize;
  }

  /**
   * Get current memory usage in bytes.
   */
  get memoryUsed(): number {
    return this._memoryUsed;
  }

  /**
   * Get number of cached chunks.
   */
  get size(): number {
    return this._cache.size;
  }

  /**
   * Calculate chunk index for a given row.
   */
  getChunkIndex(rowIndex: number): number {
    return Math.floor(rowIndex / this._config.chunkSize);
  }

  /**
   * Get row range for a chunk index.
   * @returns [startRow, endRow) - endRow is exclusive
   */
  getChunkRange(chunkIndex: number): [number, number] {
    const start = chunkIndex * this._config.chunkSize;
    const end = start + this._config.chunkSize;
    return [start, end];
  }

  /**
   * Check if a chunk is cached.
   */
  has(chunkIndex: number): boolean {
    return this._cache.has(chunkIndex);
  }

  /**
   * Get a cached chunk, marking it as recently used.
   * Returns undefined if not cached.
   */
  get(chunkIndex: number): BinaryChunk | undefined {
    const chunk = this._cache.get(chunkIndex);
    if (chunk) {
      this._markUsed(chunkIndex);
    }
    return chunk;
  }

  /**
   * Store a chunk in cache, evicting old chunks if needed.
   */
  set(chunkIndex: number, chunk: BinaryChunk): void {
    // If already exists, remove old size from memory count
    const existing = this._cache.get(chunkIndex);
    if (existing) {
      this._memoryUsed -= existing.sizeBytes;
    }

    // Evict until we have room
    while (
      this._memoryUsed + chunk.sizeBytes > this._config.maxMemoryBytes &&
      this._lruOrder.length > 0
    ) {
      this._evictLru();
    }

    // Store chunk
    this._cache.set(chunkIndex, chunk);
    this._memoryUsed += chunk.sizeBytes;
    this._markUsed(chunkIndex);

    // Report usage to global tracker
    if (this._isRegistered) {
      updateUsage(this._taskId, this._memoryUsed);
    }
  }

  /**
   * Clear all cached chunks.
   */
  clear(): void {
    this._cache.clear();
    this._lruOrder.length = 0;
    this._memoryUsed = 0;

    // Update global tracker
    if (this._isRegistered) {
      updateUsage(this._taskId, 0);
    }
  }

  /**
   * Destroy this cache and release global memory allocation.
   *
   * Call this when the cache is no longer needed to free up
   * memory budget for other operations.
   */
  destroy(): void {
    this.clear();

    // Release allocation from global tracker
    if (this._isRegistered) {
      releaseAllocation(this._taskId);
      this._isRegistered = false;
    }
  }

  /**
   * Get columns data for a range of rows.
   * Note: This might be inefficient if crossing many chunks.
   * Ideally iterate via chunks directly.
   */
  getColumns(startRow: number, count: number, columnNames: string[]): Record<string, unknown[]> {
    throw new Error('Use getChunk and iterate chunks directly for performance.');
  }

  /**
   * Mark a chunk as recently used (move to end of LRU list).
   */
  private _markUsed(chunkIndex: number): void {
    const idx = this._lruOrder.indexOf(chunkIndex);
    if (idx !== -1) {
      this._lruOrder.splice(idx, 1);
    }
    this._lruOrder.push(chunkIndex);
  }

  /**
   * Evict the least recently used chunk.
   */
  private _evictLru(): void {
    const oldest = this._lruOrder.shift();
    if (oldest !== undefined) {
      const chunk = this._cache.get(oldest);
      if (chunk) {
        this._memoryUsed -= chunk.sizeBytes;
        this._cache.delete(oldest);
      }
    }
  }

  /**
   * Estimate memory size of columnar data.
   */
  static estimateSize(columns: Vector[], rowCount: number): number {
    if (rowCount === 0) return 0;
    let totalSize = 0;

    for (const vector of columns) {
      totalSize += vector.data.byteLength;
      if (vector.kind === 'string') {
        totalSize += vector.offsets.byteLength;
      }
    }
    return totalSize;
  }

  /**
   * Check if a proposed allocation would fit within the budget.
   * Returns success result or error if denied.
   */
  checkAllocation(requestedBytes: number): { success: boolean; error?: MemoryLimitError } {
    if (!this._isRegistered) return { success: true };

    const result = requestAllocation(this._taskId, requestedBytes);
    if (!result.success && result.error) {
      return {
        success: false,
        error: new MemoryLimitError(
          result.error.requestedBytes,
          result.error.availableBytes,
          result.error.globalLimitBytes,
        ),
      };
    }
    return { success: true };
  }
}
