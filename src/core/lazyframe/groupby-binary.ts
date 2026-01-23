import { DataFrame } from '../dataframe';
import { Series } from '../series';
import type { Schema } from '../types';
import type { BinaryChunk } from './chunk-cache';

/**
 * Supported aggregation functions.
 */
export type AggFunc = 'sum' | 'count' | 'mean' | 'min' | 'max';

export interface AggDef {
  col: string;
  func: AggFunc;
  outName: string;
}

/**
 * Binary GroupBy Implementation.
 * Uses a custom Linear Probing Hash Table backed by TypedArrays.
 */
export class BinaryGroupBy {
  private readonly keys: string[];
  private readonly aggs: AggDef[];
  private readonly schema: Schema;

  // Hash Table
  private capacity: number;
  private mask: number;
  private count = 0;
  private hashes: Int32Array; // Stored hash
  private keyIndices: Int32Array; // Maps slot -> entry index

  // Storage for Keys (Flattened bytes)
  private keyStore: Uint8Array;
  private keyStorePos = 0;
  private keyEntryOffsets: Int32Array; // Entry Index -> Start in keyStore
  private keyEntryLengths: Int32Array; // Entry Index -> Length
  private keyView: DataView;

  // Aggregation State (Struct of Arrays)
  private aggValues: Record<string, Float64Array>;
  private aggCounts: Int32Array | null = null;

  constructor(keys: string[], aggs: AggDef[], schema: Schema, initialCapacity = 65536) {
    this.keys = keys;
    this.aggs = aggs;
    this.schema = schema;
    this.capacity = nextPowerOfTwo(initialCapacity);
    this.mask = this.capacity - 1;

    this.hashes = new Int32Array(this.capacity).fill(-1);
    this.keyIndices = new Int32Array(this.capacity).fill(-1);

    // Initial guess: 32 bytes per key entry
    this.keyStore = new Uint8Array(this.capacity * 32);
    this.keyView = new DataView(this.keyStore.buffer);
    this.keyEntryOffsets = new Int32Array(this.capacity);
    this.keyEntryLengths = new Int32Array(this.capacity);

    this.aggValues = {};
    for (const agg of aggs) {
      this.aggValues[agg.outName] = new Float64Array(this.capacity);
      if (agg.func === 'mean') {
        if (!this.aggCounts) this.aggCounts = new Int32Array(this.capacity);
      }
    }
  }

  processChunk(chunk: BinaryChunk, columnOrder: string[]) {
    const rowCount = chunk.rowCount;
    if (rowCount === 0) return;

    const keyIndices = this.keys.map((k) => columnOrder.indexOf(k));
    const aggIndices = this.aggs.map((a) => columnOrder.indexOf(a.col));

    for (let i = 0; i < rowCount; i++) {
      // 1. Hash
      let h = 2166136261;

      for (const kidx of keyIndices) {
        const vector = chunk.columns[kidx]!;
        if (vector.kind === 'string') {
          const start = vector.offsets[i]!;
          const len = vector.lengths[i]!;
          for (let j = 0; j < len; j++) {
            h ^= vector.data[start + j]!;
            h = Math.imul(h, 16777619);
          }
        } else {
          // Number/Bool -> Hash the value
          const val = vector.data[i]!;
          h ^= Math.floor(val) & 0xffffffff; // Simple int hash
          h = Math.imul(h, 16777619);
        }
        h ^= 0xff; // Separator
        h = Math.imul(h, 16777619);
      }

      // 2. Probe
      let slot = h & this.mask;
      let entryIdx = -1;

      while (true) {
        const existingHash = this.hashes[slot]!;
        if (existingHash === -1) {
          // Empty -> Insert
          entryIdx = this._insertKey(slot, h, chunk, i, keyIndices);
          break;
        } else if (existingHash === h) {
          // Match -> Verify Check
          const existingIdx = this.keyIndices[slot]!;
          if (this._keysEqual(existingIdx, chunk, i, keyIndices)) {
            entryIdx = existingIdx;
            break;
          }
        }
        slot = (slot + 1) & this.mask;
      }

      // 3. Aggregate
      this._updateAggs(entryIdx, chunk, i, aggIndices);
    }

    if (this.count > this.capacity * 0.7) {
      this._resize();
    }
  }

  private _insertKey(
    slot: number,
    hash: number,
    chunk: BinaryChunk,
    rowIdx: number,
    keyIndices: number[],
  ): number {
    const entryIdx = this.count++;
    this.hashes[slot] = hash;
    this.keyIndices[slot] = entryIdx;

    const startPos = this.keyStorePos;

    for (const kidx of keyIndices) {
      const vector = chunk.columns[kidx]!;
      if (vector.kind === 'string') {
        const len = vector.lengths[rowIdx]!;
        const off = vector.offsets[rowIdx]!;
        this._ensureKeyAlloc(len + 1);
        // Copy bytes
        for (let j = 0; j < len; j++) {
          this.keyStore[this.keyStorePos++] = vector.data[off + j]!;
        }
        // Null separator for string
        this.keyStore[this.keyStorePos++] = 0;
      } else {
        // Number -> 8 bytes
        this._ensureKeyAlloc(8);
        const val = vector.data[rowIdx]!;
        // Write using DataView (little endian)
        this.keyView.setFloat64(this.keyStorePos, Number(val), true);
        this.keyStorePos += 8;
      }
    }

    this.keyEntryOffsets[entryIdx] = startPos;
    this.keyEntryLengths[entryIdx] = this.keyStorePos - startPos;

    // Init aggs
    for (const agg of this.aggs) {
      const name = agg.outName;
      if (agg.func === 'min') this.aggValues[name]![entryIdx] = Number.POSITIVE_INFINITY;
      if (agg.func === 'max') this.aggValues[name]![entryIdx] = Number.NEGATIVE_INFINITY;
    }

    return entryIdx;
  }

  private _keysEqual(
    entryIdx: number,
    chunk: BinaryChunk,
    rowIdx: number,
    keyIndices: number[],
  ): boolean {
    const start = this.keyEntryOffsets[entryIdx]!;
    let storePos = start;

    for (const kidx of keyIndices) {
      const vector = chunk.columns[kidx]!;
      if (vector.kind === 'string') {
        const len = vector.lengths[rowIdx]!;
        const off = vector.offsets[rowIdx]!;
        // Compare
        for (let j = 0; j < len; j++) {
          if (this.keyStore[storePos++] !== vector.data[off + j]) return false;
        }
        if (this.keyStore[storePos++] !== 0) return false;
      } else {
        // Compare number (read 8 bytes)
        const storeVal = this.keyView.getFloat64(storePos, true);
        storePos += 8;
        if (storeVal !== Number(vector.data[rowIdx])) return false;
      }
    }
    return true;
  }

  private _ensureKeyAlloc(size: number) {
    if (this.keyStorePos + size > this.keyStore.length) {
      const newSize = this.keyStore.length * 2;
      const newStore = new Uint8Array(newSize);
      newStore.set(this.keyStore);
      this.keyStore = newStore;
      this.keyView = new DataView(this.keyStore.buffer);
    }
  }

  private _updateAggs(entryIdx: number, chunk: BinaryChunk, rowIdx: number, aggIndices: number[]) {
    for (let i = 0; i < this.aggs.length; i++) {
      const agg = this.aggs[i]!;
      const colIdx = aggIndices[i]!;
      // Simple aggregate logic: assume number
      const vec = chunk.columns[colIdx]!;
      const val = Number(vec.data[rowIdx]); // works for Int32, Float64, Bool(Uint8)

      const vals = this.aggValues[agg.outName]!;

      switch (agg.func) {
        case 'count':
          vals[entryIdx]++;
          break;
        case 'sum':
          vals[entryIdx] += val;
          break;
        case 'min':
          if (val < vals[entryIdx]!) vals[entryIdx] = val;
          break;
        case 'max':
          if (val > vals[entryIdx]!) vals[entryIdx] = val;
          break;
        case 'mean':
          vals[entryIdx] += val;
          if (this.aggCounts) this.aggCounts[entryIdx]++;
          break;
      }
    }
  }

  private _resize() {
    // Not implemented for RSS target stability.
    console.warn('BinaryGroupBy: automatic resize not fully implemented, capacity limit reached');
  }

  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  toDataFrame(): DataFrame<any> {
    // 1. Recover Keys
    const keyCols: Record<string, unknown[]> = {};
    for (const k of this.keys) {
      // Prepare arrays
      const dtype = this.schema[k];
      if (dtype!.kind === 'string') keyCols[k] = new Array(this.count);
      else keyCols[k] = new Float64Array(this.count);
    }

    // Iterate all entries and decode
    for (let i = 0; i < this.count; i++) {
      const start = this.keyEntryOffsets[i]!;
      let storePos = start;
      for (const k of this.keys) {
        const dtype = this.schema[k];
        if (dtype!.kind === 'string') {
          // read until 0
          let end = storePos;
          while (this.keyStore[end] !== 0) end++;
          // decode
          const strBytes = this.keyStore.subarray(storePos, end);
          keyCols[k]![i] = new TextDecoder().decode(strBytes);
          storePos = end + 1;
        } else {
          const val = this.keyView.getFloat64(storePos, true);
          storePos += 8;
          keyCols[k]![i] = val; // Works for int/float/bool approx
        }
      }
    }

    // 2. Prepare Aggs
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    const outColumns = new Map<string, any>();
    const outOrder: string[] = [...this.keys];

    // Add keys to map
    for (const k of this.keys) {
      const dtype = this.schema[k]!;
      const colData = keyCols[k]!;
      if (dtype.kind === 'string') {
        outColumns.set(k, Series.string(colData as string[]));
      } else if (dtype.kind === 'int32') {
        outColumns.set(k, Series.int32(colData as ArrayLike<number>));
      } else {
        outColumns.set(k, Series.float64(colData as ArrayLike<number>));
      }
    }

    for (const k of this.keys) {
      const dtype = this.schema[k]!;
      const colData = keyCols[k]!;
      if (dtype.kind === 'string') {
        outColumns.set(k, Series.string(colData as string[]));
      } else if (dtype.kind === 'int32') {
        outColumns.set(k, Series.int32(new Int32Array(colData as unknown as Float64Array)));
      } else {
        outColumns.set(k, Series.float64(colData as unknown as Float64Array));
      }
    }

    for (const agg of this.aggs) {
      const vals = this.aggValues[agg.outName]!;
      // Slice to count
      const finalVals = vals.slice(0, this.count);

      if (agg.func === 'mean') {
        const counts = this.aggCounts!;
        for (let j = 0; j < this.count; j++) {
          finalVals[j] = finalVals[j] / counts[j]!;
        }
      }

      outColumns.set(agg.outName, Series.float64(finalVals));
      outOrder.push(agg.outName);
    }

    // Construct Schema for result
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    const resultSchema: any = {};
    for (const k of this.keys) resultSchema[k] = this.schema[k];
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    for (const agg of this.aggs)
      resultSchema[agg.outName] = { kind: 'float64', params: null } as any;

    return DataFrame._fromColumns(resultSchema, outColumns, outOrder, this.count);
  }
}

function nextPowerOfTwo(v: number): number {
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  return v + 1;
}
