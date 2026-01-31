/**
 * Vectorized Aggregation Primitives.
 *
 * Provides batch processing for aggregations, operating on arrays of values
 * and group IDs instead of single value/state pairs.
 */
/** biome-ignore-all lint/style/noNonNullAssertion: Performance critical inner loops */

import { ColumnBuffer, type TypedArray } from "../buffer/column-buffer.ts";
import { type DType, DType as DTypeFactory } from "../types/dtypes.ts";
import { AggType } from "./agg-state.ts";

/**
 * Interface for vectorized aggregation.
 */
export interface BatchAggregator {
	/** Resize state storage for N groups */
	resize(numGroups: number): void;

	/**
	 * Accumulate a batch of values.
	 * @param data Raw columnar data (TypedArray) or null for CountAll
	 * @param groupIds Array of group IDs corresponding to data indices
	 * @param count Number of rows to process
	 * @param selection Optional selection vector (row indices)
	 * @param nullBitmap Optional null bitmap from the column
	 */
	accumulateBatch(
		_data: TypedArray | null,
		groupIds: Int32Array,
		count: number,
		_selection: Uint32Array | null,
		_column: ColumnBuffer | null,
	): void;

	/** Finalize and return results as a column buffer */
	finish(): ColumnBuffer;

	/** Get output dtype */
	readonly outputDType: DType;
}

/** Base class for aggregators managing a result buffer */
abstract class BaseVectorAggregator implements BatchAggregator {
	protected values: Float64Array | BigInt64Array; // Store sums/counts/etc
	protected hasValue: Uint8Array; // Bitmask or byte-array for null tracking? Byte for speed.
	protected size: number = 0;

	abstract readonly outputDType: DType;

	constructor() {
		// Start with small capacity
		this.values = new Float64Array(0);
		this.hasValue = new Uint8Array(0);
	}

	resize(numGroups: number): void {
		if (numGroups > this.values.length) {
			const newSize = Math.max(numGroups, this.values.length * 2);
			this.grow(newSize);
		}

		// Initialize new slots if necessary (depends on agg type)
		// For Sum/Count 0 is fine. For Min/Max we need initialization.
		this.initialize(this.size, numGroups);
		this.size = numGroups;
	}

	protected abstract grow(newSize: number): void;
	protected abstract initialize(start: number, end: number): void;

	abstract accumulateBatch(
		_data: TypedArray | null,
		groupIds: Int32Array,
		count: number,
		_selection: Uint32Array | null,
		_column: ColumnBuffer | null,
	): void;

	finish(): ColumnBuffer {
		const col = new ColumnBuffer(
			this.outputDType.kind,
			this.size,
			this.outputDType.nullable,
		);
		// Copy data
		const count = this.size;

		// We can iterate and set.
		// Optimization: Batch copy if possible, but null handling requires loop for now.
		for (let i = 0; i < count; i++) {
			if (this.hasValue[i]) {
				// @ts-expect-error - copying typed array elements
				col.set(i, this.values[i]);
			} else {
				col.setNull(i, true);
			}
		}
		// Hack: manually set length
		(col as unknown as { _length: number })._length = count;
		return col;
	}
}

/** Vector Sum */
export class VectorSum extends BaseVectorAggregator {
	protected declare values: Float64Array; // Override type
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(_start: number, _end: number) {
		// Float64Array initializes to 0, which is correct for Sum
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		const hasVal = this.hasValue;

		// Tight inner loop selection
		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				vals[gid]! += Number(data[row]);
				hasVal[gid] = 1;
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				vals[gid]! += Number(data[i]);
				hasVal[gid] = 1;
			}
		}
	}
}

/** Vector Count (Non-null) */
export class VectorCount extends BaseVectorAggregator {
	protected declare values: BigInt64Array;
	readonly outputDType = DTypeFactory.int64;

	constructor() {
		super();
		this.values = new BigInt64Array(1024);
		this.hasValue = new Uint8Array(1024); // Count is never null usually, but we keep structure
	}

	override resize(numGroups: number) {
		super.resize(numGroups);
		// Ensure "hasValue" is all set for counts (0 is a valid count)
		// Actually per spec, Count returns 0 if empty? or Null?
		// SQL says Count is 0 if no rows.
		// But standard AggState implementation returns 0n.
		for (let i = 0; i < numGroups; i++) this.hasValue[i] = 1;
	}

	protected grow(newSize: number) {
		const newValues = new BigInt64Array(newSize);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(_start: number, _end: number) {
		// BigInt64 initializes to 0n
	}

	accumulateBatch(
		_data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				vals[gid]!++;
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				vals[gid]!++;
			}
		}
	}

	// Override finish because Count should not return nulls
	override finish(): ColumnBuffer {
		const col = new ColumnBuffer(this.outputDType.kind, this.size, false);
		col.data.set(this.values.subarray(0, this.size));
		(col as unknown as { _length: number })._length = this.size;
		return col;
	}
}

/** Vector Count All (Star) */
export class VectorCountAll extends VectorCount {
	override accumulateBatch(
		_data: TypedArray | null,
		groupIds: Int32Array,
		count: number,
		_selection: Uint32Array | null,
		_column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		// Count ALL rows, ignoring nulls in data
		// selection still matters (we only count selected rows)
		// But we just iterate 0..count because groupIds is already aligned with selection logic in GroupBy

		for (let i = 0; i < count; i++) {
			const gid = groupIds[i]!;
			vals[gid]!++;
		}
	}
}

/** Vector Min */
export class VectorMin extends BaseVectorAggregator {
	protected declare values: Float64Array;
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024).fill(Infinity);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize).fill(Infinity);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(start: number, end: number) {
		this.values.fill(Infinity, start, end);
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		const hasVal = this.hasValue;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				const v = Number(data[row]);
				if (v < vals[gid]!) {
					vals[gid] = v;
					hasVal[gid] = 1;
				}
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				const v = Number(data[i]);
				if (v < vals[gid]!) {
					vals[gid] = v;
					hasVal[gid] = 1;
				}
			}
		}
	}
}

/** Vector Max */
export class VectorMax extends BaseVectorAggregator {
	protected declare values: Float64Array;
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024).fill(-Infinity);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize).fill(-Infinity);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(start: number, end: number) {
		this.values.fill(-Infinity, start, end);
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		const hasVal = this.hasValue;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				const v = Number(data[row]);
				if (v > vals[gid]!) {
					vals[gid] = v;
					hasVal[gid] = 1;
				}
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				const v = Number(data[i]);
				if (v > vals[gid]!) {
					vals[gid] = v;
					hasVal[gid] = 1;
				}
			}
		}
	}
}

/** Factory */
export function createVectorAggregator(aggType: AggType): BatchAggregator {
	switch (aggType) {
		case AggType.Sum:
			return new VectorSum();
		case AggType.Count:
			return new VectorCount();
		case AggType.CountAll:
			return new VectorCountAll();
		case AggType.Min:
			return new VectorMin();
		case AggType.Max:
			return new VectorMax();
		default:
			throw new Error(`Vector aggregation not implemented for type ${aggType}`);
	}
}
