/**
 * RLE/Bit-Packed Hybrid Encoding
 * Ported from hyparquet's encoding.js with TypeScript types
 */

export interface DataReader {
	view: DataView;
	offset: number;
}

/**
 * Read a variable-length integer (ULEB128).
 */
export function readVarInt(reader: DataReader): number {
	let result = 0;
	let shift = 0;
	while (true) {
		const byte = reader.view.getUint8(reader.offset++);
		result |= (byte & 0x7f) << shift;
		if (!(byte & 0x80)) return result;
		shift += 7;
	}
}

/**
 * Compute minimum bits needed to store a value.
 */
export function bitWidth(value: number): number {
	return 32 - Math.clz32(value);
}

/**
 * Read values from a run-length encoded/bit-packed hybrid encoding.
 *
 * If length is undefined, reads uint32 length at the start.
 * Output array length determines how many values to decode.
 */
export function readRleBitPackedHybrid(
	reader: DataReader,
	width: number,
	output: number[] | Uint32Array,
	length?: number,
): void {
	if (length === undefined) {
		length = reader.view.getUint32(reader.offset, true);
		reader.offset += 4;
	}
	const startOffset = reader.offset;
	let seen = 0;

	while (seen < output.length) {
		const header = readVarInt(reader);
		if (header & 1) {
			// bit-packed
			seen = readBitPacked(reader, header, width, output, seen);
		} else {
			// rle
			const count = header >>> 1;
			readRle(reader, count, width, output, seen);
			seen += count;
		}
	}
	// duckdb writes an empty block at the end - skip to expected position
	reader.offset = startOffset + length;
}

/**
 * Run-length encoding: read value with bitWidth and repeat it count times.
 */
function readRle(
	reader: DataReader,
	count: number,
	bitWidth: number,
	output: number[] | Uint32Array,
	seen: number,
): void {
	const width = (bitWidth + 7) >> 3; // bytes needed
	let value = 0;
	for (let i = 0; i < width; i++) {
		value |= reader.view.getUint8(reader.offset++) << (i << 3);
	}

	// repeat value count times
	for (let i = 0; i < count; i++) {
		output[seen + i] = value;
	}
}

/**
 * Read a bit-packed run of the rle/bitpack hybrid.
 * Supports width > 8 (crossing bytes).
 */
function readBitPacked(
	reader: DataReader,
	header: number,
	bitWidth: number,
	output: number[] | Uint32Array,
	seen: number,
): number {
	let count = (header >> 1) << 3; // values to read (groups of 8)
	const mask = (1 << bitWidth) - 1;

	let data = 0;
	if (reader.offset < reader.view.byteLength) {
		data = reader.view.getUint8(reader.offset++);
	} else if (mask) {
		throw new Error(`parquet bitpack offset ${reader.offset} out of range`);
	}
	let left = 8;
	let right = 0;

	while (count > 0) {
		// if we have crossed a byte boundary, shift the data
		if (right > 8) {
			right -= 8;
			left -= 8;
			data >>>= 8;
		} else if (left - right < bitWidth) {
			// if we don't have bitWidth bits to read, read next byte
			data |= reader.view.getUint8(reader.offset) << left;
			reader.offset++;
			left += 8;
		} else {
			if (seen < output.length) {
				// emit value
				output[seen++] = (data >> right) & mask;
			}
			count--;
			right += bitWidth;
		}
	}

	return seen;
}
