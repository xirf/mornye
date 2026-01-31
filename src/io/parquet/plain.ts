/**
 * Plain encoding value readers
 * Ported from hyparquet's plain.js with TypeScript types
 */

import type { DataReader } from "./encoding/rle.ts";

/**
 * Read `count` values of the given type from the reader.
 */
export function readPlain(
	reader: DataReader,
	type: string,
	count: number,
	fixedLength?: number,
):
	| boolean[]
	| Int32Array
	| BigInt64Array
	| bigint[]
	| Float32Array
	| Float64Array
	| Uint8Array[] {
	if (count === 0) return [];

	switch (type) {
		case "BOOLEAN":
			return readPlainBoolean(reader, count);
		case "INT32":
			return readPlainInt32(reader, count);
		case "INT64":
			return readPlainInt64(reader, count);
		case "INT96":
			return readPlainInt96(reader, count);
		case "FLOAT":
			return readPlainFloat(reader, count);
		case "DOUBLE":
			return readPlainDouble(reader, count);
		case "BYTE_ARRAY":
			return readPlainByteArray(reader, count);
		case "FIXED_LEN_BYTE_ARRAY":
			if (!fixedLength) throw new Error("parquet missing fixed length");
			return readPlainByteArrayFixed(reader, count, fixedLength);
		default:
			throw new Error(`parquet unhandled type: ${type}`);
	}
}

function readPlainBoolean(reader: DataReader, count: number): boolean[] {
	const values = new Array(count);
	for (let i = 0; i < count; i++) {
		const byteOffset = reader.offset + ((i / 8) | 0);
		const bitOffset = i % 8;
		const byte = reader.view.getUint8(byteOffset);
		values[i] = (byte & (1 << bitOffset)) !== 0;
	}
	reader.offset += Math.ceil(count / 8);
	return values;
}

function readPlainInt32(reader: DataReader, count: number): Int32Array {
	const values = isAligned(reader, 4)
		? new Int32Array(
				reader.view.buffer,
				reader.view.byteOffset + reader.offset,
				count,
			)
		: new Int32Array(align(reader, count * 4));
	reader.offset += count * 4;
	return values;
}

function readPlainInt64(reader: DataReader, count: number): BigInt64Array {
	const values = isAligned(reader, 8)
		? new BigInt64Array(
				reader.view.buffer,
				reader.view.byteOffset + reader.offset,
				count,
			)
		: new BigInt64Array(align(reader, count * 8));
	reader.offset += count * 8;
	return values;
}

function readPlainInt96(reader: DataReader, count: number): bigint[] {
	const values = new Array(count);
	for (let i = 0; i < count; i++) {
		const low = reader.view.getBigInt64(reader.offset + i * 12, true);
		const high = reader.view.getInt32(reader.offset + i * 12 + 8, true);
		values[i] = (BigInt(high) << 64n) | low;
	}
	reader.offset += count * 12;
	return values;
}

function readPlainFloat(reader: DataReader, count: number): Float32Array {
	const values = isAligned(reader, 4)
		? new Float32Array(
				reader.view.buffer,
				reader.view.byteOffset + reader.offset,
				count,
			)
		: new Float32Array(align(reader, count * 4));
	reader.offset += count * 4;
	return values;
}

function readPlainDouble(reader: DataReader, count: number): Float64Array {
	const values = isAligned(reader, 8)
		? new Float64Array(
				reader.view.buffer,
				reader.view.byteOffset + reader.offset,
				count,
			)
		: new Float64Array(align(reader, count * 8));
	reader.offset += count * 8;
	return values;
}

function readPlainByteArray(reader: DataReader, count: number): Uint8Array[] {
	const values = new Array(count);
	for (let i = 0; i < count; i++) {
		const length = reader.view.getUint32(reader.offset, true);
		reader.offset += 4;
		values[i] = new Uint8Array(
			reader.view.buffer,
			reader.view.byteOffset + reader.offset,
			length,
		);
		reader.offset += length;
	}
	return values;
}

function readPlainByteArrayFixed(
	reader: DataReader,
	count: number,
	fixedLength: number,
): Uint8Array[] {
	const values = new Array(count);
	for (let i = 0; i < count; i++) {
		values[i] = new Uint8Array(
			reader.view.buffer,
			reader.view.byteOffset + reader.offset,
			fixedLength,
		);
		reader.offset += fixedLength;
	}
	return values;
}

function isAligned(reader: DataReader, alignment: number): boolean {
	return (reader.view.byteOffset + reader.offset) % alignment === 0;
}

function align(reader: DataReader, size: number): ArrayBuffer {
	const aligned = new ArrayBuffer(size);
	new Uint8Array(aligned).set(
		new Uint8Array(
			reader.view.buffer,
			reader.view.byteOffset + reader.offset,
			size,
		),
	);
	return aligned;
}
