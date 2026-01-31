export class BinaryReader {
	private view: DataView;
	private offset: number;
	private buffer: Uint8Array;

	constructor(buffer: Uint8Array) {
		this.buffer = buffer;
		this.view = new DataView(
			buffer.buffer,
			buffer.byteOffset,
			buffer.byteLength,
		);
		this.offset = 0;
	}

	seek(pos: number) {
		this.offset = pos;
	}
	getPos() {
		return this.offset;
	}
	skip(n: number) {
		this.offset += n;
	}

	readByte(): number {
		if (this.offset >= this.view.byteLength) throw new Error("End of buffer");
		return this.view.getUint8(this.offset++);
	}

	readBytes(n: number): Uint8Array {
		if (this.offset + n > this.view.byteLength)
			throw new Error("End of buffer");
		const res = this.buffer.subarray(this.offset, this.offset + n);
		this.offset += n;
		return res;
	}

	readInt16(): number {
		const v = this.view.getInt16(this.offset, true);
		this.offset += 2;
		return v;
	}
	readInt32(): number {
		const v = this.view.getInt32(this.offset, true);
		this.offset += 4;
		return v;
	}

	// Thrift VarInts
	readVarInt(): number {
		let result = 0;
		let shift = 0;
		while (true) {
			const byte = this.readByte();
			result |= (byte & 0x7f) << shift;
			if ((byte & 0x80) === 0) return result;
			shift += 7;
		}
	}

	// 64-bit VarInt handling is tricky in JS numbers.
	// For metadata (row counts), BigInt is safer.
	readVarBigInt(): bigint {
		let result = 0n;
		let shift = 0n;
		while (true) {
			const byte = this.readByte();
			result |= BigInt(byte & 0x7f) << shift;
			if ((byte & 0x80) === 0) return result;
			shift += 7n;
		}
	}

	readZigZagVarInt(): number {
		const n = this.readVarInt();
		return (n >>> 1) ^ -(n & 1);
	}

	readZigZagVarBigInt(): bigint {
		const n = this.readVarBigInt();
		return (n >> 1n) ^ -(n & 1n);
	}
}

// Thrift Types (Compact Protocol)
export enum TType {
	STOP = 0,
	BOOLEAN_TRUE = 1,
	BOOLEAN_FALSE = 2,
	BYTE = 3,
	I16 = 4,
	I32 = 5,
	I64 = 6,
	DOUBLE = 7,
	BINARY = 8,
	LIST = 9,
	SET = 10,
	MAP = 11,
	STRUCT = 12,
}

export class CompactProtocolReader {
	reader: BinaryReader;
	lastFieldId: number = 0;
	fieldIdStack: number[] = [];

	constructor(buffer: Uint8Array) {
		this.reader = new BinaryReader(buffer);
	}

	readStructBegin() {
		this.fieldIdStack.push(this.lastFieldId);
		this.lastFieldId = 0;
	}

	readStructEnd() {
		this.lastFieldId = this.fieldIdStack.pop() || 0;
	}

	readFieldBegin(): { id: number; type: number; modifier?: number } {
		const byte = this.reader.readByte();
		if (byte === 0) return { id: 0, type: TType.STOP };

		const delta = (byte & 0xf0) >> 4;
		const type = byte & 0x0f;

		let id: number;
		if (delta === 0) {
			// If delta is 0, it doesn't mean ID is same, it means explicit ID follows
			// Exception: For TType.STOP (0), we returned early.
			id = this.reader.readZigZagVarInt();
		} else {
			id = this.lastFieldId + delta;
		}
		this.lastFieldId = id;

		return { id, type };
	}

	readBool(): boolean {
		// In compact proto, bools are often encoded in the field header type (1 or 2)
		// If readFieldBegin returned BOOLEAN_TRUE or BOOLEAN_FALSE, we are done.
		// But if we are reading a bool in a list/map, it's a byte (1=true, 0=false).
		const b = this.reader.readByte();
		return b === 1; // Strict 1 checks?
	}

	readByte(): number {
		return this.reader.readByte();
	}
	readI16(): number {
		return this.reader.readZigZagVarInt();
	}
	readI32(): number {
		return this.reader.readZigZagVarInt();
	}
	readI64(): bigint {
		return this.reader.readZigZagVarBigInt();
	}
	readDouble(): number {
		const buff = this.reader.readBytes(8);
		const view = new DataView(buff.buffer, buff.byteOffset, 8);
		return view.getFloat64(0, true);
	}

	readString(): string {
		const len = this.reader.readVarInt();
		if (len === 0) return "";
		const bytes = this.reader.readBytes(len);
		return new TextDecoder().decode(bytes);
	}

	readBinary(): Uint8Array {
		const len = this.reader.readVarInt();
		if (len === 0) return new Uint8Array(0);
		return this.reader.readBytes(len);
	}

	// List/Set
	readListBegin(): { type: number; size: number } {
		const byte = this.reader.readByte();
		let size = (byte & 0xf0) >> 4;
		const type = byte & 0x0f;
		if (size === 15) {
			size = this.reader.readVarInt();
		}
		return { type, size };
	}

	// Map
	readMapBegin(): { keyType: number; valueType: number; size: number } {
		const size = this.reader.readVarInt();
		if (size === 0) return { keyType: 0, valueType: 0, size: 0 };

		const byte = this.reader.readByte();
		const keyType = (byte & 0xf0) >> 4;
		const valueType = byte & 0x0f;
		return { keyType, valueType, size };
	}

	skip(type: number) {
		switch (type) {
			case TType.BOOLEAN_TRUE:
			case TType.BOOLEAN_FALSE:
				break; // Encoded in type
			case TType.BYTE:
				this.reader.readByte();
				break;
			case TType.I16:
			case TType.I32:
				this.reader.readZigZagVarInt();
				break;
			case TType.I64:
				this.reader.readZigZagVarBigInt();
				break;
			case TType.DOUBLE:
				this.reader.readBytes(8);
				break;
			case TType.BINARY:
				this.reader.readBytes(this.reader.readVarInt());
				break;
			case TType.STRUCT:
				this.readStructBegin();
				while (true) {
					const field = this.readFieldBegin();
					if (field.type === TType.STOP) break;
					this.skip(field.type);
				}
				this.readStructEnd();
				break;
			case TType.LIST:
			case TType.SET: {
				const { type: elemType, size } = this.readListBegin();
				for (let i = 0; i < size; i++) this.skip(elemType);
				break;
			}
			case TType.MAP: {
				const { keyType, valueType, size } = this.readMapBegin();
				for (let i = 0; i < size; i++) {
					this.skip(keyType);
					this.skip(valueType);
				}
				break;
			}
			default:
				throw new Error(`Unknown TType to skip: ${type}`);
		}
	}
}
