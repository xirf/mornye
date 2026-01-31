/** biome-ignore-all lint/suspicious/noExplicitAny: Todo fix type */
import { CompactProtocolReader, TType } from "./thrift_reader.ts";
import type {
	ColumnChunk,
	ColumnMetaData,
	CompressionCodec,
	Encoding,
	FieldRepetitionType,
	FileMetaData,
	RowGroup,
	SchemaElement,
	Type,
} from "./types.ts";

export class ThriftMetadataReader {
	reader: CompactProtocolReader;

	constructor(buffer: Uint8Array) {
		this.reader = new CompactProtocolReader(buffer);
	}

	// FileMetaData
	// 1: version (i32)
	// 2: schema (list<SchemaElement>)
	// 3: num_rows (i64)
	// 4: row_groups (list<RowGroup>)
	// 5: key_value_metadata
	// 6: created_by (string)
	readFileMetaData(): FileMetaData {
		const meta: FileMetaData = {
			version: 0,
			schema: [],
			num_rows: 0n,
			row_groups: [],
		};

		this.reader.readStructBegin();
		while (true) {
			const field = this.reader.readFieldBegin();
			if (field.type === TType.STOP) break;

			switch (field.id) {
				case 1:
					meta.version = this.reader.readI32();
					break;
				case 2:
					meta.schema = this.readSchemaList();
					break;
				case 3:
					meta.num_rows = this.reader.readI64();
					break;
				case 4:
					meta.row_groups = this.readRowGroupList();
					break;
				case 6:
					meta.created_by = this.reader.readString();
					break;
				default:
					this.reader.skip(field.type);
			}
		}
		this.reader.readStructEnd();
		return meta;
	}

	private readSchemaList(): SchemaElement[] {
		const { size } = this.reader.readListBegin();
		const list: SchemaElement[] = [];
		for (let i = 0; i < size; i++) {
			list.push(this.readSchemaElement());
		}
		return list;
	}

	// SchemaElement
	// 1: type (Type)
	// 2: type_length (i32)
	// 3: repetition_type (FieldRepetitionType)
	// 4: name (string)
	// 5: num_children (i32)
	// 6: converted_type (ConvertedType)
	// 7: scale (i32)
	// 8: precision (i32)
	// 9: field_id (i32)
	private readSchemaElement(): SchemaElement {
		const elem: SchemaElement = { name: "" };
		this.reader.readStructBegin();
		while (true) {
			const field = this.reader.readFieldBegin();
			if (field.type === TType.STOP) break;

			switch (field.id) {
				case 1:
					elem.type = this.reader.readI32() as Type;
					break;
				case 2:
					elem.type_length = this.reader.readI32();
					break;
				case 3:
					elem.repetition_type = this.reader.readI32() as FieldRepetitionType;
					break;
				case 4:
					elem.name = this.reader.readString();
					break;
				case 5:
					elem.num_children = this.reader.readI32();
					break;
				case 6:
					elem.converted_type = this.reader.readI32();
					break;
				case 7:
					elem.scale = this.reader.readI32();
					break;
				case 8:
					elem.precision = this.reader.readI32();
					break;
				case 9:
					elem.field_id = this.reader.readI32();
					break;
				default:
					this.reader.skip(field.type);
			}
		}
		this.reader.readStructEnd();
		return elem;
	}

	private readRowGroupList(): RowGroup[] {
		const { size } = this.reader.readListBegin();
		const list: RowGroup[] = [];
		for (let i = 0; i < size; i++) {
			list.push(this.readRowGroup());
		}
		return list;
	}

	// RowGroup
	// 1: columns (list<ColumnChunk>)
	// 2: total_byte_size (i64)
	// 3: num_rows (i64)
	// 4: sorting_columns
	private readRowGroup(): RowGroup {
		const rg: RowGroup = { columns: [], total_byte_size: 0n, num_rows: 0n };
		this.reader.readStructBegin();
		while (true) {
			const field = this.reader.readFieldBegin();
			if (field.type === TType.STOP) break;

			switch (field.id) {
				case 1:
					rg.columns = this.readColumnChunkList();
					break;
				case 2:
					rg.total_byte_size = this.reader.readI64();
					break;
				case 3:
					rg.num_rows = this.reader.readI64();
					break;
				default:
					this.reader.skip(field.type);
			}
		}
		this.reader.readStructEnd();
		return rg;
	}

	private readColumnChunkList(): ColumnChunk[] {
		const { size } = this.reader.readListBegin();
		const list: ColumnChunk[] = [];
		for (let i = 0; i < size; i++) {
			list.push(this.readColumnChunk());
		}
		return list;
	}

	// ColumnChunk
	// 1: file_path (string)
	// 2: file_offset (i64)
	// 3: meta_data (ColumnMetaData)
	private readColumnChunk(): ColumnChunk {
		const cc: ColumnChunk = { file_offset: 0n };
		this.reader.readStructBegin();
		while (true) {
			const field = this.reader.readFieldBegin();
			if (field.type === TType.STOP) break;

			switch (field.id) {
				case 1:
					cc.file_path = this.reader.readString();
					break;
				case 2:
					cc.file_offset = this.reader.readI64();
					break;
				case 3:
					cc.meta_data = this.readColumnMetaData();
					break;
				default:
					this.reader.skip(field.type);
			}
		}
		this.reader.readStructEnd();
		return cc;
	}

	// ColumnMetaData
	// 1: type (Type)
	// 2: encodings (list<Encoding>)
	// 3: path_in_schema (list<string>)
	// 4: codec (CompressionCodec)
	// 5: num_values (i64)
	// 6: total_uncompressed_size (i64)
	// 7: total_compressed_size (i64)
	// 8: key_value_metadata
	// 9: data_page_offset (i64)
	// 10: index_page_offset (i64)
	// 11: dictionary_page_offset (i64)
	private readColumnMetaData(): ColumnMetaData {
		const md: Partial<ColumnMetaData> = {
			path_in_schema: [],
			encodings: [],
		};
		this.reader.readStructBegin();
		while (true) {
			const field = this.reader.readFieldBegin();
			if (field.type === TType.STOP) break;

			switch (field.id) {
				case 1:
					md.type = this.reader.readI32() as Type;
					break;
				case 2:
					md.encodings = this.readEncodingList();
					break;
				case 3:
					md.path_in_schema = this.readStringList();
					break;
				case 4:
					md.codec = this.reader.readI32() as CompressionCodec;
					break;
				case 5:
					md.num_values = this.reader.readI64();
					break;
				case 6:
					md.total_uncompressed_size = this.reader.readI64();
					break;
				case 7:
					md.total_compressed_size = this.reader.readI64();
					break;
				case 9:
					md.data_page_offset = this.reader.readI64();
					break;
				case 11:
					md.dictionary_page_offset = this.reader.readI64();
					break;
				default:
					this.reader.skip(field.type);
			}
		}
		this.reader.readStructEnd();
		return md as ColumnMetaData;
	}

	private readEncodingList(): Encoding[] {
		const { size } = this.reader.readListBegin();
		const list: Encoding[] = [];
		for (let i = 0; i < size; i++) {
			list.push(this.reader.readI32() as Encoding);
		}
		return list;
	}

	private readStringList(): string[] {
		const { size } = this.reader.readListBegin();
		const list: string[] = [];
		for (let i = 0; i < size; i++) {
			list.push(this.reader.readString());
		}
		return list;
	}
}
