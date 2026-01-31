// Minimal Thrift definitions for Parquet FileMetaData
// We only implement what we need to read the schema and row groups

export enum Type {
	BOOLEAN = 0,
	INT32 = 1,
	INT64 = 2,
	INT96 = 3,
	FLOAT = 4,
	DOUBLE = 5,
	BYTE_ARRAY = 6,
	FIXED_LEN_BYTE_ARRAY = 7,
}

export enum ConvertedType {
	UTF8 = 0,
	MAP = 1,
	MAP_KEY_VALUE = 2,
	LIST = 3,
	ENUM = 4,
	DECIMAL = 5,
	DATE = 6,
	TIME_MILLIS = 7,
	TIME_MICROS = 8,
	TIMESTAMP_MILLIS = 9,
	TIMESTAMP_MICROS = 10,
	UINT_8 = 11,
	UINT_16 = 12,
	UINT_32 = 13,
	UINT_64 = 14,
	INT_8 = 15,
	INT_16 = 16,
	INT_32 = 17,
	INT_64 = 18,
	JSON = 19,
	BSON = 20,
	INTERVAL = 21,
}

export enum FieldRepetitionType {
	REQUIRED = 0,
	OPTIONAL = 1,
	REPEATED = 2,
}

export enum Encoding {
	PLAIN = 0,
	PLAIN_DICTIONARY = 2,
	RLE = 3,
	BIT_PACKED = 4,
}

export enum CompressionCodec {
	UNCOMPRESSED = 0,
	SNAPPY = 1,
	GZIP = 2,
	LZO = 3,
	BROTLI = 4,
	LZ4 = 5,
	ZSTD = 6,
}

// Structs
export interface SchemaElement {
	type?: Type;
	type_length?: number;
	repetition_type?: FieldRepetitionType;
	name: string;
	num_children?: number;
	converted_type?: ConvertedType;
	scale?: number;
	precision?: number;
	field_id?: number;
}

export interface ColumnMetaData {
	type: Type;
	encodings: Encoding[];
	path_in_schema: string[];
	codec: CompressionCodec;
	num_values: bigint;
	total_uncompressed_size: bigint;
	total_compressed_size: bigint;
	key_value_metadata?: unknown[];
	data_page_offset: bigint;
	index_page_offset?: bigint;
	dictionary_page_offset?: bigint;
}

export interface ColumnChunk {
	file_path?: string;
	file_offset: bigint;
	meta_data?: ColumnMetaData;
}

export interface RowGroup {
	columns: ColumnChunk[];
	total_byte_size: bigint;
	num_rows: bigint;
	sorting_columns?: unknown[];
}

export interface FileMetaData {
	version: number;
	schema: SchemaElement[];
	num_rows: bigint;
	row_groups: RowGroup[];
	key_value_metadata?: unknown[];
	created_by?: string;
}
