/**
 * Data type enumeration for columns
 * All numeric values stored as raw bytes in TypedArrays
 */
export const DType = {
  Float64: 'float64',
  Int32: 'int32',
  String: 'string', // Stored as dictionary-encoded Int32 indices
  Bool: 'bool', // Stored as Uint8 (bitmap)
  DateTime: 'datetime', // Stored as BigInt64 (milliseconds since epoch)
  Date: 'date', // Stored as BigInt64 (milliseconds since epoch, time=00:00:00)
} as const;

export type DType = (typeof DType)[keyof typeof DType];

/**
 * Get byte size for a data type
 * @param dtype - The data type
 * @returns Number of bytes per element
 */
export function getDTypeSize(dtype: DType): number {
  switch (dtype) {
    case DType.Float64:
      return 8;
    case DType.Int32:
      return 4;
    case DType.String:
      return 4; // Dictionary index (Int32)
    case DType.Bool:
      return 1; // Bitmap (Uint8)
    case DType.DateTime:
    case DType.Date:
      return 8; // BigInt64
    default:
      return 0;
  }
}

/**
 * Get TypedArray constructor for a data type
 * @param dtype - The data type
 * @returns TypedArray constructor
 */
export function getTypedArrayConstructor(
  dtype: DType,
):
  | Float64ArrayConstructor
  | Int32ArrayConstructor
  | Uint8ArrayConstructor
  | BigInt64ArrayConstructor {
  switch (dtype) {
    case DType.Float64:
      return Float64Array;
    case DType.Int32:
    case DType.String: // Dictionary indices
      return Int32Array;
    case DType.Bool:
      return Uint8Array;
    case DType.DateTime:
    case DType.Date:
      return BigInt64Array;
    default:
      throw new Error(`Unsupported dtype: ${dtype}`);
  }
}

/**
 * Check if a dtype is numeric (can be used in mathematical operations)
 * @param dtype - The data type
 * @returns True if numeric
 */
export function isNumericDType(dtype: DType): boolean {
  return dtype === DType.Float64 || dtype === DType.Int32;
}
