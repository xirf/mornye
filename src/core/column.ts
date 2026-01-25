import type { DType } from '../types/dtypes';
import { getDTypeSize } from '../types/dtypes';
import { type Result, err, ok, unwrapErr } from '../types/result';
import { type NullBitmap, createNullBitmap } from '../utils/nulls';
import { type RawBuffer, allocateBuffer, getBufferMemoryUsage, resizeBuffer } from './buffer';

/**
 * Column represents a single typed column of data
 * All data stored as raw bytes (Uint8Array) for zero-copy operations
 */
export interface Column {
  /** Column name */
  name?: string;
  /** Data type */
  dtype: DType;
  /** Number of rows */
  length: number;
  /** Underlying raw byte buffer */
  data: RawBuffer;
  /** DataView for reading/writing typed values */
  view: DataView;
  /** Optional null bitmap for tracking missing values */
  nullBitmap?: NullBitmap;
}

/**
 * Creates a new column with specified dtype and length
 * @param dtype - The data type
 * @param length - Number of rows
 * @param name - Optional column name
 * @returns Result with Column or error
 */
export function createColumn(dtype: DType, length: number, name?: string): Result<Column, string> {
  const bufferResult = allocateBuffer(dtype, length);
  if (!bufferResult.ok) {
    return err(unwrapErr(bufferResult));
  }

  const data = bufferResult.data;
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  return ok({
    name,
    dtype,
    length,
    data,
    view,
  });
}

/**
 * Gets the length of a column
 * @param column - The column
 * @returns Number of rows
 */
export function getColumnLength(column: Column): number {
  return column.length;
}

/**
 * Gets the dtype of a column
 * @param column - The column
 * @returns The data type
 */
export function getColumnDType(column: Column): DType {
  return column.dtype;
}

/**
 * Gets a value from the column at specified index
 * Reads from raw bytes using DataView
 * @param column - The column
 * @param index - Row index
 * @returns The value or undefined if out of bounds
 */
export function getColumnValue(column: Column, index: number): number | bigint | undefined {
  if (index < 0 || index >= column.length) {
    return undefined;
  }

  const bytesPerElement = getDTypeSize(column.dtype);
  const byteOffset = index * bytesPerElement;

  switch (column.dtype) {
    case 'float64':
      return column.view.getFloat64(byteOffset, true); // little-endian

    case 'int32':
      return column.view.getInt32(byteOffset, true);

    case 'bool':
      return column.view.getUint8(byteOffset);

    case 'string':
      // String is stored as int32 dictionary index
      return column.view.getInt32(byteOffset, true);

    case 'datetime':
    case 'date':
      return column.view.getBigInt64(byteOffset, true);

    default:
      return undefined;
  }
}

/**
 * Sets a value in the column at specified index
 * Writes to raw bytes using DataView
 * @param column - The column
 * @param index - Row index
 * @param value - The value to set
 */
export function setColumnValue(column: Column, index: number, value: number | bigint): void {
  if (index < 0 || index >= column.length) {
    return;
  }

  const bytesPerElement = getDTypeSize(column.dtype);
  const byteOffset = index * bytesPerElement;

  switch (column.dtype) {
    case 'float64':
      column.view.setFloat64(byteOffset, value as number, true);
      break;

    case 'int32':
      column.view.setInt32(byteOffset, value as number, true);
      break;

    case 'bool':
      column.view.setUint8(byteOffset, value as number);
      break;

    case 'string':
      // String is stored as int32 dictionary index
      column.view.setInt32(byteOffset, value as number, true);
      break;

    case 'datetime':
    case 'date':
      column.view.setBigInt64(byteOffset, value as bigint, true);
      break;
  }
}

/**
 * Gets memory usage of the column in bytes
 * @param column - The column
 * @returns Bytes used
 */
export function getColumnMemoryUsage(column: Column): number {
  return getBufferMemoryUsage(column.data);
}

/**
 * Enables null tracking for a column
 * Creates a null bitmap if one doesn't exist
 * @param column - The column
 */
export function enableNullTracking(column: Column): void {
  if (!column.nullBitmap) {
    column.nullBitmap = createNullBitmap(column.length);
  }
}

/**
 * Resizes a column, preserving existing data
 * @param column - The column to resize
 * @param newLength - New length
 * @returns Result with resized column or error
 */
export function resizeColumn(column: Column, newLength: number): Result<Column, string> {
  const resizedBuffer = resizeBuffer(column.data, newLength, column.dtype);
  if (!resizedBuffer.ok) {
    return err(unwrapErr(resizedBuffer));
  }

  const data = resizedBuffer.data;
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  return ok({
    name: column.name,
    dtype: column.dtype,
    length: newLength,
    data,
    view,
  });
}
