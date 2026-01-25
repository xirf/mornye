import type { DType } from '../types/dtypes';
import { getDTypeSize } from '../types/dtypes';
import { type Result, err, ok } from '../types/result';

/**
 * All data stored as raw Uint8Array for zero-copy operations
 * Use DataView to read/write typed values
 */
export type RawBuffer = Uint8Array;

/**
 * Allocates a new Uint8Array buffer for the given dtype and element count
 * All data types stored as raw bytes
 * @param dtype - The data type
 * @param length - Number of elements to allocate
 * @returns Result with allocated buffer or error
 */
export function allocateBuffer(dtype: DType, length: number): Result<RawBuffer, string> {
  if (length < 0) {
    return err('Buffer length must be non-negative');
  }

  try {
    const bytesPerElement = getDTypeSize(dtype);
    const totalBytes = bytesPerElement * length;
    const buffer = new Uint8Array(totalBytes); // Zero-length is allowed
    return ok(buffer);
  } catch (error) {
    return err(
      `Failed to allocate buffer: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Resizes a buffer, preserving existing data
 * @param buffer - The original buffer
 * @param newLength - New length in elements
 * @param dtype - The data type to calculate byte size
 * @returns Result with resized buffer or error
 */
export function resizeBuffer(
  buffer: RawBuffer,
  newLength: number,
  dtype: DType,
): Result<RawBuffer, string> {
  if (newLength <= 0) {
    return err('Buffer length must be positive');
  }

  try {
    const bytesPerElement = getDTypeSize(dtype);
    const newByteLength = bytesPerElement * newLength;
    const newBuffer = new Uint8Array(newByteLength);

    // Copy existing data (up to min of old and new byte length)
    const copyLength = Math.min(buffer.length, newByteLength);
    newBuffer.set(buffer.subarray(0, copyLength));

    return ok(newBuffer);
  } catch (error) {
    return err(
      `Failed to resize buffer: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Get memory usage of a buffer in bytes
 * @param buffer - The buffer
 * @returns Number of bytes used
 */
export function getBufferMemoryUsage(buffer: RawBuffer): number {
  return buffer.byteLength;
}
