/**
 * Null bitmap for tracking null values efficiently
 * Uses 1 bit per value (8 values per byte)
 */
export interface NullBitmap {
  /** Number of values tracked */
  length: number;
  /** Bitmap data (1 bit per value) */
  data: Uint8Array;
}

/**
 * Creates a new null bitmap
 * @param length - Number of values to track
 * @returns NullBitmap instance
 */
export function createNullBitmap(length: number): NullBitmap {
  if (length <= 0) {
    throw new Error('Bitmap length must be positive');
  }

  // Calculate bytes needed: ceil(length / 8)
  const byteLength = Math.ceil(length / 8);
  return {
    length,
    data: new Uint8Array(byteLength),
  };
}

/**
 * Checks if a value is null
 * @param bitmap - The null bitmap
 * @param index - Value index
 * @returns True if null
 */
export function isNull(bitmap: NullBitmap, index: number): boolean {
  if (index < 0 || index >= bitmap.length) {
    return false; // Out of bounds treated as not null
  }

  const byteIndex = Math.floor(index / 8);
  const bitIndex = index % 8;
  return (bitmap.data[byteIndex]! & (1 << bitIndex)) !== 0;
}

/**
 * Marks a value as null
 * @param bitmap - The null bitmap
 * @param index - Value index
 */
export function setNull(bitmap: NullBitmap, index: number): void {
  if (index < 0 || index >= bitmap.length) {
    return; // Ignore out of bounds
  }

  const byteIndex = Math.floor(index / 8);
  const bitIndex = index % 8;
  bitmap.data[byteIndex]! |= 1 << bitIndex;
}

/**
 * Marks a value as not null
 * @param bitmap - The null bitmap
 * @param index - Value index
 */
export function setNotNull(bitmap: NullBitmap, index: number): void {
  if (index < 0 || index >= bitmap.length) {
    return; // Ignore out of bounds
  }

  const byteIndex = Math.floor(index / 8);
  const bitIndex = index % 8;
  bitmap.data[byteIndex]! &= ~(1 << bitIndex);
}

/**
 * Counts the number of null values
 * @param bitmap - The null bitmap
 * @returns Number of null values
 */
export function getNullCount(bitmap: NullBitmap): number {
  let count = 0;

  // Count set bits in each byte
  for (let i = 0; i < bitmap.data.length; i++) {
    let byte = bitmap.data[i];
    // Brian Kernighan's algorithm for counting set bits
    while (byte) {
      byte &= byte - 1;
      count++;
    }
  }

  return count;
}

/**
 * Gets memory usage of the bitmap in bytes
 * @param bitmap - The null bitmap
 * @returns Bytes used
 */
export function getBitmapMemoryUsage(bitmap: NullBitmap): number {
  return bitmap.data.byteLength;
}

/**
 * Resizes a null bitmap, preserving existing null flags
 * @param bitmap - The bitmap to resize
 * @param newLength - New length
 * @returns Resized bitmap
 */
export function resizeNullBitmap(bitmap: NullBitmap, newLength: number): NullBitmap {
  if (newLength <= 0) {
    throw new Error('Bitmap length must be positive');
  }

  const newByteLength = Math.ceil(newLength / 8);
  const newData = new Uint8Array(newByteLength);

  // Copy existing data (up to min of old and new length)
  const copyBytes = Math.min(bitmap.data.length, newByteLength);
  newData.set(bitmap.data.subarray(0, copyBytes));

  return {
    length: newLength,
    data: newData,
  };
}
