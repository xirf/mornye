import { parseDate, parseDateTime } from '../utils/datetime';
import { DType } from './dtypes';
import type { Result } from './result';
import { err, ok } from './result';

/**
 * Default null value representations
 */
const DEFAULT_NULL_VALUES = ['NA', 'null', '-', ''];

/**
 * Check if a string value represents null
 */
export function isNullValue(value: string, nullValues: string[] = DEFAULT_NULL_VALUES): boolean {
  return nullValues.includes(value);
}

/**
 * Parse string to Float64
 */
export function parseFloat64(
  value: string,
  nullValues: string[] = DEFAULT_NULL_VALUES,
): Result<number | null, Error> {
  // Check for null
  if (isNullValue(value, nullValues)) {
    return ok(null);
  }

  // Trim and parse
  const trimmed = value.trim();
  if (trimmed === '') {
    return err(new Error('Empty string cannot be parsed as Float64'));
  }

  const parsed = Number(trimmed);
  if (Number.isNaN(parsed)) {
    return err(new Error(`Invalid Float64: ${value}`));
  }

  return ok(parsed);
}

/**
 * Parse string to Int32
 */
export function parseInt32(
  value: string,
  nullValues: string[] = DEFAULT_NULL_VALUES,
): Result<number | null, Error> {
  // Check for null
  if (isNullValue(value, nullValues)) {
    return ok(null);
  }

  // Trim and parse
  const trimmed = value.trim();
  if (trimmed === '') {
    return err(new Error('Empty string cannot be parsed as Int32'));
  }

  // Check if it contains a decimal point (reject floats)
  if (trimmed.includes('.')) {
    return err(new Error(`Int32 cannot have decimal point: ${value}`));
  }

  const parsed = Number(trimmed);
  if (Number.isNaN(parsed)) {
    return err(new Error(`Invalid Int32: ${value}`));
  }

  // Check Int32 range: -2^31 to 2^31-1
  if (parsed < -2147483648 || parsed > 2147483647) {
    return err(new Error(`Int32 out of range: ${value}`));
  }

  return ok(Math.floor(parsed));
}

/**
 * Parse string to Bool (stored as 0 or 1)
 */
export function parseBool(
  value: string,
  nullValues: string[] = DEFAULT_NULL_VALUES,
): Result<number | null, Error> {
  // Check for null
  if (isNullValue(value, nullValues)) {
    return ok(null);
  }

  const lower = value.toLowerCase().trim();

  if (lower === 'true' || lower === '1') {
    return ok(1);
  }

  if (lower === 'false' || lower === '0') {
    return ok(0);
  }

  return err(new Error(`Invalid Bool: ${value}`));
}

/**
 * Parse string value based on target data type
 */
export function parseValue(
  value: string,
  dtype: DType,
  nullValues: string[] = DEFAULT_NULL_VALUES,
): Result<number | bigint | string | null, Error> {
  switch (dtype) {
    case DType.Float64:
      return parseFloat64(value, nullValues);

    case DType.Int32:
      return parseInt32(value, nullValues);

    case DType.Bool:
      return parseBool(value, nullValues);

    case DType.String: {
      // For strings, return null if it's a null value, otherwise return the string
      if (isNullValue(value, nullValues)) {
        return ok(null);
      }
      return ok(value);
    }

    case DType.DateTime: {
      if (isNullValue(value, nullValues)) {
        return ok(null);
      }
      return parseDateTime(value);
    }

    case DType.Date: {
      if (isNullValue(value, nullValues)) {
        return ok(null);
      }
      return parseDate(value);
    }

    default:
      return err(new Error(`Unsupported dtype: ${dtype}`));
  }
}
