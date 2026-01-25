import type { Result } from '../types/result';
import { err, ok } from '../types/result';

/**
 * Datetime format options
 */
export type DateTimeFormat = 'iso' | 'sql' | 'unix-s' | 'unix-ms';

/**
 * Parse datetime string with explicit format
 */
export function parseDateTimeFormat(value: string, format: DateTimeFormat): Result<bigint, Error> {
  try {
    switch (format) {
      case 'iso': {
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
          return err(new Error(`Invalid ISO datetime: ${value}`));
        }
        return ok(BigInt(date.getTime()));
      }

      case 'sql': {
        // YYYY-MM-DD HH:mm:ss
        const sqlPattern = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/;
        const match = value.match(sqlPattern);
        if (!match) {
          return err(new Error(`Invalid SQL datetime format: ${value}`));
        }

        const [, year, month, day, hour, minute, second] = match;
        const date = new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}.000Z`);

        if (Number.isNaN(date.getTime())) {
          return err(new Error(`Invalid SQL datetime: ${value}`));
        }

        return ok(BigInt(date.getTime()));
      }

      case 'unix-ms': {
        const timestamp = Number(value);
        if (Number.isNaN(timestamp)) {
          return err(new Error(`Invalid unix-ms timestamp: ${value}`));
        }
        return ok(BigInt(timestamp));
      }

      case 'unix-s': {
        const timestamp = Number(value);
        if (Number.isNaN(timestamp)) {
          return err(new Error(`Invalid unix-s timestamp: ${value}`));
        }
        return ok(BigInt(timestamp * 1000));
      }

      default:
        return err(new Error(`Unknown datetime format: ${format}`));
    }
  } catch (e) {
    return err(e instanceof Error ? e : new Error(`Datetime parsing failed: ${value}`));
  }
}

/**
 * Parse datetime string with auto-detection or explicit format
 */
export function parseDateTime(value: string, format?: DateTimeFormat): Result<bigint, Error> {
  if (!value || value.trim() === '') {
    return err(new Error('Empty datetime string'));
  }

  // If format is specified, use it
  if (format) {
    return parseDateTimeFormat(value, format);
  }

  // Auto-detect format
  // Try ISO first (most common)
  if (value.includes('T') || value.includes('Z') || value.includes('+')) {
    const result = parseDateTimeFormat(value, 'iso');
    if (result.ok) return result;
  }

  // Try SQL format (YYYY-MM-DD HH:mm:ss)
  if (value.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/)) {
    return parseDateTimeFormat(value, 'sql');
  }

  // Try unix timestamp (pure numbers)
  if (/^\d+$/.test(value)) {
    // Heuristic: if > 1e12, likely milliseconds, else seconds
    const num = Number(value);
    if (num > 1e12) {
      return parseDateTimeFormat(value, 'unix-ms');
    }
    return parseDateTimeFormat(value, 'unix-s');
  }

  // Fallback to ISO
  return parseDateTimeFormat(value, 'iso');
}

/**
 * Parse date string (YYYY-MM-DD only, no time component)
 */
export function parseDate(value: string, format?: 'iso' | 'sql'): Result<bigint, Error> {
  if (!value || value.trim() === '') {
    return err(new Error('Empty date string'));
  }

  // Date format should be YYYY-MM-DD (no time)
  const datePattern = /^(\d{4})-(\d{2})-(\d{2})$/;
  const match = value.match(datePattern);

  if (!match) {
    return err(new Error(`Invalid date format: ${value}`));
  }

  const [, yearStr, monthStr, dayStr] = match;
  const year = Number(yearStr);
  const month = Number(monthStr);
  const day = Number(dayStr);

  // Validate date components
  if (month < 1 || month > 12) {
    return err(new Error(`Invalid month: ${month}`));
  }
  if (day < 1 || day > 31) {
    return err(new Error(`Invalid day: ${day}`));
  }

  const date = new Date(`${yearStr}-${monthStr}-${dayStr}T00:00:00.000Z`);

  if (Number.isNaN(date.getTime())) {
    return err(new Error(`Invalid date: ${value}`));
  }

  // Verify date wasn't auto-adjusted (e.g., Feb 29 in non-leap year)
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() + 1 !== month ||
    date.getUTCDate() !== day
  ) {
    return err(new Error(`Invalid date: ${value}`));
  }

  return ok(BigInt(date.getTime()));
}

/**
 * Format timestamp as datetime string
 */
export function formatDateTime(timestamp: bigint, format: DateTimeFormat = 'iso'): string {
  const ms = Number(timestamp);
  const date = new Date(ms);

  switch (format) {
    case 'iso':
      return date.toISOString();

    case 'sql': {
      const iso = date.toISOString();
      // Convert 2024-01-25T10:30:00.000Z -> 2024-01-25 10:30:00
      return iso.replace('T', ' ').replace(/\.\d{3}Z$/, '');
    }

    case 'unix-ms':
      return String(timestamp);

    case 'unix-s':
      return String(Math.floor(Number(timestamp) / 1000));

    default:
      return date.toISOString();
  }
}

/**
 * Format timestamp as date string (YYYY-MM-DD)
 */
export function formatDate(timestamp: bigint): string {
  const date = new Date(Number(timestamp));
  return date.toISOString().split('T')[0] ?? '';
}
