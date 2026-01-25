import { describe, expect, test } from 'bun:test';
import {
  type DateTimeFormat,
  formatDate,
  formatDateTime,
  parseDate,
  parseDateTime,
  parseDateTimeFormat,
} from '../../src/utils/datetime';

describe('parseDateTimeFormat', () => {
  test('parses ISO-8601 format', () => {
    const result = parseDateTimeFormat('2024-01-25T10:30:00.000Z', 'iso');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(1706178600000n);
    }
  });

  test('parses SQL format', () => {
    const result = parseDateTimeFormat('2024-01-25 10:30:00', 'sql');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(1706178600000n);
    }
  });

  test('parses unix-ms format', () => {
    const result = parseDateTimeFormat('1706178600000', 'unix-ms');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(1706178600000n);
    }
  });

  test('parses unix-s format', () => {
    const result = parseDateTimeFormat('1706178600', 'unix-s');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(1706178600000n);
    }
  });

  test('rejects invalid ISO format', () => {
    const result = parseDateTimeFormat('not-a-date', 'iso');
    expect(result.ok).toBe(false);
  });

  test('rejects invalid SQL format', () => {
    const result = parseDateTimeFormat('2024/01/25 10:30', 'sql');
    expect(result.ok).toBe(false);
  });

  test('rejects invalid unix timestamp', () => {
    const result = parseDateTimeFormat('not-a-number', 'unix-ms');
    expect(result.ok).toBe(false);
  });

  test('handles ISO with timezone offset', () => {
    const result = parseDateTimeFormat('2024-01-25T10:30:00+05:00', 'iso');
    expect(result.ok).toBe(true);
  });
});

describe('parseDateTime', () => {
  test('auto-detects ISO format', () => {
    const result = parseDateTime('2024-01-25T10:30:00.000Z');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(1706178600000n);
    }
  });

  test('auto-detects SQL format', () => {
    const result = parseDateTime('2024-01-25 10:30:00');
    expect(result.ok).toBe(true);
  });

  test('auto-detects unix-ms timestamp', () => {
    const result = parseDateTime('1706178600000');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(1706178600000n);
    }
  });

  test('prefers explicit format over auto-detect', () => {
    const result = parseDateTime('1706178600', 'unix-s');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toBe(1706178600000n);
    }
  });

  test('rejects invalid datetime string', () => {
    const result = parseDateTime('invalid');
    expect(result.ok).toBe(false);
  });

  test('handles empty string', () => {
    const result = parseDateTime('');
    expect(result.ok).toBe(false);
  });
});

describe('parseDate', () => {
  test('parses ISO date format', () => {
    const result = parseDate('2024-01-25');
    expect(result.ok).toBe(true);
    if (result.ok) {
      // Should be midnight UTC
      const date = new Date(Number(result.data));
      expect(date.getUTCFullYear()).toBe(2024);
      expect(date.getUTCMonth()).toBe(0); // January
      expect(date.getUTCDate()).toBe(25);
    }
  });

  test('parses SQL date format', () => {
    const result = parseDate('2024-01-25', 'sql');
    expect(result.ok).toBe(true);
  });

  test('rejects invalid date', () => {
    const result = parseDate('not-a-date');
    expect(result.ok).toBe(false);
  });

  test('rejects datetime as date', () => {
    const result = parseDate('2024-01-25T10:30:00Z');
    expect(result.ok).toBe(false);
  });
});

describe('formatDateTime', () => {
  test('formats timestamp as ISO', () => {
    const formatted = formatDateTime(1706178600000n, 'iso');
    expect(formatted).toBe('2024-01-25T10:30:00.000Z');
  });

  test('formats timestamp as SQL', () => {
    const formatted = formatDateTime(1706178600000n, 'sql');
    expect(formatted).toBe('2024-01-25 10:30:00');
  });

  test('formats timestamp as unix-ms', () => {
    const formatted = formatDateTime(1706178600000n, 'unix-ms');
    expect(formatted).toBe('1706178600000');
  });

  test('formats timestamp as unix-s', () => {
    const formatted = formatDateTime(1706178600000n, 'unix-s');
    expect(formatted).toBe('1706178600');
  });

  test('defaults to ISO format', () => {
    const formatted = formatDateTime(1706178600000n);
    expect(formatted).toBe('2024-01-25T10:30:00.000Z');
  });
});

describe('formatDate', () => {
  test('formats date timestamp', () => {
    const timestamp = 1706140800000n; // 2024-01-25 00:00:00 UTC
    const formatted = formatDate(timestamp);
    expect(formatted).toBe('2024-01-25');
  });

  test('handles date at start of epoch', () => {
    const formatted = formatDate(0n);
    expect(formatted).toBe('1970-01-01');
  });
});

describe('Edge cases', () => {
  test('handles leap year', () => {
    const result = parseDate('2024-02-29');
    expect(result.ok).toBe(true);
  });

  test('rejects invalid leap year date', () => {
    const result = parseDate('2023-02-29');
    expect(result.ok).toBe(false);
  });

  test('handles year boundaries', () => {
    const result = parseDateTime('2023-12-31T23:59:59.999Z');
    expect(result.ok).toBe(true);
  });

  test('handles large timestamps', () => {
    const largeTimestamp = 253402300799999n; // 9999-12-31T23:59:59.999Z
    const formatted = formatDateTime(largeTimestamp);
    expect(formatted).toContain('9999');
  });

  test('roundtrip ISO datetime', () => {
    const original = '2024-01-25T10:30:00.000Z';
    const parsed = parseDateTime(original);
    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      const formatted = formatDateTime(parsed.data);
      expect(formatted).toBe(original);
    }
  });

  test('roundtrip date', () => {
    const original = '2024-01-25';
    const parsed = parseDate(original);
    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      const formatted = formatDate(parsed.data);
      expect(formatted).toBe(original);
    }
  });
});
