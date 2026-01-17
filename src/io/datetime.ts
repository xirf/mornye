import type { DateTimeFormat } from './csv/options';

const MS_PER_SECOND = 1_000;
const MS_PER_MINUTE = 60_000;

/**
 * Create a lightweight datetime parser function for a given format and offset.
 * Returns epoch milliseconds or NaN on failure.
 */
export function createDateTimeParser(
  format: DateTimeFormat,
  offsetMinutes: number,
): (value: string) => number {
  switch (format) {
    case 'unix-ms':
      return (value: string) => {
        const n = Number(value.trim());
        return Number.isFinite(n) ? n : Number.NaN;
      };
    case 'unix-s':
      return (value: string) => {
        const n = Number(value.trim());
        return Number.isFinite(n) ? n * MS_PER_SECOND : Number.NaN;
      };
    case 'date':
      return (value: string) => parseIsoLike(value, offsetMinutes, { allowTime: false });
    case 'sql':
      return (value: string) => parseIsoLike(value, offsetMinutes, { separator: ' ' });
    default:
      return (value: string) => parseIsoLike(value, offsetMinutes, { separator: 'T' });
  }
}

interface IsoParseOptions {
  separator?: 'T' | ' ';
  allowTime?: boolean;
}

function parseIsoLike(value: string, defaultOffsetMinutes: number, opts: IsoParseOptions): number {
  const trimmed = value.trim();
  if (trimmed.length < 10) return Number.NaN;

  const sep = opts.separator ?? 'T';
  const sepIdx = trimmed.indexOf(sep);
  const hasTime = sepIdx !== -1;
  if (opts.allowTime === false && hasTime) return Number.NaN;

  // Date portion YYYY-MM-DD
  const year = toInt(trimmed, 0, 4);
  const month = expectChar(trimmed, 4, '-') ? toInt(trimmed, 5, 7) : Number.NaN;
  const day = expectChar(trimmed, 7, '-') ? toInt(trimmed, 8, 10) : Number.NaN;
  if (!isValidDateParts(year, month, day)) return Number.NaN;

  let hour = 0;
  let minute = 0;
  let second = 0;
  let millis = 0;
  let tzOffsetMinutes = defaultOffsetMinutes;

  if (hasTime) {
    let timePos = sepIdx + 1;
    hour = toInt(trimmed, timePos, timePos + 2);
    minute = expectChar(trimmed, timePos + 2, ':')
      ? toInt(trimmed, timePos + 3, timePos + 5)
      : Number.NaN;
    timePos += 5;

    if (Number.isNaN(hour) || Number.isNaN(minute)) return Number.NaN;

    // Optional seconds
    if (trimmed[timePos] === ':') {
      second = toInt(trimmed, timePos + 1, timePos + 3);
      timePos += 3;
      if (Number.isNaN(second)) return Number.NaN;

      // Optional fractional seconds
      if (trimmed[timePos] === '.') {
        let fracEnd = timePos + 1;
        while (
          fracEnd < trimmed.length &&
          isDigit(trimmed.charCodeAt(fracEnd)) &&
          fracEnd - timePos <= 4
        ) {
          fracEnd++;
        }
        millis = toInt(trimmed, timePos + 1, fracEnd);
        const digits = fracEnd - (timePos + 1);
        if (Number.isNaN(millis)) return Number.NaN;
        if (digits === 1) millis *= 100;
        else if (digits === 2) millis *= 10;
        else if (digits > 3) millis = Math.floor(millis / 10 ** (digits - 3));
        timePos = fracEnd;
      }
    }

    // Time zone
    const tzChar = trimmed[timePos];
    if (tzChar === 'Z' || tzChar === 'z') {
      tzOffsetMinutes = 0;
    } else if (tzChar === '+' || tzChar === '-') {
      const sign = tzChar === '-' ? -1 : 1;
      const tzHour = toInt(trimmed, timePos + 1, timePos + 3);
      const hasColon = trimmed[timePos + 3] === ':';
      const tzMinStart = hasColon ? timePos + 4 : timePos + 3;
      const tzMinute = toInt(trimmed, tzMinStart, tzMinStart + 2);
      if (Number.isNaN(tzHour) || Number.isNaN(tzMinute)) return Number.NaN;
      tzOffsetMinutes = sign * (tzHour * 60 + tzMinute);
    }
  }

  const epochMs = Date.UTC(year, month - 1, day, hour, minute, second, millis);
  if (!Number.isFinite(epochMs)) return Number.NaN;
  return epochMs - tzOffsetMinutes * MS_PER_MINUTE;
}

function toInt(str: string, start: number, end: number): number {
  if (start < 0 || end > str.length || start >= end) return Number.NaN;
  let value = 0;
  for (let i = start; i < end; i++) {
    const code = str.charCodeAt(i);
    if (code < 48 || code > 57) return Number.NaN;
    value = value * 10 + (code - 48);
  }
  return value;
}

function expectChar(str: string, pos: number, expected: string): boolean {
  return str[pos] === expected;
}

function isDigit(code: number): boolean {
  return code >= 48 && code <= 57;
}

function isValidDateParts(year: number, month: number, day: number): boolean {
  if (Number.isNaN(year) || Number.isNaN(month) || Number.isNaN(day)) return false;
  if (month < 1 || month > 12 || day < 1 || day > 31) return false;
  return true;
}
