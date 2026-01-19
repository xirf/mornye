/**
 * CSV parsing options.
 */
export interface CsvOptions {
  /** Column delimiter (default: ",") */
  delimiter?: string;

  /** Quote character (default: '"') */
  quote?: string;

  /** Whether first row is header (default: true) */
  hasHeader?: boolean;

  /** Whether to auto-detect column types (default: true) */
  inferTypes?: boolean;

  /** Number of rows to sample for type inference (default: 100) */
  sampleRows?: number;

  /** Maximum rows to read (default: Infinity) */
  maxRows?: number;

  /** Whether to track parse errors (default: true) */
  trackErrors?: boolean;

  /** AbortSignal to cancel long-running reads */
  signal?: AbortSignal;

  /** Datetime parsing options (column-specific, optional) */
  datetime?: DateTimeOptions;

  /**
   * Per-task memory limit in bytes (optional).
   *
   * When set, readCsv will check file size before loading and return
   * a memoryError in the result if the estimated memory usage exceeds this limit.
   *
   * If not set, falls back to global config (see configure()).
   *
   * @example
   * ```ts
   * const { df, memoryError } = await readCsv('./large.csv', {
   *   memoryLimitBytes: 100 * 1024 * 1024, // 100MB limit
   * });
   * ```
   */
  memoryLimitBytes?: number;
}

export type DateTimeFormat = 'iso' | 'sql' | 'date' | 'unix-ms' | 'unix-s';

export interface DateTimeColumnOptions {
  /** Format identifier (default: 'iso'). */
  format?: DateTimeFormat;
  /** Explicit zone/offset for values without an offset (e.g., 'UTC' or '+02:00'). */
  zone?: string;
}

export interface DateTimeOptions {
  /** Column-level datetime parsing configuration. */
  columns?: Record<string, DateTimeColumnOptions>;
  /** Default zone for columns without an explicit zone (default: UTC). */
  defaultZone?: string;
}

/** Default CSV options */
export const DEFAULT_CSV_OPTIONS = {
  delimiter: ',',
  quote: '"',
  hasHeader: true,
  inferTypes: true,
  sampleRows: 100,
  maxRows: Number.POSITIVE_INFINITY,
  trackErrors: true,
  signal: undefined as AbortSignal | undefined,
  datetime: {
    columns: {},
    defaultZone: 'UTC',
  } satisfies DateTimeOptions,
} as const;

/** Resolved CSV options with byte codes for internal use */
export interface ResolvedCsvOptions {
  delimiter: number;
  quote: number;
  hasHeader: boolean;
  inferTypes: boolean;
  sampleRows: number;
  maxRows: number;
  trackErrors: boolean;
  signal: AbortSignal | undefined;
  datetime: ResolvedDateTimeOptions;
}

/** Convert user-facing options to internal byte-based options */
export function resolveOptions(options?: CsvOptions): ResolvedCsvOptions {
  const opts = { ...DEFAULT_CSV_OPTIONS, ...options };
  return {
    delimiter: opts.delimiter.charCodeAt(0),
    quote: opts.quote.charCodeAt(0),
    hasHeader: opts.hasHeader,
    inferTypes: opts.inferTypes,
    sampleRows: opts.sampleRows,
    maxRows: opts.maxRows,
    trackErrors: opts.trackErrors,
    signal: opts.signal,
    datetime: resolveDateTimeOptions(opts.datetime ?? DEFAULT_CSV_OPTIONS.datetime),
  };
}

export interface ResolvedDateTimeColumn {
  format: DateTimeFormat;
  offsetMinutes: number;
}

export interface ResolvedDateTimeOptions {
  columns: Map<string, ResolvedDateTimeColumn>;
  defaultOffsetMinutes: number;
}

function resolveDateTimeOptions(options?: DateTimeOptions): ResolvedDateTimeOptions {
  const defaultOffsetMinutes = resolveOffsetMinutes(options?.defaultZone ?? 'UTC');
  const columns = new Map<string, ResolvedDateTimeColumn>();

  if (options?.columns) {
    for (const [name, cfg] of Object.entries(options.columns)) {
      const format = cfg?.format ?? 'iso';
      const offsetMinutes = resolveOffsetMinutes(cfg?.zone, defaultOffsetMinutes);
      columns.set(name, { format, offsetMinutes });
    }
  }

  return { columns, defaultOffsetMinutes };
}

function resolveOffsetMinutes(zone?: string, fallback = 0): number {
  if (!zone || zone.toUpperCase() === 'UTC' || zone === 'Z') return 0;

  // Support basic fixed offsets like +02:00 or -0530
  const match = /^([+-])(\d{2}):?(\d{2})$/.exec(zone.trim());
  if (!match) return fallback;

  const sign = match[1] === '-' ? -1 : 1;
  const hours = Number(match[2]);
  const minutes = Number(match[3]);
  if (Number.isNaN(hours) || Number.isNaN(minutes)) return fallback;
  return sign * (hours * 60 + minutes);
}

/**
 * Byte constants for parsing.
 */
export const BYTES = {
  COMMA: 44,
  QUOTE: 34,
  CR: 13,
  LF: 10,
  TAB: 9,
} as const;
