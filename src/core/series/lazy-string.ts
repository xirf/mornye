import type { Buffer } from 'node:buffer';
import type { DTypeKind } from '../types';

/**
 * Lazy string column storage: offsets into a shared Buffer plus optional dictionary coding.
 */
export interface LazyStringColumn {
  readonly kind: 'lazy-string';
  buffer: Buffer;
  offsets: Uint32Array;
  lengths: Uint32Array;
  needsUnescape?: Uint8Array; // 1 when field needs "" -> " replacement
  cache: (string | null)[];
  // Optional dictionary coding (small-cardinality columns)
  dict?: string[];
  dictLookup?: Map<string, number>;
  codes?: Uint32Array;
}

export function createLazyStringColumn(
  buffer: Buffer,
  rowCount: number,
  useDictionary: boolean,
): LazyStringColumn {
  return {
    kind: 'lazy-string',
    buffer,
    offsets: useDictionary ? new Uint32Array(0) : new Uint32Array(rowCount),
    lengths: useDictionary ? new Uint32Array(0) : new Uint32Array(rowCount),
    needsUnescape: useDictionary ? undefined : new Uint8Array(rowCount),
    cache: new Array<string | null>(rowCount).fill(null),
    dict: useDictionary ? [] : undefined,
    dictLookup: useDictionary ? new Map<string, number>() : undefined,
    codes: useDictionary ? new Uint32Array(rowCount) : undefined,
  };
}

export function isLazyStringColumn(value: unknown): value is LazyStringColumn {
  return !!value && typeof value === 'object' && (value as LazyStringColumn).kind === 'lazy-string';
}
