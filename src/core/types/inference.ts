import type { DType, DTypeKind } from './dtype';

/**
 * Mapping of DTypeKind to TypeScript definition.
 */
export interface DTypeToPrimitive {
  float64: number;
  int32: number;
  string: string;
  bool: boolean;
}

/**
 * Maps DType to its corresponding TypeScript type.
 * Used for compile-time type inference.
 */
export type InferDType<D extends DType<DTypeKind>> = DTypeToPrimitive[D['kind']];

/**
 * Maps DType to its underlying storage type.
 * Used for internal buffer representations.
 */
export type StorageType<T extends DTypeKind> = T extends 'float64'
  ? Float64Array
  : T extends 'int32'
    ? Int32Array
    : T extends 'string'
      ? string[]
      : T extends 'bool'
        ? Uint8Array
        : never;
