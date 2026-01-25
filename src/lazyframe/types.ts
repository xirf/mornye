import type { DType } from '../types/dtypes';

/**
 * Maps DType enum values to their corresponding TypeScript types
 */
export type DTypeToType<T extends DType> = T extends typeof DType.Int32
  ? number
  : T extends typeof DType.Float64
    ? number
    : T extends typeof DType.String
      ? string
      : T extends typeof DType.Bool
        ? boolean
        : T extends typeof DType.Date
          ? number
          : // Dates are typically stored as timestamps
            T extends typeof DType.DateTime
            ? number
            : // DateTime as timestamps
              never;

/**
 * Infers a row type from a Schema object (Record<string, DType>)
 */
export type InferSchemaType<S extends Record<string, DType>> = {
  [K in keyof S]: DTypeToType<S[K]>;
};

/**
 * Helper to pick columns from a schema type
 */
export type Select<T, K extends keyof T> = Pick<T, K>;
