/**
 * Type conversion operations for DataFrames
 * All operations work directly on Uint8Array buffers for maximum performance
 */

import { createColumn, enableNullTracking, getColumnValue } from '../core/column';
import { DType } from '../types/dtypes';
import { type Result, err, ok } from '../types/result';
import { isNull, setNull } from '../utils/nulls';
import {
  type DataFrame,
  addColumn,
  createDataFrame,
  getColumn,
  getColumnNames,
  getRowCount,
} from './dataframe';

/**
 * Convert column data types
 * Returns a new DataFrame with specified columns cast to new types
 *
 * @param df - Source DataFrame
 * @param dtypes - Object mapping column names to target dtypes
 * @returns Result with converted DataFrame or error
 */
export function astype(
  df: DataFrame,
  dtypes: Record<string, DType> | DType,
): Result<DataFrame, Error> {
  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary; // Share dictionary

  const allColumns = getColumnNames(df);
  const rowCount = getRowCount(df);

  // If dtypes is a single DType, apply to all columns
  const dtypeMap: Record<string, DType> =
    typeof dtypes === 'string'
      ? Object.fromEntries(allColumns.map((col) => [col, dtypes]))
      : dtypes;

  for (const colName of allColumns) {
    const sourceColResult = getColumn(df, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }

    const sourceCol = sourceColResult.data;
    const targetDType = dtypeMap[colName] ?? sourceCol.dtype;

    // Add column with target dtype
    const addResult = addColumn(resultDf, colName, targetDType, rowCount);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // Enable null tracking if source has it
    if (sourceCol.nullBitmap) {
      enableNullTracking(destCol);
    }

    // If dtypes are same, direct copy
    if (sourceCol.dtype === targetDType) {
      const sourceBytesPerElement = sourceCol.data.byteLength / sourceCol.length;
      const totalBytes = rowCount * sourceBytesPerElement;

      for (let b = 0; b < totalBytes; b++) {
        destCol.data[b] = sourceCol.data[b]!;
      }

      // Copy null bitmap
      if (sourceCol.nullBitmap && destCol.nullBitmap) {
        const bitmapBytes = sourceCol.nullBitmap.data.byteLength;
        for (let b = 0; b < bitmapBytes; b++) {
          destCol.nullBitmap.data[b] = sourceCol.nullBitmap.data[b]!;
        }
      }

      continue;
    }

    // Convert each value
    for (let row = 0; row < rowCount; row++) {
      // Check for null
      if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, row)) {
        if (destCol.nullBitmap) {
          setNull(destCol.nullBitmap, row);
        }
        continue;
      }

      const sourceValue = getColumnValue(sourceCol, row);
      if (sourceValue === undefined) {
        if (destCol.nullBitmap) {
          setNull(destCol.nullBitmap, row);
        }
        continue;
      }

      // Convert value
      const convertResult = convertValue(sourceValue, sourceCol.dtype, targetDType, df.dictionary);

      if (!convertResult.ok) {
        return err(
          new Error(`Failed to convert '${colName}' at row ${row}: ${convertResult.error}`),
        );
      }

      const convertedValue = convertResult.data;

      // Write converted value
      switch (targetDType) {
        case DType.Float64:
          destCol.view.setFloat64(row * 8, convertedValue as number, true);
          break;
        case DType.Int32:
        case DType.String: // Dictionary ID
          destCol.view.setInt32(row * 4, convertedValue as number, true);
          break;
        case DType.Bool:
          destCol.view.setUint8(row, convertedValue as number);
          break;
        case DType.DateTime:
        case DType.Date:
          destCol.view.setBigInt64(row * 8, convertedValue as bigint, true);
          break;
      }
    }
  }

  return ok(resultDf);
}

/**
 * Convert a single value from one dtype to another
 */
function convertValue(
  value: number | bigint,
  sourceDType: DType,
  targetDType: DType,
  dictionary?: { stringToId: Map<string, number>; idToString: Map<number, string> },
): Result<number | bigint, string> {
  // Float64 conversions
  if (targetDType === DType.Float64) {
    if (sourceDType === DType.Int32 || sourceDType === DType.Bool) {
      return ok(Number(value));
    }
    if (sourceDType === DType.DateTime || sourceDType === DType.Date) {
      return ok(Number(value));
    }
    if (sourceDType === DType.String) {
      // Cannot directly convert string to float without actual string value
      return err('Cannot convert String to Float64 without string value');
    }
    return ok(value as number);
  }

  // Int32 conversions
  if (targetDType === DType.Int32) {
    if (sourceDType === DType.Float64) {
      return ok(Math.trunc(value as number));
    }
    if (sourceDType === DType.Bool) {
      return ok(Number(value));
    }
    if (sourceDType === DType.DateTime || sourceDType === DType.Date) {
      return ok(Number(value));
    }
    if (sourceDType === DType.String) {
      return err('Cannot convert String to Int32 without string value');
    }
    return ok(value as number);
  }

  // Bool conversions
  if (targetDType === DType.Bool) {
    if (sourceDType === DType.Int32 || sourceDType === DType.Float64) {
      return ok((value as number) !== 0 ? 1 : 0);
    }
    if (sourceDType === DType.DateTime || sourceDType === DType.Date) {
      return ok((value as bigint) !== 0n ? 1 : 0);
    }
    if (sourceDType === DType.String) {
      return err('Cannot convert String to Bool without string value');
    }
    return ok(value as number);
  }

  // DateTime conversions
  if (targetDType === DType.DateTime || targetDType === DType.Date) {
    if (sourceDType === DType.Int32 || sourceDType === DType.Float64) {
      return ok(BigInt(Math.trunc(value as number)));
    }
    if (sourceDType === DType.Bool) {
      return ok(BigInt(value as number));
    }
    if (sourceDType === DType.String) {
      return err('Cannot convert String to DateTime without string value');
    }
    return ok(value as bigint);
  }

  // String conversions
  if (targetDType === DType.String) {
    if (!dictionary) {
      return err('Dictionary required for String conversion');
    }

    let strValue: string;

    if (sourceDType === DType.Float64) {
      strValue = (value as number).toString();
    } else if (sourceDType === DType.Int32) {
      strValue = (value as number).toString();
    } else if (sourceDType === DType.Bool) {
      strValue = (value as number) ? 'true' : 'false';
    } else if (sourceDType === DType.DateTime) {
      strValue = new Date(Number(value as bigint)).toISOString();
    } else if (sourceDType === DType.Date) {
      strValue = new Date(Number(value as bigint)).toISOString().split('T')[0]!;
    } else {
      // Already a string ID
      return ok(value as number);
    }

    // Add to dictionary if not present
    let dictId = dictionary.stringToId.get(strValue);
    if (dictId === undefined) {
      dictId = dictionary.idToString.size;
      dictionary.stringToId.set(strValue, dictId);
      dictionary.idToString.set(dictId, strValue);
    }

    return ok(dictId);
  }

  return err(`Unsupported conversion from ${sourceDType} to ${targetDType}`);
}
