/**
 * Join operations for DataFrames
 * All operations work directly on Uint8Array buffers for maximum performance
 */

import { enableNullTracking, getColumnValue, setColumnValue } from '../core/column';
import { getDTypeSize } from '../types/dtypes';
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
 * Join type for merge operations
 */
export type JoinType = 'inner' | 'left' | 'right' | 'outer';

/**
 * Join two DataFrames on their index (row numbers)
 * This is a convenience wrapper around merge() that joins on row position
 *
 * @param left - Left DataFrame
 * @param right - Right DataFrame
 * @param options - Join options
 * @returns Result with joined DataFrame or error
 */
export function join(
  left: DataFrame,
  right: DataFrame,
  options?: {
    /** Join type */
    how?: JoinType;
    /** Suffixes for overlapping column names [left_suffix, right_suffix] */
    suffixes?: [string, string];
  },
): Result<DataFrame, Error> {
  const how = options?.how ?? 'left';
  const suffixes = options?.suffixes ?? ['_x', '_y'];

  // For index-based join, we need to create temporary index columns
  const leftRowCount = getRowCount(left);
  const rightRowCount = getRowCount(right);

  // Add temporary index column to both DataFrames
  const leftWithIndex = createDataFrame();
  leftWithIndex.dictionary = left.dictionary;

  // Add index column
  const leftIndexResult = addColumn(leftWithIndex, '__index__', 'int32', leftRowCount);
  if (!leftIndexResult.ok) {
    return err(new Error(leftIndexResult.error));
  }

  // Copy all existing columns
  for (const colName of getColumnNames(left)) {
    const col = getColumn(left, colName);
    if (!col.ok) continue;

    const addResult = addColumn(leftWithIndex, colName, col.data.dtype, leftRowCount);
    if (!addResult.ok) continue;

    const destCol = getColumn(leftWithIndex, colName);
    if (!destCol.ok) continue;

    const bytesPerElement = col.data.data.byteLength / col.data.length;
    for (let b = 0; b < col.data.data.byteLength; b++) {
      destCol.data.data[b] = col.data.data[b]!;
    }

    if (col.data.nullBitmap) {
      enableNullTracking(destCol.data);
      if (destCol.data.nullBitmap) {
        for (let b = 0; b < col.data.nullBitmap.data.byteLength; b++) {
          destCol.data.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  // Fill index column
  const leftIndexCol = getColumn(leftWithIndex, '__index__');
  if (leftIndexCol.ok) {
    for (let i = 0; i < leftRowCount; i++) {
      setColumnValue(leftIndexCol.data, i, i);
    }
  }

  // Same for right DataFrame
  const rightWithIndex = createDataFrame();
  rightWithIndex.dictionary = right.dictionary;

  const rightIndexResult = addColumn(rightWithIndex, '__index__', 'int32', rightRowCount);
  if (!rightIndexResult.ok) {
    return err(new Error(rightIndexResult.error));
  }

  for (const colName of getColumnNames(right)) {
    const col = getColumn(right, colName);
    if (!col.ok) continue;

    const addResult = addColumn(rightWithIndex, colName, col.data.dtype, rightRowCount);
    if (!addResult.ok) continue;

    const destCol = getColumn(rightWithIndex, colName);
    if (!destCol.ok) continue;

    for (let b = 0; b < col.data.data.byteLength; b++) {
      destCol.data.data[b] = col.data.data[b]!;
    }

    if (col.data.nullBitmap) {
      enableNullTracking(destCol.data);
      if (destCol.data.nullBitmap) {
        for (let b = 0; b < col.data.nullBitmap.data.byteLength; b++) {
          destCol.data.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  const rightIndexCol = getColumn(rightWithIndex, '__index__');
  if (rightIndexCol.ok) {
    for (let i = 0; i < rightRowCount; i++) {
      setColumnValue(rightIndexCol.data, i, i);
    }
  }

  // Perform merge on index columns
  const mergeResult = merge(leftWithIndex, rightWithIndex, {
    on: '__index__',
    how,
    suffixes,
  });

  if (!mergeResult.ok) {
    return mergeResult;
  }

  // Remove the temporary index column from result
  const resultDf = createDataFrame();
  resultDf.dictionary = mergeResult.data.dictionary;

  const resultRowCount = getRowCount(mergeResult.data);
  for (const colName of getColumnNames(mergeResult.data)) {
    if (colName === '__index__') continue; // Skip index column

    const col = getColumn(mergeResult.data, colName);
    if (!col.ok) continue;

    const addResult = addColumn(resultDf, colName, col.data.dtype, resultRowCount);
    if (!addResult.ok) continue;

    const destCol = getColumn(resultDf, colName);
    if (!destCol.ok) continue;

    for (let b = 0; b < col.data.data.byteLength; b++) {
      destCol.data.data[b] = col.data.data[b]!;
    }

    if (col.data.nullBitmap) {
      enableNullTracking(destCol.data);
      if (destCol.data.nullBitmap) {
        for (let b = 0; b < col.data.nullBitmap.data.byteLength; b++) {
          destCol.data.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  return ok(resultDf);
}

/**
 * Merge two DataFrames using SQL-style joins
 * Returns a new DataFrame with rows from both DataFrames matched on key columns
 *
 * @param left - Left DataFrame
 * @param right - Right DataFrame
 * @param options - Merge options
 * @returns Result with merged DataFrame or error
 */
export function merge(
  left: DataFrame,
  right: DataFrame,
  options: {
    /** Column name(s) to join on */
    on?: string | string[];
    /** Left DataFrame column(s) to join on */
    leftOn?: string | string[];
    /** Right DataFrame column(s) to join on */
    rightOn?: string | string[];
    /** Join type */
    how?: JoinType;
    /** Suffixes for overlapping column names [left_suffix, right_suffix] */
    suffixes?: [string, string];
  },
): Result<DataFrame, Error> {
  const how = options.how ?? 'inner';
  const suffixes = options.suffixes ?? ['_x', '_y'];

  // Determine join keys
  let leftKeys: string[];
  let rightKeys: string[];

  if (options.on) {
    const onKeys = Array.isArray(options.on) ? options.on : [options.on];
    leftKeys = onKeys;
    rightKeys = onKeys;
  } else if (options.leftOn && options.rightOn) {
    leftKeys = Array.isArray(options.leftOn) ? options.leftOn : [options.leftOn];
    rightKeys = Array.isArray(options.rightOn) ? options.rightOn : [options.rightOn];
  } else {
    return err(new Error('Must specify either "on" or both "leftOn" and "rightOn"'));
  }

  if (leftKeys.length !== rightKeys.length) {
    return err(new Error('leftOn and rightOn must have same length'));
  }

  // Validate join keys exist
  for (const key of leftKeys) {
    const col = getColumn(left, key);
    if (!col.ok) {
      return err(new Error(`Left DataFrame missing join key: '${key}'`));
    }
  }

  for (const key of rightKeys) {
    const col = getColumn(right, key);
    if (!col.ok) {
      return err(new Error(`Right DataFrame missing join key: '${key}'`));
    }
  }

  // Build hash map for right DataFrame (for efficient lookup)
  const rightRowCount = getRowCount(right);
  const rightHashMap = new Map<string, number[]>();

  for (let rightRow = 0; rightRow < rightRowCount; rightRow++) {
    const keyValues: (number | bigint)[] = [];
    let hasNull = false;

    for (const rightKey of rightKeys) {
      const colResult = getColumn(right, rightKey);
      if (!colResult.ok) continue;

      const col = colResult.data;

      // Check for null
      if (col.nullBitmap && isNull(col.nullBitmap, rightRow)) {
        hasNull = true;
        break;
      }

      const value = getColumnValue(col, rightRow);
      if (value === undefined) {
        hasNull = true;
        break;
      }
      keyValues.push(value);
    }

    // Skip rows with null keys
    if (hasNull) continue;

    const hashKey = keyValues.join('|');
    if (!rightHashMap.has(hashKey)) {
      rightHashMap.set(hashKey, []);
    }
    rightHashMap.get(hashKey)!.push(rightRow);
  }

  // Find matching rows
  const leftRowCount = getRowCount(left);
  const matches: Array<{ leftRow: number; rightRow: number | null }> = [];

  for (let leftRow = 0; leftRow < leftRowCount; leftRow++) {
    const keyValues: (number | bigint)[] = [];
    let hasNull = false;

    for (const leftKey of leftKeys) {
      const colResult = getColumn(left, leftKey);
      if (!colResult.ok) continue;

      const col = colResult.data;

      // Check for null
      if (col.nullBitmap && isNull(col.nullBitmap, leftRow)) {
        hasNull = true;
        break;
      }

      const value = getColumnValue(col, leftRow);
      if (value === undefined) {
        hasNull = true;
        break;
      }
      keyValues.push(value);
    }

    const hashKey = keyValues.join('|');
    const rightMatches = hasNull ? [] : (rightHashMap.get(hashKey) ?? []);

    if (rightMatches.length > 0) {
      // Inner/Left/Outer: Add all matches
      for (const rightRow of rightMatches) {
        matches.push({ leftRow, rightRow });
      }
    } else if (how === 'left' || how === 'outer') {
      // Left/Outer join: Keep left row with null right side
      matches.push({ leftRow, rightRow: null });
    }
  }

  // Right/Outer join: Add unmatched right rows
  if (how === 'right' || how === 'outer') {
    const matchedRightRows = new Set(
      matches.map((m) => m.rightRow).filter((r) => r !== null) as number[],
    );

    for (let rightRow = 0; rightRow < rightRowCount; rightRow++) {
      if (!matchedRightRows.has(rightRow)) {
        matches.push({ leftRow: -1, rightRow });
      }
    }
  }

  // Build result DataFrame
  const resultDf = createDataFrame();
  resultDf.dictionary = left.dictionary; // Share dictionary (will merge if needed)

  const resultRowCount = matches.length;

  // Get all column names
  const leftColumns = getColumnNames(left);
  const rightColumns = getColumnNames(right);

  // Determine which columns overlap (excluding join keys)
  const rightKeySet = new Set(rightKeys);
  const leftColumnSet = new Set(leftColumns);
  const overlappingCols = rightColumns.filter(
    (col) => !rightKeySet.has(col) && leftColumnSet.has(col),
  );

  // Add left columns
  for (const colName of leftColumns) {
    const sourceColResult = getColumn(left, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, colName, sourceCol.dtype, resultRowCount);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    if (sourceCol.nullBitmap) {
      enableNullTracking(destCol);
    }

    const bytesPerElement = getDTypeSize(sourceCol.dtype);

    // Copy data from left DataFrame
    for (let i = 0; i < resultRowCount; i++) {
      const { leftRow } = matches[i]!;

      if (leftRow === -1) {
        // No left match (right join only)
        if (destCol.nullBitmap) {
          setNull(destCol.nullBitmap, i);
        }
      } else {
        // Copy value
        const sourceOffset = leftRow * bytesPerElement;
        const destOffset = i * bytesPerElement;

        for (let b = 0; b < bytesPerElement; b++) {
          destCol.data[destOffset + b] = sourceCol.data[sourceOffset + b]!;
        }

        // Copy null status
        if (sourceCol.nullBitmap && destCol.nullBitmap) {
          if (isNull(sourceCol.nullBitmap, leftRow)) {
            setNull(destCol.nullBitmap, i);
          }
        }
      }
    }
  }

  // Add right columns (excluding join keys and with suffix handling)
  for (const colName of rightColumns) {
    // Skip join keys (already in result from left)
    if (rightKeySet.has(colName)) {
      continue;
    }

    // Determine final column name (add suffix if overlapping)
    const finalName = overlappingCols.includes(colName) ? `${colName}${suffixes[1]}` : colName;

    // If overlapping, also rename the left column
    if (overlappingCols.includes(colName)) {
      // Rename left column by recreating it
      const leftColResult = getColumn(resultDf, colName);
      if (leftColResult.ok) {
        const leftCol = leftColResult.data;
        const newLeftName = `${colName}${suffixes[0]}`;

        // Remove old column and add with new name
        const leftColIndex = resultDf.columnOrder.indexOf(colName);
        resultDf.columns.delete(colName);
        resultDf.columnOrder[leftColIndex] = newLeftName;
        resultDf.columns.set(newLeftName, leftCol);
        leftCol.name = newLeftName;
      }
    }

    const sourceColResult = getColumn(right, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, finalName, sourceCol.dtype, resultRowCount);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, finalName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // Enable null tracking if source has it, OR if doing outer join (will have nulls)
    if (sourceCol.nullBitmap || how === 'left' || how === 'outer') {
      enableNullTracking(destCol);
    }

    const bytesPerElement = getDTypeSize(sourceCol.dtype);

    // Copy data from right DataFrame
    for (let i = 0; i < resultRowCount; i++) {
      const { rightRow } = matches[i]!;

      if (rightRow === null) {
        // No right match (left join only)
        if (destCol.nullBitmap) {
          setNull(destCol.nullBitmap, i);
        }
      } else {
        // Copy value
        const sourceOffset = rightRow * bytesPerElement;
        const destOffset = i * bytesPerElement;

        for (let b = 0; b < bytesPerElement; b++) {
          destCol.data[destOffset + b] = sourceCol.data[sourceOffset + b]!;
        }

        // Copy null status
        if (sourceCol.nullBitmap && destCol.nullBitmap) {
          if (isNull(sourceCol.nullBitmap, rightRow)) {
            setNull(destCol.nullBitmap, i);
          }
        }
      }
    }
  }

  return ok(resultDf);
}

/**
 * Concatenate DataFrames vertically (row-wise) or horizontally (column-wise)
 * Returns a new DataFrame with combined data
 *
 * @param dfs - Array of DataFrames to concatenate
 * @param options - Concatenation options
 * @returns Result with concatenated DataFrame or error
 */
export function concat(
  dfs: DataFrame[],
  options?: {
    /** Concatenation axis: 0=rows (vertical), 1=columns (horizontal) */
    axis?: 0 | 1;
    /** Whether to ignore index and reset it */
    ignoreIndex?: boolean;
  },
): Result<DataFrame, Error> {
  if (dfs.length === 0) {
    return err(new Error('Cannot concatenate empty array of DataFrames'));
  }

  if (dfs.length === 1) {
    return ok(dfs[0]!);
  }

  const axis = options?.axis ?? 0;

  if (axis === 0) {
    return concatRows(dfs);
  }
  return concatColumns(dfs);
}

/**
 * Concatenate DataFrames vertically (row-wise)
 */
function concatRows(dfs: DataFrame[]): Result<DataFrame, Error> {
  // Validate all DataFrames have same columns
  const firstCols = getColumnNames(dfs[0]!);
  const firstColSet = new Set(firstCols);

  for (let i = 1; i < dfs.length; i++) {
    const dfCols = getColumnNames(dfs[i]!);
    if (dfCols.length !== firstCols.length) {
      return err(new Error(`DataFrame ${i} has different number of columns`));
    }

    for (const col of dfCols) {
      if (!firstColSet.has(col)) {
        return err(new Error(`DataFrame ${i} has different columns`));
      }
    }
  }

  // Calculate total row count
  const totalRows = dfs.reduce((sum, df) => sum + getRowCount(df), 0);

  // Create result DataFrame
  const resultDf = createDataFrame();
  resultDf.dictionary = dfs[0]!.dictionary; // Share dictionary

  // For each column, concatenate all values
  for (const colName of firstCols) {
    const firstColResult = getColumn(dfs[0]!, colName);
    if (!firstColResult.ok) {
      return err(new Error(firstColResult.error));
    }

    const firstCol = firstColResult.data;
    const addResult = addColumn(resultDf, colName, firstCol.dtype, totalRows);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // Check if any source has null bitmap
    const hasNullBitmap = dfs.some((df) => {
      const col = getColumn(df, colName);
      return col.ok && col.data.nullBitmap !== undefined;
    });

    if (hasNullBitmap) {
      enableNullTracking(destCol);
    }

    const bytesPerElement = getDTypeSize(firstCol.dtype);
    let destRow = 0;

    // Copy from each DataFrame
    for (const df of dfs) {
      const sourceColResult = getColumn(df, colName);
      if (!sourceColResult.ok) {
        return err(new Error(sourceColResult.error));
      }

      const sourceCol = sourceColResult.data;
      const sourceRowCount = sourceCol.length;

      // Copy all rows
      for (let sourceRow = 0; sourceRow < sourceRowCount; sourceRow++) {
        const sourceOffset = sourceRow * bytesPerElement;
        const destOffset = destRow * bytesPerElement;

        // Copy bytes
        for (let b = 0; b < bytesPerElement; b++) {
          destCol.data[destOffset + b] = sourceCol.data[sourceOffset + b]!;
        }

        // Copy null status
        if (sourceCol.nullBitmap && destCol.nullBitmap) {
          if (isNull(sourceCol.nullBitmap, sourceRow)) {
            setNull(destCol.nullBitmap, destRow);
          }
        }

        destRow++;
      }
    }
  }

  return ok(resultDf);
}

/**
 * Concatenate DataFrames horizontally (column-wise)
 */
function concatColumns(dfs: DataFrame[]): Result<DataFrame, Error> {
  // Validate all DataFrames have same row count
  const firstRowCount = getRowCount(dfs[0]!);

  for (let i = 1; i < dfs.length; i++) {
    const rowCount = getRowCount(dfs[i]!);
    if (rowCount !== firstRowCount) {
      return err(new Error(`DataFrame ${i} has ${rowCount} rows, expected ${firstRowCount}`));
    }
  }

  // Check for duplicate column names
  const allColumns: string[] = [];
  for (const df of dfs) {
    allColumns.push(...getColumnNames(df));
  }

  const columnSet = new Set<string>();
  for (const col of allColumns) {
    if (columnSet.has(col)) {
      return err(new Error(`Duplicate column name: '${col}'`));
    }
    columnSet.add(col);
  }

  // Create result DataFrame
  const resultDf = createDataFrame();
  resultDf.dictionary = dfs[0]!.dictionary; // Share dictionary

  // Copy all columns from all DataFrames
  for (const df of dfs) {
    const columnNames = getColumnNames(df);

    for (const colName of columnNames) {
      const sourceColResult = getColumn(df, colName);
      if (!sourceColResult.ok) {
        return err(new Error(sourceColResult.error));
      }

      const sourceCol = sourceColResult.data;
      const addResult = addColumn(resultDf, colName, sourceCol.dtype, firstRowCount);
      if (!addResult.ok) {
        return err(new Error(addResult.error));
      }

      const destColResult = getColumn(resultDf, colName);
      if (!destColResult.ok) {
        return err(new Error(destColResult.error));
      }

      const destCol = destColResult.data;

      if (sourceCol.nullBitmap) {
        enableNullTracking(destCol);
      }

      const bytesPerElement = getDTypeSize(sourceCol.dtype);
      const totalBytes = firstRowCount * bytesPerElement;

      // Copy all data
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
    }
  }

  return ok(resultDf);
}
