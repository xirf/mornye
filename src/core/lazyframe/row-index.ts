/**
 * Row Index - Efficiently stores byte offsets for each row in a CSV file.
 */

import { IndexOutOfBoundsError } from '../../errors';

const SEGMENT_SIZE = 1_000_000;
const LF = 10;

/**
 * Builds and stores row byte offsets for lazy row access.
 */
export class RowIndex {
  readonly rowCount: number;
  private readonly _segments: Float64Array[];
  private readonly _fileSize: number;
  private readonly _dataStartOffset: number;

  private constructor(
    segments: Float64Array[],
    rowCount: number,
    fileSize: number,
    dataStartOffset: number,
  ) {
    this._segments = segments;
    this.rowCount = rowCount;
    this._fileSize = fileSize;
    this._dataStartOffset = dataStartOffset;
  }

  static async build(file: ReturnType<typeof Bun.file>, hasHeader: boolean): Promise<RowIndex> {
    const fileSize = file.size;
    const CHUNK_SIZE = 32 * 1024 * 1024; // 32MB chunks

    const segments: Float64Array[] = [];
    let currentSegment = new Float64Array(SEGMENT_SIZE);
    let countInSegment = 0;
    let totalRowCount = 0;

    const pushOffset = (offset: number) => {
      if (countInSegment === SEGMENT_SIZE) {
        segments.push(currentSegment);
        currentSegment = new Float64Array(SEGMENT_SIZE);
        countInSegment = 0;
      }
      currentSegment[countInSegment++] = offset;
      totalRowCount++;
    };

    pushOffset(0);

    let currentOffset = 0;
    while (currentOffset < fileSize) {
      const readSize = Math.min(CHUNK_SIZE, fileSize - currentOffset);
      const buffer = Buffer.from(
        await file.slice(currentOffset, currentOffset + readSize).arrayBuffer(),
      );

      let posInChunk = 0;
      while (true) {
        const idx = buffer.indexOf(LF, posInChunk);
        if (idx === -1) break;

        const globalIdx = currentOffset + idx + 1;
        if (globalIdx < fileSize) {
          pushOffset(globalIdx);
        }
        posInChunk = idx + 1;
      }
      currentOffset += readSize;
    }

    if (countInSegment > 0) {
      segments.push(currentSegment.slice(0, countInSegment));
    }

    const dataStartIndex = hasHeader ? 1 : 0;
    const dataStartOffset = segments[0] ? (segments[0]![dataStartIndex] ?? fileSize) : fileSize;

    let dataRowCount = Math.max(0, totalRowCount - dataStartIndex);

    // Skip trailing empty lines
    while (dataRowCount > 0) {
      const idx = dataStartIndex + dataRowCount - 1;
      const segIdx = Math.floor(idx / SEGMENT_SIZE);
      const offIdx = idx % SEGMENT_SIZE;
      const rowStart = segments[segIdx]![offIdx]!;

      if (rowStart < fileSize) break;
      dataRowCount--;
    }

    const finalSegments: Float64Array[] = [];
    let currentFinal = new Float64Array(SEGMENT_SIZE);
    let finalCount = 0;

    for (let i = 0; i < dataRowCount; i++) {
      const globalIdx = dataStartIndex + i;
      const segIdx = Math.floor(globalIdx / SEGMENT_SIZE);
      const offIdx = globalIdx % SEGMENT_SIZE;

      if (finalCount === SEGMENT_SIZE) {
        finalSegments.push(currentFinal);
        currentFinal = new Float64Array(SEGMENT_SIZE);
        finalCount = 0;
      }
      currentFinal[finalCount++] = segments[segIdx]![offIdx]!;
    }

    const endGlobalIdx = dataStartIndex + dataRowCount;
    const endSegIdx = Math.floor(endGlobalIdx / SEGMENT_SIZE);
    const endOffIdx = endGlobalIdx % SEGMENT_SIZE;
    const finalOffset = segments[endSegIdx]?.[endOffIdx] ?? fileSize;

    if (finalCount === SEGMENT_SIZE) {
      finalSegments.push(currentFinal);
      currentFinal = new Float64Array(SEGMENT_SIZE);
      finalCount = 0;
    }
    currentFinal[finalCount++] = finalOffset;

    if (finalCount > 0) {
      finalSegments.push(currentFinal.slice(0, finalCount));
    }

    return new RowIndex(finalSegments, dataRowCount, fileSize, dataStartOffset);
  }

  getRowOffset(rowIndex: number): number {
    if (rowIndex < 0 || rowIndex >= this.rowCount) {
      throw new IndexOutOfBoundsError(rowIndex, 0, this.rowCount - 1);
    }
    const segIdx = Math.floor(rowIndex / SEGMENT_SIZE);
    return this._segments[segIdx]![rowIndex % SEGMENT_SIZE]!;
  }

  getRowsRange(startRow: number, endRow: number): [number, number] {
    const start = this.getRowOffset(startRow);
    const segIdx = Math.floor(endRow / SEGMENT_SIZE);
    const offIdx = endRow % SEGMENT_SIZE;
    const end = this._segments[segIdx]?.[offIdx] ?? this._fileSize;
    return [start, end];
  }

  memoryUsage(): number {
    return this._segments.reduce((sum, seg) => sum + seg.byteLength, 0);
  }
}
