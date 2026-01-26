import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { type BinaryBlock, readBinaryBlocks, writeBinaryBlocks } from '../../src/io/binary-format';
import { DType } from '../../src/types/dtypes';

const TEST_CACHE_DIR = path.join(process.cwd(), '.test_cache');

beforeEach(() => {
  // Create test cache directory
  if (!fs.existsSync(TEST_CACHE_DIR)) {
    fs.mkdirSync(TEST_CACHE_DIR, { recursive: true });
  }
});

afterEach(() => {
  // Cleanup test cache
  if (fs.existsSync(TEST_CACHE_DIR)) {
    fs.rmSync(TEST_CACHE_DIR, { recursive: true, force: true });
  }
});

describe('Binary Format - Block-Based .mbin', () => {
  describe('writeBinaryBlocks', () => {
    test('should write single block with numeric columns', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_numeric.mbin');

      const blocks: BinaryBlock[] = [
        {
          blockId: 0,
          rowCount: 3,
          columns: {
            id: {
              dtype: DType.Int32,
              data: new Int32Array([1, 2, 3]),
              hasNulls: false,
            },
            value: {
              dtype: DType.Float64,
              data: new Float64Array([1.5, 2.5, 3.5]),
              hasNulls: false,
            },
          },
        },
      ];

      await writeBinaryBlocks(testFile, blocks);

      // Verify file was created
      expect(fs.existsSync(testFile)).toBe(true);

      // Verify file has content
      const stats = fs.statSync(testFile);
      expect(stats.size).toBeGreaterThan(0);
    });

    test('should write multiple blocks (simulating 512KB batches)', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_multiple_blocks.mbin');

      const blocks: BinaryBlock[] = [
        {
          blockId: 0,
          rowCount: 2,
          columns: {
            id: {
              dtype: DType.Int32,
              data: new Int32Array([1, 2]),
              hasNulls: false,
            },
          },
        },
        {
          blockId: 1,
          rowCount: 2,
          columns: {
            id: {
              dtype: DType.Int32,
              data: new Int32Array([3, 4]),
              hasNulls: false,
            },
          },
        },
      ];

      await writeBinaryBlocks(testFile, blocks);

      const stats = fs.statSync(testFile);
      expect(stats.size).toBeGreaterThan(0);
    });

    test('should write block with inline strings (length-prefixed)', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_strings.mbin');

      const blocks: BinaryBlock[] = [
        {
          blockId: 0,
          rowCount: 3,
          columns: {
            name: {
              dtype: DType.String,
              data: ['Alice', 'Bob', 'Charlie'],
              hasNulls: false,
            },
            age: {
              dtype: DType.Int32,
              data: new Int32Array([25, 30, 35]),
              hasNulls: false,
            },
          },
        },
      ];

      await writeBinaryBlocks(testFile, blocks);

      expect(fs.existsSync(testFile)).toBe(true);
    });

    test('should handle null bitmaps when hasNulls=true', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_nulls.mbin');

      const blocks: BinaryBlock[] = [
        {
          blockId: 0,
          rowCount: 3,
          columns: {
            value: {
              dtype: DType.Float64,
              data: new Float64Array([1.0, 0.0, 3.0]), // 0.0 is placeholder for null
              hasNulls: true,
              nullBitmap: new Uint8Array([0b00000101]), // positions 0 and 2 are valid, 1 is null
            },
          },
        },
      ];

      await writeBinaryBlocks(testFile, blocks);

      expect(fs.existsSync(testFile)).toBe(true);
    });
  });

  describe('readBinaryBlocks', () => {
    test('should read single block with numeric columns', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_read_numeric.mbin');

      // Write test data
      const originalBlocks: BinaryBlock[] = [
        {
          blockId: 0,
          rowCount: 3,
          columns: {
            id: {
              dtype: DType.Int32,
              data: new Int32Array([1, 2, 3]),
              hasNulls: false,
            },
            value: {
              dtype: DType.Float64,
              data: new Float64Array([1.5, 2.5, 3.5]),
              hasNulls: false,
            },
          },
        },
      ];

      await writeBinaryBlocks(testFile, originalBlocks);

      // Read back
      const result = await readBinaryBlocks(testFile);

      expect(result.ok).toBe(true);
      if (!result.ok) throw new Error('Read failed');

      expect(result.data.blocks.length).toBe(1);
      expect(result.data.totalRows).toBe(3);

      const block = result.data.blocks[0];
      if (!block) throw new Error('No block');

      expect(block.rowCount).toBe(3);
      expect(block.columns.id?.dtype).toBe(DType.Int32);
      expect(block.columns.value?.dtype).toBe(DType.Float64);

      // Verify data integrity
      const idData = block.columns.id?.data as Int32Array;
      expect(Array.from(idData)).toEqual([1, 2, 3]);

      const valueData = block.columns.value?.data as Float64Array;
      expect(Array.from(valueData)).toEqual([1.5, 2.5, 3.5]);
    });

    test('should read multiple blocks', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_read_multiple.mbin');

      const originalBlocks: BinaryBlock[] = [
        {
          blockId: 0,
          rowCount: 2,
          columns: {
            id: {
              dtype: DType.Int32,
              data: new Int32Array([1, 2]),
              hasNulls: false,
            },
          },
        },
        {
          blockId: 1,
          rowCount: 2,
          columns: {
            id: {
              dtype: DType.Int32,
              data: new Int32Array([3, 4]),
              hasNulls: false,
            },
          },
        },
      ];

      await writeBinaryBlocks(testFile, originalBlocks);

      const result = await readBinaryBlocks(testFile);
      expect(result.ok).toBe(true);
      if (!result.ok) throw new Error('Read failed');

      expect(result.data.blocks.length).toBe(2);
      expect(result.data.totalRows).toBe(4);
    });

    test('should read inline strings correctly', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_read_strings.mbin');

      const originalBlocks: BinaryBlock[] = [
        {
          blockId: 0,
          rowCount: 3,
          columns: {
            name: {
              dtype: DType.String,
              data: ['Alice', 'Bob', 'Charlie'],
              hasNulls: false,
            },
          },
        },
      ];

      await writeBinaryBlocks(testFile, originalBlocks);

      const result = await readBinaryBlocks(testFile);
      expect(result.ok).toBe(true);
      if (!result.ok) throw new Error('Read failed');

      const block = result.data.blocks[0];
      if (!block) throw new Error('No block');

      const names = block.columns.name?.data as string[];
      expect(names).toEqual(['Alice', 'Bob', 'Charlie']);
    });

    test('should validate magic number and reject invalid files', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_invalid.mbin');

      // Write garbage data (at least HEADER_SIZE bytes to pass length check)
      const garbageData = Buffer.alloc(64, 0xff); // 64 bytes of 0xFF
      fs.writeFileSync(testFile, garbageData);

      const result = await readBinaryBlocks(testFile);
      expect(result.ok).toBe(false);
      if (result.ok) throw new Error('Should have failed');

      expect(result.error.message).toContain('magic');
    });

    test('should handle null bitmaps when reading', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_read_nulls.mbin');

      const originalBlocks: BinaryBlock[] = [
        {
          blockId: 0,
          rowCount: 3,
          columns: {
            value: {
              dtype: DType.Float64,
              data: new Float64Array([1.0, 0.0, 3.0]),
              hasNulls: true,
              nullBitmap: new Uint8Array([0b00000101]),
            },
          },
        },
      ];

      await writeBinaryBlocks(testFile, originalBlocks);

      const result = await readBinaryBlocks(testFile);
      expect(result.ok).toBe(true);
      if (!result.ok) throw new Error('Read failed');

      const block = result.data.blocks[0];
      if (!block) throw new Error('No block');

      expect(block.columns.value?.hasNulls).toBe(true);
      expect(block.columns.value?.nullBitmap).toBeDefined();
    });
  });

  describe('Round-trip tests', () => {
    test('should maintain data integrity for mixed column types', async () => {
      const testFile = path.join(TEST_CACHE_DIR, 'test_roundtrip.mbin');

      const originalBlocks: BinaryBlock[] = [
        {
          blockId: 0,
          rowCount: 100,
          columns: {
            id: {
              dtype: DType.Int32,
              data: new Int32Array(Array.from({ length: 100 }, (_, i) => i)),
              hasNulls: false,
            },
            value: {
              dtype: DType.Float64,
              data: new Float64Array(Array.from({ length: 100 }, (_, i) => i * 1.5)),
              hasNulls: false,
            },
            name: {
              dtype: DType.String,
              data: Array.from({ length: 100 }, (_, i) => `Person ${i}`),
              hasNulls: false,
            },
            active: {
              dtype: DType.Bool,
              data: new Uint8Array(Array.from({ length: 100 }, (_, i) => i % 2)),
              hasNulls: false,
            },
          },
        },
      ];

      // Write and read back
      const writeResult = await writeBinaryBlocks(testFile, originalBlocks);
      expect(writeResult.ok).toBe(true);

      const result = await readBinaryBlocks(testFile);

      if (!result.ok) {
        console.error('Round-trip read failed:', result.error.message);
      }

      expect(result.ok).toBe(true);
      if (!result.ok) throw new Error('Read failed');

      const block = result.data.blocks[0];
      if (!block) throw new Error('No block');

      // Verify all data matches
      expect(Array.from(block.columns.id?.data as Int32Array)).toEqual(
        Array.from(originalBlocks[0]!.columns.id?.data as Int32Array),
      );

      expect(Array.from(block.columns.value?.data as Float64Array)).toEqual(
        Array.from(originalBlocks[0]!.columns.value?.data as Float64Array),
      );

      expect(block.columns.name?.data).toEqual(originalBlocks[0]!.columns.name?.data);

      expect(Array.from(block.columns.active?.data as Uint8Array)).toEqual(
        Array.from(originalBlocks[0]!.columns.active?.data as Uint8Array),
      );
    });
  });
});
