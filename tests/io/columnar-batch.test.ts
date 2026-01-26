import { describe, expect, test } from 'bun:test';
import { ColumnarBatchBuilder, DEFAULT_BATCH_BYTES } from '../../src/io/columnar-batch';
import { DType } from '../../src/types/dtypes';

describe('ColumnarBatchBuilder', () => {
  test('builds columnar batch with typed arrays', () => {
    const schema = {
      id: DType.Int32,
      value: DType.Float64,
      name: DType.String,
      active: DType.Bool,
    };

    const builder = new ColumnarBatchBuilder(schema, ['id', 'value', 'name', 'active'], 1);

    builder.appendValue(1);
    builder.appendValue(1.5);
    builder.appendValue('Alice');
    builder.appendValue(true);
    const batch = builder.endRow();

    expect(batch).not.toBeNull();
    if (!batch) return;

    expect(batch.rowCount).toBe(1);
    expect(batch.columns.id?.data).toBeInstanceOf(Int32Array);
    expect(batch.columns.value?.data).toBeInstanceOf(Float64Array);
    expect(batch.columns.active?.data).toBeInstanceOf(Uint8Array);
    expect(batch.columns.name?.data).toEqual(['Alice']);
  });

  test('tracks nulls with bitmap', () => {
    const schema = {
      id: DType.Int32,
      name: DType.String,
    };

    const builder = new ColumnarBatchBuilder(schema, ['id', 'name'], DEFAULT_BATCH_BYTES);

    builder.appendValue(null);
    builder.appendValue('Bob');
    const batch = builder.endRow();

    expect(batch).toBeNull();
    const finalBatch = builder.flush();
    expect(finalBatch).not.toBeNull();
    if (!finalBatch) return;

    const idColumn = finalBatch.columns.id;
    expect(idColumn?.hasNulls).toBe(true);
    expect(idColumn?.nullBitmap).toBeDefined();
    if (!idColumn?.nullBitmap) return;

    expect((idColumn.nullBitmap[0]! & 1) !== 0).toBe(true);
  });
});
