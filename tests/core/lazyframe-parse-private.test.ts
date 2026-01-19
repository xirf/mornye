import { describe, expect, test } from 'bun:test';
import { RowIndex } from '../../src/core/lazyframe';
import { scanCsv } from '../../src/io/csv';

const PATH = './tests/fixtures/lazy-private.csv';

describe('LazyFrame private parsing helpers', () => {
  test('parseLine and parseValue cover quote and bool branches', async () => {
    const csv = 'id,name,flag\n1,"hello, world",True\n2,plain,false\n';
    await Bun.write(PATH, csv);

    const lazy = await scanCsv(PATH);
    const parseLine = (
      lazy as unknown as { _parseLine: (line: string) => string[] }
    )._parseLine.bind(lazy);
    const parseValue = (
      lazy as unknown as { _parseValue: (v: string, d: { kind: string }) => unknown }
    )._parseValue.bind(lazy);

    expect(parseLine('"a,b",c')).toEqual(['a,b', 'c']);
    expect(parseValue('True', { kind: 'bool' })).toBe(true);
    expect(parseValue('nope', { kind: 'bool' })).toBe(false);
  });

  test('RowIndex memory usage reflects segments', async () => {
    const csv = 'a\n1\n';
    const path = `${PATH}.tmp`;
    await Bun.write(path, csv);
    const idx = await RowIndex.build(Bun.file(path), false);
    expect(idx.memoryUsage()).toBeGreaterThan(0);
  });
});
