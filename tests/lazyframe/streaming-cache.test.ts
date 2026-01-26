import { afterEach, describe, expect, test } from 'bun:test';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { LazyFrame } from '../../src/lazyframe/lazyframe';
import { DType } from '../../src/types/dtypes';

const CACHE_DIR = path.join(process.cwd(), '.molniya_cache');
const TEST_CACHE_DIR = path.join(process.cwd(), '.test_cache');

function ensureDirs(): void {
  if (!fs.existsSync(TEST_CACHE_DIR)) {
    fs.mkdirSync(TEST_CACHE_DIR, { recursive: true });
  }
  if (!fs.existsSync(CACHE_DIR)) {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
  }
}

afterEach(() => {
  if (fs.existsSync(TEST_CACHE_DIR)) {
    for (const file of fs.readdirSync(TEST_CACHE_DIR)) {
      fs.unlinkSync(path.join(TEST_CACHE_DIR, file));
    }
  }
  if (fs.existsSync(CACHE_DIR)) {
    for (const file of fs.readdirSync(CACHE_DIR)) {
      if (file.startsWith('stream-cache')) {
        fs.unlinkSync(path.join(CACHE_DIR, file));
      }
    }
  }
});

describe('LazyFrame streaming cache', () => {
  test('creates cache file on full scan', async () => {
    ensureDirs();

    const filePath = path.join(TEST_CACHE_DIR, 'stream-cache.csv');
    const csv = 'id,value\n1,2.5\n2,3.1\n3,4.2\n';
    fs.writeFileSync(filePath, csv);

    const schema = { id: DType.Int32, value: DType.Float64 };

    const result = await LazyFrame.scanCsv(filePath, schema).collect();
    expect(result).toBeDefined();

    const cacheFiles = fs
      .readdirSync(CACHE_DIR)
      .filter((file) => file.startsWith('stream-cache.csv.'));

    expect(cacheFiles.length).toBe(1);
  });
});
