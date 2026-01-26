import { afterEach, describe, expect, test } from 'bun:test';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { applyCacheRetention, cleanupStaleCacheFiles } from '../../src/lazyframe/executor/cache';

const TEST_CACHE_DIR = path.join(process.cwd(), '.test_cache_policy');
const TEST_DATA_DIR = path.join(process.cwd(), '.test_cache_policy_data');

function ensureDir(dirPath: string): void {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function writeFile(filePath: string, size = 1): void {
  const content = 'x'.repeat(Math.max(1, size));
  fs.writeFileSync(filePath, content);
}

afterEach(() => {
  for (const dir of [TEST_CACHE_DIR, TEST_DATA_DIR]) {
    if (fs.existsSync(dir)) {
      for (const file of fs.readdirSync(dir)) {
        fs.unlinkSync(path.join(dir, file));
      }
    }
  }
});

describe('Cache policy', () => {
  test('cleanupStaleCacheFiles removes stale cache entries', () => {
    ensureDir(TEST_CACHE_DIR);
    ensureDir(TEST_DATA_DIR);

    const csvPath = path.join(TEST_DATA_DIR, 'sample.csv');
    fs.writeFileSync(csvPath, 'id,value\n1,2\n');

    const baseName = path.basename(csvPath).replace(/[^a-zA-Z0-9._-]/g, '_');
    const mtime = Math.floor(fs.statSync(csvPath).mtimeMs);

    const staleCache = path.join(TEST_CACHE_DIR, `${baseName}.${mtime - 1000}.mbin`);
    const freshCache = path.join(TEST_CACHE_DIR, `${baseName}.${mtime}.mbin`);
    const keyedCache = path.join(TEST_CACHE_DIR, `${baseName}.${mtime}.key123.mbin`);

    writeFile(staleCache);
    writeFile(freshCache);
    writeFile(keyedCache);

    cleanupStaleCacheFiles(csvPath, TEST_CACHE_DIR);

    const remaining = fs.readdirSync(TEST_CACHE_DIR);
    expect(remaining.includes(path.basename(staleCache))).toBe(false);
    expect(remaining.includes(path.basename(freshCache))).toBe(true);
    expect(remaining.includes(path.basename(keyedCache))).toBe(true);
  });

  test('applyCacheRetention enforces age and size limits', () => {
    ensureDir(TEST_CACHE_DIR);

    const fileA = path.join(TEST_CACHE_DIR, 'a.mbin');
    const fileB = path.join(TEST_CACHE_DIR, 'b.mbin');
    const fileC = path.join(TEST_CACHE_DIR, 'c.mbin');

    writeFile(fileA, 100);
    writeFile(fileB, 100);
    writeFile(fileC, 100);

    const now = Date.now();
    fs.utimesSync(fileA, now / 1000 - 60, now / 1000 - 60); // old
    fs.utimesSync(fileB, now / 1000 - 10, now / 1000 - 10); // recent
    fs.utimesSync(fileC, now / 1000 - 5, now / 1000 - 5); // newest

    applyCacheRetention({
      cacheDir: TEST_CACHE_DIR,
      maxAgeMs: 30_000,
      maxSizeBytes: 150,
    });

    const remaining = fs.readdirSync(TEST_CACHE_DIR);

    // Oldest should be removed by age
    expect(remaining.includes('a.mbin')).toBe(false);

    // Size policy should remove additional oldest if still over limit
    const totalSize = remaining.reduce((sum, file) => {
      const stat = fs.statSync(path.join(TEST_CACHE_DIR, file));
      return sum + stat.size;
    }, 0);

    expect(totalSize).toBeLessThanOrEqual(150);
  });
});
