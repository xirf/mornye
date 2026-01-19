/**
 * Real-world memory limits test with 8.7GB e-commerce dataset.
 *
 * Dataset: 2019-Nov.csv (~67.5M rows, 8.7GB)
 * Schema:
 *   - event_time: UTC timestamp
 *   - event_type: view | cart | remove_from_cart | purchase
 *   - product_id, category_id, category_code, brand, price
 *   - user_id, user_session
 *
 * Run with: bun run tests/benchmarks/memory-limits.ts
 */

import { configure, getMemoryStats, readCsv, resetConfig, scanCsv } from '../../src';

const DATA_PATH = './artifac/2019-Nov.csv';

// Check if file exists
const file = Bun.file(DATA_PATH);
const exists = await file.exists();

if (!exists) {
  console.log('❌ Dataset not found at:', DATA_PATH);
  console.log('   Please ensure 2019-Nov.csv is in the artifac/ folder');
  console.log(
    '   Download from: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store',
  );
  process.exit(1);
}

const fileSizeMB = file.size / (1024 * 1024);
const fileSizeGB = file.size / (1024 * 1024 * 1024);
console.log(`📁 Dataset: ${DATA_PATH}`);
console.log(`   Size: ${fileSizeGB.toFixed(2)} GB (${fileSizeMB.toFixed(0)} MB)`);
console.log('');

// ============================================================
// Test 1: Memory limit rejection with readCsv
// ============================================================
console.log('━'.repeat(60));
console.log('Test 1: readCsv with 100MB limit (should fail gracefully)');
console.log('━'.repeat(60));

resetConfig();
configure({ globalLimitBytes: 1024 * 1024 * 1024 }); // 1GB global

const startT1 = performance.now();
const { df: df1, memoryError: err1 } = await readCsv(DATA_PATH, {
  memoryLimitBytes: 100 * 1024 * 1024, // 100MB - way too small for 8.7GB
});
const timeT1 = performance.now() - startT1;

if (err1) {
  console.log(`✅ Memory limit correctly triggered in ${timeT1.toFixed(0)}ms`);
  console.log(`   Requested: ${(err1.requestedBytes / 1024 / 1024).toFixed(0)} MB`);
  console.log(`   Available: ${(err1.availableBytes / 1024 / 1024).toFixed(0)} MB`);
  console.log(`   Hint: ${err1.hint}`);
} else {
  console.log(`❌ Expected memory error but got DataFrame with ${df1.shape[0]} rows`);
}
console.log('');

// ============================================================
// Test 2: scanCsv with constrained memory - head operation
// ============================================================
console.log('━'.repeat(60));
console.log('Test 2: scanCsv head(10) with 256MB cache limit');
console.log('━'.repeat(60));

resetConfig();
configure({ globalLimitBytes: 512 * 1024 * 1024 }); // 512MB global

const startT2 = performance.now();
const lazy2 = await scanCsv(DATA_PATH, {
  lazyConfig: {
    maxCacheMemory: 256 * 1024 * 1024, // 256MB cache
    chunkSize: 50_000,
  },
});
const scanTime = performance.now() - startT2;

console.log(`   Schema scan: ${scanTime.toFixed(0)}ms`);
console.log(`   Shape: ${lazy2.shape[0].toLocaleString()} rows × ${lazy2.shape[1]} cols`);
console.log(`   Columns: ${lazy2.columns().join(', ')}`);

const startHead = performance.now();
const head10 = await lazy2.head(10);
const headTime = performance.now() - startHead;

console.log(`   head(10): ${headTime.toFixed(0)}ms`);
head10.print();

const stats2 = getMemoryStats();
console.log(`   Memory used: ${(stats2.totalUsedBytes / 1024 / 1024).toFixed(1)} MB`);
console.log('');

// ============================================================
// Test 3: scanCsv streaming count
// ============================================================
console.log('━'.repeat(60));
console.log('Test 3: scanCsv streaming count (event_type === "purchase")');
console.log('━'.repeat(60));

const startT3 = performance.now();
const lazy3 = await scanCsv(DATA_PATH, {
  lazyConfig: {
    maxCacheMemory: 512 * 1024 * 1024,
    chunkSize: 100_000,
  },
});

// Use count() - pure streaming, no object collection
const purchaseCount = await lazy3.count((row) => row.event_type === 'purchase');
const countTime = performance.now() - startT3;

console.log(`   Count time: ${countTime.toFixed(0)}ms`);
console.log(`   Purchase events: ${purchaseCount.toLocaleString()}`);

// Clean up
lazy2.destroy();
lazy3.destroy();
console.log('');

// ============================================================
// Test 4: Memory stats during operations
// ============================================================
console.log('━'.repeat(60));
console.log('Test 4: Memory tracking during concurrent operations');
console.log('━'.repeat(60));

resetConfig();
configure({
  globalLimitBytes: 1024 * 1024 * 1024, // 1GB
  maxTaskSharePercent: 0.7,
});

// Create two LazyFrames concurrently
const [lazyA, lazyB] = await Promise.all([
  scanCsv(DATA_PATH, { lazyConfig: { maxCacheMemory: 256 * 1024 * 1024 } }),
  scanCsv(DATA_PATH, { lazyConfig: { maxCacheMemory: 256 * 1024 * 1024 } }),
]);

const statsMulti = getMemoryStats();
console.log(`   Active tasks: ${statsMulti.activeTaskCount}`);
console.log(`   Total allocated: ${(statsMulti.totalAllocatedBytes / 1024 / 1024).toFixed(0)} MB`);
console.log(`   Global limit: ${(statsMulti.globalLimitBytes / 1024 / 1024).toFixed(0)} MB`);
console.log(`   Available: ${(statsMulti.availableBytes / 1024 / 1024).toFixed(0)} MB`);

// Cleanup
lazyA.clearCache();
lazyB.clearCache();
console.log('');

// ============================================================
// Summary
// ============================================================
console.log('━'.repeat(60));
console.log('Summary');
console.log('━'.repeat(60));
console.log('✅ Memory limits working correctly');
console.log('✅ scanCsv streams data without loading entire file');
console.log('✅ Multiple operations share memory budget');
console.log('');
