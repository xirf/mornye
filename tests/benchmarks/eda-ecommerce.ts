/**
 * Real-World EDA (Exploratory Data Analysis) with Memory Constraints
 *
 * Dataset: 2019-Nov.csv - E-commerce behavior data
 * ~67.5 million rows, 8.7GB
 *
 * This script demonstrates how to explore a massive dataset
 * without loading it all into memory.
 *
 * Run with: bun run tests/benchmarks/eda-ecommerce.ts
 */

import { configure, getMemoryStats, scanCsv } from '../../src';

const DATA_PATH = './artifac/2019-Nov.csv';

console.log('🛒 E-Commerce Event Data - Exploratory Data Analysis');
console.log('━'.repeat(60));
console.log('');

// Configure memory limits for server-like environment
configure({
  globalLimitBytes: 1024 * 1024 * 1024, // 1GB limit
  maxTaskSharePercent: 0.7,
});

// ============================================================
// Step 1: Quick Overview
// ============================================================
console.log('📊 Step 1: Dataset Overview');
console.log('─'.repeat(40));

const startScan = performance.now();
const lazy = await scanCsv(DATA_PATH, {
  lazyConfig: {
    maxCacheMemory: 256 * 1024 * 1024,
    chunkSize: 100_000,
  },
});
const scanTime = performance.now() - startScan;

const info = lazy.info();
console.log(`   Scan time: ${scanTime.toFixed(0)}ms`);
console.log(`   Rows: ${info.rows.toLocaleString()}`);
console.log(`   Columns: ${info.columns}`);
console.log('   Schema:');
for (const [col, dtype] of Object.entries(info.dtypes)) {
  console.log(`     - ${col}: ${dtype}`);
}
console.log('');

// ============================================================
// Step 2: Sample Data
// ============================================================
console.log('📋 Step 2: Sample Data (first 5 rows)');
console.log('─'.repeat(40));

const startHead = performance.now();
const sample = await lazy.head(5);
const headTime = performance.now() - startHead;
console.log(`   Load time: ${headTime.toFixed(0)}ms`);
sample.print();
console.log('');

// ============================================================
// Step 3: Event Type Distribution
// ============================================================
console.log('📈 Step 3: Event Type Distribution (Streaming Count)');
console.log('─'.repeat(40));

// Using streaming count - VERY fast and memory safe
const eventTypes = ['view', 'cart', 'remove_from_cart', 'purchase'] as const;
const eventCounts: Record<string, number> = {};

const startEvents = performance.now();
for (const eventType of eventTypes) {
  eventCounts[eventType] = await lazy.count((row) => row.event_type === eventType);
}
const eventTime = performance.now() - startEvents;

console.log(`   Analysis time: ${eventTime.toFixed(0)}ms`);
const totalEvents = Object.values(eventCounts).reduce((a, b) => a + b, 0);
for (const [type, count] of Object.entries(eventCounts)) {
  const pct = ((count / totalEvents) * 100).toFixed(1);
  console.log(`   ${type.padEnd(20)} ${count.toLocaleString().padStart(12)} (${pct}%)`);
}
console.log('');

// ============================================================
// Step 4: Purchase Analysis
// ============================================================
console.log('💰 Step 4: Purchase Analysis');
console.log('─'.repeat(40));

const startPurchase = performance.now();
const purchaseResult = await lazy.filter((row) => row.event_type === 'purchase');
const purchases = purchaseResult.data;
const purchaseTime = performance.now() - startPurchase;

if (!purchases) {
  console.log('❌ Failed to filter purchases: memory limit exceeded');
} else {
  if (purchaseResult.memoryError) {
    console.log('⚠️ Warning: result truncated due to memory limit');
  }
  console.log(`   Filter time: ${purchaseTime.toFixed(0)}ms`);
  console.log(`   Total purchases collected: ${purchases.shape[0].toLocaleString()}`);

  // Get price statistics
  const prices = purchases.col('price');
  console.log('   Price stats:');
  console.log(`     - Min:  $${prices.min()?.toFixed(2)}`);
  console.log(`     - Max:  $${prices.max()?.toFixed(2)}`);
  console.log(`     - Mean: $${prices.mean()?.toFixed(2)}`);
  console.log(`     - Sum:  $${prices.sum()?.toLocaleString()}`);
  // ============================================================
  // Step 5: Top Products by Purchase Count
  // ============================================================
  console.log('🏆 Step 5: Top 10 Products by Purchase Count');
  console.log('─'.repeat(40));

  const startTop = performance.now();
  // @ts-ignore - resolve groupby typing later
  const topProducts = purchases.groupby('product_id').count().sort('count', false).head(10);
  const topTime = performance.now() - startTop;

  console.log(`   Analysis time: ${topTime.toFixed(0)}ms`);
  topProducts.print();
  console.log('');
}

// ============================================================
// Step 6: Memory Usage Summary
// ============================================================
console.log('💾 Step 6: Memory Usage');
console.log('─'.repeat(40));

const stats = getMemoryStats();
console.log(`   Global limit: ${(stats.globalLimitBytes / 1024 / 1024).toFixed(0)} MB`);
console.log(`   Currently allocated: ${(stats.totalAllocatedBytes / 1024 / 1024).toFixed(0)} MB`);
console.log(`   Currently used: ${(stats.totalUsedBytes / 1024 / 1024).toFixed(0)} MB`);
console.log(`   Active tasks: ${stats.activeTaskCount}`);
console.log('');

// Cleanup
lazy.clearCache();

// ============================================================
// Summary
// ============================================================
console.log('━'.repeat(60));
console.log('✅ EDA Complete!');
console.log('');
console.log('Key insights from 8.7GB dataset:');
console.log(`   - ${totalEvents.toLocaleString()} total events analyzed`);
console.log(`   - ${eventCounts.purchase?.toLocaleString()} purchases`);
console.log('   - All processed with <1GB memory limit');
console.log('');
