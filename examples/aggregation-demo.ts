/**
 * Aggregation Functions Demo
 * Demonstrates both DataFrame and Series aggregation APIs
 */

import { count, fromArrays, max, mean, min, sum } from '../src/index';

console.log('='.repeat(70));
console.log('Aggregation Functions Demo - DataFrame & Series API');
console.log('='.repeat(70));

// Create sample trading data
const df = fromArrays({
  symbol: ['AAPL', 'AAPL', 'GOOGL', 'GOOGL', 'MSFT', 'MSFT'],
  price: [150.5, 152.0, 2800.0, 2750.0, 380.0, 385.0],
  volume: [1000000, 1200000, 500000, 480000, 800000, 850000],
  profit: [5000.0, 6000.0, 12000.0, 11000.0, 8000.0, 9000.0],
});

console.log('\n📊 Sample Trading Data:');
console.log(df);

// ============================================================================
// DataFrame API - Pandas-style aggregations
// ============================================================================

console.log(`\n${'='.repeat(70)}`);
console.log('DataFrame API (Pandas-style)');
console.log('='.repeat(70));

// Aggregate single column
console.log('\n1. Single column aggregations:');
console.log(`   Total volume: ${sum(df, 'volume').toLocaleString()}`);
console.log(`   Average price: $${mean(df, 'price').toFixed(2)}`);
console.log(`   Min profit: $${min(df, 'profit').toFixed(2)}`);
console.log(`   Max profit: $${max(df, 'profit').toFixed(2)}`);
console.log(`   Row count: ${count(df, 'price')}`);

// Aggregate all numeric columns
console.log('\n2. All numeric columns aggregation:');
const allSums = sum(df);
console.log('   Sum:', allSums);

const allMeans = mean(df);
console.log('   Mean:', allMeans);

const allMins = min(df);
console.log('   Min:', allMins);

const allMaxes = max(df);
console.log('   Max:', allMaxes);

// ============================================================================
// Series API - Chainable column operations
// ============================================================================

console.log(`\n${'='.repeat(70)}`);
console.log('Series API (Chainable - Coming Soon)');
console.log('='.repeat(70));

console.log('\n⚠️  Series API requires DataFrame.get() to return Series instance');
console.log('   This will be implemented in the next update.');
console.log('\n   Example usage (planned):');
console.log('   ```typescript');
console.log("   const prices = df.get('price');  // Returns Series");
console.log('   const avgPrice = prices.mean();');
console.log('   const minPrice = prices.min();');
console.log('   const maxPrice = prices.max();');
console.log('   const totalPrice = prices.sum();');
console.log('   ```');

// ============================================================================
// Performance Comparison
// ============================================================================

console.log(`\n${'='.repeat(70)}`);
console.log('Performance Notes');
console.log('='.repeat(70));

console.log('\n✅ All aggregations use raw DataView access for maximum performance');
console.log('✅ Zero object allocation in hot loops');
console.log('✅ Direct memory access via Uint8Array buffers');
console.log('✅ Both APIs share the same optimized backend');

console.log(`\n${'='.repeat(70)}`);
console.log('✅ Aggregation Demo Complete!');
console.log('='.repeat(70));
