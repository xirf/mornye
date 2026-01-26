# Molniya Roadmap

## 🚀 Top Priority: True Streaming & Lazy Evaluation

**Status:** 🟡 In Progress - Streaming path implemented for scan/filter/select/aggregate, missing joins/window/append-only

### Why This Matters
The current LazyFrame implementation has query plan optimization, column pruning, and predicate pushdown infrastructure, but the executor still calls `readCsv()` which loads all data into memory. This defeats the purpose of lazy evaluation and prevents working with datasets larger than available RAM.

### Critical Architectural Decisions

#### 1. Columnar Batches (Not Row Objects)
**Problem:** Creating millions of row objects `{ col: val }` causes severe GC pressure in V8/JavaScriptCore.  
**Solution:** Use **columnar batches** stored as `TypedArray` vectors `{ colName: Float64Array | Int32Array }`.  
**Benefits:** Cache-friendly, SIMD-compatible, minimal GC pressure.

#### 2. Forward-Only Streaming (v0.1.0)
**Problem:** Building line-offset index requires O(N) file scan upfront, blocking instant start.  
**Solution:** Pure forward-only streaming in v0.1.0. Random access (`tail()`, `sample()`) deferred to v0.2.0 as opt-in.  
**Trade-off:** Some operations unsupported initially, but enables instant execution start.

#### 3. Backpressure Control
**Problem:** Fast disk reads + slow processing = unbounded buffer growth → OOM crash.  
**Solution:** Respect `ReadableStream` `highWaterMark`, pause reading when processing queue full.  
**Implementation:** Monitor memory budget, apply backpressure to stream.

#### 4. Lazy Caching (Hybrid Strategy)
**Problem:** Full upfront conversion blocks "time-to-first-byte," but streaming CSV repeatedly is slow.  
**Solution:** **"Lazy Caching"** - First query streams CSV (instant start), background writes parsed chunks to binary cache.  
**Benefit:** Second query uses cached binary (10-50x faster), no upfront wait, transparent optimization.

#### 5. Custom Binary Format (Not Arrow IPC)
**Problem:** Arrow IPC adds ~200KB dependency and complexity overhead.  
**Solution:** Minimal custom columnar binary format - header (schema) + raw TypedArray buffers.  
**Benefit:** Zero dependencies, maps directly to memory, simple debugging, future Arrow compatibility possible.

#### 6. Binary Disk Spills (Not JSON)
**Problem:** Serializing chunks to JSON/BSON during external sort is extremely CPU-expensive.  
**Solution:** Use custom binary format for temp files - zero-copy binary representation.  
**Benefit:** Minimizes serialization overhead, keeps library lightweight.

#### 7. Async-Only API
**Problem:** `collectSync()` works in dev (small data) but crashes in prod (large data).  
**Solution:** All terminal operations are `async`. No blocking event loop, even for small datasets.  
**Trade-off:** Requires `await`, but prevents UI freezes and server blocks.

### Phase 1: Streaming Architecture (v0.1.0 - Breaking Changes)

**Target Runtime:** Bun (only bun), No browser, Nodejs, Deno support at all.

- [x] **ColumnarBatchIterator Interface** - Iterator yielding columnar batches (TypedArray vectors) ✅
  - [x] **Batch size: ~512KB** (memory-based, NOT row-count based)
  - [x] Accumulate rows until batch size >= 512KB, then yield
  - [x] Optimizes for CPU L3 cache (256KB-1MB sweet spot)
  - [x] Zero row object creation - work directly on TypedArrays
  - [x] Prevents GC pressure and enables SIMD optimizations
- [x] **Streaming CSV Scanner** - Pure forward-only scanning (NO random access in v0.1.0) ✅
  - [x] `ReadableStream`-based file reader with backpressure control ⚠️ (implementation uses memory-budget throttling)
  - [ ] Respects `highWaterMark` to prevent buffer overflow
  - [x] Pauses reading when processing queue is full (memory budget backpressure)
  - [x] ⚠️ No line index - random access (`tail()`, `sample()`) deferred to v0.2.0
- [x] **Streaming Operations** - Columnar batch processing ⚠️ (partial)
  - [x] `filter()` operates on TypedArrays, outputs filtered batches
  - [x] Aggregations (sum/min/max/count) use SIMD-friendly accumulation (global) ⚠️
  - [x] `select()` / `drop()` zero-copy column projection
  - [x] All operations preserve columnar format
- [x] **Memory-Aware Execution** - Integrate memory budget into streaming engine ⚠️ (partial)
  - [x] Backpressure mechanism to pause/resume stream based on memory
  - [ ] Memory pressure detection with spill-to-disk (binary format)
  - [x] **Hard defaults:** `batchSize: 512KB`, `maxMemory: 512MB` (user-configurable)
  - [x] ⚠️ No auto-tuning - `process.memoryUsage()` is too noisy in GC languages
- [x] **Lazy Caching System** - Transparent background caching ✅
  - [x] First query: Stream CSV (instant start) + background write to binary cache
  - [x] Cache detection: Check for `.molniya_cache/<filename>.mbin` before reading CSV
  - [x] Second query: Read from binary cache (10-50x faster)
  - [x] Cache invalidation: Check CSV mtime, rebuild if source changed
  - [x] Temp file cleanup: Auto-delete cache on completion or explicit `.clearCache()` ⚠️ (process-exit hook pending)

### Phase 2: Complex Operations with Streaming
- [ ] **Streaming Joins** - Hash join with partitioned hash tables ❌
  - [ ] Optional disk-based merge join for large datasets
  - [ ] Broadcast join optimization for small right tables
  - [ ] Pure JS implementation (no WASM - explore JS limits first)
- [x] **External Merge Sort** - Sorted chunk files for out-of-core sorting ✅
  - [x] Use **custom binary format** for temp files (not JSON/BSON/Arrow)
  - [x] Minimizes serialization overhead during disk spills
  - [ ] Zero-copy reload of sorted chunks
- [ ] **Window Functions** - Sliding window buffers for analytical functions ❌
- [x] **Distinct/Unique** - Hash set for exact values ✅
  - [ ] ⚠️ Bloom filters only for cardinality estimation (HyperLogLog)
  - [ ] Cannot use Bloom filters for actual unique value list (false positives)
  - [ ] Optional: Bloom filter for pre-filtering before disk-based hash set
- [ ] **Streaming GroupBy** - Hash-based aggregation with partial aggregates ❌
  - [ ] Pure JS hash table implementation

### Phase 3: Incremental & Append-Only Mode
- [ ] **Incremental Updates** - Append new data without full recomputation ❌
  - [ ] Maintain sorted indices for efficient merges
  - [ ] Delta processing for time-series data
  - [ ] Lightweight state tracking (small maps/indexes)
- [ ] **Real-time Dashboards** - Support for streaming analytics pipelines ❌
  - [ ] Rolling window aggregations
  - [ ] Incremental materialized views
- [ ] **Append Operations** - Efficient row addition to existing DataFrames ❌

### Custom Binary Format Design (`.mbin` - Block-Based)

**Goal:** Lightweight block-based columnar format for streaming writes - zero dependencies, maps directly to TypedArrays.

**Why Block-Based?** Enables streaming writes without OOM. Write each 512KB block, free memory, continue. No need to buffer entire columns.

#### Format Specification (`.mbin` files)

**Structure (Parquet-style):**
```
┌─────────────────────┐
│  File Header        │  Magic number, version, schema
├─────────────────────┤
│  Block 0 Metadata   │  Row count, byte offsets per column
├─────────────────────┤
│  Block 0 Col 0 Data │  Raw bytes (Int32Array, Float64Array, etc.)
├─────────────────────┤
│  Block 0 Col 1 Data │  ...
├─────────────────────┤
│  Block 1 Metadata   │  Next ~512KB batch
├─────────────────────┤
│  Block 1 Col 0 Data │  ...
├─────────────────────┤
│  ...                │
├─────────────────────┤
│  Footer             │  Block index, total row count
└─────────────────────┘
```

**File Header (64 bytes):**
```typescript
{
  magic: [0x4D, 0x4F, 0x4C, 0x4E],  // "MOLN" - 4 bytes
  version: 1,                        // uint8 - 1 byte
  numColumns: number,                // uint16 - 2 bytes  
  numBlocks: number,                 // uint32 - 4 bytes
  footerOffset: number,              // uint64 - byte offset to footer
  reserved: [0, 0, ...],             // 47 bytes (future use)
}
```

**Block Metadata (per block, 32 bytes + column offsets):**
```typescript
{
  blockId: number,         // uint32
  rowCount: number,        // uint32 - rows in this block
  uncompressedSize: number,// uint64 - for future compression
  columnOffsets: number[], // uint64[] - byte offset for each column in block
}
```

**Column Data (per block):**
- **Numeric columns:** Raw TypedArray bytes (e.g., `Int32Array.buffer`)
- **String columns (v0.1.0):** Length-prefixed inline strings, NO dictionary
  - Format: `[length: uint32][utf8_bytes]` repeated
  - v0.2.0: Move to global dictionary in footer
- **Null bitmap:** Optional bitset (1 bit per row)

**Footer (variable length):**
```typescript
{
  totalRows: number,       // uint64 - total across all blocks
  blockIndex: Array<{      // Index of all blocks
    blockId: number,
    fileOffset: number,    // Byte offset to block start
    rowCount: number,
  }>,
  columnMetadata: Array<{  // Schema info
    name: string,
    dtype: number,         // 0=Int32, 1=Float64, 2=String, 3=Bool
    hasNulls: boolean,
  }>,
}
```

#### Implementation Tasks (v0.1.0)
- [x] **Block-Based Binary Writer** - Stream-friendly serialization ✅
  - [x] Write file header with magic number
  - [x] Accumulate rows until batch reaches ~512KB
  - [x] Write block metadata + columnar data
  - [x] Strings: inline length-prefixed (NO dictionary in v0.1.0)
  - [x] Write footer with block index
  - [x] Free memory after each block write
- [x] **Binary Reader** - Memory-map blocks on demand ✅
  - [x] Parse header, validate magic number
  - [x] Read footer to get block index
  - [x] Lazy-load blocks (only read needed blocks)
  - [x] Zero-copy TypedArray mapping for numeric columns
  - [x] Parse inline strings (length-prefixed)
- [x] **Cache Management** - Handle `.molniya_cache/` directory ⚠️ (partial)
  - [x] Cache key: Hash of file path + mtime
  - [x] Hard limit: 512MB max memory, 10GB max cache size
  - [x] Cleanup: Time-based expiry (24 hours) or manual `.clearCache()`
  - [ ] File locking for concurrent access (flock)

#### Future Enhancements (v0.2.0+)
- [ ] **Global String Dictionary** - Move from inline to footer-based dictionary
- [ ] **Column Statistics** - Min/max/null_count in footer for predicate pushdown
- [ ] **Arrow IPC Export** - Optional compatibility for interop with other tools

### Phase 4: Performance Enhancements

#### Multi-threaded Execution
- [ ] **Worker Thread Pool** - Parallel chunk processing for CPU-intensive operations ❌
  - [ ] Parallel CSV parsing across chunks
  - [ ] Parallel aggregations and filters
  - [ ] Thread-safe memory budget management
  - [ ] **Target:** 2-4x speedup on multi-core systems

#### SIMD Vectorization
- [x] Basic SIMD filters for numeric data ✅
- [x] Expand SIMD coverage to all numeric operations ⚠️ (partial)
  - [x] Aggregations (sum, min, max) ✅
  - [ ] Aggregations (mean) ❌
  - [x] Comparisons and filters ✅
  - [ ] Type conversions ❌
- [ ] Auto-fallback to scalar for unsupported types ❌

### Breaking Changes (v0.1.0)
- `collect()` returns `Promise<DataFrame>` instead of sync DataFrame
- All LazyFrame operations require `await` for terminal operations
- ⚠️ **No `collectSync()`** - Blocking event loop is anti-pattern even for small data
- All operations are async to prevent UI freezes and server unresponsiveness

---

## Core Features

### Data Structures

- [x] Core `DataFrame` structure with typed columns ✅
- [x] `Series` class for single column operations ✅
- [x] Type system (`float64`, `int32`, `string`, `bool`, `datetime`, `date`) ✅

### CSV I/O

- [x] CSV reading with type inference (`readCsv`, `readCsvFromString`) ✅
- [x] Lazy CSV scanning (`scanCsv`, `scanCsvFromString`) - **Needs true streaming implementation** ⚠️
- [ ] CSV writing (`writeCsv`, `toCsv`) ❌
- [ ] JSON support (`toJson` / `readJson`) ❌
- [ ] Parquet support (after CSV streaming is stable) ❌

### DataFrame Operations

| Method               | Description                     | Status |
| -------------------- | ------------------------------- | ------ |
| `filter()`           | Filter rows by predicate        | ✅     |
| `select()`           | Select specific columns         | ✅     |
| `drop()`             | Remove columns or rows by index | ✅     |
| `rename()`           | Rename columns                  | ✅     |
| `dropna()`           | Drop rows with missing values   | ✅     |
| `fillna()`           | Fill missing values             | ✅     |
| `isna()` / `notna()` | Detect missing values           | ✅     |
| `astype()`           | Convert column types            | ✅     |
| `head()` / `tail()`  | Get first/last N rows           | ✅     |
| `copy()`             | Deep copy DataFrame             | ❌     |
| `sample()`           | Random row sampling             | ❌     |
| `iloc()`             | Integer-location indexing       | ❌     |
| `loc()`              | Label-based indexing            | ❌     |

### Aggregation Functions

| Function/Method   | DataFrame API | Series API | Status |
| ----------------- | ------------- | ---------- | ------ |
| `sum()`           | ✅            | ✅         | ✅     |
| `mean()`          | ✅            | ✅         | ✅     |
| `min()`           | ✅            | ✅         | ✅     |
| `max()`           | ✅            | ✅         | ✅     |
| `count()`         | ✅            | ✅         | ✅     |
| `unique()`        | ❌            | ✅         | ⚠️     |
| `median()`        | ✅            | ✅         | ✅     |
| `mode()`          | ✅            | ✅         | ✅     |
| `quantile()`      | ❌            | ❌         | ❌     |
| `std()` / `var()` | ❌            | ❌         | ❌     |
| `cumsum()`        | ❌            | ✅         | ⚠️     |
| `cummax()`        | ❌            | ✅         | ⚠️     |
| `cummin()`        | ❌            | ✅         | ⚠️     |

### GroupBy Operations

- [x] Single and multi-column grouping ✅
- [x] Aggregation functions: `count`, `sum`, `mean`, `min`, `max`, `first`, `last` ✅
- [ ] Multiple aggregations per column ❌
- [ ] Custom aggregation functions ❌

## LazyFrame & Query Optimization

**Current Status:** ⚠️ Infrastructure exists but execution is not truly streaming (see top priority section)

| Feature             | Description                                | Status |
| ------------------- | ------------------------------------------ | ------ |
| `scanCsv()`         | Lazy CSV loading (on-demand parsing)       | ⚠️ (streaming path implemented) |
| `LazyFrame`         | Memory-efficient DataFrame for big CSV     | ⚠️ (streaming path implemented) |
| Row Index           | O(1) random row access via byte offsets    | ❌     |
| LRU Chunk Cache     | Configurable memory budget (100MB default) | ✅     |
| `head()` / `tail()` | Returns DataFrame without full load        | ❌     |
| `filter()`          | Streaming filter with chunked processing   | ⚠️ (streaming path implemented) |
| `select()`          | Column projection on lazy data             | ⚠️ (streaming path implemented) |
| `collect()`         | Convert to full DataFrame when needed      | ✅     |
| Column Pruning      | Skip reading unused columns                | ✅     |
| Predicate Pushdown  | Filter during CSV parsing                  | ✅     |

### Joining & Combining

| Operation          | Description                                | Status |
| ------------------ | ------------------------------------------ | ------ |
| `merge()`          | SQL-like joins (inner, left, right, outer) | ✅     |
| `concat()`         | Concatenate DataFrames vertically/horiz.   | ✅     |
| `join()`           | Join on index                              | ✅     |
| `append()`         | Append rows to DataFrame                   | ✅     |
| `dropDuplicates()` | Drop duplicate rows                        | ✅     |
| `duplicate()`      | Duplicate the dataframe                    | ✅     |
| `unique()`         | Get unique rows                            | ✅     |

### Sorting & Ordering

- [x] Single column sort ✅
- [x] Multi-column sort ✅
- [ ] Stable sort guarantee ❌
- [ ] Index-based sorting ❌

### String Operations (`Series.str`)

| Method          | Description                  | Status |
| --------------- | ---------------------------- | ------ |
| `toLowerCase()` | Convert strings to lowercase | ✅     |
| `toUpperCase()` | Convert strings to uppercase | ✅     |
| `contains()`    | Check if contains substring  | ✅     |
| `startsWith()`  | Check if starts with prefix  | ✅     |
| `endsWith()`    | Check if ends with suffix    | ✅     |
| `length()`      | Get string lengths           | ✅     |
| `split()`       | Split strings into arrays    | ❌     |
| `trim()`        | Remove whitespace            | ❌     |
| `replace()`     | Replace substring            | ❌     |

### DateTime Operations (`Series.dt`)

- [ ] `year`, `month`, `day`, `hour`, `minute`, `second` ❌
- [ ] `dayofweek`, `dayofyear` ❌
- [ ] DateTime parsing and formatting ❌

## Performance & Optimization

### Memory Management

- [x] Uint8Array-based column storage ✅
- [x] String dictionary for efficient string storage ✅
- [x] LRU cache for LazyFrame chunks ✅
- [x] Memory budget tracking ✅
- [ ] Null bitmap tracking ⚠️ (partially implemented)

### Query Optimization

- [x] Column pruning (skip unused columns in CSV) ✅
- [x] Predicate pushdown (filter during CSV parsing) ✅
- [x] SIMD-optimized filters for numeric data ✅
- [x] **🚀 True streaming CSV execution** (forward-only, no pre-scan) - **TOP PRIORITY** ⚠️ (partial)
- [x] **🚀 ColumnarBatchIterator execution model** (TypedArray batches, zero row objects) - **TOP PRIORITY** ✅
- [x] **🚀 Backpressure-aware streaming** (prevent buffer overflow) - **TOP PRIORITY** ⚠️ (memory-budget throttling)
- [x] **🚀 Memory-aware chunk processing** - **TOP PRIORITY** ⚠️ (spill-to-disk pending)
- [ ] Join optimization (hash join, broadcast join) ❌
- [ ] Expression optimization ❌
- [ ] Cost-based query planning ❌

## Code Quality Standards

- **Line limit**: 500 lines per file (hard cap: 600)
- **Performance**: Raw DataView/Uint8Array operations required
- **Type safety**: Strong typing with TypeScript
- **Zero-copy**: Minimize allocations in hot paths

## Technical Constraints & Environment

### Runtime Environment
- **Primary:** Bun (uses `Bun.file()`, native performance)
- **Secondary:** Node.js (requires polyfills for Bun APIs)
- **Not Supported:** Browser (disk spilling impossible, use in-memory mode only)

### Memory Architecture
- **Columnar storage:** All data in `TypedArray` columns (`Float64Array`, `Int32Array`, etc.)
- **No row objects:** Operations work directly on TypedArrays to avoid GC pressure
- **Custom binary format:** Minimal `.mbin` format for caching and disk spills (see Phase 3)
- **String interning:** Dictionary encoding for string columns (already implemented)
- **Lazy caching:** First query streams CSV, background writes binary cache for 10-50x faster subsequent queries

### Performance Primitives
- **SIMD:** Use `Float64Array`/`Int32Array` for auto-vectorization (already partially implemented)
- **WASM:** Optional accelerators for CPU-intensive operations (Phase 4)
- **Worker threads:** Parallel chunk processing (Phase 4)
- **Backpressure:** Stream control to prevent memory overflow

### Operations Requiring Special Handling

#### v0.1.0 Limitations (Forward-Only Streaming)
- `head(n)` ✅ Read first N rows, stop early
- `tail(n)` ❌ Requires backward seek or full scan
- `sample(n)` ❌ Requires random access
- `sort()` ⚠️ Requires external merge sort with disk spills
- `join()` ⚠️ Hash join with memory budget, optional disk partitioning
- `unique()` ⚠️ Hash set (exact), or HyperLogLog (cardinality estimate)

#### Bloom Filter Constraints
- ✅ Use case: Cardinality estimation (HyperLogLog algorithm)
- ✅ Use case: Pre-filter for disk-based hash set (eliminate definite non-matches)
- ❌ **Cannot** return actual unique values (false positives make this impossible)
- ❌ **Cannot** replace hash sets for exact distinct operations

### Disk Spilling Strategy (Lazy Caching)

**First Query (Streaming + Caching):**
1. **Check cache:** Look for `.molniya_cache/<hash>.mbin` file
2. **If cache miss:** Stream CSV with instant start
3. **Background write:** Parse chunks → write to binary cache (non-blocking)
4. **Return results:** User sees data immediately

**Second Query (Cache Hit):**
1. **Check cache:** Validate mtime matches source CSV
2. **Memory-map binary:** Load columns directly as TypedArrays (zero-copy)
3. **Execute query:** 10-50x faster than reparsing CSV

**Memory Pressure Handling:**
1. **Detect memory pressure** via memory budget tracker (80% threshold)
2. **Pause stream** (backpressure) when threshold hit
3. **Serialize current batch** to temp file in custom binary format
4. **Resume stream** after successful spill
5. **Cleanup** temp files on operation completion or error

### Architectural Decisions (Finalized)

#### ✅ 1. Block-Based Binary Format
**Decision:** Block-based structure (not monolithic columns) for streaming writes without OOM.
- **Rationale:** Can write and free each ~512KB block during CSV parsing. No need to buffer entire columns in memory.
- **String handling (v0.1.0):** Inline length-prefixed strings (no dictionary). Global dictionary deferred to v0.2.0.
- **Format:** Header → Blocks (metadata + data) → Footer (block index)

#### ✅ 2. No Compression (v0.1.0)
**Decision:** Skip compression for initial release.
- **Rationale:** Modern NVMe SSDs (3-5 GB/s) are faster than compression overhead in pure JS. CPU becomes bottleneck.
- **Future (v0.2.0+):** Add LZ4 only (fastest option), requires native bindings or later phase.

#### ✅ 3. Memory-Based Batch Sizing
**Decision:** Target ~512KB batches (NOT row-count based like "10K rows").
- **Rationale:** 10K rows of booleans = 10KB, 10K rows of strings = 100MB. Row count is meaningless.
- **Sweet spot:** 256KB-1MB fits in CPU L3 cache for maximum performance.
- **Implementation:** Accumulate rows until `currentBatchSize >= 512KB`, then yield.

#### ✅ 4. Hybrid API Design
**Decision:** Offer both columnar (fast) and row-friendly (ergonomic) APIs.

**Internal/Advanced (Fast Path):**
```typescript
df.scan(batch => {
  // Exposes TypedArrays directly - zero overhead
  const ids: Int32Array = batch.id;
  const values: Float64Array = batch.value;
  // All internal operators (filter, join) use this
});
```

**User-Friendly (Slow Path):**
```typescript
for (const row of df.rows()) {
  // Iterator yields objects - documented as "slow"
  console.log(row.id, row.value); // For debugging/display only
}
```

**Rationale:** Library targets developers, not just data scientists. Advanced users get raw performance, beginners get ergonomics. Clearly document `.rows()` as slow.

#### ✅ 5. No WASM (Explore Pure JS Limits)
**Decision:** Pure JavaScript/TypeScript implementation. No WebAssembly.
- **Rationale:** WASM adds build complexity, tooling overhead, and larger bundle size. Goal is to see what modern JS can achieve.
- **Exploration:** Push TypedArrays, SIMD auto-vectorization, and worker threads to their limits first.
- **Future consideration:** Only revisit if pure JS hits insurmountable performance walls.

#### ✅ 6. Hard Configuration Defaults (No Auto-Tuning in v0.1.0)
**Decision:** Use hard defaults, let users override. No automatic memory detection.
- **Defaults:** `batchSize: 512KB`, `maxMemory: 512MB`, `maxCacheSize: 10GB`
- **Rationale:** `process.memoryUsage()` in GC languages is noisy and lagging. Auto-tuning causes instability.
- **User control:** Provide config options, document recommended values for different workloads.

### Implementation Checklist (v0.1.0)

- [ ] Block-based `.mbin` format with inline strings
- [ ] Memory-based batch iterator (~512KB batches)
- [ ] Lazy caching system (background write during first query)
- [ ] Hybrid API: `.scan()` for columnar batches, `.rows()` for row iterator
- [ ] Hard configuration defaults (no auto-tuning)
- [ ] Pure JS implementation (no WASM dependencies)
- [ ] Cache management (24hr expiry, 10GB limit, file locking)
