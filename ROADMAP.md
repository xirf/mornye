# Molniya Roadmap

## Core Features

### Data Structures

- [x] Core `DataFrame` structure with typed columns ✅
- [x] `Series` class for single column operations ✅
- [x] Type system (`float64`, `int32`, `string`, `bool`, `datetime`, `date`) ✅

### CSV I/O

- [x] CSV reading with type inference (`readCsv`, `readCsvFromString`) ✅
- [x] Lazy CSV scanning (`scanCsv`, `scanCsvFromString`) ✅
- [ ] CSV writing (`writeCsv`, `toCsv`) ❌
- [ ] JSON support (`toJson` / `readJson`) ❌
- [ ] Parquet support (optional) ❌

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

| Feature             | Description                                | Status |
| ------------------- | ------------------------------------------ | ------ |
| `scanCsv()`         | Lazy CSV loading (on-demand parsing)       | ✅     |
| `LazyFrame`         | Memory-efficient DataFrame for big CSV     | ✅     |
| Row Index           | O(1) random row access via byte offsets    | ❌     |
| LRU Chunk Cache     | Configurable memory budget (100MB default) | ✅     |
| `head()` / `tail()` | Returns DataFrame without full load        | ❌     |
| `filter()`          | Streaming filter with chunked processing   | ✅     |
| `select()`          | Column projection on lazy data             | ✅     |
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
- [ ] **True streaming CSV execution** (currently loads entire file into memory) ❌
- [ ] Join optimization ❌
- [ ] Expression optimization ❌

## Code Quality Standards

- **Line limit**: 500 lines per file (hard cap: 600)
- **Performance**: Raw DataView/Uint8Array operations required
- **Type safety**: Strong typing with TypeScript
- **Zero-copy**: Minimize allocations in hot paths
