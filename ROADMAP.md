# Molniya Roadmap

## Core Features

- [x] Core `DataFrame` and `Series` structures
- [x] Type system (`float64`, `int32`, `string`, `bool`)
- [x] CSV reading with type inference
- [x] `head()`, `tail()`, `shape`, `columns()`, `info()`, `describe()`
- [x] `filter()`, `where()`, `select()`
- [x] `sort()` (single column)
- [x] `groupby()` with `sum`, `mean`, `min`, `max`, `count`, `first`, `last`
- [x] Series: `map()`, `unique()`, `valueCounts()`, `slice()`

| Method               | Description                     | Status |
| -------------------- | ------------------------------- | ------ |
| `drop()`             | Remove columns or rows by index | ✅     |
| `rename()`           | Rename columns                  | ✅     |
| `dropna()`           | Drop rows with missing values   | ✅     |
| `fillna()`           | Fill missing values             | ✅     |
| `isna()` / `notna()` | Detect missing values           | ✅     |
| `copy()`             | Deep copy DataFrame             | ❌     |
| `sample()`           | Random row sampling             | ❌     |
| `assign()`           | Add/modify columns              | ❌     |
| `iloc()`             | Integer-location indexing       | ❌     |
| `loc()`              | Label-based indexing            | ❌     |

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

## Joining & Combining

- [x] `merge()` - SQL-like joins (inner, left, right, outer) ✅
- [x] `concat()` - Concatenate DataFrames vertically/horizontally ✅
- [x] `join()` - Join on index ✅
- [x] `append()` - Append rows to DataFrame ✅

- [x] `dropDuplicates()` - Drop duplicate rows ✅
- [x] `duplicate()` - Duplicate the dataframe ✅
- [x] `unique()` - Get unique rows ✅

## I/O

- [ ] `toCsv()` - Write DataFrame to CSV
- [ ] `toJson()` / `readJson()` - JSON support (toJson done)
- [ ] `toParquet()` / `readParquet()` - Parquet support (optional)

## Aggregation

- [ ] `median()`, `mode()`, `quantile()`
- [ ] `cumsum()`, `cummax()`, `cummin()`, `cumprod()`

## String Operations (`Series.str`)

- [x] `lower()`, `upper()`, `strip()` ✅
- [x] `contains()`, `startswith()`, `endswith()` ✅
- [x] `replace()`, `len()` ✅
- [ ] `split()` - Split strings into arrays

## DateTime Operations (`Series.dt`)

- [ ] `year`, `month`, `day`, `hour`, `minute`, `second`
- [ ] `dayofweek`, `dayofyear`
- [ ] DateTime dtype support

## Rolling/Window

- [ ] `rolling()` with `mean`, `sum`, `min`, `max`
- [ ] `shift()`, `diff()`

- [ ] `pivot_table()`, `melt()`, `stack()`, `unstack()`
- [ ] `corr()`, `cov()` - Correlation/covariance
- [ ] `rank()`, `pct_change()`
- [x] `astype()` - Type casting ✅
- [ ] Multi-Index support
- [ ] Lazy evaluation optimization
