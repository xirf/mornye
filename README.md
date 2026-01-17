<p align="center">
  <img src="docs/public/logo.png" width="100" height="100" alt="Mornye Logo" />
</p>

<h1 align="center">Mornye</h1>

<p align="center">
  <b>Strictly typed dataframes for TypeScript.</b><br>
  Zero dependencies. SIMD accelerated. Optimized for Bun.
</p>

<p align="center">
  <a href="https://mornye.andka.id"><b>Documentation</b></a> • 
  <a href="#benchmarks"><b>Benchmarks</b></a> • 
  <a href="https://github.com/xirf/mornye/issues"><b>Issues</b></a>
</p>

<br />

## Why Mornye?

Most JavaScript data libraries treat types as an afterthought. **Mornye** puts them first.

- **🧠 True Type Inference:** It tracks your schema through filters, selects, and aggregations.
- **⚡ Bun Native:** Built specifically to leverage Bun's fast I/O and runtime capabilities.
- **🚀 SIMD Accelerated:** Uses low-level optimizations for line finding and parsing.
- **📦 Zero Dependencies:** No bloat. No massive node_modules folder.

## Install

```bash
bun add mornye
```

## Quick Start

Experience the "IDE magic" where types follow your data.

```typescript
import { DataFrame } from "mornye";

// 1. Create a DataFrame
// Mornye automatically infers:
// name: string, age: number, is_active: boolean
const df = DataFrame.fromColumns({
  name: ["Alice", "Bob", "Carol"],
  age: [25, 30, 22],
  is_active: [true, false, true],
});

// 2. Chain methods safely
const result = df
  .filter((row) => row.is_active) // Typescript knows 'row' shape here
  .assign("score", (row) => row.age * 2) // Adds 'score' to the type definition
  .select("name", "score"); // Removes 'age' and 'is_active'

// 3. Result is fully typed
// result: DataFrame<{ name: string; score: number }>
result.print();
```

## Performance

Mornye is built for speed, utilizing SIMD instructions for parsing.

| Task              | Mornye    | Native Array |
| :---------------- | :-------- | :----------- |
| **Type Safety**   | ✅ Strict | ❌ Loose     |
| **Memory Layout** | Columnar  | Row-based    |
| **Dependencies**  | 0         | 0            |

_(Detailed benchmarks against other libraries coming soon)_

## Roadmap

- [x] CSV Parsing (SIMD)
- [x] Basic Filtering & Sorting
- [ ] Parquet Support
- [ ] GroupBy Aggregations

## License

Proudly open source under the MIT License. See `LICENSE` for details.
