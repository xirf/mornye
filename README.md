<p align="center">
  <img src="docs/public/logo.png" width="100" height="100" alt="Molniya Logo" />
</p>

<h1 align="center">Molniya</h1>

<p align="center">
  A simply typed DataFrame library with zero dependencies that honors your memory.
</p>

<p align="center">
  <a href="https://molniya.andka.id"><b>Documentation</b></a> • 
  <a href="https://molniya.andka.id/guide/cookbook"><b>Cookbook</b></a> • 
  <a href="https://github.com/xirf/molniya/tree/main/examples"><b>Examples</b></a>
</p>

---

## What is Molniya?

Molniya is a DataFrame library built specifically for Bun. It helps you load, transform, and analyze structured data without the complexity of heavy frameworks.

Think Pandas for Python, but designed for TypeScript and Bun from the ground up.

## Install

```bash
bun add molniya
```

## Quick Start

Simple and clean - operations throw errors when they fail:

```typescript
import { fromArrays, filter, select } from "molniya";

const df = fromArrays({
  name: ["Alice", "Bob", "Charlie"],
  age: [25, 30, 35],
  city: ["NYC", "LA", "Chicago"],
});

// Throws if error occurs - no Result unwrapping needed
const adults = filter(df, "age", ">=", 30);
const result = select(adults, ["name", "city"]);

console.log(result.toString());
```

**Error handling**: Wrap in try/catch when you need to handle errors:

```typescript
try {
  const df = fromArrays({ ... });
  const filtered = filter(df, "age", ">=", 30);
} catch (error) {
  console.error("Operation failed:", error);
}
```

**Type inference**: TypeScript infers schema types automatically:

```typescript
const df = fromArrays({
  name: ["Alice"], // Type: DataFrame<{ name: "string", age: "float64" }>
  age: [25],
});
```

## Why Molniya?

1. **Schema-first design**  
   Define your data types once, get type safety and optimizations everywhere.

2. **Built for Bun**  
   Uses Bun's file I/O and SIMD capabilities. No polyfills, and unfortunately no Node.js compatibility layers.

3. **Zero dependencies**  
   The entire library has zero runtime dependencies. Install with confidence.

4. **Clean error handling**  
   Operations throw errors when they fail - simple and predictable. Wrap in try/catch when needed.

## LazyFrame for Large Files

For big datasets, use LazyFrame for automatic query optimization:

```typescript
import { LazyFrame, DType } from "molniya";

const schema = {
  product: DType.String,
  category: DType.String,
  revenue: DType.Float64,
};

const result = await LazyFrame.scanCsv("sales.csv", schema)
  .filter("category", "==", "Electronics") // Pushed down to scan
  .filter("revenue", ">", 1000)
  .select(["product", "revenue"]) // Only load these columns
  .collect(); // Execute optimized plan
```

LazyFrame analyzes your query and:

- **Predicate pushdown** - Filters during CSV parsing
- **Column pruning** - Only reads needed columns
- **Query fusion** - Combines operations when possible

**Real impact:** For a 1GB CSV file, this can mean reading only 100MB.

## Learn More

**New to Molniya?**

- [Introduction](https://molniya.andka.id/guide/introduction) - Core concepts
- [Getting Started](https://molniya.andka.id/guide/getting-started) - Complete walkthrough

**Ready to build?**

- [Cookbook](https://molniya.andka.id/guide/cookbook) - Copy-paste recipes
- [Examples](https://github.com/xirf/molniya/tree/main/examples) - Real code

**Need details?**

- [API Reference](https://molniya.andka.id/api/dataframe) - Full API docs
- [Data Types](https://molniya.andka.id/guide/data-types) - Type system guide
- [Lazy Evaluation](https://molniya.andka.id/guide/lazy-evaluation) - Performance optimization

## Community

- [GitHub](https://github.com/xirf/molniya) - Source code and issues
- [Discussions](https://github.com/xirf/molniya/discussions) - Ask questions
- [Contributing](./CONTRIBUTING.md) - Help improve Molniya

## License

MIT License. See [LICENSE](./LICENSE) for details.
