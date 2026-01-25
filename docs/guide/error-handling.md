# Error Handling

Molniya uses **throwing functions** by default for clean, simple code. Most operations throw errors when they fail.

## The Default Approach

Functions throw errors directly - no Result unwrapping needed:

```typescript
import { fromArrays, filter, select } from "molniya";

// Throws if error occurs
const df = fromArrays({
  id: [1, 2, 3],
  name: ["Alice", "Bob", "Charlie"],
});

const filtered = filter(df, "id", ">", 1);
const result = select(filtered, ["name"]);
console.log(result.toString());
```

**Pros:**

- ✅ Clean, simple code
- ✅ No unwrapping or checking every result
- ✅ Natural JavaScript error handling

**Cons:**

- ❌ Can crash if you don't wrap in try/catch
- ❌ Requires explicit error handling when needed

## Error Handling with Try/Catch

Wrap operations in try/catch when you need to handle errors:

```typescript
import { fromArrays, filter } from "molniya";

try {
  const df = fromArrays({
    price: [10, 20, 30],
  });

  const expensive = filter(df, "price", ">", 15);
  console.log(expensive.toString());
} catch (error) {
  console.error("Operation failed:", error);
  // Handle error appropriately
}
```

## Result-Based Functions

Some functions still return `Result<T, Error>` for operations where errors are expected:

```typescript
import { readCsv, DType } from "molniya";

const schema = { name: DType.String, age: DType.Int32 };
const result = await readCsv("users.csv", schema);

if (!result.ok) {
  console.error("Failed to read CSV:", result.error);
  return;
}

const df = result.data;
```

These include:

- File I/O operations (`readCsv`, `scanCsv`)
- LazyFrame operations that may fail during execution
- Operations on dynamic column access

## The unwrap Helper

For Result-based APIs, use `unwrap()` to throw if Result is an error:

```typescript
import { readCsv, unwrap, DType } from "molniya";

// Throws if Result is error, otherwise returns data
const df = unwrap(await readCsv("data.csv", { name: DType.String }));
```

## Best Practices

### ✅ Do: Use try/catch for pipelines

```typescript
try {
  const df = fromArrays(data);
  const filtered = filter(df, "active", "==", true);
  const selected = select(filtered, ["id", "name"]);
  console.log(selected.toString());
} catch (error) {
  console.error("Pipeline failed:", error);
}
```

### ✅ Do: Handle file I/O errors explicitly

```typescript
const result = await readCsv("data.csv", schema);
if (!result.ok) {
  console.error("Failed to load data:", result.error);
  return;
}
```

### ❌ Don't: Ignore errors in production

```typescript
// Risky: No error handling
const df = fromArrays(untrustedData);
const filtered = filter(df, columnName, ">", value);
```

### ✅ Do: Validate inputs before processing

```typescript
function processData(data: unknown) {
  // Validate first
  if (!isValidData(data)) {
    throw new Error("Invalid data format");
  }

  // Then process
  const df = fromArrays(data);
  return filter(df, "status", "==", "active");
}
```

## Type Inference

TypeScript infers schema types automatically:

```typescript
const df = fromArrays({
  name: ["Alice"], // Type: "string"
  age: [25], // Type: "float64" (default for numbers)
  active: [true], // Type: "bool"
});

// df has type: DataFrame<InferSchemaType<{
//   name: "string",
//   age: "float64",
//   active: "bool"
// }>>
```

## Summary

- **Default behavior**: Functions throw errors (simple and clean)
- **File I/O**: Returns `Result<T, Error>` (errors are expected)
- **Error handling**: Use try/catch when needed
- **unwrap()**: Convert Result to throwing for convenience
- **Type safety**: Schema types inferred from your data
