# Memory Limits

Running on a server? Edge worker? Molniya has you covered with built-in memory management.

## Why Bother?

On your laptop, loading a 2GB CSV might cause some fan noise. On a serverless function with 128MB RAM? Instant crash. Memory limits let you fail gracefully instead of spectacularly.

---

## Quick Setup

Set a global memory budget for your entire app:

```typescript
import { configure } from "molniya";

// 512MB global limit
configure({ globalLimitBytes: 512 * 1024 * 1024 });
```

That's it. Now all Molniya operations share this budget.

---

## Per-File Limits

Don't trust that incoming CSV? Set a limit just for that read:

```typescript
import { readCsv } from "molniya";

const { df, memoryError } = await readCsv("user_upload.csv", {
  memoryLimitBytes: 50 * 1024 * 1024, // 50MB max
});

if (memoryError) {
  console.log("File too large:", memoryError.message);
  // df is empty, but your server is still alive!
} else {
  df.print();
}
```

> [!TIP]
> The `memoryError` is returned, not thrown. Your code keeps running, so you can show a friendly error to users instead of a 500.

---

## Checking What's Happening

Curious about memory usage? Peek behind the curtain:

```typescript
import { getMemoryStats } from "molniya";

const stats = getMemoryStats();
console.log(`Using ${stats.totalUsedBytes} of ${stats.globalLimitBytes}`);
console.log(`Active operations: ${stats.activeTaskCount}`);
```

---

## Multiple Operations? We Share.

When you run concurrent operations (Promise.all, multiple LazyFrames), Molniya shares the memory budget fairly:

- **Single operation**: Gets 100% of the limit
- **Multiple operations**: Each gets up to 70% max (configurable)

This prevents one greedy operation from starving others.

```typescript
// Tweak the sharing ratio
configure({
  globalLimitBytes: 1024 * 1024 * 1024,
  maxTaskSharePercent: 0.6, // Each task gets max 60%
});
```

---

## Large Files? Use LazyFrame

For truly massive files, `scanCsv` creates a `LazyFrame` that streams data in chunks. It automatically respects memory limits.

```typescript
import { scanCsv } from "molniya";

// Creates a LazyFrame - doesn't load the whole file
const lazy = await scanCsv("10gb_logs.csv");

// Only loads what's needed
const recent = await lazy
  .filter(row => row.timestamp > Date.now() - 86400000)
  .head(100);

recent.print();
```

The internal cache evicts old chunks automatically when memory gets tight.

---

## Disabling Limits

Testing locally and don't want limits? Turn them off:

```typescript
configure({ enabled: false });
```

> [!WARNING]
> Don't do this in production! Memory limits exist to protect your server from OOM crashes.

---

## Configuration Reference

| Option                | Default | Description                            |
| --------------------- | ------- | -------------------------------------- |
| `globalLimitBytes`    | 1GB     | Total memory budget for all operations |
| `maxTaskSharePercent` | 0.7     | Max share when multiple operations run |
| `enabled`             | true    | Enable/disable memory tracking         |

---

## The Error Object

When limits are exceeded, you get a `MemoryLimitError` with helpful info:

```typescript
if (memoryError) {
  console.log(memoryError.requestedBytes);  // What was requested
  console.log(memoryError.availableBytes);  // What was available
  console.log(memoryError.hint);            // Suggestions to fix it
  console.log(memoryError.format());        // Pretty-printed message
}
```

## Next Steps

- **[Performance Guide](/guide/performance)** - More ways to keep things fast
- **[Loading Data](/guide/loading-data)** - CSV options and validation
- **[Best Practices](/guide/best-practices)** - Production-ready patterns
