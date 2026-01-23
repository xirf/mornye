---
title: Molniya - Ergonomic data analysis for TypeScript
layout: page
sidebar: false
---

<LandingHero />

<Showcase>

<template #csv>

```ts twoslash
// @noErrors
// ---cut---
import { scanCsv } from "molniya";

const stream = await scanCsv("bitcoin_7m_rows.csv", {
  batchSize: 50_000
});

for await (const batch of stream) {
  console.log(batch.shape);
  batch.print();
  break;
}
```

</template>

<template #filtering>

```ts twoslash
// @noErrors
import { DataFrame } from "molniya";
const df = DataFrame.fromColumns({
  timestamp: [1],
  price: [50000],
  volume: [1.2],
});
// ---cut---
// Expressive filtering
const filtered = df
  .where((col) => col("price").gt(50000))
  .select("timestamp", "price", "volume");

filtered.print();
```

</template>

<template #groupby>

```ts twoslash
// @noErrors
import { DataFrame } from "molniya";
const df = DataFrame.fromColumns({
  product: ["Laptop", "Mouse"],
  category: ["Electronics", "Accessories"],
  price: [999.99, 29.99],
});
// ---cut---
// SQL-like aggregations
const summary = df.groupby("category").agg({
  price: "mean",
});

summary.print();
```

</template>

<template #types>

```ts twoslash
// @errors: 2345
import { DataFrame } from "molniya";
// ---cut---
const df = DataFrame.fromColumns({
  price: [999.99, 29.99]
});

// Error: Column 'non_existent' not found
df.col("non_existent").mean();

// Success: Auto-complete working
df.col("price").std();
```

</template>

</Showcase>

<LandingBenchmarks />

<LandingPhilosophy />

<Footer />
