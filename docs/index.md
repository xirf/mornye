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
import { LazyFrame, DType } from "molniya";

const schema = {
  timestamp: DType.DateTime,
  price: DType.Float64,
  volume: DType.Float64,
};

// Lazy evaluation with predicate pushdown
const result = await LazyFrame.scanCsv("bitcoin.csv", schema)
  .filter("price", ">", 50000)
  .collect();

console.log(result.data.toString());
```

</template>

<template #filtering>

```ts twoslash
import { fromArrays, filter, select } from "molniya";

const df = fromArrays({
  item: ["GPU", "CPU", "RAM"],
  price: [1200.0, 450.0, 150.0],
  in_stock: [true, true, false],
});

// Clean functional API - throws on error
const filtered = filter(df, "price", ">", 500);
const result = select(filtered, ["item", "price"]);

console.log(result.toString());
```

</template>

<template #groupby>

```ts twoslash
import { fromArrays, groupby, unwrap } from "molniya";

const df = fromArrays({
  dept: ["Sales", "Sales", "Eng", "Eng", "HR"],
  salary: [60000, 65000, 90000, 95000, 55000],
});

// Group by department and calculate mean salary
const result = unwrap(
  groupby(
    df,
    ["dept"],
    [{ col: "salary", func: "mean", outName: "avg_salary" }],
  ),
);

console.log(result.toString());
```

</template>

<template #types>

```ts twoslash
// @errors: 2345
import { fromArrays, getColumn } from "molniya";

const df = fromArrays({
  price: [999.99, 29.99],
});

// Error: Column 'non_existent' not found (runtime check + type check!)
const col = getColumn(df, "non_existent");

if (!col.ok) {
  console.error(col.error);
}
```

</template>

</Showcase>

<LandingPhilosophy />

<LandingFooter />
