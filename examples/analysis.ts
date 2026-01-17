import { DataFrame } from '../src/index.ts';

// 1. Create a DataFrame (columns are fully typed!)
const df = DataFrame.fromColumns({
  product: ['Laptop', 'Mouse', 'Monitor', 'Keyboard'],
  category: ['Electronics', 'Accessories', 'Electronics', 'Accessories'],
  price: [999.99, 29.99, 199.99, 59.99],
  rating: [4.5, 4.2, 4.8, 3.9],
});

// 2. Perform your analysis
// Let's find high-rated items (rating >= 4.0) and calculate a "value score"
const result = df
  .filter((row) => row.rating >= 4.0)
  .assign('value_score', (row) => row.rating / Math.log10(row.price))
  .sort('value_score', false) // Descending sort
  .select('product', 'price', 'value_score');

// 3. See the results
result.print();
