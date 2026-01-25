import type { DataFrame } from '../src/dataframe/dataframe';
import { createDataFrame } from '../src/dataframe/dataframe';
import { fromArrays } from '../src/dataframe/factory';
import { filter, select } from '../src/dataframe/operations';
import { LazyFrame } from '../src/lazyframe/lazyframe';
import { DType } from '../src/types/dtypes';

// 1. Verify LazyFrame type inference
const schema = {
  id: DType.Int32,
  name: DType.String,
  active: DType.Bool,
};

// This should infer T = { id: number, name: string, active: boolean }
const lf = LazyFrame.scanCsv('test.csv', schema);

// Valid filter
lf.filter('id', '>', 10);
lf.filter('name', '==', 'Alice');

// @ts-expect-error - Invalid column
lf.filter('invalid', '==', 10);

// Invalid value type checks depend on T, but 'invalid' column MUST fail

// Valid select
const lf2 = lf.select(['id', 'name']);
// lf2 should be LazyFrame<{ id: number, name: string }>

// @ts-expect-error - Invalid column in select
lf.select(['id', 'invalid']);

// 2. Verify DataFrame generic operations
interface User {
  id: number;
  score: number;
}
const df = createDataFrame<User>();

// Valid operations
filter(df, 'score', '>', 50);
select(df, ['id']);

// @ts-expect-error - Invalid column
filter(df, 'wrong', '>', 50);

// @ts-expect-error - Invalid column
select(df, ['wrong']);

// @ts-expect-error - Invalid column
select(df, ['wrong']);

// 3. Verify fromArrays inference
const dfArrays = fromArrays({
  id: [1, 2],
  name: ['a', 'b'],
});

if (dfArrays.ok) {
  const d = dfArrays.data;
  // d should be DataFrame<{ id: number; name: string; }>
  select(d, ['id', 'name']);
  // @ts-expect-error - Invalid column
  select(d, ['invalid']);
}

console.log('Type compilation check passed!');
