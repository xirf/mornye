import { scanCsv } from './src';
import type { AggDef } from './src/core/lazyframe/interface';

const csvContent = `id,name,val
1,a,10
2,b,20
1,a,30
2,b,40
3,c,50`;

const path = 'test_verify.csv';
await Bun.write(path, csvContent);

try {
  console.log('Creating LazyFrame...');
  const lf = await scanCsv(path, { raw: true }); // Enable raw for binary path checking

  console.log('Verifying Parser (Head)...');
  const df = await lf.head(5);
  df.print();

  console.log('Verifying Binary GroupBy...');
  // Group by 'name', sum 'val'
  const aggs: AggDef[] = [
    { col: 'val', func: 'sum', outName: 'val_sum' },
    { col: 'val', func: 'count', outName: 'count' },
  ];

  const res = await lf.groupby(['name'], aggs);
  console.log('GroupBy Result:');
  res.data?.print();
} catch (e) {
  console.error('Verification Failed:', e);
} finally {
  // cleanup
  // await Bun.file(path).delete();
}
