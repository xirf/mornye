import { describe, expect, test } from 'bun:test';
import { LazyFrame } from '../../src/lazyframe/lazyframe';
import { DType } from '../../src/types/dtypes';

describe('LazyFrame', () => {
  test('creates LazyFrame from scanCsv', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv('data.csv', schema);

    expect(lf).toBeInstanceOf(LazyFrame);
    expect(lf.getSchema()).toEqual(schema);
    expect(lf.getColumnOrder()).toEqual(['price', 'volume']);
  });

  test('scanCsv accepts options', () => {
    const schema = { price: DType.Float64 };
    const lf = LazyFrame.scanCsv('data.csv', schema, {
      delimiter: ';',
      hasHeader: false,
      chunkSize: 5000,
    });

    const plan = lf.getPlan();
    expect(plan.type).toBe('scan');
    if (plan.type === 'scan') {
      expect(plan.delimiter).toBe(';');
      expect(plan.hasHeader).toBe(false);
      expect(plan.chunkSize).toBe(5000);
    }
  });

  test('filter builds filter plan', () => {
    const schema = { price: DType.Float64 };
    const lf = LazyFrame.scanCsv('data.csv', schema).filter('price', '>', 100);

    const plan = lf.getPlan();
    expect(plan.type).toBe('filter');
    if (plan.type === 'filter') {
      expect(plan.column).toBe('price');
      expect(plan.operator).toBe('>');
      expect(plan.value).toBe(100);
    }
  });

  test('select builds select plan', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv('data.csv', schema).select(['price']);

    const plan = lf.getPlan();
    expect(plan.type).toBe('select');
    if (plan.type === 'select') {
      expect(plan.columns).toEqual(['price']);
    }
  });

  test('groupby builds groupby plan', () => {
    const schema = { symbol: DType.String, price: DType.Float64 };
    const lf = LazyFrame.scanCsv('data.csv', schema).groupby(
      ['symbol'],
      [{ col: 'price', func: 'mean', outName: 'avg_price' }],
    );

    const plan = lf.getPlan();
    expect(plan.type).toBe('groupby');
    if (plan.type === 'groupby') {
      expect(plan.groupKeys).toEqual(['symbol']);
      expect(plan.aggregations).toHaveLength(1);
    }
  });

  test('sort builds sort plan', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv('data.csv', schema).sort('price', 'desc');

    const plan = lf.getPlan();
    expect(plan.type).toBe('sort');
    if (plan.type === 'sort') {
      expect(plan.columns).toEqual(['price']);
      expect(plan.directions).toEqual(['desc']);
    }
  });

  test('chains multiple operations', () => {
    const schema = { symbol: DType.String, price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv('data.csv', schema)
      .filter('price', '>', 100)
      .select(['symbol', 'price'])
      .filter('symbol', '==', 'BTC')
      .groupby(['symbol'], [{ col: 'price', func: 'mean', outName: 'avg_price' }]);

    const plan = lf.getPlan();
    expect(plan.type).toBe('groupby');
  });

  test('getSchema returns correct schema after operations', () => {
    const schema = { symbol: DType.String, price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv('data.csv', schema)
      .select(['symbol', 'price'])
      .groupby(
        ['symbol'],
        [
          { col: 'price', func: 'mean', outName: 'avg_price' },
          { col: 'symbol', func: 'count', outName: 'count' },
        ],
      );

    const outputSchema = lf.getSchema();
    expect(outputSchema).toEqual({
      symbol: DType.String,
      avg_price: DType.Float64,
      count: DType.Int32,
    });
  });

  test('getColumnOrder returns correct order after operations', () => {
    const schema = { symbol: DType.String, price: DType.Float64, volume: DType.Int32 };
    const lf = LazyFrame.scanCsv('data.csv', schema)
      .select(['volume', 'price', 'symbol'])
      .groupby(['symbol'], [{ col: 'price', func: 'sum', outName: 'total_price' }]);

    const columnOrder = lf.getColumnOrder();
    expect(columnOrder).toEqual(['symbol', 'total_price']);
  });

  test('explain returns readable query plan', () => {
    const schema = { price: DType.Float64 };
    const lf = LazyFrame.scanCsv('data.csv', schema).filter('price', '>', 100);

    const explanation = lf.explain();
    expect(explanation).toContain('Filter');
    expect(explanation).toContain('Scan');
    expect(explanation).toContain('price > 100');
  });

  test('operations return new LazyFrame instances', () => {
    const schema = { price: DType.Float64 };
    const lf1 = LazyFrame.scanCsv('data.csv', schema);
    const lf2 = lf1.filter('price', '>', 100);
    const lf3 = lf2.select(['price']);

    expect(lf1).not.toBe(lf2);
    expect(lf2).not.toBe(lf3);
    expect(lf1).not.toBe(lf3);
  });

  test('operations do not mutate original LazyFrame', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const lf1 = LazyFrame.scanCsv('data.csv', schema);
    const originalPlan = lf1.getPlan();

    lf1.filter('price', '>', 100);

    expect(lf1.getPlan()).toBe(originalPlan);
    expect(lf1.getPlan().type).toBe('scan');
  });

  test('complex chained query', () => {
    const schema = {
      timestamp: DType.DateTime,
      symbol: DType.String,
      side: DType.String,
      price: DType.Float64,
      volume: DType.Int32,
    };

    const lf = LazyFrame.scanCsv('trades.csv', schema)
      .filter('symbol', '==', 'BTC')
      .filter('side', '==', 'buy')
      .filter('price', '>', 50000)
      .select(['symbol', 'side', 'price', 'volume'])
      .groupby(
        ['symbol', 'side'],
        [
          { col: 'volume', func: 'sum', outName: 'total_volume' },
          { col: 'price', func: 'mean', outName: 'avg_price' },
          { col: 'price', func: 'min', outName: 'min_price' },
          { col: 'price', func: 'max', outName: 'max_price' },
        ],
      );

    const explanation = lf.explain();
    expect(explanation).toContain('GroupBy');
    expect(explanation).toContain('Select');
    expect(explanation).toContain('Filter');
    expect(explanation).toContain('Scan');

    const outputSchema = lf.getSchema();
    expect(outputSchema).toEqual({
      symbol: DType.String,
      side: DType.String,
      total_volume: DType.Int32,
      avg_price: DType.Float64,
      min_price: DType.Float64,
      max_price: DType.Float64,
    });
  });
});
