import { describe, expect, test } from 'bun:test';
import { QueryPlan } from '../../src/lazyframe/plan';
import { DType } from '../../src/types/dtypes';

describe('QueryPlan', () => {
  test('creates scan plan', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const plan = QueryPlan.scan('data.csv', schema, ['price', 'volume']);

    expect(plan.type).toBe('scan');
    expect(plan.path).toBe('data.csv');
    expect(plan.schema).toEqual(schema);
    expect(plan.columnOrder).toEqual(['price', 'volume']);
  });

  test('creates filter plan', () => {
    const schema = { price: DType.Float64 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['price']);
    const filterPlan = QueryPlan.filter(scanPlan, 'price', '>', 100);

    expect(filterPlan.type).toBe('filter');
    expect(filterPlan.column).toBe('price');
    expect(filterPlan.operator).toBe('>');
    expect(filterPlan.value).toBe(100);
    expect(filterPlan.input).toBe(scanPlan);
  });

  test('creates select plan', () => {
    const schema = { price: DType.Float64, volume: DType.Int32, side: DType.String };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['price', 'volume', 'side']);
    const selectPlan = QueryPlan.select(scanPlan, ['price', 'volume']);

    expect(selectPlan.type).toBe('select');
    expect(selectPlan.columns).toEqual(['price', 'volume']);
    expect(selectPlan.input).toBe(scanPlan);
  });

  test('creates groupby plan', () => {
    const schema = { symbol: DType.String, price: DType.Float64 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['symbol', 'price']);
    const groupbyPlan = QueryPlan.groupby(
      scanPlan,
      ['symbol'],
      [{ col: 'price', func: 'mean', outName: 'avg_price' }],
    );

    expect(groupbyPlan.type).toBe('groupby');
    expect(groupbyPlan.groupKeys).toEqual(['symbol']);
    expect(groupbyPlan.aggregations).toHaveLength(1);
    expect(groupbyPlan.aggregations[0].col).toBe('price');
    expect(groupbyPlan.aggregations[0].func).toBe('mean');
  });

  test('assigns unique IDs to plan nodes', () => {
    const schema = { price: DType.Float64 };
    const plan1 = QueryPlan.scan('data1.csv', schema, ['price']);
    const plan2 = QueryPlan.scan('data2.csv', schema, ['price']);
    const plan3 = QueryPlan.filter(plan1, 'price', '>', 100);

    expect(plan1.id).not.toBe(plan2.id);
    expect(plan1.id).not.toBe(plan3.id);
    expect(plan2.id).not.toBe(plan3.id);
  });

  test('getOutputSchema returns scan schema', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const plan = QueryPlan.scan('data.csv', schema, ['price', 'volume']);

    const outputSchema = QueryPlan.getOutputSchema(plan);
    expect(outputSchema).toEqual(schema);
  });

  test('getOutputSchema preserves schema through filter', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['price', 'volume']);
    const filterPlan = QueryPlan.filter(scanPlan, 'price', '>', 100);

    const outputSchema = QueryPlan.getOutputSchema(filterPlan);
    expect(outputSchema).toEqual(schema);
  });

  test('getOutputSchema returns selected columns only', () => {
    const schema = { price: DType.Float64, volume: DType.Int32, side: DType.String };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['price', 'volume', 'side']);
    const selectPlan = QueryPlan.select(scanPlan, ['price', 'volume']);

    const outputSchema = QueryPlan.getOutputSchema(selectPlan);
    expect(outputSchema).toEqual({ price: DType.Float64, volume: DType.Int32 });
  });

  test('getOutputSchema returns groupby schema with correct dtypes', () => {
    const schema = { symbol: DType.String, price: DType.Float64, volume: DType.Int32 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['symbol', 'price', 'volume']);
    const groupbyPlan = QueryPlan.groupby(
      scanPlan,
      ['symbol'],
      [
        { col: 'symbol', func: 'count', outName: 'count' },
        { col: 'price', func: 'mean', outName: 'avg_price' },
        { col: 'volume', func: 'sum', outName: 'total_volume' },
        { col: 'price', func: 'min', outName: 'min_price' },
      ],
    );

    const outputSchema = QueryPlan.getOutputSchema(groupbyPlan);
    expect(outputSchema).toEqual({
      symbol: DType.String, // Group key
      count: DType.Int32, // count always returns Int32
      avg_price: DType.Float64, // mean always returns Float64
      total_volume: DType.Int32, // sum preserves source dtype
      min_price: DType.Float64, // min preserves source dtype
    });
  });

  test('getColumnOrder returns scan column order', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const plan = QueryPlan.scan('data.csv', schema, ['price', 'volume']);

    const columnOrder = QueryPlan.getColumnOrder(plan);
    expect(columnOrder).toEqual(['price', 'volume']);
  });

  test('getColumnOrder preserves order through filter', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['price', 'volume']);
    const filterPlan = QueryPlan.filter(scanPlan, 'price', '>', 100);

    const columnOrder = QueryPlan.getColumnOrder(filterPlan);
    expect(columnOrder).toEqual(['price', 'volume']);
  });

  test('getColumnOrder returns selected columns in order', () => {
    const schema = { price: DType.Float64, volume: DType.Int32, side: DType.String };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['price', 'volume', 'side']);
    const selectPlan = QueryPlan.select(scanPlan, ['volume', 'price']);

    const columnOrder = QueryPlan.getColumnOrder(selectPlan);
    expect(columnOrder).toEqual(['volume', 'price']);
  });

  test('getColumnOrder returns group keys then aggregations', () => {
    const schema = { symbol: DType.String, price: DType.Float64 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['symbol', 'price']);
    const groupbyPlan = QueryPlan.groupby(
      scanPlan,
      ['symbol'],
      [
        { col: 'price', func: 'mean', outName: 'avg_price' },
        { col: 'price', func: 'sum', outName: 'total_price' },
      ],
    );

    const columnOrder = QueryPlan.getColumnOrder(groupbyPlan);
    expect(columnOrder).toEqual(['symbol', 'avg_price', 'total_price']);
  });

  test('explain generates readable plan for scan', () => {
    const schema = { price: DType.Float64 };
    const plan = QueryPlan.scan('data.csv', schema, ['price']);

    const explanation = QueryPlan.explain(plan);
    expect(explanation).toContain('Scan: data.csv');
    expect(explanation).toContain('Schema: price');
  });

  test('explain generates readable plan for filter', () => {
    const schema = { price: DType.Float64 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['price']);
    const filterPlan = QueryPlan.filter(scanPlan, 'price', '>', 100);

    const explanation = QueryPlan.explain(filterPlan);
    expect(explanation).toContain('Filter: price > 100');
    expect(explanation).toContain('Scan: data.csv');
  });

  test('explain generates readable plan for select', () => {
    const schema = { price: DType.Float64, volume: DType.Int32 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['price', 'volume']);
    const selectPlan = QueryPlan.select(scanPlan, ['price']);

    const explanation = QueryPlan.explain(selectPlan);
    expect(explanation).toContain('Select: price');
    expect(explanation).toContain('Scan: data.csv');
  });

  test('explain generates readable plan for groupby', () => {
    const schema = { symbol: DType.String, price: DType.Float64 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['symbol', 'price']);
    const groupbyPlan = QueryPlan.groupby(
      scanPlan,
      ['symbol'],
      [{ col: 'price', func: 'mean', outName: 'avg_price' }],
    );

    const explanation = QueryPlan.explain(groupbyPlan);
    expect(explanation).toContain('GroupBy: symbol');
    expect(explanation).toContain('mean(price) AS avg_price');
    expect(explanation).toContain('Scan: data.csv');
  });

  test('explain shows nested plan structure', () => {
    const schema = { symbol: DType.String, price: DType.Float64, volume: DType.Int32 };
    const scanPlan = QueryPlan.scan('data.csv', schema, ['symbol', 'price', 'volume']);
    const filterPlan = QueryPlan.filter(scanPlan, 'price', '>', 100);
    const selectPlan = QueryPlan.select(filterPlan, ['symbol', 'price']);
    const groupbyPlan = QueryPlan.groupby(
      selectPlan,
      ['symbol'],
      [{ col: 'price', func: 'mean', outName: 'avg_price' }],
    );

    const explanation = QueryPlan.explain(groupbyPlan);
    expect(explanation).toContain('GroupBy');
    expect(explanation).toContain('Select');
    expect(explanation).toContain('Filter');
    expect(explanation).toContain('Scan');
  });
});
