import { describe, expect, test } from 'bun:test';
import { type CsvParseOptions, parseCsvHeader, parseCsvLine } from '../../src/io/csv-parser';

describe('parseCsvLine', () => {
  test('parses simple comma-separated values', () => {
    const result = parseCsvLine('apple,banana,cherry');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['apple', 'banana', 'cherry']);
    }
  });

  test('handles quoted fields', () => {
    const result = parseCsvLine('"hello","world","test"');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['hello', 'world', 'test']);
    }
  });

  test('handles quotes with commas inside', () => {
    const result = parseCsvLine('"hello, world","test"');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['hello, world', 'test']);
    }
  });

  test('handles escaped quotes', () => {
    const result = parseCsvLine('"she said ""hello""","world"');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['she said "hello"', 'world']);
    }
  });

  test('handles empty fields', () => {
    const result = parseCsvLine('a,,c');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['a', '', 'c']);
    }
  });

  test('handles trailing comma', () => {
    const result = parseCsvLine('a,b,');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['a', 'b', '']);
    }
  });

  test('handles leading comma', () => {
    const result = parseCsvLine(',b,c');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['', 'b', 'c']);
    }
  });

  test('handles custom delimiter', () => {
    const result = parseCsvLine('a|b|c', { delimiter: '|' });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['a', 'b', 'c']);
    }
  });

  test('handles tab delimiter', () => {
    const result = parseCsvLine('a\tb\tc', { delimiter: '\t' });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['a', 'b', 'c']);
    }
  });

  test('handles single field', () => {
    const result = parseCsvLine('single');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['single']);
    }
  });

  test('handles empty line', () => {
    const result = parseCsvLine('');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['']);
    }
  });

  test('trims whitespace in unquoted fields', () => {
    const result = parseCsvLine('  hello  ,  world  ');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['hello', 'world']);
    }
  });

  test('preserves whitespace in quoted fields', () => {
    const result = parseCsvLine('"  hello  ","  world  "');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['  hello  ', '  world  ']);
    }
  });

  test('handles mixed quoted and unquoted', () => {
    const result = parseCsvLine('unquoted,"quoted",another');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['unquoted', 'quoted', 'another']);
    }
  });

  test('rejects unclosed quotes', () => {
    const result = parseCsvLine('"unclosed,field');
    expect(result.ok).toBe(false);
  });

  test('handles newlines inside quoted fields', () => {
    const result = parseCsvLine('"line1\nline2","field2"');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['line1\nline2', 'field2']);
    }
  });
});

describe('parseCsvHeader', () => {
  test('parses header from first line', () => {
    const result = parseCsvHeader('name,age,city');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['name', 'age', 'city']);
    }
  });

  test('trims whitespace from headers', () => {
    const result = parseCsvHeader('  name  ,  age  ,  city  ');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['name', 'age', 'city']);
    }
  });

  test('handles quoted headers', () => {
    const result = parseCsvHeader('"First Name","Last Name","Email"');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['First Name', 'Last Name', 'Email']);
    }
  });

  test('rejects empty headers', () => {
    const result = parseCsvHeader('name,,city');
    expect(result.ok).toBe(false);
  });

  test('rejects duplicate headers', () => {
    const result = parseCsvHeader('name,age,name');
    expect(result.ok).toBe(false);
  });

  test('handles custom delimiter', () => {
    const result = parseCsvHeader('name|age|city', { delimiter: '|' });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['name', 'age', 'city']);
    }
  });
});

describe('Edge cases', () => {
  test('handles very long lines', () => {
    const longField = 'a'.repeat(10000);
    const result = parseCsvLine(`${longField},b,c`);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data[0].length).toBe(10000);
    }
  });

  test('handles many fields', () => {
    const fields = Array.from({ length: 100 }, (_, i) => `field${i}`).join(',');
    const result = parseCsvLine(fields);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data.length).toBe(100);
    }
  });

  test('handles complex escaped quotes', () => {
    const result = parseCsvLine('"a""b""c","d"');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['a"b"c', 'd']);
    }
  });

  test('handles unicode characters', () => {
    const result = parseCsvLine('Ñame,日本,🌟');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['Ñame', '日本', '🌟']);
    }
  });

  test('handles semicolon delimiter (common in European CSVs)', () => {
    const result = parseCsvLine('name;age;city', { delimiter: ';' });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.data).toEqual(['name', 'age', 'city']);
    }
  });
});
