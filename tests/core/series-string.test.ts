import { describe, expect, test } from 'bun:test';
import { Series } from '../../src/core/series';

describe('Series.str', () => {
  describe('casing', () => {
    test('lower() converts to lowercase', () => {
      const s = Series.string(['Foo', 'BAR', 'baZ', null as unknown as string]);
      expect(s.str.lower().toArray()).toEqual(['foo', 'bar', 'baz', '']);
    });

    test('upper() converts to uppercase', () => {
      const s = Series.string(['foo', 'bar', 'BAZ', null as unknown as string]);
      expect(s.str.upper().toArray()).toEqual(['FOO', 'BAR', 'BAZ', '']);
    });

    test('capitalize() capitalizes first letter', () => {
      const s = Series.string(['foo', 'BAR', 'baZ', '', null as unknown as string]);
      expect(s.str.capitalize().toArray()).toEqual(['Foo', 'Bar', 'Baz', '', '']);
    });

    test('title() capitalizes words', () => {
      const s = Series.string(['hello world', 'FOO BAR', null as unknown as string]);
      expect(s.str.title().toArray()).toEqual(['Hello World', 'FOO BAR', '']);
    });
  });

  describe('trimming', () => {
    test('trim() removes whitespace from both ends', () => {
      const s = Series.string(['  foo  ', 'bar  ', '  baz', null as unknown as string]);
      expect(s.str.trim().toArray()).toEqual(['foo', 'bar', 'baz', '']);
    });

    test('trimStart() removes leading whitespace', () => {
      const s = Series.string(['  foo  ', 'bar  ', null as unknown as string]);
      expect(s.str.trimStart().toArray()).toEqual(['foo  ', 'bar  ', '']);
    });

    test('trimEnd() removes trailing whitespace', () => {
      const s = Series.string(['  foo  ', '  baz', null as unknown as string]);
      expect(s.str.trimEnd().toArray()).toEqual(['  foo', '  baz', '']);
    });
  });

  describe('search and match', () => {
    test('contains() matches string pattern', () => {
      const s = Series.string(['foo', 'bar', 'baz', null as unknown as string]);
      expect(s.str.contains('a').toArray()).toEqual([false, true, true, false]);
    });

    test('contains() matches regex pattern', () => {
      const s = Series.string(['foo', 'bar', 'baz']);
      expect(s.str.contains(/^b/).toArray()).toEqual([false, true, true]);
    });

    test('startsWith() checks prefix', () => {
      const s = Series.string(['foo', 'bar', 'baz', null as unknown as string]);
      expect(s.str.startsWith('b').toArray()).toEqual([false, true, true, false]);
    });

    test('endsWith() checks suffix', () => {
      const s = Series.string(['foo', 'bar', 'baz', null as unknown as string]);
      expect(s.str.endsWith('r').toArray()).toEqual([false, true, false, false]);
    });
  });

  describe('modification', () => {
    test('replace() string pattern', () => {
      const s = Series.string(['foo', 'bar', 'baz', null as unknown as string]);
      expect(s.str.replace('a', 'x').toArray()).toEqual(['foo', 'bxr', 'bxz', '']);
    });

    test('replace() regex pattern', () => {
      const s = Series.string(['foo', 'bar', 'baz']);
      expect(s.str.replace(/[aeiou]/g, '*').toArray()).toEqual(['f**', 'b*r', 'b*z']);
    });

    test('slice() extracts substring', () => {
      const s = Series.string(['hello', 'world', null as unknown as string]);
      expect(s.str.slice(1, 3).toArray()).toEqual(['el', 'or', '']);
    });

    test('split() splits string', () => {
      const s = Series.string(['a,b', 'c,d', null as unknown as string]);
      expect(s.str.split(',')).toEqual([['a', 'b'], ['c', 'd'], []]);
    });

    test('repeat() repeats string', () => {
      const s = Series.string(['a', 'b', null as unknown as string]);
      expect(s.str.repeat(2).toArray()).toEqual(['aa', 'bb', '']);
    });
  });

  describe('padding', () => {
    test('padStart() pads from start', () => {
      const s = Series.string(['1', '12', null as unknown as string]);
      expect(s.str.padStart(2, '0').toArray()).toEqual(['01', '12', '00']);
    });

    test('padEnd() pads from end', () => {
      const s = Series.string(['1', '12', null as unknown as string]);
      expect(s.str.padEnd(2, '0').toArray()).toEqual(['10', '12', '00']);
    });
  });

  describe('utility', () => {
    test('len() returns length', () => {
      const s = Series.string(['foo', 'ba', '', null as unknown as string]);
      expect(s.str.len().toArray()).toEqual([3, 2, 0, 0]);
    });
  });
});
