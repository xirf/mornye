import { describe, expect, test } from 'bun:test';
import {
  clearDictionary,
  createDictionary,
  getDictionaryMemoryUsage,
  getDictionarySize,
  getString,
  internString,
} from '../../src/memory/dictionary';

describe('createDictionary', () => {
  test('creates empty dictionary', () => {
    const dict = createDictionary();
    expect(getDictionarySize(dict)).toBe(0);
  });
});

describe('internString', () => {
  test('interns new string and returns ID', () => {
    const dict = createDictionary();
    const id = internString(dict, 'hello');
    expect(id).toBe(0); // First string gets ID 0
    expect(getDictionarySize(dict)).toBe(1);
  });

  test('returns same ID for duplicate string', () => {
    const dict = createDictionary();
    const id1 = internString(dict, 'hello');
    const id2 = internString(dict, 'hello');
    const id3 = internString(dict, 'hello');

    expect(id1).toBe(id2);
    expect(id2).toBe(id3);
    expect(getDictionarySize(dict)).toBe(1); // Still just one unique string
  });

  test('assigns different IDs to different strings', () => {
    const dict = createDictionary();
    const id1 = internString(dict, 'apple');
    const id2 = internString(dict, 'banana');
    const id3 = internString(dict, 'cherry');

    expect(id1).toBe(0);
    expect(id2).toBe(1);
    expect(id3).toBe(2);
    expect(getDictionarySize(dict)).toBe(3);
  });

  test('handles empty string', () => {
    const dict = createDictionary();
    const id = internString(dict, '');
    expect(id).toBe(0);
    expect(getDictionarySize(dict)).toBe(1);
  });

  test('is case sensitive', () => {
    const dict = createDictionary();
    const id1 = internString(dict, 'Hello');
    const id2 = internString(dict, 'hello');
    const id3 = internString(dict, 'HELLO');

    expect(id1).not.toBe(id2);
    expect(id2).not.toBe(id3);
    expect(getDictionarySize(dict)).toBe(3);
  });

  test('handles special characters', () => {
    const dict = createDictionary();
    const id1 = internString(dict, 'hello, world!');
    const id2 = internString(dict, '价格');
    const id3 = internString(dict, '🚀');

    expect(getDictionarySize(dict)).toBe(3);
    expect(id1).not.toBe(id2);
    expect(id2).not.toBe(id3);
  });
});

describe('getString', () => {
  test('retrieves string by ID', () => {
    const dict = createDictionary();
    const id = internString(dict, 'hello');
    expect(getString(dict, id)).toBe('hello');
  });

  test('retrieves multiple strings', () => {
    const dict = createDictionary();
    const id1 = internString(dict, 'apple');
    const id2 = internString(dict, 'banana');
    const id3 = internString(dict, 'cherry');

    expect(getString(dict, id1)).toBe('apple');
    expect(getString(dict, id2)).toBe('banana');
    expect(getString(dict, id3)).toBe('cherry');
  });

  test('returns undefined for invalid ID', () => {
    const dict = createDictionary();
    internString(dict, 'test');

    expect(getString(dict, 999)).toBeUndefined();
    expect(getString(dict, -1)).toBeUndefined();
  });

  test('handles empty string', () => {
    const dict = createDictionary();
    const id = internString(dict, '');
    expect(getString(dict, id)).toBe('');
  });
});

describe('getDictionarySize', () => {
  test('returns 0 for empty dictionary', () => {
    const dict = createDictionary();
    expect(getDictionarySize(dict)).toBe(0);
  });

  test('returns correct count after interning', () => {
    const dict = createDictionary();
    internString(dict, 'a');
    internString(dict, 'b');
    internString(dict, 'a'); // Duplicate
    internString(dict, 'c');

    expect(getDictionarySize(dict)).toBe(3); // Only unique strings
  });
});

describe('getDictionaryMemoryUsage', () => {
  test('returns 0 for empty dictionary', () => {
    const dict = createDictionary();
    expect(getDictionaryMemoryUsage(dict)).toBe(0);
  });

  test('calculates memory for strings', () => {
    const dict = createDictionary();
    internString(dict, 'hello'); // 5 chars * 2 bytes (UTF-16)
    internString(dict, 'world'); // 5 chars * 2 bytes

    const usage = getDictionaryMemoryUsage(dict);
    expect(usage).toBeGreaterThan(0);
    // Approximate size: at least 20 bytes for string data
    expect(usage).toBeGreaterThanOrEqual(20);
  });

  test("doesn't count duplicates", () => {
    const dict = createDictionary();
    internString(dict, 'test');
    const usage1 = getDictionaryMemoryUsage(dict);

    internString(dict, 'test'); // Duplicate
    const usage2 = getDictionaryMemoryUsage(dict);

    expect(usage1).toBe(usage2); // No additional memory
  });
});

describe('clearDictionary', () => {
  test('clears all entries', () => {
    const dict = createDictionary();
    internString(dict, 'a');
    internString(dict, 'b');
    internString(dict, 'c');
    expect(getDictionarySize(dict)).toBe(3);

    clearDictionary(dict);
    expect(getDictionarySize(dict)).toBe(0);
    expect(getDictionaryMemoryUsage(dict)).toBe(0);
  });

  test('can re-use after clearing', () => {
    const dict = createDictionary();
    const id1 = internString(dict, 'hello');
    expect(id1).toBe(0);

    clearDictionary(dict);

    const id2 = internString(dict, 'world');
    expect(id2).toBe(0); // IDs restart from 0
    expect(getDictionarySize(dict)).toBe(1);
  });
});

describe('Real-world scenarios', () => {
  test('handles typical CSV data', () => {
    const dict = createDictionary();

    // Simulate categorical column with repeated values
    const categories = ['buy', 'sell', 'buy', 'buy', 'sell', 'hold', 'buy'];
    const ids = categories.map((cat) => internString(dict, cat));

    // Only 3 unique categories
    expect(getDictionarySize(dict)).toBe(3);

    // "buy" appears 4 times but has same ID
    expect(ids[0]).toBe(ids[2]);
    expect(ids[0]).toBe(ids[3]);
    expect(ids[0]).toBe(ids[6]);

    // Can retrieve all strings
    expect(getString(dict, ids[0])).toBe('buy');
    expect(getString(dict, ids[1])).toBe('sell');
    expect(getString(dict, ids[5])).toBe('hold');
  });

  test('handles large number of unique strings', () => {
    const dict = createDictionary();
    const count = 10_000;

    for (let i = 0; i < count; i++) {
      internString(dict, `string_${i}`);
    }

    expect(getDictionarySize(dict)).toBe(count);

    // Verify retrieval
    expect(getString(dict, 0)).toBe('string_0');
    expect(getString(dict, 5000)).toBe('string_5000');
    expect(getString(dict, 9999)).toBe('string_9999');
  });

  test('memory efficient for repeated strings', () => {
    const dict = createDictionary();

    // Intern same string 1000 times
    for (let i = 0; i < 1000; i++) {
      internString(dict, 'repeated');
    }

    // Only stored once
    expect(getDictionarySize(dict)).toBe(1);

    // Memory usage doesn't grow with repetitions
    const usage = getDictionaryMemoryUsage(dict);
    expect(usage).toBeLessThan(100); // Should be minimal
  });
});
