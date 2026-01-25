import { describe, expect, test } from 'bun:test';
import { type Result, err, ok } from '../../src/types/result';

describe('Result Type', () => {
  describe('ok()', () => {
    test('creates successful result', () => {
      const result = ok(42);
      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.data).toBe(42);
      }
    });

    test('works with objects', () => {
      const data = { name: 'test', value: 123 };
      const result = ok(data);
      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.data).toEqual(data);
      }
    });

    test('works with null and undefined', () => {
      const nullResult = ok(null);
      const undefinedResult = ok(undefined);
      expect(nullResult.ok).toBe(true);
      expect(undefinedResult.ok).toBe(true);
    });
  });

  describe('err()', () => {
    test('creates error result with string', () => {
      const result = err('Something went wrong');
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.error).toBe('Something went wrong');
      }
    });

    test('creates error result with Error object', () => {
      const error = new Error('Test error');
      const result = err(error);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.error).toBe(error);
      }
    });

    test('creates error result with custom error', () => {
      const customError = { code: 'ERR_TEST', message: 'Custom error' };
      const result = err(customError);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.error).toEqual(customError);
      }
    });
  });

  describe('Type narrowing', () => {
    test('narrows type with ok check', () => {
      const result: Result<number, string> = ok(42);
      if (result.ok) {
        // TypeScript should know result.data is number
        const num: number = result.data;
        expect(num).toBe(42);
      } else {
        // Should not reach here
        expect(true).toBe(false);
      }
    });

    test('narrows type with error check', () => {
      const result: Result<number, string> = err('failed');
      if (!result.ok) {
        // TypeScript should know result.error is string
        const errMsg: string = result.error;
        expect(errMsg).toBe('failed');
      } else {
        // Should not reach here
        expect(true).toBe(false);
      }
    });
  });

  describe('Chaining operations', () => {
    test('can chain successful results', () => {
      const parseNumber = (str: string): Result<number, string> => {
        const num = Number.parseFloat(str);
        return Number.isNaN(num) ? err('Not a number') : ok(num);
      };

      const result1 = parseNumber('42');
      expect(result1.ok).toBe(true);

      const result2 = parseNumber('not a number');
      expect(result2.ok).toBe(false);
    });

    test('propagates errors in async operations', async () => {
      const asyncOp = async (value: number): Promise<Result<number, string>> => {
        if (value < 0) return err('Negative value');
        return ok(value * 2);
      };

      const result1 = await asyncOp(5);
      expect(result1.ok).toBe(true);
      if (result1.ok) expect(result1.data).toBe(10);

      const result2 = await asyncOp(-5);
      expect(result2.ok).toBe(false);
      if (!result2.ok) expect(result2.error).toBe('Negative value');
    });
  });
});
