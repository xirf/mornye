import { beforeEach, describe, expect, test } from 'bun:test';
import {
  type MemoryBudget,
  createMemoryBudget,
  getMemoryUsage,
  hasExceededLimit,
  hasExceededWarning,
  isNearLimit,
  trackAllocation,
  trackDeallocation,
} from '../../src/memory/budget';

describe('createMemoryBudget', () => {
  test('creates budget with default 512MB limit', () => {
    const budget = createMemoryBudget();
    expect(budget.limit).toBe(512 * 1024 * 1024); // 512MB in bytes
    expect(budget.warningThreshold).toBe(Math.floor(512 * 1024 * 1024 * 0.78)); // 78% of 512MB
    expect(budget.currentUsage).toBe(0);
  });

  test('creates budget with custom limit', () => {
    const budget = createMemoryBudget(256 * 1024 * 1024); // 256MB
    expect(budget.limit).toBe(256 * 1024 * 1024);
    expect(budget.warningThreshold).toBe(Math.floor(256 * 1024 * 1024 * 0.78)); // 78% of 256MB
  });

  test('calculates warning threshold as 78% of limit', () => {
    const budget = createMemoryBudget(1000);
    expect(budget.warningThreshold).toBe(780); // 78% of 1000
  });

  test('rejects zero limit', () => {
    expect(() => createMemoryBudget(0)).toThrow();
  });

  test('rejects negative limit', () => {
    expect(() => createMemoryBudget(-100)).toThrow();
  });
});

describe('trackAllocation', () => {
  let budget: MemoryBudget;

  beforeEach(() => {
    budget = createMemoryBudget(1000);
  });

  test('tracks allocation', () => {
    trackAllocation(budget, 100);
    expect(budget.currentUsage).toBe(100);
  });

  test('accumulates allocations', () => {
    trackAllocation(budget, 100);
    trackAllocation(budget, 200);
    trackAllocation(budget, 50);
    expect(budget.currentUsage).toBe(350);
  });

  test('rejects negative allocation', () => {
    expect(() => trackAllocation(budget, -50)).toThrow();
  });

  test('allows zero allocation', () => {
    trackAllocation(budget, 0);
    expect(budget.currentUsage).toBe(0);
  });
});

describe('trackDeallocation', () => {
  let budget: MemoryBudget;

  beforeEach(() => {
    budget = createMemoryBudget(1000);
    trackAllocation(budget, 500);
  });

  test('tracks deallocation', () => {
    trackDeallocation(budget, 100);
    expect(budget.currentUsage).toBe(400);
  });

  test('prevents negative usage', () => {
    trackDeallocation(budget, 600); // More than allocated
    expect(budget.currentUsage).toBe(0); // Clamps to 0
  });

  test('rejects negative deallocation', () => {
    expect(() => trackDeallocation(budget, -50)).toThrow();
  });

  test('allows zero deallocation', () => {
    trackDeallocation(budget, 0);
    expect(budget.currentUsage).toBe(500);
  });
});

describe('getMemoryUsage', () => {
  test('returns current usage', () => {
    const budget = createMemoryBudget(1000);
    trackAllocation(budget, 250);
    expect(getMemoryUsage(budget)).toBe(250);
  });

  test('returns usage percentage', () => {
    const budget = createMemoryBudget(1000);
    trackAllocation(budget, 250);
    const percentage = (getMemoryUsage(budget) / budget.limit) * 100;
    expect(percentage).toBe(25);
  });
});

describe('isNearLimit', () => {
  test('returns false when well below warning threshold', () => {
    const budget = createMemoryBudget(1000); // warning at 780
    trackAllocation(budget, 500);
    expect(isNearLimit(budget)).toBe(false);
  });

  test('returns true when at warning threshold', () => {
    const budget = createMemoryBudget(1000); // warning at 780
    trackAllocation(budget, 780);
    expect(isNearLimit(budget)).toBe(true);
  });

  test('returns true when above warning threshold', () => {
    const budget = createMemoryBudget(1000); // warning at 780
    trackAllocation(budget, 900);
    expect(isNearLimit(budget)).toBe(true);
  });
});

describe('hasExceededWarning', () => {
  test('returns false when below warning', () => {
    const budget = createMemoryBudget(1000);
    trackAllocation(budget, 700);
    expect(hasExceededWarning(budget)).toBe(false);
  });

  test('returns true when at warning', () => {
    const budget = createMemoryBudget(1000);
    trackAllocation(budget, 780);
    expect(hasExceededWarning(budget)).toBe(true);
  });

  test('returns true when above warning', () => {
    const budget = createMemoryBudget(1000);
    trackAllocation(budget, 850);
    expect(hasExceededWarning(budget)).toBe(true);
  });
});

describe('hasExceededLimit', () => {
  test('returns false when below limit', () => {
    const budget = createMemoryBudget(1000);
    trackAllocation(budget, 900);
    expect(hasExceededLimit(budget)).toBe(false);
  });

  test('returns true when at limit', () => {
    const budget = createMemoryBudget(1000);
    trackAllocation(budget, 1000);
    expect(hasExceededLimit(budget)).toBe(true);
  });

  test('returns true when above limit', () => {
    const budget = createMemoryBudget(1000);
    trackAllocation(budget, 1100);
    expect(hasExceededLimit(budget)).toBe(true);
  });
});

describe('Real-world scenarios', () => {
  test('typical workflow under budget', () => {
    const budget = createMemoryBudget(512 * 1024 * 1024); // 512MB

    // Load CSV (100MB)
    trackAllocation(budget, 100 * 1024 * 1024);
    expect(hasExceededWarning(budget)).toBe(false);

    // Build dictionary (50MB)
    trackAllocation(budget, 50 * 1024 * 1024);
    expect(hasExceededWarning(budget)).toBe(false);

    // Type conversion cache (150MB)
    trackAllocation(budget, 150 * 1024 * 1024);
    expect(hasExceededWarning(budget)).toBe(false);

    // Total: 300MB - still under warning
    expect(getMemoryUsage(budget)).toBe(300 * 1024 * 1024);
    expect(hasExceededWarning(budget)).toBe(false);
  });

  test('workflow triggering warning', () => {
    const budget = createMemoryBudget(512 * 1024 * 1024); // 512MB

    // Allocate 410MB
    trackAllocation(budget, 410 * 1024 * 1024);
    expect(hasExceededWarning(budget)).toBe(true);
    expect(hasExceededLimit(budget)).toBe(false);
  });

  test('workflow with deallocation to free memory', () => {
    const budget = createMemoryBudget(512 * 1024 * 1024);

    // Allocate 450MB
    trackAllocation(budget, 450 * 1024 * 1024);
    expect(hasExceededWarning(budget)).toBe(true);

    // Drop cache (200MB)
    trackDeallocation(budget, 200 * 1024 * 1024);
    expect(getMemoryUsage(budget)).toBe(250 * 1024 * 1024);
    expect(hasExceededWarning(budget)).toBe(false);
  });

  test('auto-detect system memory (simulated)', () => {
    // In real implementation, this would use Bun.availableMemory() or similar
    // For now, just test the 512MB default
    const budget = createMemoryBudget();
    expect(budget.limit).toBe(512 * 1024 * 1024);
  });
});
