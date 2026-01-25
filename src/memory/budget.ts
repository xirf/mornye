/**
 * Memory budget tracker for the entire pipeline
 */
export interface MemoryBudget {
  /** Maximum memory allowed in bytes */
  limit: number;
  /** Warning threshold in bytes (default 78% of limit) */
  warningThreshold: number;
  /** Current memory usage in bytes */
  currentUsage: number;
}

/**
 * Creates a new memory budget
 * @param limit - Maximum memory in bytes (default: 512MB)
 * @returns MemoryBudget instance
 */
export function createMemoryBudget(limit: number = 512 * 1024 * 1024): MemoryBudget {
  if (limit <= 0) {
    throw new Error('Memory limit must be positive');
  }

  return {
    limit,
    warningThreshold: Math.floor(limit * 0.78), // ~400MB for 512MB limit
    currentUsage: 0,
  };
}

/**
 * Tracks memory allocation
 * @param budget - The memory budget
 * @param bytes - Number of bytes allocated
 */
export function trackAllocation(budget: MemoryBudget, bytes: number): void {
  if (bytes < 0) {
    throw new Error('Allocation size cannot be negative');
  }
  budget.currentUsage += bytes;
}

/**
 * Tracks memory deallocation
 * @param budget - The memory budget
 * @param bytes - Number of bytes deallocated
 */
export function trackDeallocation(budget: MemoryBudget, bytes: number): void {
  if (bytes < 0) {
    throw new Error('Deallocation size cannot be negative');
  }
  budget.currentUsage = Math.max(0, budget.currentUsage - bytes);
}

/**
 * Gets current memory usage
 * @param budget - The memory budget
 * @returns Current usage in bytes
 */
export function getMemoryUsage(budget: MemoryBudget): number {
  return budget.currentUsage;
}

/**
 * Checks if memory usage is near the limit
 * @param budget - The memory budget
 * @returns True if at or above warning threshold
 */
export function isNearLimit(budget: MemoryBudget): boolean {
  return budget.currentUsage >= budget.warningThreshold;
}

/**
 * Checks if warning threshold has been exceeded
 * @param budget - The memory budget
 * @returns True if above warning threshold
 */
export function hasExceededWarning(budget: MemoryBudget): boolean {
  return budget.currentUsage >= budget.warningThreshold;
}

/**
 * Checks if memory limit has been exceeded
 * @param budget - The memory budget
 * @returns True if above limit
 */
export function hasExceededLimit(budget: MemoryBudget): boolean {
  return budget.currentUsage >= budget.limit;
}
