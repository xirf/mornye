/**
 * Global memory tracker for managing allocations across all operations.
 *
 * Implements a priority-based allocation system:
 * - Single task: can use up to 100% of global limit
 * - Multiple tasks: each can claim up to maxTaskSharePercent (default 70%)
 */

import { getConfig } from './config';

/**
 * Task allocation record.
 */
export interface TaskAllocation {
  /** Unique task identifier */
  id: string;
  /** Maximum allocated bytes for this task */
  allocatedBytes: number;
  /** Currently used bytes */
  usedBytes: number;
  /** Task start time (epoch ms) */
  startTime: number;
}

/**
 * Result of an allocation request.
 */
export interface AllocationResult {
  /** Whether allocation was successful */
  success: boolean;
  /** Allocated bytes (may be less than requested) */
  allocatedBytes: number;
  /** Error if allocation failed */
  error?: MemoryAllocationError;
}

/**
 * Memory allocation error details.
 */
export interface MemoryAllocationError {
  /** Requested bytes */
  requestedBytes: number;
  /** Available bytes at time of request */
  availableBytes: number;
  /** Global limit configured */
  globalLimitBytes: number;
  /** Number of active tasks */
  activeTaskCount: number;
}

/**
 * Memory usage statistics.
 */
export interface MemoryStats {
  /** Total bytes allocated across all tasks */
  totalAllocatedBytes: number;
  /** Total bytes currently used */
  totalUsedBytes: number;
  /** Global limit from config */
  globalLimitBytes: number;
  /** Available bytes for new allocations */
  availableBytes: number;
  /** Number of active tasks */
  activeTaskCount: number;
  /** Per-task allocations */
  tasks: ReadonlyMap<string, TaskAllocation>;
}

/** Task ID counter */
let taskIdCounter = 0;

/** Active task allocations */
const allocations = new Map<string, TaskAllocation>();

/**
 * Generate a unique task ID.
 */
export function generateTaskId(): string {
  return `task_${Date.now()}_${++taskIdCounter}`;
}

/**
 * Request a memory allocation for a task.
 *
 * Priority rules:
 * - If single task: can use up to 100% of global limit
 * - If multiple tasks: each can use up to maxTaskSharePercent (70%) of global limit
 *
 * @param taskId - Unique task identifier
 * @param requestedBytes - Bytes requested for this task
 * @returns Allocation result with success status and actual allocated bytes
 */
export function requestAllocation(taskId: string, requestedBytes: number): AllocationResult {
  const config = getConfig();

  if (!config.enabled) {
    // Memory tracking disabled - always succeed with requested amount
    return { success: true, allocatedBytes: requestedBytes };
  }

  const globalLimit = config.globalLimitBytes;
  const maxShare = config.maxTaskSharePercent;

  // Calculate currently allocated bytes (excluding this task if updating)
  let otherAllocated = 0;
  for (const [id, alloc] of allocations) {
    if (id !== taskId) {
      otherAllocated += alloc.allocatedBytes;
    }
  }

  const available = globalLimit - otherAllocated;

  // Determine max allocation for this task
  // If other tasks exist, limit to maxTaskSharePercent of global
  const otherTasksExist = allocations.size > 0 && !allocations.has(taskId);
  const maxForTask = otherTasksExist ? Math.min(globalLimit * maxShare, available) : available;

  if (requestedBytes > maxForTask) {
    // Cannot fulfill request
    return {
      success: false,
      allocatedBytes: 0,
      error: {
        requestedBytes,
        availableBytes: maxForTask,
        globalLimitBytes: globalLimit,
        activeTaskCount: allocations.size,
      },
    };
  }

  // Create or update allocation
  const existing = allocations.get(taskId);
  const allocation: TaskAllocation = {
    id: taskId,
    allocatedBytes: requestedBytes,
    usedBytes: existing?.usedBytes ?? 0,
    startTime: existing?.startTime ?? Date.now(),
  };

  allocations.set(taskId, allocation);

  return {
    success: true,
    allocatedBytes: requestedBytes,
  };
}

/**
 * Update memory usage for a task.
 *
 * @param taskId - Task identifier
 * @param usedBytes - Current bytes used by the task
 */
export function updateUsage(taskId: string, usedBytes: number): void {
  const allocation = allocations.get(taskId);
  if (allocation) {
    allocation.usedBytes = usedBytes;
  }
}

/**
 * Release a task's memory allocation.
 *
 * @param taskId - Task identifier to release
 */
export function releaseAllocation(taskId: string): void {
  allocations.delete(taskId);
}

/**
 * Get current memory statistics.
 */
export function getMemoryStats(): MemoryStats {
  const config = getConfig();

  let totalAllocated = 0;
  let totalUsed = 0;

  for (const alloc of allocations.values()) {
    totalAllocated += alloc.allocatedBytes;
    totalUsed += alloc.usedBytes;
  }

  return {
    totalAllocatedBytes: totalAllocated,
    totalUsedBytes: totalUsed,
    globalLimitBytes: config.globalLimitBytes,
    availableBytes: config.globalLimitBytes - totalAllocated,
    activeTaskCount: allocations.size,
    tasks: new Map(allocations),
  };
}

/**
 * Clear all allocations (for testing).
 */
export function clearAllAllocations(): void {
  allocations.clear();
}

/**
 * Check if a file can be loaded within memory limits.
 *
 * @param fileSizeBytes - Size of file in bytes
 * @param estimatedMultiplier - Memory overhead multiplier (default: 2.5 for typed arrays)
 * @returns Allocation result
 */
export function checkFileAllocation(
  fileSizeBytes: number,
  estimatedMultiplier = 2.5,
): AllocationResult {
  const estimatedMemory = Math.ceil(fileSizeBytes * estimatedMultiplier);
  const taskId = generateTaskId();
  const result = requestAllocation(taskId, estimatedMemory);

  // If successful, immediately release - this is just a check
  if (result.success) {
    releaseAllocation(taskId);
  }

  return result;
}
