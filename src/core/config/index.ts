/**
 * Global configuration module.
 */

export { configure, getConfig, resetConfig, getDefaultConfig } from './config';
export type { MemoryConfig } from './config';

export {
  generateTaskId,
  requestAllocation,
  updateUsage,
  releaseAllocation,
  getMemoryStats,
  clearAllAllocations,
  checkFileAllocation,
} from './memory-tracker';

export type {
  TaskAllocation,
  AllocationResult,
  MemoryAllocationError,
  MemoryStats,
} from './memory-tracker';
