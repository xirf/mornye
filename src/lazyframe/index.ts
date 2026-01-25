// Query plan
export type {
  PlanNode,
  ScanPlan,
  FilterPlan,
  SelectPlan,
  GroupByPlan,
} from './plan';
export { QueryPlan } from './plan';

// LazyFrame
export { LazyFrame } from './lazyframe';

// Executor
export { executePlan, optimizePlan, explainQueryPlan } from './executor';

// Optimizer
export {
  deduplicateFilters,
  explainPlan,
  type OptimizationStats,
} from './optimizer';

// Cache system
export type { StringDictionaryCache } from './cache';
export {
  TypeConversionCache,
  ComputedCache,
  CacheManager,
  getCacheManager,
  resetCacheManager,
} from './cache';
