/**
 * Memory configuration for server-side safety.
 */
export interface MemoryConfig {
  /** Global memory budget in bytes (default: 1GB) */
  globalLimitBytes: number;

  /** Max percentage a single task can take when multiple are active (default: 0.70) */
  maxTaskSharePercent: number;

  /** Whether to enable memory tracking (default: true) */
  enabled: boolean;
}

/**
 * Default memory configuration.
 */
const DEFAULT_CONFIG: MemoryConfig = {
  globalLimitBytes: 1024 * 1024 * 1024, // 1GB
  maxTaskSharePercent: 0.7, // 70%
  enabled: true,
};

/** Current global configuration */
let currentConfig: MemoryConfig = { ...DEFAULT_CONFIG };

/**
 * Configure global memory limits.
 *
 * @example
 * ```ts
 * import { configure } from 'mornye';
 *
 * // Set 512MB global limit
 * configure({ globalLimitBytes: 512 * 1024 * 1024 });
 *
 * // Disable memory tracking entirely
 * configure({ enabled: false });
 * ```
 */
export function configure(options: Partial<MemoryConfig>): void {
  currentConfig = { ...currentConfig, ...options };
}

/**
 * Get current memory configuration.
 */
export function getConfig(): Readonly<MemoryConfig> {
  return currentConfig;
}

/**
 * Reset configuration to defaults.
 */
export function resetConfig(): void {
  currentConfig = { ...DEFAULT_CONFIG };
}

/**
 * Get default configuration.
 */
export function getDefaultConfig(): Readonly<MemoryConfig> {
  return DEFAULT_CONFIG;
}
