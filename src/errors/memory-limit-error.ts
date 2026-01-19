import { MolniyaError } from './base';

/**
 * Error returned when memory limits are exceeded.
 *
 * This error is designed to be returned in result objects, not thrown,
 * following the "never throw" pattern for server-side safety.
 *
 * @example
 * ```ts
 * const { df, memoryError } = await readCsv('./huge.csv', {
 *   memoryLimitBytes: 100 * 1024 * 1024, // 100MB
 * });
 *
 * if (memoryError) {
 *   console.log(memoryError.format());
 *   // Handle gracefully - df may be partial or empty
 * }
 * ```
 */
export class MemoryLimitError extends MolniyaError {
  readonly code = 'MEMORY_LIMIT_EXCEEDED';

  /** Bytes that were requested */
  readonly requestedBytes: number;

  /** Bytes that were available */
  readonly availableBytes: number;

  /** Global limit configured */
  readonly globalLimitBytes: number;

  /** Number of active tasks at time of error */
  readonly activeTaskCount: number;

  constructor(
    requestedBytes: number,
    availableBytes: number,
    globalLimitBytes: number,
    activeTaskCount = 0,
  ) {
    const requestedMB = (requestedBytes / (1024 * 1024)).toFixed(1);
    const availableMB = (availableBytes / (1024 * 1024)).toFixed(1);
    const globalMB = (globalLimitBytes / (1024 * 1024)).toFixed(1);

    const message = `Memory limit exceeded: requested ${requestedMB}MB but only ${availableMB}MB available${activeTaskCount > 1 ? ` (${activeTaskCount} active tasks sharing ${globalMB}MB limit)` : ''}`;

    const hint =
      activeTaskCount > 1
        ? 'Consider reducing concurrent operations or increasing globalLimitBytes via configure()'
        : 'Consider using scanCsv() for streaming large files, or increase globalLimitBytes';

    super(message, hint);
    this.name = 'MemoryLimitError';
    this.requestedBytes = requestedBytes;
    this.availableBytes = availableBytes;
    this.globalLimitBytes = globalLimitBytes;
    this.activeTaskCount = activeTaskCount;
  }

  protected override _getExpression(): string {
    return 'readCsv(path, { memoryLimitBytes: ... })';
  }

  protected override _getDetail(): string {
    return `requested ${this.requestedBytes} bytes, available ${this.availableBytes} bytes`;
  }

  /**
   * Create from an allocation error.
   */
  static fromAllocationError(error: {
    requestedBytes: number;
    availableBytes: number;
    globalLimitBytes: number;
    activeTaskCount: number;
  }): MemoryLimitError {
    return new MemoryLimitError(
      error.requestedBytes,
      error.availableBytes,
      error.globalLimitBytes,
      error.activeTaskCount,
    );
  }
}
