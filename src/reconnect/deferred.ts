/**
 * Small deferred promise helper used to expose `socket.ready()`.
 */
export interface Deferred<T> {
  /** Promise exposed to callers waiting for readiness. */
  readonly promise: Promise<T>;
  /** Whether the deferred value has already resolved or rejected. */
  readonly settled: boolean;
  /** Resolves the promise exactly once. */
  resolve(value: T): void;
  /** Rejects the promise exactly once. */
  reject(error: unknown): void;
}

/**
 * Creates a promise plus idempotent `resolve` and `reject` callbacks.
 */
export function deferred<T>(): Deferred<T> {
  let settled = false;
  let resolvePromise!: (value: T) => void;
  let rejectPromise!: (error: unknown) => void;
  const promise = new Promise<T>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  promise.catch(() => undefined);

  return {
    promise,
    /**
     * Reports whether the deferred promise is already settled.
     */
    get settled() {
      return settled;
    },
    /**
     * Resolves the deferred promise unless it was already settled.
     */
    resolve(value) {
      if (settled) return;
      settled = true;
      resolvePromise(value);
    },
    /**
     * Rejects the deferred promise unless it was already settled.
     */
    reject(error) {
      if (settled) return;
      settled = true;
      rejectPromise(error);
    }
  };
}
