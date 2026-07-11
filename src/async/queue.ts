/** Shared terminal iterator result reused after completion. */
const DONE: IteratorResult<never> = Object.freeze({done: true, value: undefined as never});
/** Shared resolved terminal promise reused by completed queues. */
const DONE_PROMISE = Promise.resolve(DONE);

/**
 * FIFO queue that exposes pushed values through the async iterator protocol.
 *
 * The queue is deliberately tiny: it supports values, terminal completion, and
 * terminal errors. It is used where the library has to adapt callback-style
 * Reactive Streams subscribers into `for await` consumption while preserving
 * order and avoiding repeated array shifts.
 *
 * @typeParam T - Value type delivered by the queue.
 */
export class AsyncQueue<T> implements AsyncIterable<T>, AsyncIterator<T> {
    private readonly values: T[] = [];
    private readonly waiters: Array<{
        resolve: (result: IteratorResult<T>) => void;
        reject: (error: unknown) => void;
    }> = [];
    private readIndex = 0;
    private waiterIndex = 0;
    private closed = false;
    private failure: unknown;
    private failed = false;

    /**
     * Adds a value to the queue or resolves the oldest pending consumer.
     *
     * Values pushed after completion or error are ignored because the async
     * iterator contract has already reached a terminal state.
     */
    push(value: T): void {
        if (this.closed || this.failed) return;
        const waiter = this.nextWaiter();
        if (waiter) {
            waiter.resolve({done: false, value});
            return;
        }
        this.values.push(value);
    }

    /**
     * Completes the queue and resolves every pending consumer with `done: true`.
     */
    complete(): void {
        if (this.closed || this.failed) return;
        this.closed = true;
        this.flushTerminal();
    }

    /**
     * Fails the queue and rejects every pending or future consumer.
     */
    error(error: unknown): void {
        if (this.closed || this.failed) return;
        this.failed = true;
        this.failure = error;
        this.flushTerminal();
    }

    /**
     * Reads the next queued value or waits until a producer pushes one.
     *
     * @returns A promise for the next iterator result.
     */
    next(): Promise<IteratorResult<T>> {
        if (this.readIndex < this.values.length) {
            const value = this.values[this.readIndex] as T;
            this.readIndex += 1;
            this.compact();
            return Promise.resolve({done: false, value});
        }
        if (this.failed) return Promise.reject(this.failure);
        if (this.closed) return this.donePromise();

        return new Promise<IteratorResult<T>>((resolve, reject) => {
            this.waiters.push({resolve, reject});
        });
    }

    /**
     * Stops iteration from the consumer side and marks the queue complete.
     */
    return(): Promise<IteratorResult<T>> {
        this.closed = true;
        this.failed = false;
        this.failure = undefined;
        this.values.length = 0;
        this.readIndex = 0;
        this.flushTerminal();
        return this.donePromise();
    }

    /**
     * Returns this queue as its own async iterator.
     */
    [Symbol.asyncIterator](): AsyncIterator<T> {
        return this;
    }

    /**
     * Resolves or rejects all consumers that were already waiting for a value.
     */
    private flushTerminal(): void {
        for (; ;) {
            const waiter = this.nextWaiter();
            if (!waiter) break;
            if (this.failed) waiter.reject(this.failure);
            else waiter.resolve(this.done());
        }
        this.waiters.length = 0;
        this.waiterIndex = 0;
    }

    /**
     * Compacts consumed buffered values after enough reads have accumulated.
     */
    private compact(): void {
        if (this.readIndex === this.values.length) {
            this.values.length = 0;
            this.readIndex = 0;
            return;
        }
        if (this.readIndex < 1024 || this.readIndex * 2 < this.values.length) return;
        compactArray(this.values, this.readIndex);
        this.readIndex = 0;
    }

    /**
     * Returns the oldest waiting consumer, skipping already consumed waiter slots.
     */
    private nextWaiter(): {
        resolve: (result: IteratorResult<T>) => void;
        reject: (error: unknown) => void
    } | undefined {
        if (this.waiterIndex >= this.waiters.length) return undefined;
        const waiter = this.waiters[this.waiterIndex];
        this.waiterIndex += 1;
        this.compactWaiters();
        return waiter;
    }

    /**
     * Compacts consumed waiter slots after enough waiters have been served.
     */
    private compactWaiters(): void {
        if (this.waiterIndex === this.waiters.length) {
            this.waiters.length = 0;
            this.waiterIndex = 0;
            return;
        }
        if (this.waiterIndex < 1024 || this.waiterIndex * 2 < this.waiters.length) return;
        compactArray(this.waiters, this.waiterIndex);
        this.waiterIndex = 0;
    }

    /**
     * Creates the terminal iterator result required by async iterators.
     */
    private done(): IteratorResult<T> {
        return DONE as IteratorResult<T>;
    }

    /**
     * Returns a shared resolved terminal promise.
     */
    private donePromise(): Promise<IteratorResult<T>> {
        return DONE_PROMISE as Promise<IteratorResult<T>>;
    }
}

/**
 * Removes consumed prefix entries without allocating the array returned by `splice`.
 */
function compactArray<T>(values: T[], consumed: number): void {
    const remaining = values.length - consumed;
    for (let index = 0; index < remaining; index++) values[index] = values[index + consumed] as T;
    values.length = remaining;
}
