/**
 * Request-channel input adaptation helpers.
 */
import { Flux } from "reactor-core-ts";
import type { RSocketChannelInput, RSocketPayloadInput } from "@/types/index.js";

/** Shared terminal iterator result for synchronous iterable adapters. */
const DONE_RESULT = Object.freeze({ done: true, value: undefined as never });
/** Shared resolved terminal result to avoid allocating on every adapter return. */
const DONE_PROMISE = Promise.resolve(DONE_RESULT);
/** Internal marker for wrappers that preserve a prefetch-safe source category. */
const PREFETCHABLE_CHANNEL_INPUT = Symbol("rsocket.prefetchableChannelInput");

/**
 * Iterator returned for request-channel sources.
 */
export type RSocketChannelInputIterator<D = unknown, M = unknown> =
  | Iterator<RSocketPayloadInput<D, M>>
  | AsyncIterator<RSocketPayloadInput<D, M>>;

/**
 * Returns an async iterable for any supported request-channel input.
 */
export function channelInputIterable<D, M>(
  input: RSocketChannelInput<D, M>
): AsyncIterable<RSocketPayloadInput<D, M>> {
  if (isChannelInputAsyncIterable<RSocketPayloadInput<D, M>>(input)) return input;
  if (isChannelInputIterable<RSocketPayloadInput<D, M>>(input)) return iterableAsAsync(input);
  return Flux.from(input) as AsyncIterable<RSocketPayloadInput<D, M>>;
}

/**
 * Returns the cheapest iterator for a request-channel input source.
 */
export function channelInputIterator<D, M>(
  input: RSocketChannelInput<D, M>
): RSocketChannelInputIterator<D, M> {
  if (isChannelInputAsyncIterable<RSocketPayloadInput<D, M>>(input)) return input[Symbol.asyncIterator]();
  if (isChannelInputIterable<RSocketPayloadInput<D, M>>(input)) return input[Symbol.iterator]();
  return (Flux.from(input) as AsyncIterable<RSocketPayloadInput<D, M>>)[Symbol.asyncIterator]();
}

/**
 * Detects inputs that already expose async iteration.
 */
export function isChannelInputAsyncIterable<T>(input: unknown): input is AsyncIterable<T> {
  return typeof input === "object"
    && input !== null
    && typeof (input as Partial<AsyncIterable<T>>)[Symbol.asyncIterator] === "function";
}

/**
 * Detects synchronous iterable inputs such as arrays.
 */
export function isChannelInputIterable<T>(input: unknown): input is Iterable<T> {
  return typeof input === "object"
    && input !== null
    && typeof (input as Partial<Iterable<T>>)[Symbol.iterator] === "function";
}

/**
 * Marks a wrapper as preserving a source that may be read one item ahead for terminal completion.
 */
export function markPrefetchableChannelInput<T extends object>(input: T): T {
  (input as { [PREFETCHABLE_CHANNEL_INPUT]?: true })[PREFETCHABLE_CHANNEL_INPUT] = true;
  return input;
}

/**
 * Returns whether request-channel may read one item ahead when demand reaches zero.
 */
export function canPrefetchChannelInput(input: unknown): boolean {
  if (typeof input !== "object" || input === null) {
    return false;
  }

  const source = input as {
    readonly [PREFETCHABLE_CHANNEL_INPUT]?: true;
    readonly subscribe?: unknown;
    readonly then?: unknown;
    readonly [Symbol.asyncIterator]?: unknown;
    readonly [Symbol.iterator]?: unknown;
  };

  return source[PREFETCHABLE_CHANNEL_INPUT] === true ||
    typeof source.subscribe === "function" ||
    typeof source.then === "function" ||
    (typeof source[Symbol.asyncIterator] !== "function" && typeof source[Symbol.iterator] === "function");
}

/**
 * Prepends one initial payload while preserving the source's cheapest iteration path.
 */
export function prependChannelPayload<D, M>(
  payload: RSocketPayloadInput<D, M>,
  input: RSocketChannelInput<D, M>
): RSocketChannelInput<D, M> {
  if (!isChannelInputAsyncIterable(input) && isChannelInputIterable<RSocketPayloadInput<D, M>>(input)) {
    return prependChannelPayloadSync(payload, input);
  }
  const prefixed = prependChannelPayloadAsync(payload, input);
  return canPrefetchChannelInput(input) ? markPrefetchableChannelInput(prefixed) : prefixed;
}

/** Prepends one payload to a synchronous channel input. */
function* prependChannelPayloadSync<D, M>(
  payload: RSocketPayloadInput<D, M>,
  input: Iterable<RSocketPayloadInput<D, M>>
): Iterable<RSocketPayloadInput<D, M>> {
  yield payload;
  yield* input;
}

/** Prepends one payload to an asynchronous or publisher-backed channel input. */
async function* prependChannelPayloadAsync<D, M>(
  payload: RSocketPayloadInput<D, M>,
  input: RSocketChannelInput<D, M>
): AsyncIterable<RSocketPayloadInput<D, M>> {
  yield payload;
  for await (const item of channelInputIterable(input)) yield item;
}

/**
 * Wraps a synchronous iterator in the async iterator contract.
 */
function iterableAsAsync<T>(input: Iterable<T>): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      const iterator = input[Symbol.iterator]();
      return {
        next: () => Promise.resolve(iterator.next()),
        return: () => {
          try {
            iterator.return?.();
            return DONE_PROMISE as Promise<IteratorReturnResult<T>>;
          } catch (error) {
            return Promise.reject(error);
          }
        }
      };
    }
  };
}
