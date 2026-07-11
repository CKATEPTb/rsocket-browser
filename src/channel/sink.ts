/**
 * Sink-style request-channel helper used by the high-level RSocket facade.
 */
import {
  Sinks,
  type Disposable,
  type Subscriber
} from "reactor-core-ts";
import { RSocketConnectionError } from "@/errors/index.js";
import type { RSocketFlux } from "@/stream/index.js";
import type {
  RSocketChannelInput,
  RSocketPayloadFrame,
  RSocketPayloadInput
} from "@/types/index.js";

/**
 * Factory used by sink-style request-channel helpers to create response Fluxes.
 */
type ChannelResponseFactory<D, M> = (
  payloads: RSocketChannelInput<D, M>
) => RSocketFlux<RSocketPayloadFrame<D, M>>;

/**
 * Minimal sink operations used by the non-replayable channel helper.
 */
interface UnicastChannelSink<D, M> {
  /** Emits one payload to the underlying Reactor sink. */
  tryEmitNext(payload: RSocketPayloadInput<D, M>): unknown;
  /** Completes the underlying Reactor sink. */
  tryEmitComplete(): unknown;
  /** Fails the underlying Reactor sink. */
  tryEmitError(error: unknown): unknown;
}

/**
 * Imperative request-channel helper returned by `socket.requestChannel()`.
 */
export class RSocketChannel<D = unknown, M = unknown> implements AsyncIterable<RSocketPayloadFrame<D, M>> {
  /** Flux of response payloads produced by the responder side of the channel. */
  readonly responses: RSocketFlux<RSocketPayloadFrame<D, M>>;
  /** Reactor-style sink facade for pushing outbound channel payloads. */
  readonly sink: {
    next: (payload: RSocketPayloadInput<D, M>) => RSocketChannel<D, M>;
    complete: () => void;
    error: (error: unknown) => void;
  };

  /**
   * Creates a channel helper around a low-level request-channel interaction.
   */
  constructor(responseFactory: ChannelResponseFactory<D, M>) {
    const payloads = Sinks.many().unicast().onBackpressureBuffer<RSocketPayloadInput<D, M>>();
    this.responses = responseFactory(payloads.asFlux());
    this.sink = {
      next: (payload) => this.nextUnicast(payloads, payload),
      complete: () => assertEmission(payloads.tryEmitComplete()),
      error: (error) => assertEmission(payloads.tryEmitError(error))
    };
  }

  /**
   * Pushes one outbound payload into the request channel.
   */
  next(payload: RSocketPayloadInput<D, M>): RSocketChannel<D, M> {
    return this.sink.next(payload);
  }

  /**
   * Completes the outbound side of the request channel.
   */
  complete(): void {
    this.sink.complete();
  }

  /**
   * Fails the outbound side of the request channel.
   */
  error(error: unknown): void {
    this.sink.error(error);
  }

  /**
   * Returns the response `Flux` for advanced Reactor-style composition.
   */
  asFlux(): RSocketFlux<RSocketPayloadFrame<D, M>> {
    return this.responses;
  }

  /** Subscribes to channel responses with a full Reactor subscriber. */
  subscribe(subscriber: Subscriber<RSocketPayloadFrame<D, M>>): void;
  /** Subscribes to channel responses with callback functions. */
  subscribe(
    onNext?: (value: RSocketPayloadFrame<D, M>) => void,
    onError?: (error: unknown) => void,
    onComplete?: () => void
  ): Disposable;
  /**
   * Subscribes to responder payloads emitted by the request channel.
   */
  subscribe(
    subscriberOrNext?: Subscriber<RSocketPayloadFrame<D, M>> | ((value: RSocketPayloadFrame<D, M>) => void),
    onError?: (error: unknown) => void,
    onComplete?: () => void
  ): Disposable | void {
    return this.responses.subscribe(subscriberOrNext as any, onError, onComplete);
  }

  /**
   * Allows `for await ... of` consumption of responder channel payloads.
   */
  [Symbol.asyncIterator](): AsyncIterator<RSocketPayloadFrame<D, M>> {
    return this.responses[Symbol.asyncIterator]();
  }

  /**
   * Pushes one payload into a single-subscription Reactor sink.
   */
  private nextUnicast(
    payloads: UnicastChannelSink<D, M>,
    payload: RSocketPayloadInput<D, M>
  ): RSocketChannel<D, M> {
    assertEmission(payloads.tryEmitNext(payload));
    return this;
  }
}

/**
 * Converts Reactor sink emission failures into RSocket connection errors.
 */
function assertEmission(result: unknown): void {
  if (result !== "OK") {
    throw new RSocketConnectionError(`RSocket channel sink emission failed: ${String(result)}`);
  }
}
