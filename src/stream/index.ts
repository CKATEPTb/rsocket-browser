/**
 * Reactive Streams implementation for RSocket request-stream and request-channel
 * responses.
 */
import { Flux, type Subscriber, type Subscription } from "reactor-core-ts";
import {
  CancelFrame,
  ErrorFrame,
  FrameErrorCode,
  PayloadFlag,
  PayloadFrame,
  RequestNFrame,
  type Frame
} from "rsocket-frames-ts";
import { MAX_REQUEST_N } from "@/protocol/index.js";
import { AsyncQueue } from "@/async/index.js";
import { decodeFramePayload, errorPayload } from "@/payload/index.js";
import type { RSocketPayloadFrame } from "@/types/index.js";
import { errorFromFrame, RSocketProtocolError } from "@/errors/index.js";

/** No-op subscription used when stream setup fails before a real subscription exists. */
const EMPTY_SUBSCRIPTION: Subscription = Object.freeze({
  /** Ignores demand because the stream is already terminal. */
  request() {},
  /** Ignores cancellation because there is no upstream to cancel. */
  cancel() {}
});

/** Internal key used to access an RSocket Flux subscription without nesting another Reactor subscription. */
const DIRECT_SUBSCRIPTION = Symbol("RSocket.directSubscription");

/**
 * Optional hook for subscriptions that need to delay signals until after
 * `onSubscribe` has been delivered by `RSocketFlux`.
 */
interface AfterSubscribeSubscription extends Subscription {
  /** Called immediately after the downstream subscriber receives this subscription. */
  afterSubscribe(): void;
}

/**
 * Detects subscriptions with a post-`onSubscribe` hook.
 */
function hasAfterSubscribe(subscription: Subscription): subscription is AfterSubscribeSubscription {
  return typeof (subscription as Partial<AfterSubscribeSubscription>).afterSubscribe === "function";
}

/**
 * Minimal session operations needed by stream subscriptions.
 */
export interface StreamSession {
  /** Sends a frame through the active requester session. */
  sendFrame(frame: Frame): void;
  /** Removes a stream controller and any stored fragments. */
  unregisterStream(streamId: number): void;
  /** Closes the session because a protocol violation was detected. */
  protocolError(error: RSocketProtocolError): void;
}

/**
 * Handles incoming frames for a single stream id.
 */
export interface StreamController {
  /** Client stream id owned by this controller. */
  readonly streamId: number;
  /** Handles a PAYLOAD frame for this stream. */
  handlePayload(frame: PayloadFrame): void;
  /** Handles an ERROR frame for this stream. */
  handleError(frame: ErrorFrame): void;
  /** Handles responder demand for request-channel outbound payloads. */
  handleRequestN(frame: RequestNFrame): void;
  /** Handles responder cancellation. */
  handleCancel(): void;
  /** Fails the stream and notifies the subscriber if appropriate. */
  fail(error: unknown): void;
}

/**
 * Outbound request-channel direction controlled by responder demand.
 */
export interface OutboundChannel {
  /** Adds demand received from responder REQUEST_N frames. */
  addDemand(n: number): void;
  /** Aborts the outbound publisher and releases resources. */
  abort(error?: unknown): void;
}

/**
 * Starts a stream after initial subscriber demand arrives.
 */
export type StartStream = (
  streamId: number,
  initialRequestN: number,
  subscription: RSocketStreamSubscription
) => void;

/**
 * `Flux` implementation that starts an RSocket stream only after demand.
 */
export class RSocketFlux<T extends RSocketPayloadFrame = RSocketPayloadFrame> extends Flux<T> {
  /**
   * Creates a Flux backed by an RSocket stream subscription factory.
   */
  constructor(private readonly subscriptionFactory: (subscriber: Subscriber<T>) => Subscription) {
    super((signal) => createAsyncIterable(signal, subscriptionFactory));
  }

  /**
   * Subscribes a Reactive Streams subscriber to an RSocket stream.
   */
  protected override subscribeActual(subscriber: Subscriber<T>): void {
    let subscription: Subscription;
    try {
      subscription = this.subscriptionFactory(subscriber);
    } catch (error) {
      try {
        subscriber.onSubscribe(EMPTY_SUBSCRIPTION);
        subscriber.onError(error);
      } catch {
        // Subscriber callbacks are user code; setup has already failed.
      }
      return;
    }
    try {
      subscriber.onSubscribe(subscription);
    } catch {
      try {
        subscription.cancel();
      } catch {
        // The subscriber already rejected the subscription; cleanup is best-effort.
      }
      return;
    }
    if (hasAfterSubscribe(subscription)) subscription.afterSubscribe();
  }

  /**
   * Creates the underlying subscription for internal facade composition.
   */
  [DIRECT_SUBSCRIPTION](subscriber: Subscriber<T>): Subscription {
    return this.subscriptionFactory(subscriber);
  }
}

/**
 * Unwraps an RSocket Flux inside another RSocket Flux without a nested subscribe call.
 */
export function directRSocketFluxSubscription<T extends RSocketPayloadFrame>(
  source: RSocketFlux<T>,
  subscriber: Subscriber<T>
): Subscription {
  return source[DIRECT_SUBSCRIPTION](subscriber);
}

/**
 * Bridges a Reactive Streams subscription into an async iterable.
 *
 * Each `next()` call requests one response payload, which keeps async iterator
 * consumption aligned with RSocket backpressure.
 */
function createAsyncIterable<T extends RSocketPayloadFrame>(
  signal: AbortSignal,
  subscriptionFactory: (subscriber: Subscriber<T>) => Subscription
): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]: () => {
      const queue = new AsyncQueue<T>();
      let subscription: Subscription | undefined;
      let abortRegistered = false;
      const cleanupAbortListener = (): void => {
        if (!abortRegistered) return;
        abortRegistered = false;
        signal.removeEventListener("abort", abort);
      };
      function abort(): void {
        cleanupAbortListener();
        subscription?.cancel();
        void queue.return();
      }

      const subscriber: Subscriber<T> = {
        /** Stores the subscription so iterator pulls can request one item. */
        onSubscribe(nextSubscription) {
          subscription = nextSubscription;
        },
        /** Queues the next response payload for the async iterator. */
        onNext(value) {
          queue.push(value);
        },
        /** Propagates stream errors to the async iterator. */
        onError(error) {
          cleanupAbortListener();
          queue.error(error);
        },
        /** Completes the async iterator when the stream completes. */
        onComplete() {
          cleanupAbortListener();
          queue.complete();
        }
      };
      let setupFailed = false;
      try {
        const streamSubscription = subscriptionFactory(subscriber);
        subscriber.onSubscribe(streamSubscription);
        if (hasAfterSubscribe(streamSubscription)) streamSubscription.afterSubscribe();
      } catch (error) {
        setupFailed = true;
        queue.error(error);
      }

      if (!setupFailed) {
        if (signal.aborted) abort();
        else {
          signal.addEventListener("abort", abort, { once: true });
          abortRegistered = true;
        }
      }

      return {
        next: () => {
          subscription?.request(1);
          return queue.next();
        },
        return: () => {
          cleanupAbortListener();
          subscription?.cancel();
          return queue.return();
        },
        throw: (error: unknown) => {
          cleanupAbortListener();
          subscription?.cancel();
          queue.error(error);
          return Promise.reject(error);
        }
      };
    }
  };
}

/**
 * Subscription that maps Reactive Streams demand and cancellation to RSocket.
 */
export class RSocketStreamSubscription implements Subscription, StreamController {
  private assignedStreamId: number | undefined;
  private started = false;
  private cancelled = false;
  private responseTerminated = false;
  private outboundTerminated = true;
  private requested = 0;
  private outbound?: OutboundChannel;

  /**
   * Creates a stream subscription for a specific client stream id.
   */
  constructor(
    private readonly session: StreamSession,
    private readonly subscriber: Subscriber<RSocketPayloadFrame>,
    private readonly allocateStreamId: () => number,
    private readonly startStream: StartStream
  ) {}

  /**
   * Client stream ID, or zero before initial demand allocates one.
   */
  get streamId(): number {
    return this.assignedStreamId ?? 0;
  }

  /**
   * Attaches the outbound half of a request-channel interaction.
   */
  attachOutbound(outbound: OutboundChannel): void {
    this.outbound = outbound;
    this.outboundTerminated = false;
  }

  /**
   * Marks request-channel outbound publishing complete.
   */
  markOutboundComplete(): void {
    this.outboundTerminated = true;
    this.disposeIfDone();
  }

  /**
   * Requests more response payloads from the responder.
   */
  request(n: number): void {
    if (this.cancelled || this.responseTerminated) return;

    try {
      if (n === Number.POSITIVE_INFINITY) {
        this.requestChunk(MAX_REQUEST_N);
        return;
      }
      if (!Number.isFinite(n) || !Number.isInteger(n) || n <= 0 || n > Number.MAX_SAFE_INTEGER) {
        throw new RangeError("Reactive Streams request(n) expects a strictly positive integer");
      }
      if (n <= MAX_REQUEST_N) {
        this.requestChunk(n);
        return;
      }

      let remaining = n;
      while (remaining > 0) {
        const chunk = Math.min(remaining, MAX_REQUEST_N);
        this.requestChunk(chunk);
        remaining -= chunk;
      }
    } catch (error) {
      this.fail(error);
    }
  }

  /**
   * Cancels the stream and sends CANCEL if the stream was already started.
   */
  cancel(): void {
    if (this.cancelled || (this.responseTerminated && this.outboundTerminated)) return;
    this.cancelled = true;
    this.outbound?.abort();
    if (this.started) {
      try {
        this.session.sendFrame(new CancelFrame(this.streamId));
      } catch {
        // The stream is already locally cancelled; a closed socket cannot observe CANCEL.
      }
    }
    this.dispose();
  }

  /**
   * Delivers a PAYLOAD frame to the subscriber while enforcing demand.
   */
  handlePayload(frame: PayloadFrame): void {
    if (this.cancelled || this.responseTerminated) return;
    const hasFollows = frame.hasFollows();
    const isNext = frame.isNext();
    const isComplete = frame.isComplete();
    if (hasFollows) {
      this.fail(new RSocketProtocolError("Unexpected fragmented PAYLOAD reached stream handler", { streamId: this.streamId }));
      return;
    }

    if (isNext) {
      if (this.requested <= 0) {
        this.session.protocolError(
          new RSocketProtocolError("Responder sent PAYLOAD without requester demand", {
            code: FrameErrorCode.CONNECTION_ERROR,
            streamId: this.streamId
          })
        );
        return;
      }
      if (this.requested !== Number.POSITIVE_INFINITY) this.requested -= 1;

      try {
        this.subscriber.onNext(decodeFramePayload(frame));
      } catch (error) {
        this.cancel();
        this.signalError(error);
        return;
      }
      if (this.cancelled || this.responseTerminated) return;
    }

    if (isComplete) {
      this.completeResponse();
      return;
    }

    // Frames without NEXT or COMPLETE do not alter the stream sequence.
  }

  /**
   * Converts a stream ERROR frame to a subscriber error.
   */
  handleError(frame: ErrorFrame): void {
    this.fail(errorFromFrame(frame));
  }

  /**
   * Routes responder demand to the request-channel outbound publisher.
   */
  handleRequestN(frame: RequestNFrame): void {
    if (this.outbound === undefined) {
      return;
    }

    if (!Number.isInteger(frame.request) || frame.request <= 0 || frame.request > MAX_REQUEST_N) {
      this.session.protocolError(
        new RSocketProtocolError("Responder sent invalid REQUEST_N", {
          code: FrameErrorCode.CONNECTION_ERROR,
          streamId: this.streamId
        })
      );
      return;
    }

    this.outbound.addDemand(frame.request);
  }

  /**
   * Ignores responder CANCEL on a requester-owned stream.
   */
  handleCancel(): void {
    // This side is the requester; an unexpected responder CANCEL is ignored.
  }

  /**
   * Fails the stream locally and notifies the subscriber once.
   */
  fail(error: unknown): void {
    if (this.cancelled) return;
    const shouldSignal = !this.responseTerminated;
    this.cancelled = true;
    this.outbound?.abort(error);
    this.dispose();
    if (shouldSignal) this.signalError(error);
  }

  /**
   * Fails the outbound half of request-channel and sends APPLICATION_ERROR.
   */
  failOutbound(error: unknown): void {
    if (this.cancelled) return;
    this.cancelled = true;
    this.outboundTerminated = true;
    if (this.started) {
      try {
        this.session.sendFrame(new ErrorFrame(this.streamId, FrameErrorCode.APPLICATION_ERROR, errorPayload(error)));
      } catch {
        // The subscriber still needs the local failure even if the socket is closed.
      }
    }
    this.dispose();
    if (!this.responseTerminated) this.signalError(error);
  }

  /**
   * Sends REQUEST_N after the initial stream frame has already been sent.
   */
  private sendRequestN(n: number): void {
    this.addRequested(n);
    this.session.sendFrame(new RequestNFrame(this.streamId, n));
  }

  /**
   * Starts the stream on the first request or sends additional REQUEST_N chunks.
   */
  private requestChunk(chunk: number): void {
    if (!this.started) {
      this.assignedStreamId = this.allocateStreamId();
      this.started = true;
      this.addRequested(chunk);
      this.startStream(this.streamId, chunk, this);
      return;
    }

    this.sendRequestN(chunk);
  }

  /**
   * Tracks local demand and clamps it to a safe JavaScript integer.
   */
  private addRequested(n: number): void {
    if (this.requested === Number.POSITIVE_INFINITY) return;
    this.requested = n >= Number.MAX_SAFE_INTEGER - this.requested ? Number.MAX_SAFE_INTEGER : this.requested + n;
  }

  /**
   * Completes the response half and disposes when outbound is also finished.
   */
  private completeResponse(): void {
    if (this.responseTerminated) return;
    this.responseTerminated = true;
    try {
      this.subscriber.onComplete();
    } catch {
      // Subscriber terminal callbacks must not turn a completed stream into a protocol failure.
    } finally {
      this.disposeIfDone();
    }
  }

  /**
   * Delivers an error to the subscriber without allowing user code to break session dispatch.
   */
  private signalError(error: unknown): void {
    try {
      this.subscriber.onError(error);
    } catch {
      // Subscriber terminal callbacks are user code; the stream is already terminal.
    }
  }

  /**
   * Disposes only when both response and outbound halves are done.
   */
  private disposeIfDone(): void {
    if (this.responseTerminated && this.outboundTerminated) this.dispose();
  }

  /**
   * Removes this stream from the owning session.
   */
  private dispose(): void {
    if (this.assignedStreamId !== undefined) this.session.unregisterStream(this.assignedStreamId);
  }
}
