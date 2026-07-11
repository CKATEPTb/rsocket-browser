/**
 * Reactive Streams implementation for RSocket request-stream and request-channel
 * responses.
 */
import {Flux, type Subscriber, type Subscription} from "reactor-core-ts";
import {CancelFrame, ErrorFrame, type Frame, FrameErrorCode, PayloadFrame, RequestNFrame} from "rsocket-frames-ts";
import {MAX_REQUEST_N} from "@/protocol/index.js";
import {AsyncQueue} from "@/async/index.js";
import {decodeFramePayload, errorPayload} from "@/payload/index.js";
import type {RSocketPayloadFrame} from "@/types/index.js";
import {errorFromFrame, RSocketProtocolError} from "@/errors/index.js";
import {addReactiveDemand, normalizeReactiveDemand} from "@/stream/demand.js";

/** No-op subscription used when stream setup fails before a real subscription exists. */
const EMPTY_SUBSCRIPTION: Subscription = Object.freeze({
    /** Ignores demand because the stream is already terminal. */
    request() {
    },
    /** Ignores cancellation because there is no upstream to cancel. */
    cancel() {
    }
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

    /** Sends an initial REQUEST frame while consuming one requester lease credit. */
    sendRequestFrame(frame: Frame): void;

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
                    signal.addEventListener("abort", abort, {once: true});
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
    /** Suppresses demand re-entered from the NEXT callback of a final payload. */
    private completing = false;
    private outboundTerminated = true;
    private requested = 0;
    /** Response credits currently granted to the peer but not yet consumed. */
    private wireRequested = 0;
    private outbound: OutboundChannel | undefined;
    private disposed = false;
    private session: StreamSession | undefined;
    private allocateStreamId: (() => number) | undefined;
    private startStream: StartStream | undefined;
    /** Downstream callbacks released as soon as this subscription terminates. */
    private subscriber: Subscriber<RSocketPayloadFrame> | undefined;

    /**
     * Creates a stream subscription for a specific client stream id.
     */
    constructor(
        session: StreamSession,
        subscriber: Subscriber<RSocketPayloadFrame>,
        allocateStreamId: () => number,
        startStream: StartStream
    ) {
        this.session = session;
        this.subscriber = subscriber;
        this.allocateStreamId = allocateStreamId;
        this.startStream = startStream;
    }

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
        this.outbound = undefined;
        this.outboundTerminated = true;
        this.disposeIfDone();
    }

    /**
     * Requests more response payloads from the responder.
     */
    request(n: number): void {
        if (this.cancelled || this.responseTerminated || this.completing) return;

        try {
            this.requested = addReactiveDemand(this.requested, normalizeReactiveDemand(n));
            this.requestDemand();
        } catch (error) {
            this.failInvalidDemand(error);
        }
    }

    /**
     * Cancels the stream and sends CANCEL if the stream was already started.
     */
    cancel(): void {
        if (this.cancelled || (this.responseTerminated && this.outboundTerminated)) return;
        this.cancelled = true;
        this.subscriber = undefined;
        const outbound = this.outbound;
        this.outbound = undefined;
        outbound?.abort();
        if (this.started) {
            try {
                this.session?.sendFrame(new CancelFrame(this.streamId));
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
            this.fail(new RSocketProtocolError("Unexpected fragmented PAYLOAD reached stream handler", {streamId: this.streamId}));
            return;
        }

        this.completing = isComplete;
        if (isNext) {
            if (this.requested <= 0 || this.wireRequested <= 0) {
                this.session?.protocolError(
                    new RSocketProtocolError("Responder sent PAYLOAD without requester demand", {
                        code: FrameErrorCode.CONNECTION_ERROR,
                        streamId: this.streamId
                    })
                );
                return;
            }
            if (this.requested !== Number.POSITIVE_INFINITY) this.requested -= 1;
            this.wireRequested -= 1;

            const subscriber = this.subscriber;
            if (subscriber === undefined) return;
            try {
                subscriber.onNext(decodeFramePayload(frame));
            } catch (error) {
                this.cancel();
                signalSubscriberError(subscriber, error);
                return;
            }
            if (this.cancelled || this.responseTerminated) return;
        }

        if (isComplete) {
            this.completing = false;
            this.completeResponse();
            return;
        }

        if (isNext) {
            try {
                this.replenishDemand();
            } catch (error) {
                this.fail(error);
            }
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
            this.session?.protocolError(
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
        const subscriber = shouldSignal ? this.subscriber : undefined;
        this.subscriber = undefined;
        this.cancelled = true;
        const outbound = this.outbound;
        this.outbound = undefined;
        outbound?.abort(error);
        this.dispose();
        if (subscriber !== undefined) signalSubscriberError(subscriber, error);
    }

    /**
     * Fails request-channel locally and notifies the peer when its initial
     * REQUEST_CHANNEL was already sent.
     */
    failOutbound(error: unknown, notifyPeer = true): void {
        if (this.cancelled) return;
        const subscriber = this.responseTerminated ? undefined : this.subscriber;
        this.subscriber = undefined;
        this.cancelled = true;
        this.outbound = undefined;
        this.outboundTerminated = true;
        if (notifyPeer && this.started) {
            try {
                this.session?.sendFrame(new ErrorFrame(this.streamId, FrameErrorCode.APPLICATION_ERROR, errorPayload(error)));
            } catch {
                // The subscriber still needs the local failure even if the socket is closed.
            }
        }
        this.dispose();
        if (subscriber !== undefined) signalSubscriberError(subscriber, error);
    }

    /** Starts the stream or grants currently available downstream demand. */
    private requestDemand(): void {
        if (!this.started) {
            const allocateStreamId = this.allocateStreamId;
            const startStream = this.startStream;
            if (allocateStreamId === undefined || startStream === undefined) return;
            this.assignedStreamId = allocateStreamId();
            this.started = true;
            this.allocateStreamId = undefined;
            this.startStream = undefined;
            const initialRequestN = Math.min(this.requested, MAX_REQUEST_N);
            this.wireRequested = initialRequestN;
            startStream(this.streamId, initialRequestN, this);
            return;
        }

        this.grantAvailableDemand();
    }

    /** Grants at most one full protocol window without emitting millions of frames. */
    private grantAvailableDemand(): void {
        const session = this.session;
        if (session === undefined) return;
        const grant = Math.min(this.requested, MAX_REQUEST_N) - this.wireRequested;
        if (grant <= 0) return;
        this.wireRequested += grant;
        try {
            session.sendFrame(new RequestNFrame(this.streamId, grant));
        } catch (error) {
            this.wireRequested -= grant;
            throw error;
        }
    }

    /** Refills a large demand window only after half of its credits were consumed. */
    private replenishDemand(): void {
        if (this.wireRequested <= (MAX_REQUEST_N >>> 1)) this.grantAvailableDemand();
    }

    /** Terminates invalid demand and cancels a stream already known to the peer. */
    private failInvalidDemand(error: unknown): void {
        if (this.started) {
            try {
                this.session?.sendFrame(new CancelFrame(this.streamId));
            } catch {
                // Local termination still has to notify the subscriber when the transport is unavailable.
            }
        }
        this.fail(error);
    }

    /**
     * Completes the response half and disposes when outbound is also finished.
     */
    private completeResponse(): void {
        if (this.responseTerminated) return;
        this.responseTerminated = true;
        const subscriber = this.subscriber;
        this.subscriber = undefined;
        try {
            subscriber?.onComplete();
        } catch {
            // Subscriber terminal callbacks must not turn a completed stream into a protocol failure.
        } finally {
            this.disposeIfDone();
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
        if (this.disposed) return;
        this.disposed = true;
        const session = this.session;
        this.session = undefined;
        this.allocateStreamId = undefined;
        this.startStream = undefined;
        this.outbound = undefined;
        this.subscriber = undefined;
        if (this.assignedStreamId !== undefined) session?.unregisterStream(this.assignedStreamId);
    }
}

/** Delivers a terminal error without allowing user code to escape dispatch. */
function signalSubscriberError(subscriber: Subscriber<RSocketPayloadFrame>, error: unknown): void {
    try {
        subscriber.onError(error);
    } catch {
        // Subscriber terminal callbacks are user code; the stream is already terminal.
    }
}
