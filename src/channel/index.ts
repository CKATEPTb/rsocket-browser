/**
 * Request-channel outbound publisher implementation.
 *
 * RSocket request-channel is duplex: the requester sends payloads while also
 * receiving payloads. This class owns the requester-to-responder direction and
 * waits for responder `REQUEST_N` before sending any payload after the initial
 * `REQUEST_CHANNEL` frame.
 */
import {type MimeType, PayloadFlag, PayloadFrame, RequestChannelFlag, RequestChannelFrame} from "rsocket-frames-ts";
import {canPrefetchChannelInput, channelInputIterator, type RSocketChannelInputIterator} from "@/channel/input.js";
import {encodePayloadInput} from "@/payload/index.js";
import {type OutboundChannel, type StreamSession} from "@/stream/index.js";
import {addReactiveDemand} from "@/stream/demand.js";
import type {RSocketChannelInput, RSocketPayloadInput} from "@/types/index.js";

/**
 * Value that may already be available or may resolve from an async iterator.
 */
type MaybePromise<T> = T | PromiseLike<T>;

/**
 * Result produced by any supported request-channel iterator.
 */
type ChannelIteratorResult = IteratorResult<RSocketPayloadInput<any, any>>;

/**
 * Reads one iterator item without forcing synchronous iterables through `await`.
 */
function readChannelInput(
    iterator: RSocketChannelInputIterator<any, any>
): MaybePromise<ChannelIteratorResult> {
    return iterator.next() as MaybePromise<ChannelIteratorResult>;
}

/**
 * Detects pending iterator reads while keeping plain iterator results on the fast path.
 */
function isPromiseLike<T>(value: MaybePromise<T>): value is PromiseLike<T> {
    return typeof value === "object" &&
        value !== null &&
        typeof (value as { then?: unknown }).then === "function";
}

/**
 * Sends request-channel outbound payloads while respecting responder demand.
 */
export class RequestChannelOutbound implements OutboundChannel {
    private demand = 0;
    private aborted = false;
    private iterator: RSocketChannelInputIterator<any, any> | undefined;
    private iteratorClosed = false;
    /** Whether the peer has observed this stream's initial REQUEST_CHANNEL. */
    private requestStarted = false;
    private demandWaiter: (() => void) | undefined;

    /**
     * Creates the outbound channel coordinator.
     */
    constructor(
        private readonly session: StreamSession,
        private readonly streamId: number,
        private readonly initialRequestN: number,
        private readonly input: RSocketChannelInput,
        private readonly dataMimeType: MimeType<any>,
        private readonly metadataMimeType: MimeType<any>,
        private readonly onComplete: () => void,
        private readonly onError: (error: unknown, requestStarted: boolean) => void
    ) {
    }

    /**
     * Starts draining the configured publisher.
     */
    start(): void {
        void this.drain();
    }

    /**
     * Adds responder demand received through REQUEST_N.
     */
    addDemand(n: number): void {
        if (this.aborted || n <= 0) return;
        this.demand = addReactiveDemand(this.demand, n);
        this.wakeDemand();
    }

    /**
     * Cancels the outbound publisher and closes its iterator if possible.
     */
    abort(_error?: unknown): void {
        if (this.aborted) return;
        this.aborted = true;
        this.wakeDemand();
        void this.closeIterator();
    }

    /**
     * Sends the initial frame, then waits for demand before sending more payloads.
     */
    private async drain(): Promise<void> {
        let iterator: RSocketChannelInputIterator<any, any> | undefined;

        try {
            iterator = channelInputIterator(this.input);
            const canPrefetch = canPrefetchChannelInput(this.input);
            this.iterator = iterator;
            const firstRead = readChannelInput(iterator);
            const first = isPromiseLike(firstRead) ? await firstRead : firstRead;
            if (this.aborted) return;

            if (first.done) {
                this.iteratorClosed = true;
                this.session.sendRequestFrame(
                    new RequestChannelFrame(
                        this.streamId,
                        RequestChannelFlag.COMPLETE,
                        this.initialRequestN
                    )
                );
                this.requestStarted = true;
                this.onComplete();
                return;
            }

            const initial = encodePayloadInput(first.value, this.dataMimeType, this.metadataMimeType);
            this.session.sendRequestFrame(
                new RequestChannelFrame(
                    this.streamId,
                    RequestChannelFlag.NONE,
                    this.initialRequestN,
                    initial.metadata,
                    initial.payload
                )
            );
            this.requestStarted = true;

            let pending: MaybePromise<ChannelIteratorResult> | undefined;
            while (!this.aborted) {
                if (pending === undefined) {
                    if (this.demand <= 0) await this.awaitDemand();
                    if (this.aborted) return;
                    pending = readChannelInput(iterator);
                }

                const next = isPromiseLike(pending) ? await pending : pending;
                pending = undefined;
                if (this.aborted) return;

                if (next.done) {
                    this.iteratorClosed = true;
                    this.session.sendFrame(new PayloadFrame(this.streamId, PayloadFlag.COMPLETE));
                    this.onComplete();
                    return;
                }

                if (this.demand <= 0) await this.awaitDemand();
                if (this.aborted) return;

                this.demand -= 1;
                const encoded = encodePayloadInput(next.value, this.dataMimeType, this.metadataMimeType);
                this.session.sendFrame(
                    new PayloadFrame(
                        this.streamId,
                        PayloadFlag.NEXT,
                        encoded.metadata,
                        encoded.payload
                    )
                );

                if (canPrefetch && this.demand <= 0) pending = readChannelInput(iterator);
            }
        } catch (error) {
            if (this.aborted) return;
            this.aborted = true;
            this.onError(error, this.requestStarted);
        } finally {
            if (iterator !== undefined) {
                if (this.iterator === iterator) this.iterator = undefined;
                await this.closeIterator(iterator);
            }
        }
    }

    /**
     * Waits until responder demand is available or the channel is aborted.
     */
    private async awaitDemand(): Promise<void> {
        while (!this.aborted && this.demand <= 0) {
            await new Promise<void>((resolve) => {
                this.demandWaiter = resolve;
            });
        }
    }

    /**
     * Wakes a pending demand waiter.
     */
    private wakeDemand(): void {
        const waiter = this.demandWaiter;
        this.demandWaiter = undefined;
        waiter?.();
    }

    /**
     * Calls `return()` on the async iterator to release publisher resources.
     */
    private async closeIterator(iterator = this.iterator): Promise<void> {
        if (iterator === undefined || this.iteratorClosed) return;
        this.iteratorClosed = true;
        try {
            const returned = iterator.return?.();
            if (returned !== undefined && isPromiseLike(returned)) await returned;
        } catch {
            // The stream is already being cancelled; iterator cleanup errors cannot be reported reliably.
        }
    }
}
