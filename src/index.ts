import {Flux, Mono, Publisher, Schedulers, SinkPublisher, Sinks, Subscription} from "reactor-core-ts";
import {
    CancelFrame,
    ErrorFrame,
    FireAndForgetFlag,
    Frame,
    FrameDeserializer,
    FrameErrorCode,
    FrameType,
    KeepaliveFlag,
    KeepaliveFrame,
    Metadata,
    MetadataPushFrame,
    MimeType,
    Payload as FramePayload,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFlag,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFlag,
    RequestResponseFrame,
    RequestStreamFlag,
    RequestStreamFrame,
    SetupFlag,
    SetupFrame
} from "rsocket-frames-ts";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/**
 * A generic RSocket payload consisting of optional data and metadata.
 *
 * @typeParam P - The application data type.
 * @typeParam M - The metadata type.
 */
export interface Payload<P, M> {
    /** Application data. */
    readonly data?: P;
    /** Associated metadata. */
    readonly metadata?: M;
}

/**
 * Client‑side RSocket interface that exposes the four interaction models
 * defined by the RSocket protocol (plus {@link metadataPush}), as well as
 * connection lifecycle methods.
 *
 * @typeParam P - The application data type.
 * @typeParam M - The metadata type.
 *
 * @see {@link https://rsocket.io/about/protocol | RSocket Protocol}
 */
export interface RSocket<P, M> {
    /**
     * Establish the underlying transport connection and perform the
     * RSocket SETUP handshake.
     *
     * @returns A {@link Mono} that completes once the connection is ready.
     * @throws Error if a connection is already active.
     */
    connect(): Mono<void>;

    /**
     * Gracefully (or forcefully) tear down the connection.
     *
     * @param force - When `true` the outbound sink is completed immediately
     *                without draining queued frames. Defaults to `false`.
     * @returns A {@link Mono} that completes once the connection is closed.
     * @throws Error if there is no active connection.
     */
    disconnect(force?: boolean): Mono<void>;

    /**
     * Fire‑And‑Forget interaction model — sends a single payload with no
     * response expected.
     *
     * Per the RSocket specification the requester MUST NOT send
     * {@link RequestNFrame} or expect any response frame for this stream.
     *
     * @param payload - The payload to send.
     * @returns A {@link Mono} that completes once the frame is enqueued.
     */
    fireAndForget(payload: Payload<P, M>): Mono<void>;

    /**
     * Request‑Response interaction model — sends a single payload and
     * receives exactly one response payload.
     *
     * A {@link CancelFrame} is sent if the subscriber unsubscribes before
     * the response arrives, as required by the RSocket specification.
     *
     * @param payload - The request payload.
     * @returns A {@link Mono} emitting the single response payload.
     */
    requestResponse(payload: Payload<P, M>): Mono<Payload<P, M>>;

    /**
     * Request‑Stream interaction model — sends a single payload and
     * receives a stream of response payloads controlled by back‑pressure
     * via {@link RequestNFrame}.
     *
     * @param payload        - The request payload.
     * @param initialRequestN - Number of items to request initially
     *                          (sent in the initial frame per spec §5.4).
     * @returns A {@link Flux} emitting response payloads.
     */
    requestStream(payload: Payload<P, M>, initialRequestN: number): Flux<Payload<P, M>>;

    /**
     * Request‑Channel interaction model — a bidirectional stream. The
     * requester sends a stream of payloads while simultaneously receiving
     * a stream of response payloads.
     *
     * The first upstream payload is bundled into the initial
     * {@link RequestChannelFrame} as mandated by the spec (§5.5).
     *
     * @param payloads        - A {@link Publisher} of outgoing payloads.
     * @param initialRequestN - Number of items to request initially from
     *                          the responder.
     * @returns A {@link Flux} emitting response payloads.
     */
    requestChannel(payloads: Publisher<Payload<P, M>>, initialRequestN: number): Flux<Payload<P, M>>;

    /**
     * Metadata Push — sends metadata without a response on stream ID 0.
     *
     * @param metadata - The metadata to push.
     * @returns A {@link Mono} that completes once the frame is enqueued.
     */
    metadataPush(metadata: Metadata<any>): Mono<void>;

    /**
     * Whether the underlying transport is connected and the SETUP
     * handshake has been performed.
     */
    isConnected(): boolean;
}

// ---------------------------------------------------------------------------
// Factory & implementation
// ---------------------------------------------------------------------------

export namespace RSocket {
    /**
     * Configuration options for {@link RSocket.create}.
     *
     * @typeParam P - The application data type.
     * @typeParam M - The metadata type.
     */
    export interface CreateOptions<P, M> {
        /** WebSocket URL to connect to (e.g. `ws://localhost:7000`). */
        readonly url: string;
        /** SETUP frame parameters. */
        readonly setup: {
            /**
             * Time (in milliseconds) between KEEPALIVE frames sent by the
             * client. Must be > 0 per the spec.
             */
            readonly keepAlive: number;
            /**
             * Time (in milliseconds) the client will allow between
             * KEEPALIVE responses before considering the connection dead.
             * Must be > 0 per the spec.
             */
            readonly lifetime: number;
            /** MIME types used for encoding / decoding. */
            readonly mimetype: {
                readonly metadata: MimeType<M>;
                readonly data: MimeType<P>;
            };
            /** Optional payload to include in the SETUP frame. */
            readonly payload?: Payload<P, M>;
        },
        /**
         * Optional diagnostic logging flags.
         * When enabled, frames are logged via the Reactor operator `.log()`.
         */
        readonly logs?: {
            /** Log every inbound frame to the console. */
            readonly inbound?: boolean;
            /** Log every outbound frame to the console. */
            readonly outbound?: boolean;
        };
    }

    /**
     * Create a new client‑side {@link RSocket} instance backed by a
     * WebSocket transport.
     *
     * The connection is *not* established until {@link RSocket.connect} is
     * called.
     *
     * @typeParam P - The application data type.
     * @typeParam M - The metadata type.
     * @param options - Connection and SETUP configuration.
     * @returns A new (disconnected) {@link RSocket} instance.
     */
    export function create<P, M>(options: CreateOptions<P, M>): RSocket<P, M> {
        return new RSocketClient<P, M>(options);
    }
}

// ---------------------------------------------------------------------------
// Internal implementation
// ---------------------------------------------------------------------------

/**
 * Concrete RSocket client implementation over a WebSocket transport.
 *
 * Stream IDs are allocated as odd numbers starting at 1, as required by
 * the spec for client‑initiated streams (§5.1.1).
 *
 * @internal
 */
class RSocketClient<P, M> implements RSocket<P, M> {
    private readonly url: string;
    private readonly keepAliveInterval: number;
    private readonly lifetimeTimeout: number;
    private readonly metadataMimeType: MimeType<M>;
    private readonly dataMimeType: MimeType<P>;
    private readonly setupFrame: SetupFrame;
    private readonly inboundLogs: boolean
    private readonly outboundLogs: boolean

    /**
     * Last assigned stream ID. Client‑side streams MUST be odd (spec §5.1.1).
     * We start at −1 so the first increment yields 1.
     */
    private lastStreamId = -1;

    /** Shared inbound frame stream — defined only while connected. */
    private inbound: Flux<Frame> | undefined;

    /** Buffered queue for outgoing frames submitted by interaction methods. */
    private frameQueue: SinkPublisher<Frame> = Sinks.many().unicast().onBackpressureBuffer();
    private frameQueueFlux: Flux<Frame> = this.frameQueue.asFlux();

    /**
     * The outbound sink that feeds the WebSocket. Frames land here either
     * from the queue or directly (keepalive responses, setup).
     */
    private outboundSink: SinkPublisher<Frame>;
    private outboundFlux: Flux<Frame>;

    /**
     * @param options - Connection and SETUP frame configuration.
     *                  The connection is not established here; call
     *                  {@link connect} to initiate the handshake.
     */
    constructor(options: RSocket.CreateOptions<P, M>) {
        this.url = options.url;
        this.keepAliveInterval = options.setup.keepAlive;
        this.lifetimeTimeout = options.setup.lifetime;
        this.metadataMimeType = options.setup.mimetype.metadata;
        this.dataMimeType = options.setup.mimetype.data;
        this.inboundLogs = options.logs?.inbound || false;
        this.outboundLogs = options.logs?.outbound || false;

        this.outboundSink = Sinks.many().unicast().onBackpressureBuffer();
        if (this.outboundLogs) this.outboundFlux = this.outboundSink.asFlux().log("RSOCKET-OUTBOUND").share();
        else this.outboundFlux = this.outboundSink.asFlux().share();

        this.setupFrame = new SetupFrame(
            this.keepAliveInterval,
            this.lifetimeTimeout,
            this.metadataMimeType,
            this.dataMimeType,
            undefined,
            undefined,
            undefined,
            SetupFlag.NONE,
            this.encodeMetadata(options.setup.payload?.metadata, options.setup.payload?.data),
            this.encodeData(options.setup.payload?.data)
        );
    }

    // -----------------------------------------------------------------------
    // Connection lifecycle
    // -----------------------------------------------------------------------

    /** @inheritDoc */
    public connect(): Mono<void> {
        return Mono.defer(() => {
            if (this.isConnected()) {
                return Mono.error(new Error("Already connected"));
            }

            this.inbound = Flux.using<Frame, WebSocket>(
                () => this.openWebSocket(),
                (ws) => this.createInboundStream(ws),
                (ws) => this.cleanupConnection(ws),
            )
                .onErrorContinue((error) => {
                    console.error("[rsocket-client] inbound error:", error);
                    return true;
                })
            if (this.inboundLogs) this.inbound = this.inbound.log("RSOCKET-INBOUND")
            this.inbound = this.inbound.share();

            this.scheduleKeepalive();
            return Mono.empty();
        });
    }

    /** @inheritDoc */
    public disconnect(force = false): Mono<void> {
        return Mono.defer(() => {
            if (!this.isConnected()) {
                return Mono.error(new Error("Not connected"));
            }

            if (force) {
                this.outboundSink.complete();
                return Mono.empty();
            }

            // Graceful: complete the queue, wait until the last frame is
            // flushed through the outbound before tearing down.
            return Mono.fromCallable(() => this.frameQueue.complete())
                .then(this.outboundFlux.last())
                .then();
        });
    }

    /** @inheritDoc */
    public isConnected(): boolean {
        return this.inbound !== undefined;
    }

    // -----------------------------------------------------------------------
    // Interaction models
    // -----------------------------------------------------------------------

    /** @inheritDoc */
    public fireAndForget(payload: Payload<P, M>): Mono<void> {
        return this.enqueueFrame(
            new RequestFireAndForgetFrame(
                this.nextStreamId(),
                FireAndForgetFlag.NONE,
                this.encodeMetadata(payload.metadata, payload.data),
                this.encodeData(payload.data)
            ),
        );
    }

    /** @inheritDoc */
    public metadataPush(metadata: Metadata<any>): Mono<void> {
        return this.enqueueFrame(new MetadataPushFrame(metadata));
    }

    /** @inheritDoc */
    public requestResponse(payload: Payload<P, M>): Mono<Payload<P, M>> {
        const streamId = this.nextStreamId();

        const requestFrame = new RequestResponseFrame(
            streamId,
            RequestResponseFlag.NONE,
            this.encodeMetadata(payload.metadata, payload.data),
            this.encodeData(payload.data)
        );

        return this.enqueueFrame(requestFrame)
            .then(
                Mono.defer(() =>
                    this.inboundForStream(streamId)
                        .first()
                        .flatMap((frame) => this.handleSingleResponse<P, M>(frame, streamId)),
                ),
            )
            .doOnSubscribe((subscription) => {
                const originalUnsubscribe = subscription.unsubscribe.bind(subscription);
                subscription.unsubscribe = () => {
                    originalUnsubscribe();
                    this.enqueueFrame(new CancelFrame(streamId)).subscribe();
                };
            });
    }

    /** @inheritDoc */
    public requestStream(payload: Payload<P, M>, initialRequestN: number): Flux<Payload<P, M>> {
        return Flux.defer(() => {
            const streamId = this.nextStreamId();

            const requestFrame = new RequestStreamFrame(
                streamId,
                RequestStreamFlag.NONE,
                initialRequestN,
                this.encodeMetadata(payload.metadata, payload.data),
                this.encodeData(payload.data)
            );

            return this.enqueueFrame(requestFrame).thenMany(
                Flux.create<Payload<P, M>>((sink) => {
                    let credited = -initialRequestN;

                    const subscription = this.inboundForStream(streamId).subscribe(
                        (frame) => this.dispatchStreamFrame(frame, streamId, sink, "REQUEST_STREAM"),
                        (err) => sink.error(err),
                        () => sink.complete(),
                    );

                    sink.onRequest((n) => {
                        credited += n;
                        if (credited > 0) {
                            this.enqueueFrame(new RequestNFrame(streamId, credited)).subscribe();
                            credited = 0;
                        }
                    });

                    sink.onCancel(() => {
                        subscription.unsubscribe();
                        this.enqueueFrame(new CancelFrame(streamId)).subscribe();
                    });

                    sink.onDispose(() => subscription.unsubscribe());
                }),
            );
        });
    }

    /** @inheritDoc */
    public requestChannel(
        payloads: Publisher<Payload<P, M>>,
        initialRequestN: number,
    ): Flux<Payload<P, M>> {
        return Flux.defer(() => {
            const streamId = this.nextStreamId();

            return Flux.create<Payload<P, M>>((sink) => {
                let isFirstPayload = true;
                let upstreamSubscription: Subscription | undefined;

                // --- Inbound (responder → requester) ---
                const inboundSubscription = this.inboundForStream(streamId).subscribe(
                    (frame) => {
                        switch (frame.type) {
                            case FrameType.PAYLOAD:
                                this.emitPayloadFrame(frame as PayloadFrame, sink);
                                break;
                            case FrameType.REQUEST_N:
                                upstreamSubscription?.request((frame as RequestNFrame).request);
                                break;
                            case FrameType.CANCEL:
                                upstreamSubscription?.unsubscribe();
                                break;
                            case FrameType.ERROR: {
                                const ef = frame as ErrorFrame;
                                sink.error(
                                    new Error(`[REQUEST_CHANNEL:${streamId}] Error ${ef.code}: ${ef.payload}`),
                                );
                                break;
                            }
                            default:
                                sink.error(
                                    new Error(`[REQUEST_CHANNEL:${streamId}] Unexpected frame type: ${frame.type}`),
                                );
                        }
                    },
                    (err) => sink.error(err),
                    () => sink.complete(),
                );

                // --- Outbound (requester → responder) ---
                payloads.subscribe({
                    onSubscribe: (sub) => {
                        upstreamSubscription = sub;
                        // Request the first item so we can embed it in the
                        // initial RequestChannelFrame (spec §5.5).
                        sub.request(1);
                    },
                    onNext: (p) => {
                        if (isFirstPayload) {
                            isFirstPayload = false;
                            this.enqueueFrame(
                                new RequestChannelFrame(
                                    streamId,
                                    RequestChannelFlag.NONE,
                                    initialRequestN,
                                    this.encodeMetadata(p.metadata, p.data),
                                    this.encodeData(p.data),
                                ),
                            ).subscribe();
                        } else {
                            this.enqueueFrame(
                                new PayloadFrame(
                                    streamId,
                                    PayloadFlag.NEXT,
                                    this.encodeMetadata(p.metadata, p.data),
                                    this.encodeData(p.data),
                                ),
                            ).subscribe();
                        }
                    },
                    onError: (err) => {
                        this.enqueueFrame(
                            new ErrorFrame(streamId, FrameErrorCode.APPLICATION_ERROR),
                        ).subscribe();
                        sink.error(err);
                    },
                    onComplete: () => {
                        this.enqueueFrame(
                            new PayloadFrame(streamId, PayloadFlag.COMPLETE),
                        ).subscribe();
                    },
                });

                // --- Back‑pressure (requester → responder) ---
                let requestedFromResponder = -initialRequestN;
                sink.onRequest((n) => {
                    requestedFromResponder += n;
                    if (requestedFromResponder > 0) {
                        this.enqueueFrame(new RequestNFrame(streamId, requestedFromResponder)).subscribe();
                        requestedFromResponder = 0;
                    }
                });

                sink.onCancel(() => {
                    inboundSubscription.unsubscribe();
                    upstreamSubscription?.unsubscribe();
                    this.enqueueFrame(new CancelFrame(streamId)).subscribe();
                });

                sink.onDispose(() => {
                    inboundSubscription.unsubscribe();
                    upstreamSubscription?.unsubscribe();
                });
            });
        });
    }

    // -----------------------------------------------------------------------
    // Transport helpers
    // -----------------------------------------------------------------------

    /**
     * Open a new WebSocket with `arraybuffer` binary type.
     */
    private openWebSocket(): WebSocket {
        const ws = new WebSocket(this.url);
        ws.binaryType = "arraybuffer";
        return ws;
    }

    /**
     * Wire up WebSocket events into a reactive {@link Flux} of inbound
     * frames. On the `open` event the outbound pipeline and SETUP frame
     * are initialised.
     */
    private createInboundStream(ws: WebSocket): Flux<Frame> {
        return Flux.create((sink) => {
            ws.addEventListener("open", () => {
                // Pipe outbound frames to the socket.
                this.outboundFlux.subscribe(
                    (frame) => ws.send(frame.toUint8Array()),
                    (error) => sink.error(error),
                    () => sink.complete(),
                );

                // Send SETUP as the very first frame (spec §5.1).
                this.outboundSink.next(this.setupFrame);

                // Emit an initial keepalive so the keepalive scheduler starts.
                sink.next(new KeepaliveFrame(KeepaliveFlag.RESPOND));

                // Bridge the queued frames into the outbound sink.
                this.frameQueueFlux.subscribe({
                    onNext: (value) => this.outboundSink.next(value),
                    onError: (error) => this.outboundSink.error(error),
                    onComplete: () => this.outboundSink.complete(),
                    onSubscribe: (sub) => sub.request(Number.MAX_SAFE_INTEGER),
                });
            });

            ws.addEventListener("message", (ev) => {
                try {
                    sink.next(
                        FrameDeserializer.deserialize(
                            new Uint8Array(ev.data),
                            this.metadataMimeType,
                            this.dataMimeType,
                        ),
                    );
                } catch (e) {
                    sink.error(e as Error);
                }
            });

            ws.addEventListener("error", (ev) => {
                console.error(ev);
                sink.error(new Error("WebSocket error"));
            });

            ws.addEventListener("close", () => {
                sink.complete();
            });
        });
    }

    /**
     * Teardown callback invoked when the inbound {@link Flux.using}
     * resource is released (on error or completion).
     *
     * Resets all internal state so the client can be reconnected.
     */
    private cleanupConnection(ws: WebSocket): void {
        ws.close();
        this.outboundSink.complete();
        this.frameQueue.complete();

        this.inbound = undefined;
        this.lastStreamId = -1;

        this.frameQueue = Sinks.many().unicast().onBackpressureBuffer();
        this.frameQueueFlux = this.frameQueue.asFlux();

        this.outboundSink = Sinks.many().unicast().onBackpressureBuffer();
        if (this.outboundLogs) this.outboundFlux = this.outboundSink.asFlux().log("RSOCKET-OUTBOUND").share();
        else this.outboundFlux = this.outboundSink.asFlux().share();
    }

    // -----------------------------------------------------------------------
    // Stream & frame helpers
    // -----------------------------------------------------------------------

    /**
     * Allocate the next client‑initiated stream ID. Per the RSocket spec
     * (§5.1.1) client stream IDs MUST be odd and monotonically increasing.
     */
    private nextStreamId(): number {
        this.lastStreamId += 2;
        return this.lastStreamId;
    }

    /**
     * Filter the shared inbound stream to frames belonging to `streamId`.
     */
    private inboundForStream(streamId: number): Flux<Frame> {
        return this.safeInbound.filter((f) => f.header.streamId === streamId);
    }

    /**
     * Safely access the inbound flux, returning {@link Flux.empty} when
     * disconnected.
     */
    private get safeInbound(): Flux<Frame> {
        return this.inbound ?? Flux.empty();
    }

    /**
     * Enqueue a frame for transmission and return a {@link Mono} that
     * completes once the frame has been observed on the shared outbound.
     */
    private enqueueFrame(frame: Frame): Mono<void> {
        return Mono.fromCallable(() => this.frameQueue.next(frame))
            .then(this.outboundFlux.any((value) => value === frame))
            .then();
    }

    // -----------------------------------------------------------------------
    // Encoding helpers
    // -----------------------------------------------------------------------

    /**
     * Encode application data using the configured MIME type, or return
     * `undefined` when no data is present.
     */
    private encodeData(data: P | undefined): FramePayload<P> | undefined {
        return data !== undefined ? this.dataMimeType.toPayload(data) : undefined;
    }

    /**
     * Encode metadata using the configured MIME type, or return `undefined`
     * when no metadata is present.
     */
    private encodeMetadata(metadata: M | undefined, data: P | undefined): Metadata<M> | undefined {
        return metadata !== undefined ? this.metadataMimeType.toMetadata(metadata, data != undefined) : undefined;
    }

    // -----------------------------------------------------------------------
    // Frame dispatch helpers
    // -----------------------------------------------------------------------

    /**
     * Handle the single response frame for a Request‑Response interaction.
     * Accepts PAYLOAD and ERROR; rejects anything else.
     */
    private handleSingleResponse<D, MD>(frame: Frame, streamId: number): Mono<Payload<D, MD>> {
        switch (frame.type) {
            case FrameType.PAYLOAD: {
                const pf = frame as PayloadFrame;
                return Mono.just({
                    data: pf.payload as D,
                    metadata: pf.metadata as MD,
                });
            }
            case FrameType.ERROR: {
                const ef = frame as ErrorFrame;
                return Mono.error(
                    new Error(`[REQUEST_RESPONSE:${streamId}] Error ${ef.code}: ${ef.payload}`),
                );
            }
            default:
                return Mono.error(
                    new Error(`[REQUEST_RESPONSE:${streamId}] Unexpected frame type: ${frame.type}`),
                );
        }
    }

    /**
     * Dispatch an inbound frame for streaming interactions
     * (Request‑Stream). Handles PAYLOAD (NEXT / COMPLETE), ERROR, and
     * rejects unexpected types.
     */
    private dispatchStreamFrame(
        frame: Frame,
        streamId: number,
        sink: { next(v: Payload<P, M>): void; complete(): void; error(e: Error): void },
        label: string,
    ): void {
        switch (frame.type) {
            case FrameType.PAYLOAD:
                this.emitPayloadFrame(frame as PayloadFrame, sink);
                break;
            case FrameType.ERROR: {
                const ef = frame as ErrorFrame;
                sink.error(new Error(`[${label}:${streamId}] Error ${ef.code}: ${ef.payload}`));
                break;
            }
            default:
                sink.error(new Error(`[${label}:${streamId}] Unexpected frame type: ${frame.type}`));
        }
    }

    /**
     * Emit a PAYLOAD frame through the given sink, respecting the NEXT
     * and COMPLETE flags as defined in the spec (§5.3).
     */
    private emitPayloadFrame(
        pf: PayloadFrame,
        sink: { next(v: Payload<P, M>): void; complete(): void },
    ): void {
        if (pf.isFlagSet(PayloadFlag.NEXT)) {
            sink.next({
                data: pf.payload as P,
                metadata: pf.metadata as M,
            });
        }
        if (pf.isFlagSet(PayloadFlag.COMPLETE)) {
            sink.complete();
        }
    }

    // -----------------------------------------------------------------------
    // Keepalive
    // -----------------------------------------------------------------------

    /**
     * Start the KEEPALIVE state machine.
     *
     * Per the RSocket spec (§5.3):
     * - When a KEEPALIVE with RESPOND flag arrives, the client MUST
     *   reply with a KEEPALIVE without the RESPOND flag.
     * - When a KEEPALIVE response arrives (no RESPOND flag), the client
     *   resets its liveness timer. If no response is received within
     *   `keepAlive + lifetime` milliseconds, the connection is considered
     *   dead and is disconnected.
     */
    private scheduleKeepalive(): void {
        const requestFrame = new KeepaliveFrame(KeepaliveFlag.RESPOND);
        const responseFrame = new KeepaliveFrame();

        const timers: Array<{ cancel(): void }> = [];

        const cancelAllTimers = (): void => {
            timers.forEach((t) => t.cancel());
            timers.length = 0;
        };

        this.safeInbound
            .filter((f) => f.type === FrameType.KEEPALIVE)
            .cast<KeepaliveFrame>()
            .doOnNext((frame) => {
                if (frame.isRequireRespond()) {
                    // Server asked for a keepalive response.
                    this.outboundSink.next(responseFrame);
                } else {
                    // We received a keepalive response — reset timers.
                    cancelAllTimers();

                    timers.push(
                        Schedulers.delay(this.keepAliveInterval).schedule(() => {
                            // No response for `keepAlive` ms — send a probe.
                            timers.shift();
                            this.outboundSink.next(requestFrame);

                            timers.push(
                                Schedulers.delay(this.lifetimeTimeout).schedule(() => {
                                    // No response within `lifetime` — assume dead.
                                    timers.shift();
                                    this.disconnect().subscribe();
                                }),
                            );
                        }),
                    );
                }
            })
            .doFinally(() => cancelAllTimers())
            .subscribe();
    }
}