/**
 * Browser-side RSocket requester session implemented over a reactive WebSocket
 * transport.
 *
 * This module owns SETUP, keepalive, leases, frame dispatch, stream
 * registration, request interactions, payload reassembly, and protocol-level
 * connection shutdown.
 */
import {type Disposable, Mono} from "reactor-core-ts";
import {
    CancelFrame,
    ErrorFrame,
    ExtensionFrame,
    type Frame,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    KeepaliveFlag,
    KeepaliveFrame,
    LeaseFrame,
    Metadata,
    MetadataPushFrame,
    type MimeType,
    Payload,
    PayloadFlag,
    PayloadFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    ResumeFrame,
    SetupFlag,
    SetupFrame
} from "rsocket-frames-ts";
import {KEEPALIVE_DATA_MIME_TYPE} from "@/protocol/index.js";
import {RequestChannelOutbound} from "@/channel/index.js";
import {
    connectionClosedError,
    errorFromFrame,
    RSocketConnectionError,
    RSocketFrameSizeError,
    RSocketLeaseError,
    RSocketProtocolError
} from "@/errors/index.js";
import {emitOutboundFrameFragments, outboundFrameLength} from "@/fragmentation/index.js";
import {receiveResumeOkFrame, sendHandshakeFrame} from "@/client/handshake.js";
import {normalizeClientOptions, type NormalizedClientOptions} from "@/client/options.js";
import {encodeMetadataInput, encodePayloadInput, errorPayload} from "@/payload/index.js";
import {MonoRequestController} from "@/client/request-response.js";
import {type PayloadFragmentMap, reassemblePayloadFrame} from "@/reassembly/index.js";
import {
    RSocketFlux,
    RSocketStreamSubscription,
    type StartStream,
    type StreamController,
    type StreamSession
} from "@/stream/index.js";
import type {
    RSocketChannelInput,
    RSocketClientOptions,
    RSocketFrameActivityListener,
    RSocketPayloadFrame,
    RSocketPayloadInput,
    RSocketRequestOptions,
    RSocketStreamRequestOptions,
} from "@/types/index.js";
import {createReactiveWebSocketConnection, type ReactiveWebSocketConnection} from "@/transport/websocket/connection.js";
import {
    deserializeFrame,
    readFrameStreamId,
    readFrameTypeAndFlags,
    readKeepalivePosition
} from "@/transport/websocket/frames.js";
import {WS_CLOSE_NORMAL, WS_CLOSE_RSOCKET_PROTOCOL_ERROR, WS_OPEN} from "@/transport/websocket/constants.js";
import {validateWebSocketClose} from "@/transport/websocket/spec.js";
import {RSocketReplayBuffer, type RSocketResumeState} from "@/resume/index.js";

/**
 * Options required to start a protocol Resume handshake.
 */
interface ResumeHandshake {
    /** Last positions captured from the previous physical connection. */
    readonly state: RSocketResumeState;
    /** Suspended logical session whose active streams must survive Resume. */
    readonly client: BrowserRSocketClient;
}

/**
 * Listener invoked exactly once when the client session closes.
 */
type CloseListener = (error: unknown) => void;

/**
 * Alias for the optional frame activity observer.
 */
type ActivityListener = RSocketFrameActivityListener;

/**
 * Direction of a frame activity event.
 */
type ActivityDirection = Parameters<ActivityListener>[0]["direction"];

/**
 * Internal prepared options reused by the high-level facade across reconnects.
 */
type PreparedClientOptions<D = unknown, M = unknown> = RSocketClientOptions<D, M> & {
    /** Cached normalized options for repeated physical connects. */
    readonly normalizedOptions?: NormalizedClientOptions;
};

/**
 * Shared empty options object used to avoid allocating per request.
 */
const EMPTY_REQUEST_OPTIONS: RSocketRequestOptions = {};

/** Shared empty byte buffer used on hot paths that need an empty payload. */
const EMPTY_BYTES = new Uint8Array(0);
/** Reusable empty KEEPALIVE payload sent by requester heartbeats. */
const EMPTY_KEEPALIVE_PAYLOAD = KEEPALIVE_DATA_MIME_TYPE.toPayload(EMPTY_BYTES);
/** MIME overrides used to keep incoming payload fragments as raw bytes until reassembly. */
const FRAGMENT_MIME_OVERRIDES = Object.freeze({
    metadataMimeType: KEEPALIVE_DATA_MIME_TYPE,
    dataMimeType: KEEPALIVE_DATA_MIME_TYPE
});

/**
 * Low-level browser RSocket requester bound to exactly one WebSocket.
 *
 * Instances are created through `BrowserRSocketClient.connect(...)`; the public
 * `RSocket` facade owns reconnect loops and swaps closed sessions for new
 * instances when the browser WebSocket drops.
 */
export class BrowserRSocketClient implements StreamSession {
    private readonly streams = new Map<number, StreamController>();
    private readonly fragments: PayloadFragmentMap = new Map();
    private nextStreamId = 1;
    private clientPosition = 0n;
    private serverPosition = 0n;
    /** Position/replay state allocated only for resumable logical sessions. */
    private readonly replayBuffer: RSocketReplayBuffer | undefined;
    private pendingFrames: Frame[] | undefined;
    private setupAccepted: boolean;
    private closed = false;
    private suspended = false;
    private terminated = false;
    private gracefulCloseError: unknown;
    private closeError: unknown;
    private keepAliveTimer: ReturnType<typeof setInterval> | undefined;
    private lifetimeTimer: ReturnType<typeof setTimeout> | undefined;
    private lastReceivedAt = Date.now();
    private leaseRemaining = 0;
    private leaseExpiresAt = 0;
    private closeListeners = new Set<CloseListener>();
    private activityListeners: Set<ActivityListener> | undefined;
    private readonly transportDisposables: Disposable[] = [];
    private readonly sendFragment = (frame: Frame): void => this.sendSerializedFrame(frame);
    private connection: ReactiveWebSocketConnection;

    /**
     * Creates a client around an already-created reactive WebSocket transport.
     *
     * The constructor is private so callers cannot bypass the async open and
     * SETUP handshake performed by `connect`.
     */
    private constructor(
        connection: ReactiveWebSocketConnection,
        private readonly options: NormalizedClientOptions
    ) {
        this.connection = connection;
        this.replayBuffer = options.setup.resumeToken === undefined ? undefined : new RSocketReplayBuffer();
        this.setupAccepted = false;
        this.attachConnection();
        if (options.activityListener !== undefined) this.onActivity(options.activityListener);
    }

    /**
     * Opens the WebSocket, sends the RSocket SETUP frame, and starts keepalive.
     */
    static async connect<D = unknown, M = unknown>(options: RSocketClientOptions<D, M>): Promise<BrowserRSocketClient> {
        const normalized = preparedOptions(options).normalizedOptions ?? normalizeClientOptions(options);
        const connection = createReactiveWebSocketConnection(
            options.webSocketFactory,
            options.url,
            options.protocols,
            normalized.connectTimeoutMs,
            options.abortSignal,
            options.webSocketEndpoint
        );
        const client = new BrowserRSocketClient(connection, normalized);

        try {
            await connection.opened.block();
            client.sendSetupFrame();
            client.assertHandshakeConnectionOpen();
            client.startKeepAlive();
            return client;
        } catch (error) {
            client.terminateSession(error, true);
            throw error;
        }
    }

    /**
     * Opens the WebSocket, sends RESUME, waits for RESUME_OK, and starts keepalive.
     */
    static async resume<D = unknown, M = unknown>(
        options: RSocketClientOptions<D, M>,
        resume: ResumeHandshake
    ): Promise<BrowserRSocketClient> {
        const normalized = preparedOptions(options).normalizedOptions ?? normalizeClientOptions(options);
        const token = normalized.setup.resumeToken;
        if (token === undefined) {
            throw new RSocketProtocolError("RSocket Resume requires a resume token");
        }

        const client = resume.client;
        if (!client.suspended || client.terminated) {
            throw new RSocketProtocolError("RSocket Resume requires a suspended logical session");
        }

        const connection = createReactiveWebSocketConnection(
            options.webSocketFactory,
            options.url,
            options.protocols,
            normalized.connectTimeoutMs,
            options.abortSignal,
            options.webSocketEndpoint
        );

        try {
            await connection.opened.block();
            const frame = new ResumeFrame(
                token,
                resume.state.serverPosition,
                resume.state.firstAvailableClientPosition,
                normalized.setup.majorVersion,
                normalized.setup.minorVersion
            );
            sendHandshakeFrame(connection, normalized, frame);

            const response = await receiveResumeOkFrame(connection, normalized, options.abortSignal);
            if (response.lastReceivedClientPosition > resume.state.clientPosition) {
                throw new RSocketProtocolError("RSocket Resume responder acknowledged an impossible client position");
            }

            client.resumeWith(connection, response.lastReceivedClientPosition);
            return client;
        } catch (error) {
            try {
                connection.close(WS_CLOSE_RSOCKET_PROTOCOL_ERROR, "RSocket resume failed");
            } catch {
                // Preserve the original resume failure; cleanup close errors are secondary.
            }
            throw error;
        }
    }

    /**
     * Indicates whether this concrete WebSocket-backed RSocket session is closed.
     */
    get isClosed(): boolean {
        return this.closed;
    }

    /** Whether this logical session is waiting for a protocol Resume attempt. */
    get isSuspended(): boolean {
        return this.suspended && !this.terminated;
    }

    /**
     * Error or close reason that caused this session to terminate.
     */
    get closedError(): unknown {
        return this.closeError;
    }

    /**
     * Default data MIME type negotiated through SETUP.
     */
    get dataMimeType(): MimeType<any> {
        return this.options.setup.dataMimeType;
    }

    /**
     * Default metadata MIME type negotiated through SETUP.
     */
    get metadataMimeType(): MimeType<any> {
        return this.options.setup.metadataMimeType;
    }

    /**
     * Returns the current protocol resume position snapshot.
     */
    resumeState(): RSocketResumeState {
        return {
            clientPosition: this.clientPosition,
            firstAvailableClientPosition: this.replayBuffer?.firstAvailablePosition(this.clientPosition) ??
                this.clientPosition,
            serverPosition: this.serverPosition
        };
    }

    /**
     * Closes stale sessions whose keepalive lifetime expired while the browser was suspended.
     */
    checkLifetime(now = Date.now()): boolean {
        if (this.closed) return false;
        if (now - this.lastReceivedAt < this.options.setup.lifetimeMs) return true;
        this.loseTransport(new RSocketConnectionError("RSocket keepalive lifetime expired"), true);
        return false;
    }

    /**
     * Closes the RSocket session and its underlying WebSocket.
     */
    close(code = 1000, reason = "RSocket client closed"): void {
        validateWebSocketClose(code, reason);
        if (this.terminated) return;
        const wasActive = !this.closed;
        try {
            if (wasActive && this.connection.readyState === WS_OPEN) {
                this.sendFrame(new ErrorFrame(0, FrameErrorCode.CONNECTION_ERROR, errorPayload(reason)));
            }
        } finally {
            this.terminateSession(connectionClosedError(reason), false);
            if (wasActive) this.connection.close(code, reason);
        }
    }

    /**
     * Terminates a suspended logical session when Resume will not be retried.
     */
    abandonResume(error: unknown = this.closeError): void {
        this.terminateSession(error, false);
    }

    /**
     * Registers a listener that runs once when the session closes.
     *
     * If the session is already closed, the listener is called immediately with
     * the stored close reason.
     */
    onClose(listener: CloseListener): () => void {
        if (this.closed) {
            listener(this.closeError);
            return () => undefined;
        }

        this.closeListeners.add(listener);
        return () => {
            this.closeListeners.delete(listener);
        };
    }

    /**
     * Registers a diagnostic frame activity listener for sent and received frames.
     */
    onActivity(listener: ActivityListener): () => void {
        const listeners = this.activityListeners ??= new Set<ActivityListener>();
        listeners.add(listener);
        return () => {
            listeners.delete(listener);
            if (listeners.size === 0 && this.activityListeners === listeners) this.activityListeners = undefined;
        };
    }

    /**
     * Starts a request-response interaction and emits exactly one decoded payload.
     *
     * The returned `Mono` is cold: the request frame is written only when the Mono
     * is subscribed or blocked.
     */
    requestResponse<D = unknown, M = unknown>(
        payload: RSocketPayloadInput<D, M>,
        options: RSocketRequestOptions = EMPTY_REQUEST_OPTIONS
    ): Mono<RSocketPayloadFrame> {
        return Mono.create<RSocketPayloadFrame>((sink) => {
            let streamId: number | undefined;
            let controller: MonoRequestController | undefined;
            let timeout: ReturnType<typeof setTimeout> | undefined;
            try {
                this.assertCanStartRequest();
                streamId = this.allocateStreamId();
                const timeoutMs = requestTimeoutMs(options);
                controller = new MonoRequestController(
                    this,
                    streamId,
                    timeoutMs === undefined
                        ? sink
                        : {
                            success: (value) => {
                                if (timeout !== undefined) clearTimeout(timeout);
                                timeout = undefined;
                                sink.success(value);
                            },
                            error: (error) => {
                                if (timeout !== undefined) clearTimeout(timeout);
                                timeout = undefined;
                                sink.error(error);
                            }
                        }
                );
                this.streams.set(streamId, controller);
                if (timeoutMs !== undefined) {
                    timeout = setTimeout(() => {
                        if (streamId === undefined || controller === undefined || this.streams.get(streamId) !== controller) return;
                        controller.fail(new RSocketConnectionError(`RSocket request-response timed out after ${timeoutMs}ms`));
                        try {
                            this.sendFrame(new CancelFrame(streamId));
                        } catch {
                            // The timeout has already failed the Mono; a closed socket cannot observe CANCEL.
                        }
                    }, timeoutMs);
                }
                const encoded = this.encode(payload, options);
                this.sendRequestFrame(new RequestResponseFrame(streamId, FrameFlag.NONE, encoded.metadata, encoded.payload));
            } catch (error) {
                if (timeout !== undefined) clearTimeout(timeout);
                timeout = undefined;
                if (streamId !== undefined) this.streams.delete(streamId);
                sink.error(error);
            }

            sink.onCancel(() => {
                if (timeout !== undefined) clearTimeout(timeout);
                timeout = undefined;
                if (streamId === undefined || !this.streams.delete(streamId)) return;
                try {
                    this.sendFrame(new CancelFrame(streamId));
                } catch {
                    // The stream is already locally cancelled; a closed socket cannot observe CANCEL.
                }
            });
        });
    }

    /**
     * Starts a fire-and-forget interaction and completes after the frame is sent.
     */
    fireAndForget<D = unknown, M = unknown>(
        payload: RSocketPayloadInput<D, M>,
        options: RSocketRequestOptions = EMPTY_REQUEST_OPTIONS
    ): Mono<void> {
        return Mono.create<void>((sink) => {
            try {
                this.assertCanStartRequest();
                const streamId = this.allocateStreamId();
                const encoded = this.encode(payload, options);
                this.sendRequestFrame(new RequestFireAndForgetFrame(streamId, FrameFlag.NONE, encoded.metadata, encoded.payload));
                sink.success();
            } catch (error) {
                sink.error(error);
            }
        });
    }

    /**
     * Starts a request-stream interaction backed by RSocket REQUEST_N demand.
     */
    requestStream<D = unknown, M = unknown>(
        payload: RSocketPayloadInput<D, M>,
        options: RSocketStreamRequestOptions = EMPTY_REQUEST_OPTIONS
    ): RSocketFlux {
        return this.createRequestFlux((streamId, initialRequestN) => {
            this.assertCanStartRequest();
            const encoded = this.encode(payload, options);
            this.sendRequestFrame(
                new RequestStreamFrame(
                    streamId,
                    FrameFlag.NONE,
                    initialRequestN,
                    encoded.metadata,
                    encoded.payload
                )
            );
        });
    }

    /**
     * Starts a request-channel interaction using the supplied outbound publisher.
     */
    requestChannel<D = unknown, M = unknown>(
        payloads: RSocketChannelInput<D, M>,
        options: RSocketStreamRequestOptions = EMPTY_REQUEST_OPTIONS
    ): RSocketFlux {
        return this.createRequestFlux((streamId, initialRequestN, subscription) => {
            this.assertCanStartRequest();
            const outbound = new RequestChannelOutbound(
                this,
                streamId,
                initialRequestN,
                payloads,
                options.dataMimeType ?? this.dataMimeType,
                options.metadataMimeType ?? this.metadataMimeType,
                () => subscription.markOutboundComplete(),
                (error, requestStarted) => subscription.failOutbound(error, requestStarted)
            );
            subscription.attachOutbound(outbound);
            outbound.start();
        });
    }

    /**
     * Sends connection-level metadata without opening a stream.
     */
    metadataPush<M = unknown>(metadata: M | Metadata<M>, options: RSocketRequestOptions = EMPTY_REQUEST_OPTIONS): Mono<void> {
        return Mono.create<void>((sink) => {
            try {
                const mimeType = options.metadataMimeType ?? this.metadataMimeType;
                const frameMetadata = encodeMetadataInput(metadata, mimeType);
                this.sendFrame(new MetadataPushFrame(frameMetadata));
                sink.success();
            } catch (error) {
                sink.error(error);
            }
        });
    }

    /**
     * Serializes and writes one frame to the active WebSocket immediately.
     */
    sendFrame(frame: Frame): void {
        if (this.suspended) {
            this.queueFrame(frame);
            return;
        }
        if (this.terminated) throw connectionClosedError(this.closeError);

        const maxFrameLength = this.options.maxFrameLength;
        const frameLength = outboundFrameLength(frame);
        if (frameLength !== undefined && frameLength > maxFrameLength) {
            emitOutboundFrameFragments(
                frame,
                frameLength,
                maxFrameLength,
                this.sendFragment
            );
            return;
        }

        const bytes = frame.toUint8Array();
        if (bytes.length <= maxFrameLength) {
            this.sendSerializedFrame(frame, bytes);
            return;
        }

        emitOutboundFrameFragments(
            frame,
            bytes.length,
            maxFrameLength,
            this.sendFragment
        );
    }

    /**
     * Sends an initial interaction frame and consumes a lease only once the
     * request is ready for the wire.
     */
    sendRequestFrame(frame: Frame): void {
        const consumesLease = this.options.setup.honorLease;
        if (consumesLease) {
            this.assertLeaseAvailable();
            this.leaseRemaining -= 1;
        }

        try {
            this.sendFrame(frame);
        } catch (error) {
            if (consumesLease) this.leaseRemaining += 1;
            throw error;
        }
    }

    /**
     * Serializes and writes one already-sized frame to the active WebSocket.
     */
    private sendSerializedFrame(frame: Frame, bytes = frame.toUint8Array()): void {
        if (this.suspended) {
            this.queueFrame(frame);
            return;
        }
        if (this.closed) throw connectionClosedError(this.closeError);
        if (this.connection.readyState !== WS_OPEN) {
            const error = new RSocketConnectionError("WebSocket is not open");
            this.loseTransport(error, false);
            if (this.suspended) {
                this.queueFrame(frame);
                return;
            }
            throw error;
        }

        const maxFrameLength = this.options.maxFrameLength;
        if (bytes.length > maxFrameLength) {
            throw new RSocketFrameSizeError(bytes.length, maxFrameLength);
        }

        try {
            this.connection.sendNow(bytes);
            const replayBuffer = this.replayBuffer;
            if (replayBuffer !== undefined && isResumePositionFrame(frame.type)) {
                this.clientPosition = replayBuffer.record(
                    this.clientPosition,
                    this.activityListeners === undefined ? undefined : frame,
                    bytes
                );
            }
            if (this.activityListeners !== undefined) this.emitActivity("send", frame);
        } catch (error) {
            this.loseTransport(error, true);
            if (this.suspended) {
                this.queueFrame(frame);
                return;
            }
            throw error;
        }
    }

    /**
     * Removes a stream and any payload fragments associated with it.
     */
    unregisterStream(streamId: number): void {
        this.streams.delete(streamId);
        this.fragments.delete(streamId);
        if (this.streams.size === 0 && this.gracefulCloseError !== undefined) {
            const error = this.gracefulCloseError;
            this.gracefulCloseError = undefined;
            this.terminateSession(error, true, WS_CLOSE_NORMAL, "RSocket connection closed");
        }
    }

    /**
     * Sends an RSocket ERROR frame when possible and closes the session.
     */
    protocolError(error: RSocketProtocolError): void {
        try {
            this.sendFrame(new ErrorFrame(0, error.code ?? FrameErrorCode.CONNECTION_ERROR, errorPayload(error)));
        } catch {
            // Closing is still required even when the error frame cannot be written.
        }
        this.terminateSession(error, true);
    }

    /**
     * Creates a cold `Flux` that allocates its stream id after initial demand.
     */
    private createRequestFlux(start: StartStream): RSocketFlux {
        return new RSocketFlux((subscriber) => {
            const subscription = new RSocketStreamSubscription(this, subscriber, () => this.allocateStreamId(), (id, requestN, current) => {
                this.streams.set(id, current);
                try {
                    start(id, requestN, current);
                } catch (error) {
                    this.streams.delete(id);
                    throw error;
                }
            });
            return subscription;
        });
    }

    /**
     * Encodes data and metadata with per-request MIME overrides when supplied.
     */
    private encode(input: RSocketPayloadInput<any, any>, options: RSocketRequestOptions) {
        return encodePayloadInput(
            input,
            options.dataMimeType ?? this.dataMimeType,
            options.metadataMimeType ?? this.metadataMimeType
        );
    }

    /**
     * Allocates the next available odd requester stream id.
     */
    private allocateStreamId(): number {
        for (let attempts = 0; attempts <= 0x40000000; attempts += 1) {
            const streamId = this.nextStreamId;
            this.nextStreamId += 2;
            if (this.nextStreamId > 0x7fffffff) this.nextStreamId = 1;
            if (!this.streams.has(streamId)) return streamId;
        }
        throw new RSocketProtocolError("No free client stream IDs are available");
    }

    /**
     * Verifies that the session is open and that requester lease permits demand.
     */
    private assertCanStartRequest(): void {
        if (this.closed) throw connectionClosedError(this.closeError);
        if (this.gracefulCloseError !== undefined) {
            throw new RSocketConnectionError("RSocket responder is closing the connection", this.gracefulCloseError);
        }
        if (!this.options.setup.honorLease) return;
        this.assertLeaseAvailable();
    }

    /** Verifies that the current requester lease grants another interaction. */
    private assertLeaseAvailable(): void {
        if (this.leaseRemaining <= 0 || Date.now() >= this.leaseExpiresAt) {
            throw new RSocketLeaseError("No active RSocket lease is available for a new request");
        }
    }

    /**
     * Writes the initial SETUP frame using normalized protocol options.
     */
    private sendSetupFrame(): void {
        const setup = this.options.setup;
        const encoded = encodePayloadInput(setup.setupPayload, setup.dataMimeType, setup.metadataMimeType);
        const flags = setup.honorLease ? SetupFlag.LEASE : FrameFlag.NONE;

        this.sendFrame(
            new SetupFrame(
                setup.keepAliveMs,
                setup.lifetimeMs,
                setup.metadataMimeType,
                setup.dataMimeType,
                setup.resumeToken,
                setup.majorVersion,
                setup.minorVersion,
                flags,
                encoded.metadata,
                encoded.payload
            )
        );
    }

    /**
     * Starts requester keepalive and lifetime monitoring.
     */
    private startKeepAlive(): void {
        if (this.closed) return;
        const keepAliveMs = this.options.setup.keepAliveMs;
        if (keepAliveMs <= 0) return;
        this.lastReceivedAt = Date.now();
        this.scheduleLifetimeCheck();

        this.keepAliveTimer = setInterval(() => {
            if (this.closed) return;
            try {
                this.sendFrame(new KeepaliveFrame(KeepaliveFlag.RESPOND, this.serverPosition, EMPTY_KEEPALIVE_PAYLOAD));
            } catch (error) {
                this.loseTransport(error, true);
            }
        }, keepAliveMs);
    }

    /**
     * Attaches a replacement WebSocket, replays unacknowledged frames, and
     * flushes frames produced by active streams while the transport was down.
     */
    private resumeWith(connection: ReactiveWebSocketConnection, peerPosition: bigint): void {
        if (!this.suspended || this.terminated) {
            throw new RSocketProtocolError("RSocket logical session is no longer resumable");
        }

        this.connection = connection;
        this.closed = false;
        this.suspended = true;
        this.closeError = undefined;
        this.setupAccepted = true;
        this.leaseRemaining = 0;
        this.leaseExpiresAt = 0;
        this.attachConnection();

        try {
            const replayBuffer = this.replayBuffer;
            if (replayBuffer === undefined) throw new RSocketProtocolError("RSocket session has no replay buffer");
            replayBuffer.replayFrom(
                peerPosition,
                this.clientPosition,
                (frame, bytes) => this.sendReplayFrame(frame, bytes)
            );
            this.assertHandshakeConnectionOpen();
            this.suspended = false;
            this.flushPendingFrames();
            this.assertHandshakeConnectionOpen();
            this.startKeepAlive();
        } catch (error) {
            this.loseTransport(error, true);
            throw error;
        }
    }

    /** Writes retained bytes without assigning a second implied position. */
    private sendReplayFrame(frame: Frame | undefined, bytes: Uint8Array): void {
        if (this.connection.readyState !== WS_OPEN) {
            throw new RSocketConnectionError("WebSocket closed during RSocket frame replay");
        }
        this.connection.sendNow(bytes);
        if (frame !== undefined && this.activityListeners !== undefined) this.emitActivity("send", frame);
    }

    /** Flushes stream frames queued while the logical session was suspended. */
    private flushPendingFrames(): void {
        const frames = this.pendingFrames;
        if (frames === undefined || frames.length === 0) {
            this.pendingFrames = undefined;
            return;
        }

        this.pendingFrames = undefined;
        for (let index = 0; index < frames.length; index += 1) {
            this.sendFrame(frames[index] as Frame);
            if (!this.suspended) continue;

            const pending = this.pendingFrames ??= [];
            for (let remaining = index + 1; remaining < frames.length; remaining += 1) {
                pending.push(frames[remaining] as Frame);
            }
            throw connectionClosedError(this.closeError);
        }
    }

    /** Queues resumable work while dropping transport-scoped heartbeats. */
    private queueFrame(frame: Frame): void {
        if (frame.type === FrameType.KEEPALIVE) return;
        (this.pendingFrames ??= []).push(frame);
    }

    /**
     * Schedules an accurate lifetime check without resetting a timer on every inbound frame.
     */
    private scheduleLifetimeCheck(): void {
        if (this.closed) return;
        const remaining = this.options.setup.lifetimeMs - (Date.now() - this.lastReceivedAt);
        if (remaining <= 0) {
            this.checkLifetime();
            return;
        }
        this.lifetimeTimer = setTimeout(() => {
            this.lifetimeTimer = undefined;
            if (this.checkLifetime()) this.scheduleLifetimeCheck();
        }, remaining);
    }

    /**
     * Subscribes the RSocket session to WebSocket messages, errors, and closes.
     */
    private attachConnection(): void {
        const connection = this.connection;
        const subscribe = (factory: () => Disposable): void => {
            if (this.closed) return;
            const disposable = factory();
            if (this.closed) disposable.dispose();
            else this.transportDisposables.push(disposable);
        };

        subscribe(() => connection.messages.subscribe(
            (bytes) => {
                if (this.connection === connection) this.handleIncomingBytes(bytes);
            },
            (error) => {
                if (this.connection === connection) {
                    this.protocolError(new RSocketProtocolError("Failed to decode incoming WebSocket message", {cause: error}));
                }
            }
        ));
        subscribe(() => connection.errors.subscribe((event) => {
            if (this.connection === connection) {
                this.loseTransport(new RSocketConnectionError("WebSocket error", event), true);
            }
        }));
        subscribe(() => connection.closes.subscribe(() => {
            if (this.connection === connection) {
                this.loseTransport(connectionClosedError("WebSocket closed"), false);
            }
        }));
    }

    /**
     * Decodes one inbound binary WebSocket message into an RSocket frame.
     */
    private handleIncomingBytes(bytes: Uint8Array): void {
        const maxFrameLength = this.options.maxFrameLength;
        if (bytes.length > maxFrameLength) {
            this.protocolError(new RSocketFrameSizeError(bytes.length, maxFrameLength));
            return;
        }

        try {
            const streamId = readFrameStreamId(bytes);
            const typeAndFlags = readFrameTypeAndFlags(bytes);
            const frameType = (typeAndFlags >>> 10) as FrameType;
            const payloadStream = frameType === FrameType.PAYLOAD
                ? this.streams.get(streamId)
                : undefined;
            const decodePayloadAsRaw =
                frameType === FrameType.PAYLOAD &&
                (
                    (typeAndFlags & PayloadFlag.FOLLOWS) !== 0 ||
                    this.fragments.has(streamId) ||
                    payloadStream === undefined
                );
            const frame = deserializeFrame(
                bytes,
                this.metadataMimeType,
                this.dataMimeType,
                decodePayloadAsRaw ? FRAGMENT_MIME_OVERRIDES : undefined,
                frameType
            );
            const replayBuffer = this.replayBuffer;
            if (replayBuffer !== undefined && frameType === FrameType.KEEPALIVE) {
                replayBuffer.acknowledge(readKeepalivePosition(bytes), this.clientPosition);
            }
            if (replayBuffer !== undefined && isResumePositionFrame(frameType)) {
                this.serverPosition += BigInt(bytes.byteLength);
            }
            this.lastReceivedAt = Date.now();
            if (this.activityListeners !== undefined) this.emitActivity("receive", frame);
            const dispatchStream = payloadStream !== undefined && this.activityListeners !== undefined &&
            this.streams.get(streamId) !== payloadStream
                ? undefined
                : payloadStream;
            this.handleFrame(frame, dispatchStream, streamId);
        } catch (error) {
            this.protocolError(new RSocketProtocolError("Failed to decode incoming RSocket frame", {cause: error}));
        }
    }

    /**
     * Dispatches one decoded frame to connection-level or stream-level handlers.
     */
    private handleFrame(frame: Frame, payloadStream: StreamController | undefined, streamId: number): void {
        const frameType = frame.type;
        if (!this.validateFrameStreamId(frame, streamId)) return;
        const activeStream = frameType === FrameType.PAYLOAD
            ? payloadStream !== undefined
            : this.streams.has(streamId);
        if (!this.setupAccepted && confirmsSetup(frame, activeStream)) {
            this.setupAccepted = true;
        }
        switch (frameType) {
            case FrameType.KEEPALIVE:
                this.handleKeepAlive(frame as KeepaliveFrame);
                return;
            case FrameType.LEASE:
                this.handleLease(frame as LeaseFrame);
                return;
            case FrameType.PAYLOAD:
                this.handlePayloadFrame(frame as PayloadFrame, payloadStream);
                return;
            case FrameType.ERROR:
                this.handleErrorFrame(frame as ErrorFrame);
                return;
            case FrameType.REQUEST_N: {
                const stream = this.streams.get(streamId);
                if (stream !== undefined) stream.handleRequestN(frame as RequestNFrame);
                return;
            }
            case FrameType.CANCEL: {
                const stream = this.streams.get(streamId);
                if (stream !== undefined) stream.handleCancel();
                return;
            }
            case FrameType.METADATA_PUSH:
            case FrameType.SETUP:
            case FrameType.RESUME:
            case FrameType.RESUME_OK:
                return;
            case FrameType.EXT:
                this.handleExtension(frame as ExtensionFrame);
                return;
            default:
                this.rejectResponderFrame(frame);
        }
    }

    /**
     * Enforces stream zero for connection frames that cannot be ignored leniently.
     */
    private validateFrameStreamId(frame: Frame, streamId: number): boolean {
        const frameType = frame.type;
        const invalid = isStrictConnectionFrame(frameType) && streamId !== 0;
        if (!invalid) return true;

        this.protocolError(
            new RSocketProtocolError("Responder sent an RSocket frame with an invalid stream ID", {
                code: FrameErrorCode.CONNECTION_ERROR,
                streamId
            })
        );
        return false;
    }

    /**
     * Responds to KEEPALIVE frames that require a responder echo.
     */
    private handleKeepAlive(frame: KeepaliveFrame): void {
        if (!frame.isRequireRespond()) return;
        this.sendFrame(new KeepaliveFrame(KeepaliveFlag.NONE, this.serverPosition, frame.payload as Payload<any> | undefined));
    }

    /**
     * Stores responder lease allowance for future requester interactions.
     */
    private handleLease(frame: LeaseFrame): void {
        this.leaseRemaining = frame.requestLimit;
        this.leaseExpiresAt = Date.now() + frame.ttl;
    }

    /**
     * Reassembles fragmented payloads and forwards complete PAYLOAD frames.
     */
    private handlePayloadFrame(frame: PayloadFrame, stream: StreamController | undefined): void {
        const streamId = frame.header.streamId;
        if (stream === undefined) {
            this.fragments.delete(streamId);
            return;
        }

        const payload = reassemblePayloadFrame(frame, this.fragments, this.metadataMimeType, this.dataMimeType);
        if (payload === undefined) return;
        stream.handlePayload(payload);
    }

    /**
     * Applies connection-level or stream-level ERROR frames.
     */
    private handleErrorFrame(frame: ErrorFrame): void {
        const streamId = frame.header.streamId;
        if (streamId === 0) {
            if (this.setupAccepted && isIgnoredPostSetupError(frame.code)) return;
            if (isStreamErrorCode(frame.code)) {
                this.protocolError(invalidErrorStreamId(frame));
                return;
            }
            const error = errorFromFrame(frame);
            if (frame.code === FrameErrorCode.CONNECTION_CLOSE) {
                this.beginGracefulClose(error);
                return;
            }
            this.terminateSession(error, true);
            return;
        }

        if (isConnectionErrorCode(frame.code)) {
            this.protocolError(invalidErrorStreamId(frame));
            return;
        }

        const stream = this.streams.get(streamId);
        if (stream !== undefined) stream.handleError(frame);
    }

    /** Stops new requests and waits for active streams after CONNECTION_CLOSE. */
    private beginGracefulClose(error: unknown): void {
        if (this.closed || this.gracefulCloseError !== undefined) return;
        this.gracefulCloseError = error;
        if (this.streams.size !== 0) return;
        this.gracefulCloseError = undefined;
        this.terminateSession(error, true, WS_CLOSE_NORMAL, "RSocket connection closed");
    }

    /**
     * Rejects required extension frames because this client has no extension
     * responder implementation.
     */
    private handleExtension(frame: ExtensionFrame): void {
        if (frame.canBeIgnored()) return;
        const streamId = frame.header.streamId;
        this.protocolError(
            new RSocketProtocolError("Unsupported required RSocket extension frame", {
                code: FrameErrorCode.CONNECTION_ERROR,
                streamId
            })
        );
    }

    /**
     * Rejects frames that only make sense for an RSocket responder.
     */
    private rejectResponderFrame(frame: Frame): void {
        if (frame.canBeIgnored()) return;
        const streamId = frame.header.streamId;
        if (streamId <= 0 || streamId % 2 !== 0 || this.streams.has(streamId)) return;
        this.sendFrame(new ErrorFrame(streamId, FrameErrorCode.REJECTED, errorPayload("Responder is not configured")));
    }

    /**
     * Rejects a handshake that lost its WebSocket before the client became usable.
     */
    private assertHandshakeConnectionOpen(): void {
        if (!this.closed && this.connection.readyState === WS_OPEN) return;
        throw connectionClosedError(this.closeError ?? "WebSocket closed during RSocket handshake");
    }

    /**
     * Suspends a resumable logical session after transport loss, or terminates
     * a non-resumable session immediately.
     */
    private loseTransport(error: unknown, closeSocket: boolean): void {
        if (this.closed) return;
        if (this.replayBuffer === undefined || this.gracefulCloseError !== undefined) {
            this.terminateSession(error, closeSocket);
            return;
        }

        this.closed = true;
        this.suspended = true;
        this.closeError = error;
        this.leaseRemaining = 0;
        this.leaseExpiresAt = 0;
        this.stopTransport();
        this.emitClose(error);
        if (closeSocket) this.closeTransportQuietly();
    }

    /**
     * Terminates the logical session, fails active streams, and releases every
     * retained replay/transport resource.
     */
    private terminateSession(
        error: unknown,
        closeSocket: boolean,
        closeCode = WS_CLOSE_RSOCKET_PROTOCOL_ERROR,
        closeReason = "RSocket protocol error"
    ): void {
        if (this.terminated) return;
        const shouldEmitClose = !this.closed;
        this.closed = true;
        this.suspended = false;
        this.terminated = true;
        this.gracefulCloseError = undefined;
        this.closeError = error;
        this.stopTransport();

        for (const controller of this.streams.values()) {
            try {
                controller.fail(error);
            } catch {
                // User subscribers must not interrupt session cleanup.
            }
        }
        this.streams.clear();
        this.fragments.clear();
        this.pendingFrames = undefined;
        this.replayBuffer?.clear();
        this.activityListeners?.clear();
        this.activityListeners = undefined;
        if (shouldEmitClose) this.emitClose(error);
        if (closeSocket) this.closeTransportQuietly(closeCode, closeReason);
    }

    /** Stops heartbeat timers and removes listeners from the current transport. */
    private stopTransport(): void {
        if (this.keepAliveTimer !== undefined) clearInterval(this.keepAliveTimer);
        this.keepAliveTimer = undefined;
        if (this.lifetimeTimer !== undefined) clearTimeout(this.lifetimeTimer);
        this.lifetimeTimer = undefined;

        let disposable: Disposable | undefined;
        while ((disposable = this.transportDisposables.pop()) !== undefined) {
            try {
                disposable.dispose();
            } catch {
                // Transport listener cleanup must not suppress the session close signal.
            }
        }
    }

    /** Closes a failed physical WebSocket without replacing the primary error. */
    private closeTransportQuietly(
        code = WS_CLOSE_RSOCKET_PROTOCOL_ERROR,
        reason = "RSocket protocol error"
    ): void {
        try {
            this.connection.close(code, reason);
        } catch {
            // The logical session state already reflects the original failure.
        }
    }

    /**
     * Notifies close listeners while isolating listener exceptions.
     */
    private emitClose(error: unknown): void {
        const listeners = this.closeListeners;
        for (const listener of listeners) {
            try {
                listener(error);
            } catch {
                // Consumer close handlers must not break connection cleanup.
            }
        }
        listeners.clear();
    }

    /**
     * Sends diagnostic frame activity to registered listeners.
     */
    private emitActivity(direction: ActivityDirection, frame: Frame): void {
        const listeners = this.activityListeners;
        if (listeners === undefined || listeners.size === 0 || this.options.activityEnabled?.() === false) return;
        const activity = {direction, frame};
        for (const listener of listeners) {
            try {
                listener(activity);
            } catch {
                // Activity listeners are diagnostic-only.
            }
        }
    }
}

/**
 * Returns whether a frame type is valid only on stream zero.
 */
function isStrictConnectionFrame(type: FrameType): boolean {
    return type === FrameType.SETUP ||
        type === FrameType.LEASE ||
        type === FrameType.KEEPALIVE ||
        type === FrameType.METADATA_PUSH ||
        type === FrameType.RESUME ||
        type === FrameType.RESUME_OK;
}

/**
 * Returns whether a connection ERROR is required to be ignored after setup.
 */
function isIgnoredPostSetupError(code: FrameErrorCode): boolean {
    return code === FrameErrorCode.INVALID_SETUP ||
        code === FrameErrorCode.UNSUPPORTED_SETUP ||
        code === FrameErrorCode.REJECTED_SETUP ||
        code === FrameErrorCode.REJECTED_RESUME;
}

/** Returns whether a known ERROR code is valid only on stream zero. */
function isConnectionErrorCode(code: FrameErrorCode): boolean {
    return code === FrameErrorCode.INVALID_SETUP ||
        code === FrameErrorCode.UNSUPPORTED_SETUP ||
        code === FrameErrorCode.REJECTED_SETUP ||
        code === FrameErrorCode.REJECTED_RESUME ||
        code === FrameErrorCode.CONNECTION_ERROR ||
        code === FrameErrorCode.CONNECTION_CLOSE;
}

/** Returns whether a known ERROR code requires a non-zero stream ID. */
function isStreamErrorCode(code: FrameErrorCode): boolean {
    return code === FrameErrorCode.APPLICATION_ERROR ||
        code === FrameErrorCode.REJECTED ||
        code === FrameErrorCode.CANCELED ||
        code === FrameErrorCode.INVALID;
}

/** Creates a connection-level protocol error for an ERROR stream mismatch. */
function invalidErrorStreamId(frame: ErrorFrame): RSocketProtocolError {
    return new RSocketProtocolError("Responder sent ERROR with an invalid stream ID for its code", {
        code: FrameErrorCode.CONNECTION_ERROR,
        streamId: frame.header.streamId,
        frame
    });
}

/**
 * Detects responder traffic that confirms the initial SETUP was accepted.
 */
function confirmsSetup(frame: Frame, activeStream: boolean): boolean {
    const type = frame.type;
    if (type === FrameType.LEASE) return true;
    if (type === FrameType.REQUEST_RESPONSE ||
        type === FrameType.REQUEST_FNF ||
        type === FrameType.REQUEST_STREAM ||
        type === FrameType.REQUEST_CHANNEL) {
        return true;
    }
    return activeStream &&
        (type === FrameType.PAYLOAD || type === FrameType.ERROR || type === FrameType.REQUEST_N);
}

/**
 * Returns whether a frame contributes bytes to protocol Resume positions.
 */
function isResumePositionFrame(type: FrameType): boolean {
    return type >= FrameType.REQUEST_RESPONSE && type <= FrameType.ERROR;
}

/**
 * Reads a positive request timeout from per-request options.
 */
function requestTimeoutMs(options: RSocketRequestOptions): number | undefined {
    return options.timeout !== undefined && Number.isFinite(options.timeout) && options.timeout > 0
        ? options.timeout
        : undefined;
}

/**
 * Narrows client options to the internal prepared shape used by the facade.
 */
function preparedOptions<D, M>(options: RSocketClientOptions<D, M>): PreparedClientOptions<D, M> {
    return options as PreparedClientOptions<D, M>;
}
