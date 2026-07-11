/**
 * SETUP/RESUME handshake helpers used before the full client dispatcher starts.
 */
import {type Disposable} from "reactor-core-ts";
import {ErrorFrame, type Frame, type MimeType, ResumeOkFrame} from "rsocket-frames-ts";
import {
    connectionClosedError,
    errorFromFrame,
    RSocketConnectionError,
    RSocketFrameSizeError,
    RSocketProtocolError
} from "@/errors/index.js";
import type {RSocketFrameActivityListener} from "@/types/index.js";
import type {ReactiveWebSocketConnection} from "@/transport/websocket/connection.js";
import {deserializeFrame} from "@/transport/websocket/frames.js";

/**
 * Normalized options required by pre-client handshake helpers.
 */
export interface RSocketHandshakeOptions {
    /** Optional WebSocket or RSocket handshake timeout. */
    readonly connectTimeoutMs: number | undefined;
    /** Maximum serialized RSocket frame length accepted or sent. */
    readonly maxFrameLength: number;
    /** Optional diagnostic callback for raw frame send/receive events. */
    readonly activityListener: RSocketFrameActivityListener | undefined;
    /** Optional dynamic switch for activity logging. */
    readonly activityEnabled: (() => boolean) | undefined;
    /** MIME types needed to decode the first responder frame. */
    readonly setup: {
        readonly metadataMimeType: MimeType<any>;
        readonly dataMimeType: MimeType<any>;
    };
}

/**
 * Sends one handshake frame before the normal client dispatcher is attached.
 */
export function sendHandshakeFrame(
    connection: ReactiveWebSocketConnection,
    options: RSocketHandshakeOptions,
    frame: Frame
): void {
    const bytes = frame.toUint8Array();
    if (bytes.length > options.maxFrameLength) {
        throw new RSocketFrameSizeError(bytes.length, options.maxFrameLength);
    }

    connection.sendNow(bytes);
    emitHandshakeActivity(options, "send", frame);
}

/**
 * Waits for and validates the responder's RESUME_OK frame.
 */
export async function receiveResumeOkFrame(
    connection: ReactiveWebSocketConnection,
    options: RSocketHandshakeOptions,
    abortSignal: AbortSignal | undefined
): Promise<ResumeOkFrame> {
    const bytes = await receiveFirstMessage(connection, options.connectTimeoutMs, abortSignal);
    if (bytes.length > options.maxFrameLength) {
        throw new RSocketFrameSizeError(bytes.length, options.maxFrameLength);
    }

    const frame = deserializeFrame(bytes, options.setup.metadataMimeType, options.setup.dataMimeType);
    emitHandshakeActivity(options, "receive", frame);
    if (frame instanceof ResumeOkFrame) return frame;
    if (frame instanceof ErrorFrame) throw errorFromFrame(frame);
    throw new RSocketProtocolError("RSocket Resume expected RESUME_OK from responder");
}

/**
 * Resolves with the next raw WebSocket message or rejects on close/error/abort.
 */
function receiveFirstMessage(
    connection: ReactiveWebSocketConnection,
    timeoutMs: number | undefined,
    abortSignal: AbortSignal | undefined
): Promise<Uint8Array> {
    return new Promise<Uint8Array>((resolve, reject) => {
        let settled = false;
        let timeout: ReturnType<typeof setTimeout> | undefined;
        const disposables: Disposable[] = [];

        const cleanup = (): void => {
            if (timeout !== undefined) clearTimeout(timeout);
            let disposable: Disposable | undefined;
            while ((disposable = disposables.pop()) !== undefined) disposable.dispose();
            abortSignal?.removeEventListener("abort", onAbort);
        };

        const finish = (callback: () => void): void => {
            if (settled) return;
            settled = true;
            cleanup();
            callback();
        };

        const onAbort = (): void => finish(() => reject(new RSocketConnectionError("RSocket resume aborted")));

        const subscribe = (factory: () => Disposable): void => {
            if (settled) return;
            const disposable = factory();
            if (settled) disposable.dispose();
            else disposables.push(disposable);
        };

        subscribe(() => connection.messages.subscribe(
            (bytes) => finish(() => resolve(bytes)),
            (error) => finish(() => reject(error))
        ));
        subscribe(() => connection.errors.subscribe((event) => {
            finish(() => reject(new RSocketConnectionError("WebSocket error during RSocket resume", event)));
        }));
        subscribe(() => connection.closes.subscribe(() => {
            finish(() => reject(connectionClosedError("WebSocket closed during RSocket resume")));
        }));

        if (settled) return;

        if (abortSignal?.aborted) {
            onAbort();
            return;
        }
        abortSignal?.addEventListener("abort", onAbort, {once: true});
        if (settled) return;

        if (timeoutMs !== undefined && timeoutMs > 0) {
            timeout = setTimeout(() => {
                finish(() => reject(new RSocketConnectionError(`RSocket resume timed out after ${timeoutMs}ms`)));
            }, timeoutMs);
        }
    });
}

/**
 * Emits diagnostic activity for frames exchanged before the client is attached.
 */
function emitHandshakeActivity(
    options: RSocketHandshakeOptions,
    direction: Parameters<RSocketFrameActivityListener>[0]["direction"],
    frame: Frame
): void {
    if (options.activityListener === undefined || options.activityEnabled?.() === false) return;
    try {
        options.activityListener({direction, frame});
    } catch {
        // Handshake activity listeners are diagnostic-only.
    }
}
