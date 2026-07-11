/**
 * Reactor wrappers around browser WebSocket events.
 */
import {Flux, Mono} from "reactor-core-ts";
import {RSocketConnectionError} from "@/errors/index.js";
import type {RSocketWebSocket} from "@/types/index.js";
import {WS_CLOSED, WS_CLOSING, WS_OPEN} from "@/transport/websocket/constants.js";
import {messageDataToUint8Array} from "@/transport/websocket/frames.js";

type WebSocketEventType = "open" | "message" | "error" | "close";

/** Shared resolved promise used as the initial ordered-message queue tail. */
const RESOLVED_PROMISE = Promise.resolve();

/**
 * Converts a WebSocket event type into a Reactor `Flux`.
 */
export function webSocketEventFlux<T extends Event>(
    socket: RSocketWebSocket,
    type: WebSocketEventType
): Flux<T> {
    return Flux.create<T>((sink) => {
        const listener = (event: T): void => {
            if (!sink.isCancelled()) sink.next(event);
        };
        socket.addEventListener(type as any, listener as any);
        sink.onCancel(() => removeListener(socket, type, listener));
    });
}

/**
 * Emits binary WebSocket messages as ordered byte arrays.
 */
export function webSocketMessageBytes(socket: RSocketWebSocket): Flux<Uint8Array> {
    return Flux.create<Uint8Array>((sink) => {
        let queue = RESOLVED_PROMISE;
        let queuedTasks = 0;
        let terminated = false;

        const cleanup = (): void => {
            removeListener(socket, "message", listener);
        };

        const fail = (error: unknown): void => {
            if (terminated) return;
            terminated = true;
            cleanup();
            if (!sink.isCancelled()) sink.error(error);
        };

        const listener = (event: MessageEvent): void => {
            if (terminated) return;
            let data: Uint8Array | Promise<Uint8Array>;
            try {
                data = messageDataToUint8Array(event.data);
            } catch (error) {
                fail(error);
                return;
            }

            if (data instanceof Uint8Array && queuedTasks === 0) {
                if (!sink.isCancelled()) sink.next(data);
                return;
            }

            queuedTasks += 1;
            queue = queue
                .then(() => {
                    if (terminated || sink.isCancelled()) return;
                    if (data instanceof Uint8Array) {
                        sink.next(data);
                        return;
                    }
                    return data.then((bytes) => {
                        if (!terminated && !sink.isCancelled()) sink.next(bytes);
                    });
                })
                .then(() => {
                    queuedTasks -= 1;
                }, (error) => {
                    queuedTasks -= 1;
                    fail(error);
                });
        };
        socket.addEventListener("message", listener);
        sink.onCancel(() => {
            terminated = true;
            cleanup();
        });
    });
}

/**
 * Returns a `Mono` that completes when the WebSocket opens.
 */
export function openWebSocket(
    socket: RSocketWebSocket,
    timeoutMs: number | undefined,
    abortSignal?: AbortSignal
): Mono<void> {
    return Mono.create<void>((sink) => {
        if (abortSignal?.aborted) {
            sink.error(new RSocketConnectionError("WebSocket connection aborted"));
            return;
        }

        if (socket.readyState === WS_OPEN) {
            sink.success();
            return;
        }
        if (socket.readyState === WS_CLOSING || socket.readyState === WS_CLOSED) {
            sink.error(new RSocketConnectionError("WebSocket is already closed"));
            return;
        }

        let timeout: ReturnType<typeof setTimeout> | undefined;
        let settled = false;

        const cleanup = (): void => {
            if (timeout !== undefined) clearTimeout(timeout);
            timeout = undefined;
            removeListener(socket, "open", onOpen);
            removeListener(socket, "error", onError);
            removeListener(socket, "close", onClose);
            abortSignal?.removeEventListener("abort", onAbort);
        };

        const finish = (callback?: () => void): void => {
            if (settled) return;
            settled = true;
            cleanup();
            callback?.();
        };

        const onOpen = (): void => {
            finish(() => sink.success());
        };

        const onError = (event: Event): void => {
            finish(() => sink.error(new RSocketConnectionError("WebSocket connection failed", event)));
        };

        const onClose = (): void => {
            finish(() => sink.error(new RSocketConnectionError("WebSocket closed before it opened")));
        };

        const onAbort = (): void => {
            finish(() => sink.error(new RSocketConnectionError("WebSocket connection aborted")));
        };

        const listen = (type: "open" | "error" | "close", listener: (event: any) => void): void => {
            if (settled) return;
            socket.addEventListener(type as any, listener as any);
            if (settled) removeListener(socket, type, listener);
        };

        listen("open", onOpen);
        listen("error", onError);
        listen("close", onClose);
        if (!settled) abortSignal?.addEventListener("abort", onAbort, {once: true});
        sink.onCancel(() => finish());

        if (settled) return;
        if (abortSignal?.aborted) onAbort();
        else if (socket.readyState === WS_OPEN) onOpen();
        else if (socket.readyState === WS_CLOSING || socket.readyState === WS_CLOSED) onClose();
        if (settled) return;

        if (timeoutMs !== undefined && timeoutMs > 0) {
            timeout = setTimeout(() => {
                finish(() => sink.error(new RSocketConnectionError(`WebSocket connection timed out after ${timeoutMs}ms`)));
            }, timeoutMs);
        }
    });
}

/**
 * Removes an event listener only when the WebSocket implementation supports it.
 */
function removeListener(
    socket: RSocketWebSocket,
    type: WebSocketEventType,
    listener: (event: any) => void
): void {
    const remove = socket.removeEventListener as ((eventType: string, eventListener: (event: any) => void) => void) | undefined;
    try {
        remove?.call(socket, type, listener);
    } catch {
        // Listener cleanup cannot change an already delivered transport signal.
    }
}
