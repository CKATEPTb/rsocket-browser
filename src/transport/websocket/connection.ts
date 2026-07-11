/**
 * Reactor-oriented WebSocket connection wrapper.
 */
import {Flux, Mono} from "reactor-core-ts";
import {RSocketConnectionError} from "@/errors/index.js";
import type {RSocketWebSocket, RSocketWebSocketFactory} from "@/types/index.js";
import {defaultWebSocketFactory} from "@/transport/websocket/factory.js";
import {openWebSocket, webSocketEventFlux, webSocketMessageBytes} from "@/transport/websocket/events.js";
import {WS_CLOSED, WS_CLOSING, WS_OPEN} from "@/transport/websocket/constants.js";
import {
    type NormalizedWebSocketEndpoint,
    normalizeWebSocketEndpoint,
    validateWebSocketClose
} from "@/transport/websocket/spec.js";

/**
 * Wraps browser WebSocket operations into Reactor primitives.
 */
export class ReactiveWebSocketConnection {
    /** Mono that completes when the WebSocket open handshake succeeds. */
    readonly opened: Mono<void>;
    /** Flux of ordered binary WebSocket messages. */
    readonly messages: Flux<Uint8Array>;
    /** Flux of WebSocket error events. */
    readonly errors: Flux<Event>;
    /** Flux of WebSocket close events. */
    readonly closes: Flux<CloseEvent>;

    /**
     * Configures the socket and exposes open/message/error/close streams.
     */
    constructor(
        /** Underlying native or custom WebSocket implementation. */
        readonly socket: RSocketWebSocket,
        timeoutMs: number | undefined,
        abortSignal?: AbortSignal
    ) {
        this.socket.binaryType = "arraybuffer";
        this.opened = openWebSocket(socket, timeoutMs, abortSignal);
        this.messages = webSocketMessageBytes(socket);
        this.errors = webSocketEventFlux<Event>(socket, "error");
        this.closes = webSocketEventFlux<CloseEvent>(socket, "close");
    }

    /**
     * Current WebSocket ready state.
     */
    get readyState(): number {
        return this.socket.readyState;
    }

    /**
     * Sends bytes as a `Mono`, reporting send failures to the subscriber.
     */
    send(bytes: Uint8Array): Mono<void> {
        return Mono.create<void>((sink) => {
            try {
                this.sendNow(bytes);
                sink.success();
            } catch (error) {
                sink.error(error);
            }
        });
    }

    /**
     * Sends bytes immediately and throws if the socket is not writable.
     */
    sendNow(bytes: Uint8Array): void {
        if (this.socket.readyState !== WS_OPEN) {
            throw new RSocketConnectionError("WebSocket is not open");
        }

        try {
            this.socket.send(bytes);
        } catch (error) {
            throw new RSocketConnectionError("WebSocket send failed", error);
        }
    }

    /**
     * Closes the underlying WebSocket if it is not already closing or closed.
     */
    close(code?: number, reason?: string): void {
        validateWebSocketClose(code, reason);
        if (this.socket.readyState !== WS_CLOSING && this.socket.readyState !== WS_CLOSED) {
            try {
                this.socket.close(code, reason);
            } catch (error) {
                if (this.socket.readyState === WS_CLOSING || this.socket.readyState === WS_CLOSED) return;
                throw new RSocketConnectionError("WebSocket close failed", error);
            }
        }
    }
}

/**
 * Creates a `ReactiveWebSocketConnection` from a custom or native factory.
 */
export function createReactiveWebSocketConnection(
    factory: RSocketWebSocketFactory | undefined,
    url: string | URL,
    protocols: string | string[] | undefined,
    timeoutMs: number | undefined,
    abortSignal?: AbortSignal,
    webSocketEndpoint?: NormalizedWebSocketEndpoint
): ReactiveWebSocketConnection {
    const endpoint = webSocketEndpoint ?? normalizeWebSocketEndpoint(url, protocols);
    return new ReactiveWebSocketConnection(
        (factory ?? defaultWebSocketFactory)(endpoint.url, endpoint.protocols),
        timeoutMs,
        abortSignal
    );
}
