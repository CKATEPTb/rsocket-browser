/**
 * Browser WebSocket factory.
 */
import {RSocketConnectionError} from "@/errors/index.js";
import type {RSocketWebSocket} from "@/types/index.js";

/**
 * Creates a native browser `WebSocket`.
 */
export function defaultWebSocketFactory(url: string | URL, protocols?: string | string[]): RSocketWebSocket {
    if (typeof WebSocket === "undefined") {
        throw new RSocketConnectionError("WebSocket is not available in this runtime");
    }
    return new WebSocket(url, protocols) as RSocketWebSocket;
}
