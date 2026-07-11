/**
 * In-memory WebSocket implementation used by protocol and reconnect tests.
 */
import type { Frame, MimeType } from "rsocket-frames-ts";
import { FrameDeserializer, FrameType, Header, WellKnownMimeType } from "rsocket-frames-ts";
import bebyte from "bebyte";
import type { RSocketWebSocket, RSocketWebSocketData } from "@/types/index.js";
import { WS_CLOSED, WS_CONNECTING, WS_OPEN } from "@/transport/websocket/index.js";

/**
 * Generic DOM-style event listener used by the fake socket.
 */
type Listener = (event: any) => void;

/**
 * Minimal controllable WebSocket test double.
 *
 * The fake records binary sends, exposes helper methods for server-side events,
 * and keeps enough DOM WebSocket behavior to exercise the production transport.
 */
export class FakeWebSocket implements RSocketWebSocket {
  /** Binary mode requested by the production connection wrapper. */
  binaryType: BinaryType = "blob";
  /** Exposed for WebSocket interface compatibility. */
  bufferedAmount = 0;
  /** Current fake WebSocket ready state. */
  readyState = WS_CONNECTING;
  /** Raw frames sent by the RSocket requester. */
  readonly sent: Uint8Array[] = [];
  /** Optional hook invoked after every successful send. */
  onSend?: (bytes: Uint8Array) => void;
  private readonly listeners = new Map<string, Set<Listener>>();

  /**
   * Records a sent message and notifies the optional send hook.
   */
  send(data: RSocketWebSocketData): void {
    if (this.readyState !== WS_OPEN) throw new Error("FakeWebSocket is not open");
    const bytes = toBytes(data);
    this.sent.push(bytes);
    this.onSend?.(bytes);
  }

  /**
   * Closes the fake socket and dispatches a close event.
   */
  close(code?: number, reason?: string): void {
    if (this.readyState === WS_CLOSED) return;
    this.readyState = WS_CLOSED;
    this.dispatch("close", { code, reason });
  }

  /**
   * Registers an event listener for a WebSocket event type.
   */
  addEventListener(type: "open" | "message" | "error" | "close", listener: Listener): void {
    let listeners = this.listeners.get(type);
    if (!listeners) {
      listeners = new Set();
      this.listeners.set(type, listeners);
    }
    listeners.add(listener);
  }

  /**
   * Removes a previously registered event listener.
   */
  removeEventListener(type: "open" | "message" | "error" | "close", listener: Listener): void {
    this.listeners.get(type)?.delete(listener);
  }

  /**
   * Returns the current listener count for transport cleanup assertions.
   */
  listenerCount(type: "open" | "message" | "error" | "close"): number {
    return this.listeners.get(type)?.size ?? 0;
  }

  /**
   * Moves the fake socket to OPEN and dispatches the open event.
   */
  open(): void {
    this.readyState = WS_OPEN;
    this.dispatch("open", {});
  }

  /**
   * Serializes an RSocket frame and dispatches it as a server message.
   */
  serverSend(frame: Frame): void {
    const bytes = frame.toUint8Array();
    this.dispatchMessage(bytes);
  }

  /**
   * Dispatches raw binary bytes as a WebSocket message.
   */
  dispatchMessage(bytes: Uint8Array): void {
    this.dispatch("message", { data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
  }

  /**
   * Dispatches an arbitrary message payload for transport edge-case tests.
   */
  dispatchRawMessage(data: unknown): void {
    this.dispatch("message", { data });
  }

  /**
   * Decodes one recorded outbound RSocket frame for assertions.
   */
  decodeSent(index: number, metadataMimeType: MimeType<any>, dataMimeType: MimeType<any>): Frame {
    const bytes = this.sent[index];
    if (!bytes) throw new Error(`No sent frame at index ${index}`);
    const header = Header.from(bebyte.reader(bytes));
    const payloadMimeType =
      header.frameType === FrameType.KEEPALIVE
        ? WellKnownMimeType.APPLICATION_OCTET_STREAM
        : header.frameType === FrameType.ERROR
          ? WellKnownMimeType.TEXT_PLAIN
          : dataMimeType;
    return FrameDeserializer.deserialize(bytes, metadataMimeType, payloadMimeType);
  }

  /**
   * Dispatches one fake DOM event to registered listeners.
   */
  private dispatch(type: string, event: any): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

/**
 * Creates a WebSocket factory that always returns the provided fake socket.
 */
export function fakeWebSocketFactory(socket: FakeWebSocket) {
  return () => socket;
}

/**
 * Normalizes WebSocket send payloads into bytes for assertions.
 */
function toBytes(data: RSocketWebSocketData): Uint8Array {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  if (typeof data === "string") return new TextEncoder().encode(data);
  throw new Error("Blob sends are not supported by FakeWebSocket");
}
