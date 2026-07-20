/**
 * Real WebSocket test adapter that records raw RSocket messages in both directions.
 */
import {FrameType} from "rsocket-frames-ts";
import type {RSocketWebSocket, RSocketWebSocketData} from "@/types/index.js";

/**
 * Header fields and raw bytes captured for one RSocket WebSocket message.
 */
export interface ObservedRSocketFrame {
  /** Complete RSocket frame bytes, without a transport length prefix. */
  readonly bytes: Uint8Array;
  /** Decoded 31-bit RSocket stream identifier. */
  readonly streamId: number;
  /** Decoded six-bit frame type. */
  readonly type: FrameType;
  /** Decoded ten-bit frame flags. */
  readonly flags: number;
}

/**
 * Delegates to a native WebSocket while retaining immutable copies of wire messages.
 */
export class ObservedWebSocket implements RSocketWebSocket {
  /** Raw messages sent by the requester. */
  readonly sent: Uint8Array[] = [];
  /** Raw messages received from the responder. */
  readonly received: Uint8Array[] = [];
  private readonly socket: WebSocket;

  /**
   * Opens a native WebSocket and installs capture before protocol listeners run.
   */
  constructor(url: string | URL, protocols?: string | string[]) {
    this.socket = protocols === undefined ? new WebSocket(url) : new WebSocket(url, protocols);
    this.socket.binaryType = "arraybuffer";
    this.socket.addEventListener("message", (event) => {
      const bytes = webSocketDataBytes(event.data);
      if (bytes !== undefined) this.received.push(bytes);
    });
  }

  /** Binary message representation used by the protocol transport. */
  get binaryType(): BinaryType {
    return this.socket.binaryType;
  }

  set binaryType(value: BinaryType) {
    this.socket.binaryType = value;
  }

  /** Number of bytes queued by the native WebSocket implementation. */
  get bufferedAmount(): number {
    return this.socket.bufferedAmount;
  }

  /** Current native WebSocket ready state. */
  get readyState(): number {
    return this.socket.readyState;
  }

  /**
   * Records and forwards one outbound WebSocket message.
   */
  send(data: RSocketWebSocketData): void {
    const bytes = webSocketDataBytes(data);
    if (bytes !== undefined) this.sent.push(bytes);
    this.socket.send(data as any);
  }

  /**
   * Closes the native WebSocket.
   */
  close(code?: number, reason?: string): void {
    this.socket.close(code, reason);
  }

  /**
   * Registers a native WebSocket event listener.
   */
  addEventListener(
    type: "open" | "message" | "error" | "close",
    listener: (event: any) => void
  ): void {
    this.socket.addEventListener(type, listener as EventListener);
  }

  /**
   * Removes a native WebSocket event listener.
   */
  removeEventListener(
    type: "open" | "message" | "error" | "close",
    listener: (event: any) => void
  ): void {
    this.socket.removeEventListener(type, listener as EventListener);
  }
}

/**
 * Decodes fixed RSocket header fields from captured WebSocket messages.
 */
export function observedRSocketFrames(messages: readonly Uint8Array[]): ObservedRSocketFrame[] {
  return messages.map((bytes) => {
    if (bytes.byteLength < 6) throw new Error(`RSocket frame is shorter than its 6-byte header: ${bytes.byteLength}`);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const typeAndFlags = view.getUint16(4);
    return {
      bytes,
      streamId: view.getUint32(0) & 0x7fffffff,
      type: typeAndFlags >>> 10 as FrameType,
      flags: typeAndFlags & 0x03ff
    };
  });
}

/**
 * Copies a synchronous WebSocket payload into stable bytes for later assertions.
 */
function webSocketDataBytes(data: unknown): Uint8Array | undefined {
  if (typeof data === "string") return new TextEncoder().encode(data);
  if (data instanceof ArrayBuffer) return new Uint8Array(data.slice(0));
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength));
  }
  return undefined;
}
