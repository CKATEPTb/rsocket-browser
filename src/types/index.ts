/**
 * Shared public and internal TypeScript types for the browser RSocket client.
 */
import type {
  Frame,
  Metadata,
  MimeType,
  Payload
} from "rsocket-frames-ts";
import type { Publisher } from "reactor-core-ts";
import type { NormalizedWebSocketEndpoint } from "@/transport/websocket/spec.js";

/**
 * Input accepted by request-channel publishers.
 */
export type PublisherInput<T> = Publisher<T> | Iterable<T> | AsyncIterable<T> | PromiseLike<T>;

/**
 * Binary or textual data accepted by the browser WebSocket `send` API.
 */
export type RSocketWebSocketData = string | ArrayBufferLike | Blob | ArrayBufferView;

/**
 * Minimal WebSocket surface needed by the browser requester and tests.
 */
export interface RSocketWebSocket {
  /** Binary representation requested from the browser WebSocket. */
  binaryType: BinaryType;
  /** Number of buffered bytes reported by the underlying WebSocket. */
  readonly bufferedAmount: number;
  /** Current WebSocket ready state constant. */
  readonly readyState: number;
  /** Sends a raw WebSocket payload. */
  send(data: RSocketWebSocketData): void;
  /** Closes the WebSocket connection. */
  close(code?: number, reason?: string): void;
  /** Registers an open event listener. */
  addEventListener(type: "open", listener: (event: Event) => void): void;
  /** Registers a message event listener. */
  addEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  /** Registers an error event listener. */
  addEventListener(type: "error", listener: (event: Event) => void): void;
  /** Registers a close event listener. */
  addEventListener(type: "close", listener: (event: CloseEvent) => void): void;
  /** Removes an open event listener when the implementation supports removal. */
  removeEventListener?(type: "open", listener: (event: Event) => void): void;
  /** Removes a message event listener when the implementation supports removal. */
  removeEventListener?(type: "message", listener: (event: MessageEvent) => void): void;
  /** Removes an error event listener when the implementation supports removal. */
  removeEventListener?(type: "error", listener: (event: Event) => void): void;
  /** Removes a close event listener when the implementation supports removal. */
  removeEventListener?(type: "close", listener: (event: CloseEvent) => void): void;
}

/**
 * Factory used to create WebSocket instances.
 */
export type RSocketWebSocketFactory = (
  url: string | URL,
  protocols?: string | string[]
) => RSocketWebSocket;

/**
 * Direction of a frame observed by diagnostic logging.
 */
export type RSocketFrameDirection = "send" | "receive";

/**
 * Diagnostic frame event emitted before a frame is written or after it is read.
 */
export interface RSocketFrameActivity {
  /** Whether the frame was sent or received. */
  readonly direction: RSocketFrameDirection;
  /** Raw decoded RSocket frame. */
  readonly frame: Frame;
}

/**
 * Listener invoked for diagnostic frame activity.
 */
export type RSocketFrameActivityListener = (activity: RSocketFrameActivity) => void;

/**
 * Options that become the RSocket SETUP frame.
 */
export interface RSocketSetupOptions<D = unknown, M = unknown> {
  /** Keepalive frame interval in milliseconds. */
  readonly keepAliveMs?: number;
  /** Maximum accepted silence interval in milliseconds. */
  readonly lifetimeMs?: number;
  /** RSocket major protocol version. */
  readonly majorVersion?: number;
  /** RSocket minor protocol version. */
  readonly minorVersion?: number;
  /** Optional resume token sent in SETUP. */
  readonly resumeToken?: string;
  /** Whether requester-side lease limits should be enforced. */
  readonly honorLease?: boolean;
  /** Default MIME type used to encode data payloads. */
  readonly dataMimeType?: MimeType<D>;
  /** Default MIME type used to encode metadata payloads. */
  readonly metadataMimeType?: MimeType<M>;
  /** Optional data/metadata payload included in SETUP. */
  readonly payload?: RSocketPayloadInput<D, M>;
  /** Optional metadata merged into the SETUP payload. */
  readonly metadata?: M | Metadata<M>;
}

/**
 * Low-level client options consumed by `BrowserRSocketClient`.
 */
export interface RSocketClientOptions<D = unknown, M = unknown> {
  /** Browser WebSocket URL. */
  readonly url: string | URL;
  /** Optional WebSocket protocol or ordered protocol list. */
  readonly protocols?: string | string[];
  /** Prevalidated WebSocket endpoint reused by reconnect attempts. */
  readonly webSocketEndpoint?: NormalizedWebSocketEndpoint;
  /** Optional custom WebSocket factory. */
  readonly webSocketFactory?: RSocketWebSocketFactory;
  /** Optional raw frame activity listener. */
  readonly activityListener?: RSocketFrameActivityListener;
  /** Optional dynamic switch for frame activity delivery. */
  readonly activityEnabled?: () => boolean;
  /** Optional signal used to abort connection opening. */
  readonly abortSignal?: AbortSignal;
  /** SETUP frame options. */
  readonly setup?: RSocketSetupOptions<D, M>;
  /** Optional WebSocket open timeout in milliseconds. */
  readonly connectTimeoutMs?: number;
  /** Maximum serialized frame length. */
  readonly maxFrameLength?: number;
}

/**
 * Per-request payload encoding options.
 */
export interface RSocketRequestOptions {
  /** MIME override for this request's data payload. */
  readonly dataMimeType?: MimeType<any>;
  /** MIME override for this request's metadata payload. */
  readonly metadataMimeType?: MimeType<any>;
  /** Optional request-response timeout in milliseconds. */
  readonly timeout?: number;
}

/**
 * Stream request options. Kept as a separate name for API readability.
 */
export type RSocketStreamRequestOptions = RSocketRequestOptions;

/**
 * Internal payload envelope used by codecs, controllers, and channel items.
 */
export interface RSocketPayload<D = unknown, M = unknown> {
  /** Application data value. */
  readonly data?: D;
  /** Application metadata value. */
  readonly metadata?: M;
  /** MIME override for this payload's data value. */
  readonly dataMimeType?: MimeType<D>;
  /** MIME override for this payload's metadata value. */
  readonly metadataMimeType?: MimeType<M>;
}

/**
 * Any payload form accepted by low-level interactions and channel publishers.
 */
export type RSocketPayloadInput<D = unknown, M = unknown> =
  | RSocketPayload<D, M>
  | Payload<D>
  | Metadata<M>
  | D;

/**
 * Decoded frame payload returned by requester interactions.
 */
export interface RSocketPayloadFrame<D = unknown, M = unknown> extends RSocketPayload<D, M> {
  /** Raw decoded RSocket frame that carried the payload. */
  readonly frame: Frame;
  /** Raw data payload object, when the frame carried data. */
  readonly dataPayload?: Payload<D>;
  /** Raw metadata payload object, when the frame carried metadata. */
  readonly metadataPayload?: Metadata<M>;
}

/**
 * Request-channel outbound input source.
 */
export type RSocketChannelInput<D = unknown, M = unknown> = PublisherInput<RSocketPayloadInput<D, M>>;
