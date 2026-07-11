/**
 * Public constructor option shapes and normalization helpers for the RSocket facade.
 */
import {
  Metadata,
  MimeType
} from "rsocket-frames-ts";
import type { RSocketConnectionEventHandlers, RSocketReconnectOptionInput } from "@/reconnect/index.js";
import type {
  RSocketClientOptions,
  RSocketPayloadInput,
  RSocketRequestOptions,
  RSocketWebSocket,
  RSocketWebSocketFactory
} from "@/types/index.js";
import type { RSocketLogInput } from "@/logging/index.js";

/**
 * Input accepted anywhere the facade accepts a MIME type.
 */
export type MimeTypeInput<T = any> = MimeType<T> | string;

/**
 * Public custom transport factory shape used by `setup.transport`.
 */
export type RSocketTransportFactory = (url: string | URL) => RSocketWebSocket;

/**
 * User-facing constructor options for `new RSocket(url, options)`.
 */
export interface RSocketOptions<D = unknown, M = unknown> extends RSocketReconnectOptionInput {
  /** Optional constructor-time logging configuration. */
  readonly log?: RSocketLogInput;
  /** Optional constructor-time lifecycle event handlers for UI state. */
  readonly events?: RSocketConnectionEventHandlers;
  /** SETUP frame parameters sent after the WebSocket opens. */
  readonly setup?: RSocketSetupOptions<D, M>;
}

/**
 * Nested SETUP options used by the public socket constructor.
 */
export interface RSocketSetupOptions<D = unknown, M = unknown> {
  /** Interval between requester KEEPALIVE frames, in milliseconds. */
  readonly keepAlive?: number;
  /** Maximum silence interval before the connection is considered dead. */
  readonly lifetime?: number;
  /** MIME codecs used for request data and metadata. */
  readonly mimetype?: RSocketMimeTypes<D, M>;
  /** Optional payload sent in the SETUP frame. */
  readonly payload?: RSocketPayloadInput<D, M>;
  /** Optional WebSocket-like transport factory. Defaults to browser `WebSocket`. */
  readonly transport?: RSocketTransportFactory;
}

/**
 * Data and metadata MIME codecs used by SETUP or one positional interaction.
 */
export interface RSocketMimeTypes<D = unknown, M = unknown> {
  /** Metadata MIME codec or MIME string. */
  readonly metadata?: MimeTypeInput<M>;
  /** Data MIME codec or MIME string. */
  readonly data?: MimeTypeInput<D>;
}

/**
 * Object constructor form accepted for callers that prefer one options object.
 */
export interface RSocketConstructorOptions<D = unknown, M = unknown> extends RSocketOptions<D, M> {
  /** WebSocket endpoint URL for the RSocket requester connection. */
  readonly url: string | URL;
}

/**
 * URL and options pair produced by constructor overload normalization.
 */
export interface ResolvedRSocketConstructorOptions<D = unknown, M = unknown> {
  /** WebSocket endpoint URL for the requester connection. */
  readonly url: string | URL;
  /** Constructor options without the URL field. */
  readonly options: RSocketOptions<D, M>;
}

/**
 * Shared empty request options object used to avoid per-call allocation.
 */
export const EMPTY_REQUEST_OPTIONS: RSocketRequestOptions = Object.freeze({});

/**
 * Legacy flat constructor options still accepted at runtime as a migration aid.
 */
interface LegacyRSocketOptions<D = unknown, M = unknown> {
  readonly protocols?: string | string[];
  readonly webSocketFactory?: RSocketWebSocketFactory;
  readonly timeout?: number;
  readonly connectTimeout?: number;
  readonly connectTimeoutMs?: number;
  readonly maxFrameLength?: number;
  readonly keepAlive?: number;
  readonly keepAliveMs?: number;
  readonly lifetime?: number;
  readonly lifetimeMs?: number;
  readonly majorVersion?: number;
  readonly minorVersion?: number;
  readonly lease?: boolean;
  readonly honorLease?: boolean;
  readonly dataMimeType?: MimeTypeInput<D>;
  readonly metadataMimeType?: MimeTypeInput<M>;
  readonly setupPayload?: RSocketPayloadInput<D, M>;
  readonly payload?: RSocketPayloadInput<D, M>;
  readonly setupMetadata?: M | Metadata<M>;
  readonly metadata?: M | Metadata<M>;
}

/**
 * Older nested SETUP aliases still read at runtime without polluting constructor autocomplete.
 */
interface LegacyRSocketSetupOptions<D = unknown, M = unknown> {
  readonly mimeType?: RSocketMimeTypes<D, M>;
  readonly metadata?: M | Metadata<M>;
  readonly majorVersion?: number;
  readonly minorVersion?: number;
  readonly lease?: boolean;
}

/**
 * Converts public facade options into low-level client options.
 */
export function toClientOptions<D, M>(
  url: string | URL,
  options: RSocketOptions<D, M>,
  resumeToken?: string
): RSocketClientOptions<D, M> {
  const legacy = options as RSocketOptions<D, M> & LegacyRSocketOptions<D, M>;
  const setupInput = options.setup as (RSocketSetupOptions<D, M> & LegacyRSocketSetupOptions<D, M>) | undefined;
  const mimetype = setupInput?.mimetype ?? setupInput?.mimeType;
  const setup: NonNullable<RSocketClientOptions<D, M>["setup"]> = {};
  const clientOptions: RSocketClientOptions<D, M> = {
    url,
    setup
  };

  assignOptional(clientOptions, "protocols", legacy.protocols);
  assignOptional(clientOptions, "webSocketFactory", setupInput?.transport ?? legacy.webSocketFactory);
  assignOptional(clientOptions, "connectTimeoutMs", legacy.connectTimeoutMs ?? legacy.connectTimeout ?? legacy.timeout);
  assignOptional(clientOptions, "maxFrameLength", legacy.maxFrameLength);

  assignOptional(setup, "keepAliveMs", setupInput?.keepAlive ?? legacy.keepAliveMs ?? legacy.keepAlive);
  assignOptional(setup, "lifetimeMs", setupInput?.lifetime ?? legacy.lifetimeMs ?? legacy.lifetime);
  assignOptional(setup, "majorVersion", setupInput?.majorVersion ?? legacy.majorVersion);
  assignOptional(setup, "minorVersion", setupInput?.minorVersion ?? legacy.minorVersion);
  assignOptional(setup, "resumeToken", resumeToken);
  assignOptional(setup, "honorLease", setupInput?.lease ?? legacy.honorLease ?? legacy.lease);
  assignOptional(setup, "dataMimeType", resolveMimeType(mimetype?.data ?? legacy.dataMimeType));
  assignOptional(setup, "metadataMimeType", resolveMimeType(mimetype?.metadata ?? legacy.metadataMimeType));
  assignOptional(setup, "payload", setupInput?.payload ?? legacy.setupPayload ?? legacy.payload);
  assignOptional(setup, "metadata", setupInput?.metadata ?? legacy.setupMetadata ?? legacy.metadata);

  return clientOptions;
}

/**
 * Resolves MIME string aliases in per-request options.
 */
export function normalizeRequestOptions(options: RSocketRequestOptions): RSocketRequestOptions {
  if (options.dataMimeType === undefined && options.metadataMimeType === undefined) return options;

  const dataMimeType = resolveMimeType(options.dataMimeType);
  const metadataMimeType = resolveMimeType(options.metadataMimeType);
  if (dataMimeType === options.dataMimeType && metadataMimeType === options.metadataMimeType) return options;

  const normalized: RSocketRequestOptions = {};
  assignOptional(normalized, "dataMimeType", dataMimeType);
  assignOptional(normalized, "metadataMimeType", metadataMimeType);
  assignOptional(normalized, "timeout", options.timeout);
  return normalized;
}

/**
 * Converts the positional interaction MIME argument into low-level request options.
 */
export function requestOptionsFromMimeTypes<D, M>(
  mimetype: RSocketMimeTypes<D, M> | undefined
): RSocketRequestOptions {
  if (mimetype === undefined) return EMPTY_REQUEST_OPTIONS;
  const options: RSocketRequestOptions = {};
  assignOptional(options, "dataMimeType", resolveMimeType(mimetype.data));
  assignOptional(options, "metadataMimeType", resolveMimeType(mimetype.metadata));
  return options;
}

/**
 * Normalizes both supported constructor forms into a URL plus options object.
 */
export function resolveConstructorOptions<D, M>(
  urlOrOptions: string | URL | RSocketConstructorOptions<D, M>,
  options: RSocketOptions<D, M>
): ResolvedRSocketConstructorOptions<D, M> {
  if (typeof urlOrOptions === "string" || urlOrOptions instanceof URL) {
    return { url: urlOrOptions, options };
  }
  const { url, ...rest } = urlOrOptions;
  return { url, options: rest };
}

/**
 * Converts a MIME string into a `MimeType` instance when needed.
 */
function resolveMimeType<T>(mimeType: MimeTypeInput<T> | undefined): MimeType<T> | undefined {
  if (mimeType === undefined) return undefined;
  if (typeof mimeType === "string") return MimeType.valueOf<T>(mimeType);
  return mimeType;
}

/**
 * Assigns optional properties without emitting explicit `undefined` fields.
 */
function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined): void {
  if (value !== undefined) target[key] = value;
}
