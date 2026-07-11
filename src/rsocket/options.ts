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
  readonly mimetype?: RSocketSetupMimeTypes<D, M>;
  /** Optional payload sent in the SETUP frame. */
  readonly payload?: RSocketPayloadInput<D, M>;
  /** Optional WebSocket-like transport factory. Defaults to browser `WebSocket`. */
  readonly transport?: RSocketTransportFactory;
}

/**
 * Data and metadata MIME codecs used by SETUP and default request encoding.
 */
export interface RSocketSetupMimeTypes<D = unknown, M = unknown> {
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
export interface ResolvedRSocketConstructorOptions {
  /** WebSocket endpoint URL for the requester connection. */
  readonly url: string | URL;
  /** Constructor options without the URL field. */
  readonly options: RSocketOptions;
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
  readonly mimeType?: RSocketSetupMimeTypes<D, M>;
  readonly metadata?: M | Metadata<M>;
  readonly majorVersion?: number;
  readonly minorVersion?: number;
  readonly lease?: boolean;
}

/**
 * Converts public facade options into low-level client options.
 */
export function toClientOptions(url: string | URL, options: RSocketOptions, resumeToken?: string): RSocketClientOptions {
  const legacy = options as RSocketOptions & LegacyRSocketOptions;
  const setupInput = options.setup as (RSocketSetupOptions & LegacyRSocketSetupOptions) | undefined;
  const mimetype = setupInput?.mimetype ?? setupInput?.mimeType;
  const setup: NonNullable<RSocketClientOptions["setup"]> = {};
  const clientOptions: RSocketClientOptions = {
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
 * Detects the no-payload request-channel overload.
 */
export function isRequestOptions(value: unknown): value is RSocketRequestOptions {
  if (!isPlainObject(value)) return false;
  if ("data" in value || "metadata" in value) return false;
  const source = value as {
    readonly subscribe?: unknown;
    readonly then?: unknown;
    readonly [Symbol.iterator]?: unknown;
    readonly [Symbol.asyncIterator]?: unknown;
  };
  if (
    typeof source.subscribe === "function" ||
    typeof source.then === "function" ||
    typeof source[Symbol.iterator] === "function" ||
    typeof source[Symbol.asyncIterator] === "function"
  ) {
    return false;
  }
  return !hasOwnEnumerableKey(value) ||
    "dataMimeType" in value ||
    "metadataMimeType" in value ||
    "timeout" in value;
}

/**
 * Normalizes both supported constructor forms into a URL plus options object.
 */
export function resolveConstructorOptions(
  urlOrOptions: string | URL | RSocketConstructorOptions,
  options: RSocketOptions
): ResolvedRSocketConstructorOptions {
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

/** Cached intrinsic used by request-options scans. */
const HAS_OWN_PROPERTY = Object.prototype.hasOwnProperty;

/**
 * Detects plain option bags without confusing publishers for options.
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/**
 * Checks whether an options candidate has own enumerable keys without allocating `Object.keys(...)`.
 */
function hasOwnEnumerableKey(value: Record<string, unknown>): boolean {
  for (const key in value) {
    if (HAS_OWN_PROPERTY.call(value, key)) return true;
  }
  return false;
}

/**
 * Assigns optional properties without emitting explicit `undefined` fields.
 */
function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined): void {
  if (value !== undefined) target[key] = value;
}
