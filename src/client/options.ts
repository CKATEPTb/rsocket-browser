/**
 * Low-level client option normalization for browser RSocket sessions.
 */
import type { MimeType } from "rsocket-frames-ts";
import {
  DEFAULT_DATA_MIME_TYPE,
  DEFAULT_KEEP_ALIVE_MS,
  DEFAULT_LIFETIME_MS,
  DEFAULT_MAX_FRAME_LENGTH,
  DEFAULT_METADATA_MIME_TYPE,
  MAX_REQUEST_N
} from "@/protocol/index.js";
import { RSocketConnectionError } from "@/errors/index.js";
import type {
  RSocketClientOptions,
  RSocketFrameActivityListener,
  RSocketPayloadInput,
  RSocketSetupOptions
} from "@/types/index.js";

/**
 * SETUP frame options after defaults and MIME aliases have been resolved.
 */
export interface NormalizedSetup {
  /** Interval between requester keepalive frames in milliseconds. */
  readonly keepAliveMs: number;
  /** Maximum silence interval before the connection is considered dead. */
  readonly lifetimeMs: number;
  /** RSocket major protocol version sent in SETUP. */
  readonly majorVersion: number;
  /** RSocket minor protocol version sent in SETUP. */
  readonly minorVersion: number;
  /** Optional resume token included for protocol compatibility. */
  readonly resumeToken: string | undefined;
  /** Whether requester-side lease accounting must be enforced. */
  readonly honorLease: boolean;
  /** MIME type used to encode request data payloads by default. */
  readonly dataMimeType: MimeType<any>;
  /** MIME type used to encode request metadata payloads by default. */
  readonly metadataMimeType: MimeType<any>;
  /** Optional payload carried by the initial SETUP frame. */
  readonly setupPayload?: RSocketPayloadInput<any, any>;
}

/**
 * Fully normalized client options consumed by `BrowserRSocketClient`.
 */
export interface NormalizedClientOptions {
  /** Optional WebSocket open timeout. */
  readonly connectTimeoutMs: number | undefined;
  /** Maximum serialized RSocket frame length accepted or sent. */
  readonly maxFrameLength: number;
  /** Optional diagnostic callback for raw frame send/receive events. */
  readonly activityListener: RSocketFrameActivityListener | undefined;
  /** Optional dynamic switch for activity logging. */
  readonly activityEnabled: (() => boolean) | undefined;
  /** Normalized SETUP frame options. */
  readonly setup: NormalizedSetup;
}

/** Smallest useful frame length because every RSocket frame starts with a 6-byte header. */
const MIN_FRAME_LENGTH = 6;

/**
 * Applies defaults and normalizes user-facing client options.
 */
export function normalizeClientOptions<D, M>(options: RSocketClientOptions<D, M>): NormalizedClientOptions {
  const setup = options.setup ?? {};
  const keepAliveMs = positiveMilliseconds(setup.keepAliveMs ?? DEFAULT_KEEP_ALIVE_MS, "setup.keepAlive");
  const lifetimeMs = positiveMilliseconds(setup.lifetimeMs ?? DEFAULT_LIFETIME_MS, "setup.lifetime");
  const connectTimeoutMs = options.connectTimeoutMs === undefined
    ? 4_000
    : positiveMilliseconds(options.connectTimeoutMs, "connectTimeout");
  return {
    connectTimeoutMs,
    maxFrameLength: frameLength(options.maxFrameLength ?? DEFAULT_MAX_FRAME_LENGTH),
    activityListener: options.activityListener,
    activityEnabled: options.activityEnabled,
    setup: {
      keepAliveMs,
      lifetimeMs,
      majorVersion: unsignedInteger(setup.majorVersion ?? 1, 0xffff, "setup.majorVersion"),
      minorVersion: unsignedInteger(setup.minorVersion ?? 0, 0xffff, "setup.minorVersion"),
      resumeToken: setup.resumeToken,
      honorLease: setup.honorLease ?? false,
      dataMimeType: setup.dataMimeType ?? DEFAULT_DATA_MIME_TYPE,
      metadataMimeType: setup.metadataMimeType ?? DEFAULT_METADATA_MIME_TYPE,
      setupPayload: mergeSetupPayload(setup)
    }
  };
}

/**
 * Validates SETUP timing fields that the RSocket protocol requires to be positive.
 */
function positiveMilliseconds(value: number, option: string): number {
  const milliseconds = Math.floor(value);
  if (Number.isFinite(value) && milliseconds > 0 && milliseconds <= MAX_REQUEST_N) return milliseconds;
  throw new RSocketConnectionError(`RSocket ${option} must be between 1 and ${MAX_REQUEST_N} milliseconds`);
}

/**
 * Validates configured frame length before the WebSocket is opened.
 */
function frameLength(value: number): number {
  const length = Math.floor(value);
  if (Number.isFinite(value) && length >= MIN_FRAME_LENGTH && length <= DEFAULT_MAX_FRAME_LENGTH) return length;
  throw new RSocketConnectionError(
    `RSocket maxFrameLength must be between ${MIN_FRAME_LENGTH} and ${DEFAULT_MAX_FRAME_LENGTH} bytes`
  );
}

/**
 * Validates an unsigned integer stored in a fixed-width RSocket frame field.
 */
function unsignedInteger(value: number, max: number, option: string): number {
  if (Number.isInteger(value) && value >= 0 && value <= max) return value;
  throw new RSocketConnectionError(`RSocket ${option} must be an integer between 0 and ${max}`);
}

/**
 * Merges legacy setup payload and metadata aliases into one payload input.
 */
function mergeSetupPayload(setup: RSocketSetupOptions<any, any>): RSocketPayloadInput<any, any> | undefined {
  if (setup.metadata === undefined) return setup.payload;
  if (setup.payload === undefined) return { metadata: setup.metadata };
  if (isPayloadEnvelope(setup.payload)) {
    return { ...setup.payload, metadata: setup.metadata };
  }
  return { data: setup.payload, metadata: setup.metadata };
}

/**
 * Detects a setup payload envelope without treating codec payload objects as envelopes.
 */
function isPayloadEnvelope(value: unknown): value is {
  data?: unknown;
  metadata?: unknown;
  dataMimeType?: MimeType<any>;
  metadataMimeType?: MimeType<any>;
} {
  return typeof value === "object" &&
    value !== null &&
    ("data" in value || "metadata" in value || "dataMimeType" in value || "metadataMimeType" in value);
}
