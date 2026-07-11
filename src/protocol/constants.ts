/**
 * RSocket protocol defaults and well-known mime types used by the requester.
 */
import { WellKnownMimeType, type MimeType } from "rsocket-frames-ts";

/** Largest legal Reactive Streams request count representable by REQUEST_N. */
export const MAX_REQUEST_N = 0x7fffffff;
/** Default requester keepalive interval sent in SETUP, in milliseconds. */
export const DEFAULT_KEEP_ALIVE_MS = 20_000;
/** Default maximum peer silence before the requester considers the connection dead. */
export const DEFAULT_LIFETIME_MS = 90_000;
/** Default maximum RSocket frame length accepted by this client. */
export const DEFAULT_MAX_FRAME_LENGTH = 0xffffff;

/** Default data mime type for user payloads. */
export const DEFAULT_DATA_MIME_TYPE: MimeType<any> = WellKnownMimeType.APPLICATION_JSON;
/** Default metadata mime type for routes and composite metadata. */
export const DEFAULT_METADATA_MIME_TYPE: MimeType<any> = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
/** Mime type used for ERROR frame payloads. */
export const ERROR_DATA_MIME_TYPE: MimeType<any> = WellKnownMimeType.TEXT_PLAIN;
/** Mime type used for KEEPALIVE frame payload bytes. */
export const KEEPALIVE_DATA_MIME_TYPE: MimeType<any> = WellKnownMimeType.APPLICATION_OCTET_STREAM;
