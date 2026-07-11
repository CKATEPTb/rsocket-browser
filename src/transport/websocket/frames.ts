/**
 * WebSocket message and RSocket frame conversion helpers.
 */
import {
  FrameDeserializer,
  FrameType,
  type Frame,
  type MimeType
} from "rsocket-frames-ts";
import {
  ERROR_DATA_MIME_TYPE,
  KEEPALIVE_DATA_MIME_TYPE
} from "@/protocol/index.js";
import { RSocketProtocolError } from "@/errors/index.js";

/** Shared empty MIME override bag for the inbound frame hot path. */
const NO_MIME_OVERRIDES: Readonly<{ metadataMimeType?: MimeType<any>; dataMimeType?: MimeType<any> }> = Object.freeze({});

/**
 * Deserializes a raw RSocket frame from a binary WebSocket message.
 */
export function deserializeFrame(
  buffer: Uint8Array,
  metadataMimeType: MimeType<any>,
  dataMimeType: MimeType<any>,
  overrides: { metadataMimeType?: MimeType<any>; dataMimeType?: MimeType<any> } = NO_MIME_OVERRIDES,
  frameType: FrameType = readFrameTypeAndFlags(buffer) >>> 10
): Frame {
  const effectiveDataMimeType = overrides.dataMimeType ?? dataMimeType;
  const payloadMimeType =
    frameType === FrameType.KEEPALIVE
      ? KEEPALIVE_DATA_MIME_TYPE
      : frameType === FrameType.ERROR
        ? ERROR_DATA_MIME_TYPE
        : effectiveDataMimeType;

  return FrameDeserializer.deserialize(buffer, overrides.metadataMimeType ?? metadataMimeType, payloadMimeType);
}

/**
 * Reads the 31-bit stream ID directly from the fixed RSocket frame header.
 */
export function readFrameStreamId(buffer: Uint8Array): number {
  return (((buffer[0] as number) & 0x7f) << 24) |
    ((buffer[1] as number) << 16) |
    ((buffer[2] as number) << 8) |
    (buffer[3] as number);
}

/**
 * Reads the combined 6-bit frame type and 10-bit flags word without allocating a reader.
 */
export function readFrameTypeAndFlags(buffer: Uint8Array): number {
  return ((buffer[4] as number) << 8) | (buffer[5] as number);
}

/**
 * Converts browser WebSocket message data into bytes.
 */
export function messageDataToUint8Array(data: unknown): Uint8Array | Promise<Uint8Array> {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  if (typeof Blob !== "undefined" && data instanceof Blob) {
    return data.arrayBuffer().then((buffer) => new Uint8Array(buffer));
  }

  throw new RSocketProtocolError("RSocket WebSocket transport expects binary messages");
}
