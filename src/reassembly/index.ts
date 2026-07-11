/**
 * Incoming RSocket PAYLOAD fragment reassembly helpers.
 *
 * The low-level client decodes fragmented payload pieces as raw bytes and this
 * module joins them into a single logical PAYLOAD frame before stream handlers
 * see the message.
 */
import {
  FrameFlag,
  Metadata,
  Payload,
  PayloadFlag,
  PayloadFrame,
  type MimeType
} from "rsocket-frames-ts";
import { KEEPALIVE_DATA_MIME_TYPE } from "@/protocol/index.js";

/** Shared empty byte buffer used when a fragmented sequence has no data bytes. */
const EMPTY_BYTES = new Uint8Array(0);

/**
 * Buffered fragments for one incoming fragmented PAYLOAD sequence.
 */
interface PayloadFragmentState {
  /** Metadata byte chunks received so far. */
  metadataChunks?: Uint8Array[];
  /** Data byte chunks received so far. */
  dataChunks?: Uint8Array[];
  /** Whether any fragment carried metadata, including zero-length metadata. */
  hasMetadata: boolean;
  /** Total metadata bytes already buffered. */
  metadataLength: number;
  /** Total data bytes already buffered. */
  dataLength: number;
  /** Combined payload flags with FOLLOWS and METADATA removed. */
  flags: number;
}

/**
 * Fragment state keyed by stream id.
 */
export type PayloadFragmentMap = Map<number, PayloadFragmentState>;

/**
 * Reassembles a PAYLOAD fragment sequence into one logical frame.
 */
export function reassemblePayloadFrame(
  frame: PayloadFrame,
  fragments: PayloadFragmentMap,
  metadataMimeType: MimeType<any>,
  dataMimeType: MimeType<any>
): PayloadFrame | undefined {
  const streamId = frame.header.streamId;
  // COMPLETE wins when a peer sends the otherwise contradictory F|C pair.
  const follows = frame.hasFollows() && !frame.isComplete();
  let state = fragments.get(streamId);
  if (!follows && state === undefined && !frame.hasFollows()) return frame;

  state ??= newPayloadFragmentState();
  state.flags |= frame.header.flags & ~PayloadFlag.FOLLOWS & ~FrameFlag.METADATA;
  if (!state.hasMetadata && frame.hasMetadata()) state.hasMetadata = true;
  appendMetadata(state, framePartBytes(frame.metadata));
  appendData(state, framePartBytes(frame.payload));

  if (follows) {
    fragments.set(streamId, state);
    return undefined;
  }

  fragments.delete(streamId);
  const metadataBytes = concatBytes(state.metadataChunks, state.metadataLength);
  const dataBytes = concatBytes(state.dataChunks, state.dataLength);
  const metadata = state.hasMetadata
    ? metadataMimeType.toMetadata(metadataBytes, false)
    : undefined;
  const payload = state.dataLength > 0 || (state.flags & PayloadFlag.NEXT) === PayloadFlag.NEXT
    ? dataMimeType.toPayload(dataBytes)
    : undefined;

  return new PayloadFrame(streamId, state.flags, metadata, payload as Payload<any> | undefined);
}

/**
 * Creates an empty fragment state for one stream.
 */
function newPayloadFragmentState(): PayloadFragmentState {
  return {
    hasMetadata: false,
    metadataLength: 0,
    dataLength: 0,
    flags: FrameFlag.NONE
  };
}

/**
 * Appends metadata bytes to the selected fragment buffer.
 */
function appendMetadata(state: PayloadFragmentState, bytes: Uint8Array | undefined): void {
  if (bytes === undefined || bytes.byteLength === 0) return;
  (state.metadataChunks ??= []).push(bytes);
  state.metadataLength += bytes.byteLength;
}

/**
 * Appends data bytes to the selected fragment buffer.
 */
function appendData(state: PayloadFragmentState, bytes: Uint8Array | undefined): void {
  if (bytes === undefined || bytes.byteLength === 0) return;
  (state.dataChunks ??= []).push(bytes);
  state.dataLength += bytes.byteLength;
}

/**
 * Extracts raw bytes from payload or metadata parts during reassembly.
 */
function framePartBytes(part: unknown): Uint8Array | undefined {
  if (part === undefined || part === null) return undefined;
  if (part instanceof Uint8Array) return part;
  if (part instanceof Payload || part instanceof Metadata) {
    return part.mimeType === KEEPALIVE_DATA_MIME_TYPE && part.payload instanceof Uint8Array
      ? part.payload
      : part.toUint8Array();
  }
  if (typeof part === "object") {
    const toUint8Array = (part as { readonly toUint8Array?: unknown }).toUint8Array;
    if (typeof toUint8Array !== "function") return undefined;
    const bytes = toUint8Array.call(part);
    if (bytes instanceof Uint8Array) return bytes;
  }
  return undefined;
}

/**
 * Concatenates byte chunks using the already tracked total length.
 */
function concatBytes(chunks: readonly Uint8Array[] | undefined, length: number): Uint8Array {
  if (length === 0) return EMPTY_BYTES;
  const available = chunks as readonly Uint8Array[];
  if (available.length === 1) return available[0] as Uint8Array;

  const result = new Uint8Array(length);
  let offset = 0;
  for (let index = 0; index < available.length; index++) {
    const chunk = available[index] as Uint8Array;
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}
