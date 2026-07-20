/**
 * Protocol-level tests for outbound RSocket frame fragmentation.
 */
import {describe, expect, it} from "vitest";
import {
  type Frame,
  FrameFlag,
  Metadata,
  PayloadFlag,
  PayloadFrame,
  RequestChannelFlag,
  RequestChannelFrame,
  RequestFireAndForgetFrame,
  RequestResponseFrame,
  RequestStreamFrame,
  WellKnownMimeType
} from "rsocket-frames-ts";
import {emitOutboundFrameFragments, outboundFrameLength} from "@/fragmentation/index.js";
import {reassemblePayloadFrame} from "@/reassembly/index.js";

const MAX_FRAME_LENGTH = 64;
const STREAM_ID = 17;
const metadataBytes = patternedBytes(137, 29);
const dataBytes = patternedBytes(211, 83);
const metadataMimeType = WellKnownMimeType.APPLICATION_OCTET_STREAM;
const dataMimeType = WellKnownMimeType.APPLICATION_OCTET_STREAM;

/**
 * One fragmentable frame shape and its frame-specific assertions.
 */
interface FragmentationCase {
  /** Human-readable case name. */
  readonly name: string;
  /** Original oversized frame. */
  readonly frame: Frame;
  /** Constructor expected for the first emitted fragment. */
  readonly firstType: abstract new (...args: any[]) => Frame;
  /** Optional assertion for fields unique to the initial frame type. */
  readonly assertInitial?: (frame: Frame) => void;
  /** Whether COMPLETE must move to the final fragment. */
  readonly completeOnFinal?: boolean;
}

const fragmentationCases: readonly FragmentationCase[] = [
  {
    name: "REQUEST_FNF",
    frame: new RequestFireAndForgetFrame(
      STREAM_ID,
      FrameFlag.NONE,
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: RequestFireAndForgetFrame
  },
  {
    name: "REQUEST_RESPONSE",
    frame: new RequestResponseFrame(
      STREAM_ID,
      FrameFlag.NONE,
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: RequestResponseFrame
  },
  {
    name: "REQUEST_STREAM",
    frame: new RequestStreamFrame(
      STREAM_ID,
      FrameFlag.NONE,
      23,
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: RequestStreamFrame,
    assertInitial: (frame) => expect((frame as RequestStreamFrame).request).toBe(23)
  },
  {
    name: "REQUEST_CHANNEL",
    frame: new RequestChannelFrame(
      STREAM_ID,
      RequestChannelFlag.COMPLETE,
      31,
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: RequestChannelFrame,
    assertInitial: (frame) => expect((frame as RequestChannelFrame).request).toBe(31),
    completeOnFinal: true
  },
  {
    name: "PAYLOAD",
    frame: new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: PayloadFrame,
    completeOnFinal: true
  }
];

describe("RSocket outbound fragmentation", () => {
  for (const testCase of fragmentationCases) {
    it(`fragments ${testCase.name} without changing its logical payload`, () => {
      const estimatedLength = outboundFrameLength(testCase.frame);
      const fragments: Frame[] = [];

      expect(estimatedLength).toBe(testCase.frame.toUint8Array().byteLength);
      expect(estimatedLength).toBeGreaterThan(MAX_FRAME_LENGTH);
      emitOutboundFrameFragments(
        testCase.frame,
        estimatedLength!,
        MAX_FRAME_LENGTH,
        (fragment) => fragments.push(fragment)
      );

      expect(fragments.length).toBeGreaterThan(2);
      expect(fragments[0]).toBeInstanceOf(testCase.firstType);
      expect(fragments.slice(1).every((frame) => frame instanceof PayloadFrame)).toBe(true);
      expect(fragments.every((frame) => frame.header.streamId === STREAM_ID)).toBe(true);
      expect(fragments.every((frame) => frame.toUint8Array().byteLength <= MAX_FRAME_LENGTH)).toBe(true);
      expect(fragments.slice(0, -1).every(hasFollows)).toBe(true);
      expect(hasFollows(fragments.at(-1)!)).toBe(false);
      testCase.assertInitial?.(fragments[0]!);

      const continuationFrames = fragments.slice(1) as PayloadFrame[];
      expect(continuationFrames.every((frame) => frame.isNext())).toBe(true);
      if (testCase.completeOnFinal) {
        expect(fragments.slice(0, -1).every((frame) => !isComplete(frame))).toBe(true);
        expect(isComplete(fragments.at(-1)!)).toBe(true);
      }

      assertMetadataBeforeData(fragments);
      expect(concatFramePart(fragments, "metadata")).toEqual(metadataBytes);
      expect(concatFramePart(fragments, "payload")).toEqual(dataBytes);
    });
  }

  it("does not fragment a frame that exactly fits the configured limit", () => {
    const frame = new RequestResponseFrame(
      STREAM_ID,
      FrameFlag.NONE,
      undefined,
      dataMimeType.toPayload(patternedBytes(53, 7))
    );
    const length = outboundFrameLength(frame)!;
    const emitted: Frame[] = [];

    emitOutboundFrameFragments(frame, length, length, (fragment) => emitted.push(fragment));

    expect(emitted).toEqual([frame]);
  });

  it("rejects a limit that cannot fit the fixed REQUEST_STREAM fields", () => {
    const frame = new RequestStreamFrame(STREAM_ID, FrameFlag.NONE, 1);
    const length = outboundFrameLength(frame)!;

    expect(() => emitOutboundFrameFragments(frame, length, 6, () => {})).toThrow("maxFrameLength 6");
  });
});

describe("RSocket inbound fragment reassembly", () => {
  it("reassembles multi-frame metadata before data without losing flags or bytes", () => {
    const fragments = new Map();
    const first = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS),
      new Metadata(metadataMimeType, metadataBytes.subarray(0, 64))
    );
    const second = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS),
      new Metadata(metadataMimeType, metadataBytes.subarray(64)),
      dataMimeType.toPayload(dataBytes.subarray(0, 41))
    );
    const last = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
      undefined,
      dataMimeType.toPayload(dataBytes.subarray(41))
    );

    expect(reassemblePayloadFrame(first, fragments, metadataMimeType, dataMimeType)).toBeUndefined();
    expect(reassemblePayloadFrame(second, fragments, metadataMimeType, dataMimeType)).toBeUndefined();
    const reassembled = reassemblePayloadFrame(last, fragments, metadataMimeType, dataMimeType);

    expect(reassembled).toBeDefined();
    expect(reassembled!.isNext()).toBe(true);
    expect(reassembled!.isComplete()).toBe(true);
    expect(reassembled!.hasFollows()).toBe(false);
    expect(partBytes(reassembled!.metadata)).toEqual(metadataBytes);
    expect(partBytes(reassembled!.payload)).toEqual(dataBytes);
    expect(fragments.size).toBe(0);
  });
});

/**
 * Returns whether a fragment advertises another fragment.
 */
function hasFollows(frame: Frame): boolean {
  const candidate = frame as Frame & {hasFollows?: () => boolean};
  return candidate.hasFollows?.() === true;
}

/**
 * Returns whether a fragment carries terminal completion.
 */
function isComplete(frame: Frame): boolean {
  const candidate = frame as Frame & {isComplete?: () => boolean};
  return candidate.isComplete?.() === true;
}

/**
 * Verifies the protocol rule that all metadata bytes precede all data bytes.
 */
function assertMetadataBeforeData(frames: readonly Frame[]): void {
  let dataStarted = false;
  for (const frame of frames) {
    const metadata = partBytes(frame.metadata);
    const data = partBytes(frame.payload);
    if (dataStarted) expect(metadata?.byteLength ?? 0).toBe(0);
    if ((data?.byteLength ?? 0) > 0) dataStarted = true;
  }
}

/**
 * Concatenates one payload part across a complete fragment sequence.
 */
function concatFramePart(frames: readonly Frame[], part: "metadata" | "payload"): Uint8Array {
  const chunks = frames
    .map((frame) => partBytes(frame[part]))
    .filter((bytes): bytes is Uint8Array => bytes !== undefined && bytes.byteLength > 0);
  const length = chunks.reduce((total, bytes) => total + bytes.byteLength, 0);
  const result = new Uint8Array(length);
  let offset = 0;
  for (const bytes of chunks) {
    result.set(bytes, offset);
    offset += bytes.byteLength;
  }
  return result;
}

/**
 * Extracts serialized bytes from a payload or metadata wrapper.
 */
function partBytes(part: unknown): Uint8Array | undefined {
  if (part === undefined || part === null) return undefined;
  if (part instanceof Uint8Array) return part;
  const serializer = (part as {toUint8Array?: unknown}).toUint8Array;
  if (typeof serializer !== "function") return undefined;
  const bytes = serializer.call(part);
  return bytes instanceof Uint8Array ? bytes : undefined;
}

/**
 * Creates deterministic non-compressible-enough bytes for exact reconstruction checks.
 */
function patternedBytes(length: number, seed: number): Uint8Array {
  return Uint8Array.from({length}, (_value, index) => (index * 37 + seed) & 0xff);
}
