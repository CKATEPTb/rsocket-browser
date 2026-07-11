/**
 * Request-response stream controller used by the browser client session.
 */
import {
  ErrorFrame,
  PayloadFrame,
  RequestNFrame
} from "rsocket-frames-ts";
import { errorFromFrame, RSocketProtocolError } from "@/errors/index.js";
import { decodeFramePayload } from "@/payload/index.js";
import type { StreamController, StreamSession } from "@/stream/index.js";
import type { RSocketPayloadFrame } from "@/types/index.js";

/**
 * Sink callbacks supplied by `Mono.create(...)`.
 */
export interface MonoRequestSink {
  /** Resolves the request-response Mono with an optional payload. */
  success(value?: RSocketPayloadFrame): void;
  /** Rejects the request-response Mono with a failure. */
  error(error: unknown): void;
}

/**
 * Stream controller for request-response interactions.
 *
 * Request-response is represented internally as a one-result stream so that the
 * main frame dispatcher can use the same stream registry for all interactions.
 */
export class MonoRequestController implements StreamController {
  readonly streamId: number;
  private settled = false;

  /**
   * Creates a stream controller that resolves or rejects the supplied `Mono`
   * sink when the responder sends PAYLOAD, ERROR, or CANCEL.
   */
  constructor(
    private readonly session: StreamSession,
    streamId: number,
    private readonly sink: MonoRequestSink
  ) {
    this.streamId = streamId;
  }

  /**
   * Completes the request-response `Mono` from the first responder PAYLOAD.
   */
  handlePayload(frame: PayloadFrame): void {
    if (this.settled) return;
    if (frame.hasFollows()) {
      this.fail(new RSocketProtocolError("Unexpected fragmented PAYLOAD reached request-response handler", { streamId: this.streamId }));
      return;
    }

    // RSocket requires request-response to assume COMPLETE when it is omitted.
    this.complete(frame.isNext() ? decodeFramePayload(frame) : undefined);
  }

  /**
   * Converts an RSocket ERROR frame into a rejected `Mono`.
   */
  handleError(frame: ErrorFrame): void {
    this.fail(errorFromFrame(frame));
  }

  /**
   * Ignores unexpected REQUEST_N according to RSocket's lenient frame rules.
   */
  handleRequestN(_frame: RequestNFrame): void {
    // Request-response has no request publisher for responder demand.
  }

  /**
   * Ignores responder CANCEL because only this requester may cancel the stream.
   */
  handleCancel(): void {
    // Unexpected frames that do not alter the request-response sequence are ignored.
  }

  /**
   * Fails the interaction once and unregisters it from the session registry.
   */
  fail(error: unknown): void {
    if (this.settled) return;
    this.settled = true;
    this.session.unregisterStream(this.streamId);
    this.sink.error(error);
  }

  /**
   * Resolves the interaction once and unregisters it from the session registry.
   */
  private complete(value?: RSocketPayloadFrame): void {
    this.settled = true;
    this.session.unregisterStream(this.streamId);
    this.sink.success(value);
  }
}
