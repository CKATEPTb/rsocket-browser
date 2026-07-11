/**
 * Compile-time assertions for MIME-driven positional interaction arguments.
 *
 * This file is checked by `tsc` and intentionally excluded from Vitest runtime
 * discovery because none of these requests should be subscribed.
 */
import { MimeType } from "rsocket-frames-ts";
import { RequestResponseController, RSocket } from "@";

/** Data encoded by the connection-level test codec. */
interface TypedData {
  /** Numeric request identifier. */
  readonly id: number;
}

/** Metadata encoded by the connection-level test codec. */
interface TypedMetadata {
  /** Tenant associated with the request. */
  readonly tenant: string;
}

const dataMimeType = new MimeType<TypedData>("application/vnd.example.data");
const metadataMimeType = new MimeType<TypedMetadata>("application/vnd.example.metadata");
const textMimeType = new MimeType<string>("application/vnd.example.text");
const socket = new RSocket("ws://localhost/rsocket", {
  setup: {
    mimetype: {
      data: dataMimeType,
      metadata: metadataMimeType
    }
  }
});

/** Class controller proving that declarative calls stay isolated behind process. */
class TypedController extends RequestResponseController<TypedData, string> {
  /** Route consumed by the typed test controller. */
  protected readonly route = "typed.request";
}

socket.fireAndForget({ id: 1 }, { tenant: "acme" });
socket.requestResponse({ id: 1 }, { tenant: "acme" }).map((payload) => {
  const data: TypedData | undefined = payload.data;
  const metadata: TypedMetadata | undefined = payload.metadata;
  return { data, metadata };
});
socket.requestStream({ id: 1 }, { tenant: "acme" });
socket.requestChannel([{ id: 1 }], { tenant: "acme" });
socket.requestChannel().next({ id: 1 });
socket.process(TypedController, { id: 1 });
socket.process(new TypedController({ timeout: 1_000 }), { id: 1 });

socket.fireAndForget("text", { tenant: "acme" }, { data: textMimeType });
socket.requestChannel(["text"], { tenant: "acme" }, { data: textMimeType });

// @ts-expect-error SETUP data MIME accepts TypedData, not string.
socket.fireAndForget("invalid", { tenant: "acme" });
// @ts-expect-error SETUP metadata MIME accepts TypedMetadata, not string.
socket.requestResponse({ id: 1 }, "invalid");
// @ts-expect-error Per-request text MIME accepts string data, not number.
socket.requestStream(42, { tenant: "acme" }, { data: textMimeType });
// @ts-expect-error Controllers are executed through process, not requestResponse.
socket.requestResponse(TypedController, { id: 1 });

socket.connect().map((connected) => {
  connected.fireAndForget({ id: 2 }, { tenant: "connected" });
  // @ts-expect-error Connected facade preserves the SETUP data type.
  connected.fireAndForget("invalid", { tenant: "connected" });
  return connected;
});
