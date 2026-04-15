# rsocket-browser

> A fully spec-compliant, TypeScript-first RSocket client for the browser — built from scratch because the original had to be.

[![npm version](https://img.shields.io/npm/v/rsocket-browser)](https://www.npmjs.com/package/rsocket-browser)
[![license](https://img.shields.io/badge/license-LGPL--3.0--only-blue)](./LICENSE.md)

---

## Why this exists

The original [rsocket-js](https://github.com/rsocket/rsocket-js) was effectively abandoned. The `0.x` line shipped without TypeScript support, had a brittle codebase, and implemented only a subset of the spec. The `1.x` line promised to fix all of that — and spent several years not leaving `alpha`. Meanwhile, production systems were already running on RSocket.

This library is the result of needing a real, working RSocket client that could be trusted in production.

---

## What makes it different

Everything.

Rather than patching the original, this was built from the ground up using two companion libraries that also had to be written first:

- **[rsocket-frames-ts](https://github.com/CKATEPTb/rsocket-frames-ts)** — a full implementation of the RSocket frame spec using `Uint8Array` with flexible, type-safe encoders/decoders for every MIME type. Supports [custom serializer/deserializer definitions](https://github.com/CKATEPTb/rsocket-frames-ts?tab=readme-ov-file#describe-your-own-serializerdeserializer-for-mimetype).
- **[reactor-core-ts](https://github.com/CKATEPTb/reactor-core-ts)** — a minimal TypeScript port of Project Reactor (`Mono`, `Flux`, back-pressure, schedulers). All interaction models are reactive from top to bottom.

The client itself targets the **browser** as its primary environment (WebSocket transport, `Uint8Array` binary frames, no Node.js-specific APIs), though the underlying frame implementation covers the full spec and can be used outside the browser just as well.

---

## Installation

```bash
npm install rsocket-browser rsocket-frames-ts reactor-core-ts bebyte
```

---

## Quick start

```typescript
import { RSocket } from "rsocket-browser";
import { WellKnownAuthType, WellKnownMimeType } from "rsocket-frames-ts";
import { Flux, Mono } from "reactor-core-ts";

const socket = RSocket.create({
    url: "wss://example.com/ws",
    setup: {
        keepAlive: 30_000,   // ms between keepalive probes
        lifetime: 90_000,    // ms before the connection is considered dead
        mimetype: {
            data:     WellKnownMimeType.APPLICATION_JSON,
            metadata: WellKnownMimeType.MESSAGE_RSOCKET_ROUTING,
        },
    },
    logs: {
        inbound:  true,  // log every inbound frame
        outbound: true,  // log every outbound frame
    },
});
```

---

## Connection lifecycle

```typescript
// Connect — returns a Mono<void> that completes once the SETUP handshake is done.
await socket.connect().toPromise();

// Check connection state at any time.
console.log(socket.isConnected()); // true

// Graceful disconnect — drains the outbound queue before closing.
socket.disconnect().doFinally(() => {
    console.log("disconnected");
}).subscribe();

// Force disconnect — closes immediately without draining.
socket.disconnect(true).subscribe();
```

---

## Interaction models

### Metadata Push

Push connection-level metadata (e.g. authentication).

```typescript
socket.metadataPush(
    WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
        WellKnownAuthType.SIMPLE.auth({
            username: "username@example.com",
            password: "********************",
        }),
    ),
).subscribe();
```

### Fire and Forget

Send a payload with no response expected.

```typescript
socket.fireAndForget({
    data:     { key: "value" },
    metadata: ["example.fire.and.forget.route"],
}).subscribe();
```

### Request–Response

Send a request, receive exactly one response.

```typescript
socket.requestResponse({
    data:     { key: "value" },
    metadata: ["example.request.response.route"],
})
    .doOnNext(payload => {
        console.log(payload.data);
        console.log(payload.metadata);
    })
    .doOnError(error => console.error(error))
    .subscribe();
```

### Request–Stream

Send a single request, receive a back-pressure-controlled stream of responses.

```typescript
socket.requestStream(
    {
        data:     { key: "value" },
        metadata: ["example.request.stream.route"],
    },
    30, // ask the server to send up to 30 items into the buffer immediately
).subscribe({
    onSubscribe: sub => sub.request(5), // pull 5 items from the buffer to start
    onNext:      payload => {
        console.log(payload.data);
        console.log(payload.metadata);
    },
    onError:    error => console.error(error),
    onComplete: ()    => console.log("stream completed"),
});
```

### Request–Channel

A fully bidirectional stream — send a publisher of payloads, receive a stream of responses.

```typescript
socket.requestChannel(
    Flux.range(0, 100).map(value => ({
        data:     { key: value },
        metadata: ["example.request.channel.route"],
    })),
    30, // initial request-N to the responder
).subscribe({
    onSubscribe: sub => sub.request(5),
    onNext:      payload => {
        console.log(payload.data);
        console.log(payload.metadata);
    },
    onError:    error => console.error(error),
    onComplete: ()    => console.log("channel completed"),
});
```

---

## Cancellation

Any subscription can be cancelled at any time. A `CANCEL` frame is sent to the server automatically.

```typescript
const subscription = socket.requestStream({ /* ... */ }, 10).subscribe();

// Cancel after 2 seconds.
setTimeout(() => subscription.unsubscribe(), 2_000);
```

---

## Configuration reference

| Option | Type | Description |
|---|---|---|
| `url` | `string` | WebSocket URL (`ws://` or `wss://`) |
| `setup.keepAlive` | `number` | Milliseconds between KEEPALIVE probes |
| `setup.lifetime` | `number` | Milliseconds before the connection is declared dead without a KEEPALIVE response |
| `setup.mimetype.data` | `MimeType<P>` | MIME type used to encode/decode payload data |
| `setup.mimetype.metadata` | `MimeType<M>` | MIME type used to encode/decode payload metadata |
| `setup.payload` | `Payload<P, M>` | Optional payload to attach to the SETUP frame |
| `logs.inbound` | `boolean` | Log inbound frames (default: `false`) |
| `logs.outbound` | `boolean` | Log outbound frames (default: `false`) |

---

## License

LGPL-3.0-only — see [LICENSE.md](./LICENSE.md).

---

## Authors

[CKATEPTb](https://github.com/CKATEPTb), [fakeivchenko](https://github.com/fakeivchenko)

Feel free to open issues and submit pull requests to improve the library!
