# rsocket-browser

`rsocket-browser` is a browser-first, WebSocket-only RSocket requester client
that exposes a focused public surface:

```bash
npm install rsocket-browser
```

```ts
import { RSocket, RequestResponseController } from "rsocket-browser";
```

Use it when a browser frontend needs to talk to any backend that exposes an
RSocket responder over WebSocket. Spring Boot is a common target, but the client
is not tied to Spring; the fixed boundary is the transport protocol: WebSocket,
not TCP.

The client is built around:

- `reactor-core-ts` for `Mono`, `Flux`, subscriptions, demand, and cancellation.
- `rsocket-frames-ts` for spec-level RSocket frames.
- `bebyte` for efficient byte reading and frame parsing.

Those packages are installed automatically as normal package dependencies. You
install `rsocket-browser`; the frame and byte tooling needed by the client comes
with it.

Use a bundler such as Vite, Webpack, Rollup, or a modern framework build system.
The package ships ESM JavaScript and TypeScript declarations from `dist`.
The npm package exposes only the root entry point, so imports stay predictable:

```ts
import { RSocket } from "rsocket-browser";
```

## What This Solves

RSocket is not just "WebSocket with JSON". WebSocket gives you a pipe. RSocket
defines what flows through that pipe:

- setup negotiation
- keepalive
- request-response
- fire-and-forget
- request-stream
- request-channel
- request cancellation
- backpressure with `REQUEST_N`
- metadata routing
- protocol error frames

This package handles that protocol work in the browser and gives your frontend
a full-scale reactive RSocket API.

## Browser First, WebSocket Only

This client is designed for browser-first applications and intentionally
supports only WebSocket transport.

It does not expose TCP transport. It does not try to run a server. It does not
implement an RSocket responder API. It is a requester client for frontend apps
and browser-compatible WebSocket-like transports.

That narrow scope keeps the package easier to bundle, easier to understand, and
better aligned with real frontend use.

## Quick Start

```ts
import { RSocket } from "rsocket-browser";
import { WellKnownMimeType } from "rsocket-frames-ts";

const route = (name: string) =>
  WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata([
    WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata([name])
  ]);

const socket = new RSocket("wss://api.example.com/rsocket", {
  setup: {
    keepAlive: 20_000,
    lifetime: 90_000,
    mimetype: {
      data: WellKnownMimeType.APPLICATION_JSON,
      metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
    }
  }
});

const connected = (await socket.connect().block())!;

const payload = await connected
  .requestResponse({
    data: { id: 42 },
    metadata: route("user.find")
  })
  .block();

const user = payload?.data as { id: number; name: string };
console.log(user.name);
```

The important parts:

- `new RSocket(...)` creates a disconnected client instance.
- `await socket.connect().block()` opens WebSocket, sends RSocket `SETUP`, and returns a connected state surface.
- `requestResponse(...)`, `fireAndForget(...)`, `requestStream(...)`, and `requestChannel(...)` send direct RSocket interactions.
- `.block()` is a convenient way to await the `Mono` in async code.

## The Mental Model

### Mono

`Mono<T>` means "zero or one value later".

Use it for:

- `fireAndForget(...)`
- `requestResponse(...)`
- `metadataPush(...)`

Example:

```ts
const payload = await socket.requestResponse({ data: { id: 1 } }).block();
```

Add a per-request timeout when the UI should stop waiting for a response. When
the timeout expires, the client fails the `Mono` and sends `CANCEL` for that
stream.

```ts
await socket
  .requestResponse({ data: { id: 1 } }, { timeout: 5_000 })
  .block();
```

### Flux

`Flux<T>` means "many values over time".

Use it for:

- `requestStream(...)`
- `requestChannel(...)`

A `Flux` does not just dump values as fast as possible. The subscriber asks for
demand with `subscription.request(n)`. That demand becomes RSocket `REQUEST_N`.
This is the core backpressure idea.

```ts
socket.requestStream({ data: { limit: 100 } }).subscribe({
  onSubscribe(subscription) {
    subscription.request(10);
  },
  onNext(payload) {
    console.log(payload.data);
  },
  onError(error) {
    console.error(error);
  },
  onComplete() {}
});
```

If you forget to request demand, a stream may look like it is doing nothing. That
is usually correct reactive behavior, not a bug.

## Constructor Options

```ts
const socket = new RSocket("wss://api.example.com/rsocket");

const sameSocket = new RSocket({
  url: "wss://api.example.com/rsocket"
});
```

| Option | Meaning |
| --- | --- |
| `url` | Required in the object constructor form, or passed as the first constructor argument. Use `ws://` or `wss://`. |
| `setup.keepAlive` | Milliseconds between requester KEEPALIVE frames. |
| `setup.lifetime` | Maximum silence before the connection is treated as dead. |
| `setup.mimetype.data` | Default MIME codec for request data. Accepts a `MimeType` from `rsocket-frames-ts` or a MIME string. |
| `setup.mimetype.metadata` | Default MIME codec for request metadata. Accepts a `MimeType` from `rsocket-frames-ts` or a MIME string. |
| `setup.payload` | Optional payload sent in the `SETUP` frame. Keep its data and metadata encoding consistent with `setup.mimetype`. |
| `setup.transport` | Optional WebSocket-like factory, `(url: string \| URL) => websocketLike`. Defaults to browser `WebSocket`. |
| `reconnect` | `true`, `false`, or a reconnect options object. Defaults to enabled. |
| `reconnect.resume` | Enables protocol Resume. Use a TTL number in milliseconds or `{ ttl: number }`. |
| `events` | Constructor-time lifecycle handlers for UI state. |
| `log` | Constructor-time logging configuration. |

Custom WebSocket-like transport:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  setup: {
    transport: (url) => new WebSocket(url)
  }
});
```

## MIME Types And Metadata

`RSocket` does not expose static helper namespaces. Import the WebSocket client
from this package, import declarative controllers as separate named exports, and
import MIME codecs from `rsocket-frames-ts`:

```ts
import { RSocket, RequestResponseController } from "rsocket-browser";
import {
  MimeType,
  WellKnownAuthType,
  WellKnownMimeType
} from "rsocket-frames-ts";
```

`rsocket-frames-ts` is the frame and metadata codec used by this package. It
provides `MimeType`, `WellKnownMimeType`, `Metadata`, `Payload`, composite
metadata support, routing metadata, authentication metadata, and the low-level
frame codecs. See [CKATEPTb/rsocket-frames-ts](https://github.com/CKATEPTb/rsocket-frames-ts)
when you need the lower-level frame API.

For Spring RSocket controllers, the usual metadata setup is:

```ts
setup: {
  mimetype: {
    metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
  }
}
```

Controller routes support both standard RSocket metadata layouts:

- With `MESSAGE_RSOCKET_COMPOSITE_METADATA`, the client wraps the routing entry
  in composite metadata automatically.
- With `MESSAGE_RSOCKET_ROUTING`, the client sends the routing entry directly.

For direct request methods, pass a routing entry and let the configured SETUP
metadata MIME determine its wire representation:

```ts
const route = (name: string) =>
  WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata([name]);

const payload = await socket
  .requestResponse({
    data: { id: 1 },
    metadata: route("user.find")
  })
  .block();
```

Composite metadata is a container of independently typed metadata entries.
RSocket authentication is metadata too: use `SIMPLE.auth(...)` for a username
and password or `BEARER.auth(...)` for a token, then encode the result with the
protocol authentication MIME type. The following request carries a route,
bearer authentication, and one custom application entry in the same payload:

```ts
const TENANT = new MimeType<Uint8Array>("application/vnd.example.tenant");
const utf8 = new TextEncoder();
const accessToken = "ey...";

const bearerAuthentication =
  WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
    WellKnownAuthType.BEARER.auth(accessToken)
  );

const simpleAuthentication =
  WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
    WellKnownAuthType.SIMPLE.auth({
      username: "daniel",
      password: "secret"
    })
  );

const metadata =
  WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata([
    WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["user.find"]),
    bearerAuthentication,
    TENANT.toMetadata(utf8.encode("acme"))
  ]);

const payload = await socket.requestResponse({
  data: { id: 1 },
  metadata
}).block();
```

Use `simpleAuthentication` instead of `bearerAuthentication` when the responder
expects RSocket Simple Authentication. Both values are ordinary MIME-typed
`Metadata` entries and can be placed in composite metadata, stored with
`metadataUpdate(...)`, or sent through `metadataPush(...)`.

Use `WellKnownMimeType` when the RSocket registry already defines the type. Its
numeric identifier gives composite metadata a compact representation. For an
application-specific type, create `MimeType<T>` with its full MIME name; do not
assign a well-known identifier. The
[rsocket-frames-ts reference](https://github.com/CKATEPTb/rsocket-frames-ts)
also shows how to subclass `MimeType<T>` when a custom value needs its own
serializer and deserializer.

Class-based controllers add the same route metadata internally from their
protected readonly `route` field. The route becomes either a composite entry or
direct routing metadata according to `setup.mimetype.metadata`.

### Metadata Push

`metadataPush(...)` sends a real RSocket `METADATA_PUSH` frame on the active
connection. Some responders do not implement this frame, so for Spring
authentication it is usually safer to send route metadata and authorization
metadata as part of the individual request payload.

### Metadata Update

`metadataUpdate(...)` stores MIME-keyed client-side defaults for subsequent
outgoing metadata. Use this when every interaction should carry the same
authorization, tenant, locale, tracing metadata, or fallback route.

Metadata is merged by MIME type. Metadata supplied by a particular interaction
wins for that interaction only; defaults with other MIME types remain. For
example, a controller route replaces a route stored by `metadataUpdate(...)`,
but a stored Bearer authentication entry is sent alongside that controller
route. An interaction without its own route uses the stored route. The same
precedence applies to `fireAndForget`, `requestResponse`, `requestStream`,
`requestChannel`, controller calls, and `metadataPush`.

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  setup: {
    mimetype: {
      metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
    }
  }
});

socket.metadataUpdate((metadata) => {
  metadata.set(
    WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION,
    WellKnownAuthType.BEARER.auth("local-test-token")
  );
});

// For example, after logout:
socket.metadataUpdate((metadata) => {
  metadata.remove(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION);
});
```

The callback is transactional and also provides `get`, `has`, and `size`.
Nothing changes if it throws. You can still update multiple entries through
MIME-keyed maps or iterable tuples. A `null`, `undefined`, or `false` patch
value removes the entry. Do not use string-keyed object patches; pass the actual
`MimeType` or `WellKnownMimeType` value.

`metadataUpdate(...)` validates each `set` against `setup.mimetype.metadata`.
A direct SETUP MIME accepts only that exact MIME type: direct authentication
accepts only authentication, and direct routing accepts only routing. Every
other `set` throws immediately. Composite metadata can contain routing,
authentication, and custom entries at the same time. Configure
`WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA` whenever more than one
metadata type is required.

```ts
socket.metadataUpdate(new Map([
  [
    WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION,
    WellKnownAuthType.BEARER.auth("local-test-token")
  ],
  [WellKnownMimeType.APPLICATION_JSON, { tenant: "acme" }]
]));

socket.metadataUpdate([
  [WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION, null]
]);
```

`metadataUpdate(...)` is local to the browser client. It does not send a frame
by itself; the metadata is attached to later interactions. Use
`metadataPush(...)` only when you specifically need the RSocket
`METADATA_PUSH` frame; stored defaults are merged into that frame by the same
MIME precedence rules.

## Fire And Forget

Fire-and-forget sends one frame and does not wait for a response payload.

Use it for telemetry, UI events, notifications, or commands where the server
does not return a value.

```ts
await socket.fireAndForget({
  data: { page: "dashboard" },
  metadata: route("analytics.pageOpened")
}).block();
```

The returned `Mono<void>` completes after the frame is written locally.

## Request Response

Request-response sends one request and expects one response.

```ts
type User = {
  id: number;
  name: string;
};

const payload = await socket.requestResponse({
  data: { id: 42 },
  metadata: route("user.find")
}).block();

const user = payload.data as User;
console.log(user.name);
```

The response object includes:

- `data`: decoded application data
- `metadata`: decoded application metadata
- `frame`: the raw RSocket frame
- `dataPayload`: raw data payload when present
- `metadataPayload`: raw metadata payload when present

## Request Stream

Request-stream sends one request and receives many responses.

```ts
const stream = socket.requestStream({
  data: { category: "news" },
  metadata: route("articles.byCategory")
});

stream.subscribe({
  onSubscribe(subscription) {
    subscription.request(20);
  },
  onNext(payload) {
    console.log("article", payload.data);
  },
  onError(error) {
    console.error("stream failed", error);
  },
  onComplete() {
    console.log("stream complete");
  }
});
```

The first `request(n)` starts the RSocket stream. Later `request(n)` calls send
more `REQUEST_N` frames.

You can also consume streams with async iteration:

```ts
for await (const payload of socket.requestStream({ data: { limit: 10 } })) {
  console.log(payload.data);
}
```

## Request Channel

Request-channel is bidirectional: the browser sends a stream of payloads and the
server sends a stream of payloads back.

### Sink Style

Use sink style when your UI pushes values over time.

```ts
const channel = socket.requestChannel();

channel.subscribe({
  onSubscribe(subscription) {
    subscription.request(64);
  },
  onNext(payload) {
    console.log("server:", payload.data);
  },
  onError(error) {
    console.error("channel failed", error);
  },
  onComplete() {
    console.log("channel complete");
  }
});

channel.next({
  metadata: route("chat.messages")
});

channel.next({
  data: { room: "general", text: "hello" }
});

channel.sink.next({ data: { room: "general", text: "second message" } });
channel.complete();
```

`channel.next(...)` and `channel.sink.next(...)` do the same thing. The `sink`
property exists for people who prefer an explicit sink-shaped API.

### Publisher Style

Use publisher style when you already have an iterable, async iterable, promise,
or Reactor publisher.

```ts
async function* messages() {
  yield { data: { room: "general", text: "hello" } };
  yield { data: { room: "general", text: "still here" } };
}

async function* routedMessages() {
  yield { metadata: route("chat.messages") };
  yield* messages();
}

socket.requestChannel(routedMessages()).subscribe({
  onSubscribe(subscription) {
    subscription.request(32);
  },
  onNext(payload) {
    console.log(payload.data);
  },
  onError(error) {
    console.error(error);
  },
  onComplete() {}
});
```

Outbound channel frames are sent according to responder demand. If the server
does not send `REQUEST_N`, the client will not flood it with payloads.

## Declarative Controllers

Low-level methods are always available:

- `process`
- `fireAndForget`
- `metadataPush`
- `metadataUpdate`
- `requestResponse`
- `requestStream`
- `requestChannel`

For application code, class-based declarative controllers are usually cleaner.
You create one focused class per backend route, choose the interaction model with
`extends`, and put the request and response body types in generics.

Use `socket.process(Controller, ...args)` when you want one method that dispatches
to `fireAndForget`, `requestResponse`, `requestStream`, or `requestChannel` from
the controller type.

The controller base classes are separate root exports. Import only the
interaction models you need:

```ts
import {
  FireAndForgetController,
  RequestResponseController,
  RequestStreamController,
  RequestChannelController
} from "rsocket-browser";
```

Then declare controllers in the same simple style you would use for a Spring or
Java route class.

### Request Response Controller

```ts
import { RequestResponseController } from "rsocket-browser";

export type ChangePasswordRequest = {
  currentPassword: string;
  newPassword: string;
};

export type TokenResponse = {
  token: string;
};

export default class ChangePasswordController extends RequestResponseController<
  ChangePasswordRequest,
  TokenResponse
> {
  protected readonly route = "changePassword";
}
```

Use it through `process(...)`:

```ts
const token = await socket
  .process(ChangePasswordController, {
    currentPassword: "old-password",
    newPassword: "new-password"
  })
  .block();
```

You can also pass an instance:

```ts
const controller = new ChangePasswordController();

const token = await socket
  .process(controller, {
    currentPassword: "old-password",
    newPassword: "new-password"
  })
  .block();
```

TypeScript knows that `ChangePasswordController` expects
`ChangePasswordRequest` and returns `Mono<TokenResponse>`.

### Fire And Forget Controller

```ts
import { FireAndForgetController } from "rsocket-browser";

export type DeactivateAccountRequest = {
  reason: string;
};

export default class DeactivateAccountController extends FireAndForgetController<
  DeactivateAccountRequest
> {
  protected readonly route = "deactivateAccount";
}
```

Usage:

```ts
await socket
  .process(DeactivateAccountController, { reason: "requested by user" })
  .block();
```

### Request Stream Controller

```ts
import { RequestStreamController } from "rsocket-browser";

export type IdWrapper<T> = {
  id: T;
};

export type OnlineResponse = {
  accountId: number;
  lastOnlineAt: number;
  isOnline: boolean;
};

export default class SubscribeOnlineController extends RequestStreamController<
  IdWrapper<number>,
  OnlineResponse
> {
  protected readonly route = "subscribeOnline";
}
```

Usage:

```ts
socket.process(SubscribeOnlineController, { id: 7 }).subscribe({
  onSubscribe(subscription) {
    subscription.request(10);
  },
  onNext(status) {
    console.log(status.isOnline);
  },
  onError(error) {
    console.error(error);
  },
  onComplete() {}
});
```

### Request Channel Controller

```ts
import { RequestChannelController } from "rsocket-browser";

export type ChatMessage = {
  text: string;
};

export type ChatAck = {
  delivered: boolean;
};

export default class ChatController extends RequestChannelController<
  ChatMessage,
  ChatAck
> {
  protected readonly route = "chat.messages";
}
```

Usage:

```ts
async function* messages() {
  yield { data: { text: "hello" } };
  yield { data: { text: "how are you?" } };
}

socket.process(ChatController, messages()).subscribe({
  onSubscribe(subscription) {
    subscription.request(50);
  },
  onNext(ack) {
    console.log(ack.delivered);
  },
  onError(error) {
    console.error(error);
  },
  onComplete() {}
});
```

For route-based request-channel controllers, the client prepends a route-only
payload before user payloads. This matches a common Spring RSocket routing
pattern.

### Custom Mapping

By default, class controllers send the process argument as request `data` and
decode the response from `payload.data`.

If you need custom mapping, override `data(...)` or `response(...)`:

```ts
type User = {
  id: number;
  name: string;
};

class FindUserController extends RequestResponseController<number, User> {
  protected readonly route = "user.find";

  protected override data(id: number) {
    return { id };
  }

  protected override response(payload: any): User {
    return payload.data as User;
  }
}
```

## Logging

`rsocket-browser` keeps the socket instance surface focused. Socket-level
logging is configured through constructor options, while controllers still
support a local Reactor-style `.log()` helper.

### Socket Logging

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  log: {
    frames: true,
    lifecycle: true,
    interactions: false,
    payload: false,
    logger(event) {
      console.debug(event);
    }
  }
});
```

Payload logging is disabled by default so logs do not accidentally dump large or
sensitive application data. Enable it only when you need it locally:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  log: { payload: true }
});
```

### Controller Logging

Controllers also support `.log()`.

```ts
const loggedGetUser = new FindUserController().log("user.find");

await socket.process(loggedGetUser, 42).block();
```

Logging accepts the same compact forms on sockets and controllers:

- `true` enables the default log group.
- `"category.name"` enables logging with a category.
- `(event) => { ... }` installs a custom sink.
- `{ frames, lifecycle, interactions, payload, logger }` configures exact output.
- `false` disables logging again.

Controller logs focus on the interaction:

- send
- receive
- complete
- error

They do not require logging every frame in the socket.

## Reconnect Behavior

Browsers lose WebSocket connections. Phones sleep. Laptops change networks.
Tabs are suspended. This client expects that.

By default, when a WebSocket-backed RSocket session closes unexpectedly, the
high-level `RSocket` facade opens a new WebSocket and sends a new `SETUP` frame.
The reconnect loop follows the same practical model used by PartySocket:

- the first reconnect attempt is immediate
- later reconnect attempts use bounded exponential backoff
- a connection must survive a short minimum uptime before retry count resets
- WebSocket open has a connection timeout
- browser `online`, `pageshow`, `focus`, and `visibilitychange` events wake the reconnect loop
- when the browser reports offline, reconnect waits instead of burning attempts

Those browser wake checks are especially useful on iOS, where the device may
suspend timers and leave a stale WebSocket object after the screen wakes.

Most servers, including common Spring RSocket deployments, do not implement
resume in practice. By default, when a connection drops, in-flight streams and
channels are failed. The client then creates a new physical connection for
future interactions.

Use constructor `events` handlers for UI banners and app state.

If your UI cannot recreate active state after a disconnect, listen for
`disconnect` and reload the page or reset the affected screen.

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  events: {
    disconnect() {
      window.location.reload();
    }
  }
});
```

### Reconnect Options

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  reconnect: true
});
```

Reconnect backoff uses internal PartySocket-style browser defaults. Keep
application code focused on whether reconnect is enabled and whether protocol
Resume is supported by your backend.

Disable reconnect:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  reconnect: false
});
```

### Protocol Resume

If your backend supports RSocket Resume, configure the backend's resume state
lifetime in milliseconds:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  reconnect: {
    resume: {
      ttl: 10 * 60_000
    }
  }
});
```

You can also use the compact number form:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  reconnect: {
    resume: 10 * 60_000
  }
});
```

When the first connection is established, the client generates an opaque resume
token and includes it in `SETUP`. If the WebSocket drops while the TTL window is
open, a reconnect attempt sends `RESUME` with that token and the last known byte
positions. If the server replies with `RESUME_OK`, the physical WebSocket is
reattached without a new `SETUP`.

If the browser wakes from sleep and the old WebSocket did not emit `close`, the
client checks the RSocket keepalive lifetime on the wake event. A stale session
is closed locally, which starts the same reconnect path and attempts `RESUME`
while the configured TTL is still valid.

If the TTL expires, or the server rejects `RESUME`, the client falls back to a
fresh `SETUP` on a new connection. When the responder explicitly sends
`REJECTED_RESUME`, the `resumeRejected` lifecycle event is emitted before the
fresh `SETUP` is opened, so the UI can reload route state, clear local caches, or
show that the previous server session was lost. A physical close still
terminates the currently active browser-side publishers with an error. The
high-level facade can use the resumed or fresh session for later calls, including
calls that were created while reconnect was waiting for a usable connection; it
does not emulate stream resubscription or channel replay.

## Lifecycle API

Lifecycle events are meant for UI state. They carry a friendly `status`,
booleans, retry metadata, an optional error, and a short message that can be
shown in a banner or notification.

Each event includes:

- `type`: exact lifecycle event name
- `status`: `connecting`, `connected`, `disconnected`, `reconnecting`, or `closed`
- `connected`: whether a usable session is available
- `recovering`: whether reconnect is currently in progress
- `attempt`: reconnect attempt number
- `reconnect`: whether this event belongs to reconnect
- `willReconnect`: whether another reconnect attempt is expected
- `delayMs`: delay before reconnect, only on `reconnecting`
- `error`: optional failure reason
- `message`: short UI-friendly text

### Constructor Handlers

Use constructor handlers when your app shell wants to wire connection UI as soon
as the socket is created:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  events: {
    disconnect(event) {
      showBanner(event.message);
    },
    reconnecting(event) {
      showBanner(`Connection interrupted. Retrying in ${event.delayMs}ms`);
    },
    resumeRejected(event) {
      router.refresh();
      showBanner(event.message);
    },
    connected(event) {
      if (event.reconnect) showToast("Connection restored");
      hideBanner();
    },
    closed(event) {
      showBanner(event.message);
    }
  }
});
```

Use `event` to receive every lifecycle event:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  events: {
    event(event) {
      connectionStore.set({
        status: event.status,
        connected: event.connected,
        recovering: event.recovering,
        message: event.message
      });
    }
  }
});
```

Available event names:

- `connecting`
- `connected`
- `disconnect`
- `reconnecting`
- `resumeRejected`
- `reconnectFailed`
- `closed`

## Error Handling

The client throws library-specific errors for common failure categories:

- `RSocketConnectionError`: WebSocket closed, send failed, or socket not ready.
- `RSocketProtocolError`: incoming or outgoing frames violated protocol rules.
- `RSocketLeaseError`: lease enforcement rejects a request.
- `RSocketFrameSizeError`: a frame exceeds the client frame-size limit.

These classes are intentionally not exported as separate root exports. The root
runtime API stays focused on `RSocket` plus declarative controller helpers. In
application code, you can usually handle them as normal `Error` values.

```ts
try {
  await socket.requestResponse({ data: { id: 1 } }).block();
} catch (error) {
  console.error("RSocket request failed", error);
}
```

## Disconnecting

```ts
const connected = await socket.connect().block();

connected?.disconnect(1000, "User signed out");
```

After `disconnect(...)`:

- reconnect is cancelled
- active streams are failed
- a `disconnect` lifecycle event is emitted
- the facade can be connected again with `connect().block()`

Use the same `RSocket` instance when you want to reconnect with the same
options and metadata state.

## Spring RSocket Notes

For a typical Spring route:

```java
@MessageMapping("user.find")
Mono<User> findUser(UserRequest request) {
    return service.find(request.id());
}
```

Use this browser request:

```ts
import { WellKnownMimeType } from "rsocket-frames-ts";

type User = {
  id: number;
  name: string;
};

const route = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata([
  WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["user.find"])
]);

const payload = await socket
  .requestResponse({
    data: { id: 42 },
    metadata: route
  })
  .block();

const user = payload?.data as User;
```

Make sure the server and client agree on:

- WebSocket endpoint path
- data MIME type
- metadata MIME type
- route names
- authentication metadata

For Spring route metadata, `MESSAGE_RSOCKET_COMPOSITE_METADATA` is usually the
right metadata MIME type.

## Authentication Metadata

RSocket defines authentication as protocol metadata. Do not invent an
`authorization` JSON field when the responder expects the RSocket Authentication
extension. Create the authentication entry through its well-known MIME and auth
type:

```ts
import {
  WellKnownAuthType,
  WellKnownMimeType
} from "rsocket-frames-ts";

const simpleAuthentication =
  WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
    WellKnownAuthType.SIMPLE.auth({
      username: "daniel",
      password: "secret"
    })
  );

const bearerAuthentication =
  WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
    WellKnownAuthType.BEARER.auth("ey...")
  );
```

To authenticate the connection itself, place either entry in the composite
metadata carried by the `SETUP` payload:

```ts
const setupMetadata =
  WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata([
    bearerAuthentication
  ]);

const authenticatedSocket = new RSocket("wss://api.example.com/rsocket", {
  setup: {
    mimetype: {
      data: WellKnownMimeType.APPLICATION_JSON,
      metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
    },
    payload: { metadata: setupMetadata }
  }
});
```

Put the authentication entry beside routing metadata in the composite metadata
of an individual request:

```ts
const metadata =
  WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata([
    WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["user.find"]),
    bearerAuthentication
  ]);

const payload = await socket.requestResponse({
  data: { id: 42 },
  metadata
}).block();
```

To attach the same credentials to every later request, store the authentication
value in the client metadata state. Replacing Simple with Bearer uses the same
MIME key, while logout removes that key:

```ts
socket.metadataUpdate((metadata) => {
  metadata.set(
    WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION,
    WellKnownAuthType.BEARER.auth("ey...")
  );
});

socket.metadataUpdate((metadata) => {
  metadata.remove(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION);
});
```

For responders that support RSocket `METADATA_PUSH`, send a real metadata-push
frame. A MIME-typed authentication entry is wrapped in negotiated composite
metadata automatically:

```ts
await socket.metadataPush(bearerAuthentication).block();
```

Many Spring deployments do not implement `METADATA_PUSH`. In that case use the
individual-request form or `metadataUpdate(...)`; both attach authentication to
normal request payloads instead of relying on a server-side metadata-push
handler.

## Performance Notes

The implementation is designed to avoid unnecessary work in normal browser
usage:

- frame parsing is delegated to `rsocket-frames-ts`
- byte reading uses `bebyte`
- WebSocket messages stay as `Uint8Array`
- request ids are allocated without random lookups
- streams are stored by stream id in `Map`
- oversized outbound `REQUEST_*` and `PAYLOAD` frames are fragmented before send
- inbound payload fragments are buffered only while a fragmented payload is incomplete
- request-channel outbound data is gated by responder demand
- logging omits heavy payload data unless `payload: true` is enabled
- reconnect creates a fresh session unless protocol Resume succeeds
- offline browser periods wait for `online`/wake events instead of spinning

## Limitations

This client is intentionally strict about its scope.

- WebSocket only. No TCP transport.
- Requester client only. No responder/server API.
- Protocol Resume requires backend support and currently works only when the
  server accepts the last known client byte position without asking the browser
  client to replay old frames.
- No emulated stream resubscribe or channel replay after a fresh `SETUP`.
- Required extension frames are rejected unless they can be ignored.
- Reactive Streams demand matters. If you do not request demand, streams do not
  emit.
- The backend must support RSocket over WebSocket.

## Pros And Cons

Pros:

- Focused public runtime API: import `RSocket` and only the controller base classes you use.
- Browser-first, WebSocket-only design.
- RSocket frame implementation is based on `rsocket-frames-ts`.
- Request-stream and request-channel respect backpressure.
- Works naturally with Spring route metadata.
- Declarative controllers give strong TypeScript inference.
- Built-in reconnect opens a fresh WebSocket after real browser disconnects.
- Diagnostic `.log()` can be enabled only where you need it.

Cons:

- Reactive programming has a learning curve.
- In-flight work is failed by default when the physical connection drops.
- Protocol Resume depends on backend support and resume state retention.
- Browser WebSocket behavior depends on the browser, network, and device sleep.
- If you need a Node TCP requester, this is the wrong package.

## Troubleshooting

### "RSocket is not connected"

Connect before starting interactions:

```ts
const connected = (await socket.connect().block())!;
```

After a disconnect, call `connect()` again or wait for the reconnect `connected`
event before starting new critical work.

### Stream does not emit values

Check that your subscriber requests demand:

```ts
onSubscribe(subscription) {
  subscription.request(10);
}
```

### Spring route is not found

Check that:

- metadata MIME type is `MESSAGE_RSOCKET_COMPOSITE_METADATA` or `MESSAGE_RSOCKET_ROUTING`
- route metadata is present
- route name matches `@MessageMapping`

```ts
import { WellKnownMimeType } from "rsocket-frames-ts";

const metadata = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata([
  WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["your.route"])
]);

await socket
  .requestResponse({
    data: requestBody,
    metadata
  })
  .block();
```

### Logs do not show payload data

Payload data is hidden by default:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  log: { payload: true }
});
```

### Everything fails after a phone wakes from sleep

That usually means the old WebSocket died. This client will open a new
connection. By default, active streams and channels fail because Spring does not
usually support RSocket Resume. If your backend supports Resume, configure
`reconnect.resume.ttl` to match the backend resume state lifetime. The client
also listens to browser wake events and checks keepalive lifetime immediately
after the page becomes active again. Otherwise, listen for `disconnect` and
recreate the affected screen or subscriptions.

## Development

Install dependencies:

```bash
npm install
```

Run tests:

```bash
npm test
```

Build the package:

```bash
npm run build
```

`npm test` runs TypeScript type checking, unit tests, and the Java-backed
integration suite. `npm run build` cleans `dist`, builds ESM JavaScript, emits
TypeScript declarations, rewrites local declaration aliases, and verifies the
published package shape.

Build checks enforce the library shape:

- Vite emits ESM into `dist` with module structure preserved.
- Runtime dependencies stay external: `bebyte`, `reactor-core-ts`, and
  `rsocket-frames-ts`.
- Type declarations are emitted separately and local `@` aliases are rewritten
  out of published `.d.ts` files.
- package verification checks the single root package export, the limited runtime
  root export list, the `src` layout rule, published files, and `sideEffects: false`.

The integration part of `npm test` starts a WebSocket responder built with
official `rsocket-java` from `test/rsocket-java-server` and verifies real
request-response, fire-and-forget, request-stream backpressure, request-channel,
metadata-push, large outbound payload fragmentation, request-response timeout
behavior, stream ERROR propagation, queued reconnect requests, and protocol
Resume compatibility.

The single GitHub Actions CI/CD workflow runs `npm test`, then `npm run build`,
then publishes to npm from the `production` branch.

## Public API Summary

Runtime imports:

```ts
import {
  FireAndForgetController,
  RSocket,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController
} from "rsocket-browser";
```

Type-only imports:

```ts
import type { RSocketControllerRoute, RSocketPayloadDecoder } from "rsocket-browser";
```

The package has one public package entry: the root import `"rsocket-browser"`.
There are no documented deep imports. Runtime examples are intentionally built
around `RSocket` plus controller base classes.

Constructor forms:

- `new RSocket(url, options?)`
- `new RSocket({ url, ...options })`

Disconnected `RSocket` instance methods:

- `fireAndForget(payloadOrController, ...argsOrOptions): Mono<void>`
- `metadataPush(metadata, options?): Mono<void>`
- `metadataUpdate(update): ReadonlyMap<MimeType<any>, Metadata<any>>`
- `process(controller, ...args): Mono<...> | Flux<...>`
- `requestResponse(payloadOrController, ...argsOrOptions): Mono<...>`
- `requestStream(payloadOrController, ...argsOrOptions): Flux<...>`
- `requestChannel(payloadsOrControllerOrOptions, ...argsOrOptions): Flux<...> | RSocketChannel`
- `connect(): Mono<ConnectedRSocket>`

Connected facade methods:

- `fireAndForget(payloadOrController, ...argsOrOptions): Mono<void>`
- `metadataPush(metadata, options?): Mono<void>`
- `metadataUpdate(update): ReadonlyMap<MimeType<any>, Metadata<any>>`
- `process(controller, ...args): Mono<...> | Flux<...>`
- `requestResponse(payloadOrController, ...argsOrOptions): Mono<...>`
- `requestStream(payloadOrController, ...argsOrOptions): Flux<...>`
- `requestChannel(payloadsOrControllerOrOptions, ...argsOrOptions): Flux<...> | RSocketChannel`
- `disconnect(code?: number, reason?: string): DisconnectedRSocket`

`RSocket` has no static controller or MIME namespaces. Use the controller
exports above directly, and import MIME helpers such as `WellKnownMimeType` from
`rsocket-frames-ts`.
