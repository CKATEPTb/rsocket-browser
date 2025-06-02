# rsocket-browser

A lightweight RSocket client implementation tailored for browser environments,
utilizing WebSocket as the transport layer.

## 🚀 Feature

| RSocket Feature          | Status            | Notes                                                                                                                                                                                                                |
|--------------------------|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Transport`              | WebSocket only    | `TCP`, `Aeron`, `HTTP/2 Stream` are not provided for browser implementation, ~~but it doesn't mean that you can't implement them yourself.~~                                                                         |
| `Core Support`           | ✅ Implemented     | `SETUP`, `KeepAlive`, `FireAndForget`, `RequestResponce`, `ReqeustStream`, `RequestChannel`, `MetadataPush`, `RequestN`, `Cancel`, `Payload`, `Error`, `Ext`                                                         |
| `MimeType`               | ✅ Implemented     | Supports serialization and deserialization of metadata and payloads (see [`rsocket-frames-ts`](https://www.npmjs.com/package/rsocket-frames-ts)).                                                                    |
| `Lease`, `Resume`, `RPC` | ❌ Not Implemented | if necessary, you can implement it yourself using [`rsocket-frames-ts`](https://www.npmjs.com/package/rsocket-frames-ts). Refer to [`RSocket Protocol`](https://github.com/rsocket/rsocket/blob/master/Protocol.md). |

## 📦 Installation

To install the package using **npm**:

```bash
npm install rsocket-browser rsocket-frames-ts bebyte reactor-core-ts
```

To install the package using **pnpm**:

```bash
pnpm install rsocket-browser rsocket-frames-ts bebyte reactor-core-ts
```

To install the package using **yarn**:

```bash
yarn install rsocket-browser rsocket-frames-ts bebyte reactor-core-ts
```

## 🧰 Usage

```ts

// Create a new connection with setup parameters
const connection = new Connection({
    url: 'ws://example.com', // WebSocket server URL
    setup: {
        keepAlive: 15000, // Interval (ms) to send keepalive frames
        lifetime: 15000,  // Max connection lifetime (ms) without receiving responses
        mimetype: {
            metadata: WellKnownMimeType.MESSAGE_RSOCKET_ROUTING, // Routing metadata MIME type
            payload: WellKnownMimeType.APPLICATION_JSON           // Payload MIME type as JSON
        }
    }
});

// Create metadata matching the specified MIME type
const metadata = Mono.just(['route']); // For routing MIME type, use an array of route strings

// Create payload matching the specified MIME type
const payload = Mono.just({
    data: 'any' // Any JSON-serializable object
});

// Fire-and-forget interaction (no response expected)
connection.fireAndForget(metadata, payload);

// Request-response interaction (expect a single response)
connection.requestResponse(metadata, payload)
    // You can use any reactor-core-ts operators here
    // .map(...)
    // .flatMap(...)
    // .filter(...)
    .subscribe()  // Subscription initiates the request
    .request(1);  // Request one item from the response stream

// Request-stream interaction (expect multiple responses)
connection.requestStream(metadata, payload)
    // Reactor operators can be chained here as well
    .subscribe()  // Subscription triggers the stream
    .request(N);  // Request N items from the stream

// Request-channel interaction (bi-directional streaming)
connection.requestChannel(
    Flux.fromIterable([]), // Example upstream stream; in real cases use Flux.from(sink)
    metadata,
    payload
)
    // Chain operators as needed
    .subscribe()  // Starts the channel interaction
    .request(N);  // Request N items from the incoming stream

// Metadata-push interaction (e.g., for sending authentication data)
connection.metadataPush(
    Mono.just(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata( // Specify metadata MIME type
        WellKnownAuthType.SIMPLE.auth({ // Use SIMPLE auth (username/password)
            username: 'username',
            password: 'payload'
        })
    ))
)
```

## ⚠️ Serialization/Deserialization Notice

Support for serialization/deserialization of `application/json` or other MIME types (excluding message mimetypes from
rsocket (routing, composite, ...)) is **not implemented** in this library.

To properly handle payloads and metadata, you must configure serializers manually.  
Refer to the [`rsocket-frames-ts`](https://github.com/CKATEPTb/rsocket-frames-ts) documentation for instructions on how
to implement and register custom serializers based on your MIME types.

## Contributing

### 1. Clone the repository:

```bash
git clone https://github.com/CKATEPTb/rsocket-browser.git
```

### 2. Install dependencies::

```bash
pnpm install
```

### 3. Run the build::

```bash
pnpm run build
```

## License

### This project is licensed under the LGPL-3.0-only License.

See the [LICENSE.md](LICENSE.md) file for details.

## Author

### [CKATEPTb](https://github.com/CKATEPTb), [fakeivchenko](https://github.com/fakeivchenko)

Feel free to open issues and submit pull requests to improve the library!