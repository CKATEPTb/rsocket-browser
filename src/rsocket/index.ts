/**
 * Public browser-first RSocket facade.
 *
 * The package root exports the `RSocket` constructor separately from
 * controller base classes. The `RSocket` class itself intentionally has no
 * controller or MIME static namespaces.
 */
import {
  Flux,
  Mono,
  type Subscriber,
  type Subscription
} from "reactor-core-ts";
import {
  FrameErrorCode,
  Metadata,
  WellKnownMimeType,
  type MimeType
} from "rsocket-frames-ts";
import { RSocketChannel } from "@/channel/sink.js";
import { BrowserRSocketClient } from "@/client/index.js";
import {
  normalizeClientOptions,
  type NormalizedClientOptions
} from "@/client/options.js";
import { processController } from "@/controllers/index.js";
import type {
  AnyRSocketController,
  ControllerArgs,
  ControllerReturn,
  FireAndForgetControllerDefinition,
  RequestChannelControllerDefinition,
  RequestResponseControllerDefinition,
  RequestStreamControllerDefinition,
  RSocketControllerConnection
} from "@/controllers/index.js";
import {
  emitLog,
  frameLogEvent,
  lifecycleLogEvent,
  normalizeLogOptions,
  SOCKET_LOG_DEFAULTS,
  type NormalizedRSocketLogOptions,
  type RSocketLogInput
} from "@/logging/index.js";
import {
  applyMetadataUpdate,
  clientMetadataValue,
  isCompositeMetadataMimeType,
  mergeClientMetadata,
  metadataEntries,
  withClientMetadata,
  withClientMetadataInput,
  type RSocketMetadataMap,
  type RSocketMetadataPatch,
  type RSocketMetadataState,
  type RSocketMetadataUpdater
} from "@/metadata/index.js";
import {
  connectionEvent,
  deferred,
  browserReconnectSignals,
  normalizeReconnectOptions,
  reconnectDelay,
  RSocketEventHub,
  type RSocketAnyConnectionEventListener,
  type Deferred,
  type RSocketConnectionEvent,
  type RSocketConnectionEventDraft,
  type RSocketConnectionEventHandlers,
  type RSocketReconnectOptions
} from "@/reconnect/index.js";
import {
  EMPTY_RESUME_STATE,
  normalizeResumeOptions,
  type RSocketResumeOptions,
  type RSocketResumeState
} from "@/resume/index.js";
import type {
  RSocketChannelInput,
  RSocketClientOptions,
  RSocketFrameActivity,
  RSocketFrameActivityListener,
  RSocketPayloadFrame,
  RSocketPayloadInput,
  RSocketRequestOptions,
  RSocketStreamRequestOptions
} from "@/types/index.js";
import { RSocketConnectionError } from "@/errors/index.js";
import { directRSocketFluxSubscription, RSocketFlux } from "@/stream/index.js";
import {
  EMPTY_REQUEST_OPTIONS,
  isRequestOptions,
  normalizeRequestOptions,
  resolveConstructorOptions,
  toClientOptions,
  type RSocketConstructorOptions,
  type RSocketOptions
} from "@/rsocket/options.js";
import {
  controllerArgs,
  controllerInstance,
  isControllerInput,
  type RequestChannelResult,
  type RequestResponseResult,
  type RequestStreamResult,
  type RSocketControllerInstanceCache,
  type RSocketControllerInput
} from "@/rsocket/controllers.js";
import {
  normalizeWebSocketEndpoint,
  validateWebSocketClose
} from "@/transport/websocket/spec.js";

/**
 * Shared request options used when client-wide metadata forces composite metadata encoding.
 */
const COMPOSITE_METADATA_REQUEST_OPTIONS: RSocketRequestOptions = Object.freeze({
  metadataMimeType: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
});

/**
 * Low-level options after immutable setup and WebSocket endpoint normalization.
 */
type PreparedRSocketClientOptions = RSocketClientOptions & {
  /** Cached normalized setup/options reused by physical reconnect attempts. */
  readonly normalizedOptions: NormalizedClientOptions;
};

/**
 * Value that can be available immediately or after reconnect completes.
 */
type DeferredFluxSource = RSocketFlux | PromiseLike<{ readonly source: RSocketFlux }>;

/**
 * Binds a high-level `Flux` to the currently active low-level RSocket client
 * while keeping Reactive Streams `onSubscribe` synchronous for consumers.
 */
class DeferredRSocketFluxSubscription implements Subscription {
  private upstream: Subscription | undefined;
  private pendingRequested = 0;
  private pendingTerminal: (() => void) | undefined;
  private subscribed = false;
  private cancelled = false;
  private terminal = false;

  /**
   * Resolves the concrete source and attaches it to this outer subscription.
   */
  constructor(
    private readonly subscriber: Subscriber<RSocketPayloadFrame>,
    sourceFactory: () => DeferredFluxSource
  ) {
    try {
      const source = sourceFactory();
      if (source instanceof RSocketFlux) this.subscribeTo(source);
      else void source.then((resolved) => this.subscribeTo(resolved.source), (error) => this.fail(error));
    } catch (error) {
      this.fail(error);
    }
  }

  /**
   * Requests frames from the attached stream or records demand until reconnect
   * produces a new client.
   */
  request(n: number): void {
    if (this.cancelled || this.terminal) return;
    if (this.upstream !== undefined) {
      this.upstream.request(n);
      return;
    }
    try {
      const request = normalizeDeferredRequest(n);
      this.pendingRequested = addDeferredDemand(this.pendingRequested, request);
    } catch (error) {
      this.fail(error);
    }
  }

  /**
   * Cancels the pending or attached stream.
   */
  cancel(): void {
    if (this.cancelled) return;
    this.cancelled = true;
    this.pendingRequested = 0;
    this.pendingTerminal = undefined;
    this.upstream?.cancel();
  }

  /**
   * Allows `RSocketFlux` to deliver any terminal signal that arrived while this
   * bridge was being constructed only after downstream `onSubscribe`.
   */
  afterSubscribe(): void {
    this.subscribed = true;
    const terminal = this.pendingTerminal;
    this.pendingTerminal = undefined;
    terminal?.();
  }

  /**
   * Subscribes the bridge to the concrete `RSocketFlux`.
   */
  private subscribeTo(source: RSocketFlux): void {
    if (this.cancelled || this.terminal) return;
    try {
      source.subscribe({
        onSubscribe: (subscription) => this.attach(subscription),
        onNext: (value) => this.next(value),
        onError: (error) => this.fail(error),
        onComplete: () => this.complete()
      });
    } catch (error) {
      this.fail(error);
    }
  }

  /**
   * Attaches the real subscription and replays any demand that arrived first.
   */
  private attach(subscription: Subscription): void {
    if (this.cancelled || this.terminal) {
      subscription.cancel();
      return;
    }
    this.upstream = subscription;
    const pendingRequested = this.pendingRequested;
    this.pendingRequested = 0;
    if (pendingRequested > 0) subscription.request(pendingRequested);
  }

  /**
   * Delivers one payload while isolating subscriber callback failures.
   */
  private next(value: RSocketPayloadFrame): void {
    if (this.cancelled || this.terminal) return;
    try {
      this.subscriber.onNext(value);
    } catch (error) {
      this.abortWithError(error);
    }
  }

  /**
   * Fails the outer subscriber exactly once.
   */
  private fail(error: unknown): void {
    if (this.cancelled || this.terminal) return;
    if (!this.subscribed) {
      this.pendingTerminal = () => this.fail(error);
      return;
    }
    this.terminal = true;
    this.cancelled = true;
    this.pendingRequested = 0;
    this.pendingTerminal = undefined;
    try {
      this.subscriber.onError(error);
    } catch {
      // Subscriber failures are terminal for this stream and must not escape.
    }
  }

  /**
   * Completes the outer subscriber exactly once.
   */
  private complete(): void {
    if (this.cancelled || this.terminal) return;
    if (!this.subscribed) {
      this.pendingTerminal = () => this.complete();
      return;
    }
    this.terminal = true;
    this.pendingRequested = 0;
    this.pendingTerminal = undefined;
    try {
      this.subscriber.onComplete();
    } catch {
      // Completion callback failures must not affect connection lifecycle.
    }
  }

  /**
   * Cancels upstream and reports an error raised by the subscriber itself.
   */
  private abortWithError(error: unknown): void {
    if (this.cancelled || this.terminal) return;
    this.terminal = true;
    this.cancelled = true;
    this.pendingRequested = 0;
    this.pendingTerminal = undefined;
    this.upstream?.cancel();
    try {
      this.subscriber.onError(error);
    } catch {
      // The original subscriber callback already failed.
    }
  }
}

/**
 * Validates deferred demand before a concrete stream subscription exists.
 */
function normalizeDeferredRequest(n: number): number {
  if (n === Number.POSITIVE_INFINITY) return n;
  if (!Number.isFinite(n) || !Number.isInteger(n) || n <= 0 || n > Number.MAX_SAFE_INTEGER) {
    throw new RangeError("Reactive Streams request(n) expects a strictly positive integer");
  }
  return n;
}

/**
 * Aggregates deferred demand without overflowing JavaScript's safe integer range.
 */
function addDeferredDemand(current: number, next: number): number {
  if (current === Number.POSITIVE_INFINITY || next === Number.POSITIVE_INFINITY) return Number.POSITIVE_INFINITY;
  return Math.min(Number.MAX_SAFE_INTEGER, current + next);
}

/**
 * Disconnected state surface returned by `disconnect()`.
 */
interface DisconnectedRSocket {
  /** Opens the WebSocket and sends the RSocket SETUP frame. */
  connect(): Mono<ConnectedRSocket>;
  /** Executes a declarative controller through its declared interaction model. */
  process<C extends AnyRSocketController>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): ControllerReturn<C>;
  /** Sends a fire-and-forget request after a connection is available. */
  fireAndForget<C extends FireAndForgetControllerDefinition<readonly any[]>>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): Mono<void>;
  fireAndForget<D = unknown, M = unknown>(payload: RSocketPayloadInput<D, M>, options?: RSocketRequestOptions): Mono<void>;
  /** Sends a request-response request after a connection is available. */
  requestResponse<C extends RequestResponseControllerDefinition<readonly any[], any>>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): Mono<RequestResponseResult<C>>;
  requestResponse<D = unknown, M = unknown>(payload: RSocketPayloadInput<D, M>, options?: RSocketRequestOptions): Mono<RSocketPayloadFrame>;
  /** Sends a request-stream request after a connection is available. */
  requestStream<C extends RequestStreamControllerDefinition<readonly any[], any>>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): Flux<RequestStreamResult<C>>;
  requestStream<D = unknown, M = unknown>(payload: RSocketPayloadInput<D, M>, options?: RSocketStreamRequestOptions): RSocketFlux;
  /** Starts request-channel after a connection is available. */
  requestChannel<C extends RequestChannelControllerDefinition<readonly any[], any>>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): Flux<RequestChannelResult<C>>;
  requestChannel<D = unknown, M = unknown>(payloads: RSocketChannelInput<D, M>, options?: RSocketStreamRequestOptions): RSocketFlux;
  requestChannel<D = unknown, M = unknown>(options?: RSocketStreamRequestOptions): RSocketChannel<D, M>;
  /** Sends a connection-level METADATA_PUSH frame. */
  metadataPush<M = unknown>(metadataPayload: M | Metadata<M>, options?: RSocketRequestOptions): Mono<void>;
  /** Updates MIME-keyed defaults merged into subsequent outgoing metadata. */
  metadataUpdate(update: RSocketMetadataPatch | RSocketMetadataUpdater): RSocketMetadataMap;
}

/**
 * Connected state surface returned by `connect()`.
 */
interface ConnectedRSocket extends Omit<DisconnectedRSocket, "connect"> {
  /** Disconnects the active session and returns the disconnected surface. */
  disconnect(code?: number, reason?: string): DisconnectedRSocket;
}

/**
 * Browser-first RSocket requester with a WebSocket-only transport boundary.
 *
 * The facade owns reconnect policy, event emission, logging, and the current
 * low-level WebSocket session. Every interaction is executed through Reactor
 * `Mono` or `Flux` types from `reactor-core-ts`.
 */
export class RSocket {
  private readonly clientOptions: RSocketClientOptions;
  private readonly reconnectOptions: RSocketReconnectOptions;
  private readonly resumeOptions: RSocketResumeOptions;
  private readonly setupMetadataMimeType: MimeType<any>;
  private readonly metadataState: RSocketMetadataState = new Map();
  /** Singleton class-controller instances scoped to this facade. */
  private readonly controllerInstances: RSocketControllerInstanceCache = new WeakMap();
  private metadataEntryList: readonly Metadata<any>[] = metadataEntries(this.metadataState);
  private metadataValue: Metadata<any> | undefined;
  private metadataMergeCache: WeakMap<Metadata<any>, Metadata<any>> | undefined;
  private readonly eventHub = new RSocketEventHub();
  private anyEventListeners: Set<RSocketAnyConnectionEventListener> | undefined;
  private readonly activityListener: RSocketFrameActivityListener | undefined;
  private client: BrowserRSocketClient | undefined;
  private logOptions: NormalizedRSocketLogOptions | undefined;
  private readyDeferred: Deferred<this> = deferred<this>();
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private stableConnectionTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectAttempts = 0;
  private resumeState: RSocketResumeState = EMPTY_RESUME_STATE;
  private resumeDeadlineAt = 0;
  private connectToken = 0;
  private connectAbortController: AbortController | undefined;
  private connecting = false;
  private closeRequested = true;
  private validatedClientOptions: RSocketClientOptions | undefined;
  private lastError: unknown;
  private disposeWakeListener: (() => void) | undefined;
  /** Connected state facade with a deliberately tiny public surface. */
  private readonly connectedSurface: ConnectedRSocket = Object.freeze({
    process: this.process.bind(this) as ConnectedRSocket["process"],
    fireAndForget: this.fireAndForget.bind(this) as ConnectedRSocket["fireAndForget"],
    requestResponse: this.requestResponse.bind(this) as ConnectedRSocket["requestResponse"],
    requestStream: this.requestStream.bind(this) as ConnectedRSocket["requestStream"],
    requestChannel: this.requestChannel.bind(this) as ConnectedRSocket["requestChannel"],
    metadataPush: this.metadataPush.bind(this) as ConnectedRSocket["metadataPush"],
    metadataUpdate: this.metadataUpdate.bind(this),
    disconnect: (code?: number, reason?: string) => this.disconnectNow(code, reason)
  });
  /** Controller execution surface reused by every declarative controller call. */
  private readonly controllerSurface: RSocketControllerConnection = Object.freeze({
    fireAndForget: (payload: RSocketPayloadInput<any, any>, options?: RSocketRequestOptions) =>
      this.fireAndForget(payload, options),
    requestResponse: (payload: RSocketPayloadInput<any, any>, options?: RSocketRequestOptions) =>
      this.requestResponse(payload, options) as Mono<RSocketPayloadFrame>,
    requestStream: (payload: RSocketPayloadInput<any, any>, options?: RSocketStreamRequestOptions) => this.requestStreamFlux(
      payload,
      normalizeFacadeRequestOptions(options)
    ),
    requestChannel: (payloads: RSocketChannelInput<any, any>, options?: RSocketStreamRequestOptions) => this.requestChannelFlux(
      payloads,
      normalizeFacadeRequestOptions(options)
    )
  });

  /**
   * Creates a disconnected browser WebSocket RSocket requester.
   */
  constructor(url: string | URL, options?: RSocketOptions);
  constructor(options: RSocketConstructorOptions);
  constructor(urlOrOptions: string | URL | RSocketConstructorOptions, options: RSocketOptions = {}) {
    const resolved = resolveConstructorOptions(urlOrOptions, options);
    if (resolved.options.log !== undefined) this.configureLog(resolved.options.log);
    if (resolved.options.events !== undefined) this.registerEventHandlers(resolved.options.events);
    this.resumeOptions = normalizeResumeOptions(resolved.options as Parameters<typeof normalizeResumeOptions>[0]);
    const clientOptions = toClientOptions(resolved.url, resolved.options, this.resumeOptions.token);
    this.setupMetadataMimeType = clientOptions.setup?.metadataMimeType ??
      WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
    this.metadataValue = clientMetadataValue(this.metadataEntryList, this.setupMetadataMimeType);
    if (this.logOptions?.frames === true) {
      this.activityListener = (activity: RSocketFrameActivity): void => this.logFrame(activity);
      this.clientOptions = { ...clientOptions, activityListener: this.activityListener };
    } else {
      this.clientOptions = clientOptions;
    }
    this.reconnectOptions = normalizeReconnectOptions(resolved.options);
  }

  /**
   * Opens the WebSocket, sends SETUP, and emits the connected state surface.
   *
   * The returned `Mono` is cold: the connection attempt starts when the Mono is
   * subscribed or blocked.
   */
  connect(): Mono<ConnectedRSocket> {
    return Mono.defer(() => this.connectNow());
  }

  /**
   * Starts or joins the current connection attempt for `connect()`.
   */
  private connectNow(): Mono<ConnectedRSocket> {
    if (this.connected) return Mono.just(this.connectedFacade());
    try {
      this.validateConnectOptions();
    } catch (error) {
      return Mono.error(error);
    }
    if (!this.connecting && this.reconnectTimer === undefined) {
      this.closeRequested = false;
      this.ensureWakeListener();
      this.ensureReadyPending();
      this.openConnection(false);
    }
    return Mono.fromPromise(this.readyDeferred.promise.then(() => this.connectedFacade()));
  }

  /**
   * Indicates whether a live WebSocket-backed RSocket session is available.
   */
  private get connected(): boolean {
    return this.client !== undefined && !this.client.isClosed;
  }

  /**
   * Enables, updates, or disables Reactor-style socket logging.
   */
  private configureLog(options: RSocketLogInput = true): void {
    const next = normalizeLogOptions(options, SOCKET_LOG_DEFAULTS, this.logOptions);
    this.logOptions = next.enabled ? next : undefined;
  }

  /**
   * Disconnects the active session but keeps the facade reusable.
   */
  private disconnectNow(code?: number, reason = "RSocket client disconnected"): DisconnectedRSocket {
    if (this.closeRequested && this.client === undefined && !this.connecting && this.reconnectTimer === undefined) {
      return this.disconnectedFacade();
    }
    validateWebSocketClose(code, reason);
    this.closeRequested = true;
    this.connectToken += 1;
    this.abortConnect();
    this.clearReconnectTimer();
    this.clearStableConnectionTimer();
    this.clearWakeListener();
    this.resumeDeadlineAt = 0;

    const error = new RSocketConnectionError(reason);
    this.lastError = error;
    this.readyDeferred.reject(error);

    const client = this.client;
    this.client = undefined;
    if (client) this.closeClientQuietly(client, code, reason);

    this.emitLifecycle({
      type: "disconnect",
      attempt: this.reconnectAttempts,
      reconnect: false,
      error,
      willReconnect: false
    });
    return this.disconnectedFacade();
  }

  /**
   * Executes a declarative controller through its declared interaction model.
   */
  process<C extends AnyRSocketController>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): ControllerReturn<C> {
    return this.executeController(controllerInstance(controllerDefinition, this.controllerInstances), args);
  }

  /**
   * Sends a fire-and-forget request after a connection is available.
   */
  fireAndForget<C extends FireAndForgetControllerDefinition<readonly any[]>>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): Mono<void>;
  fireAndForget<D = unknown, M = unknown>(
    payload: RSocketPayloadInput<D, M>,
    options?: RSocketRequestOptions
  ): Mono<void>;
  fireAndForget<D = unknown, M = unknown>(
    payloadOrController: RSocketPayloadInput<D, M> | RSocketControllerInput<FireAndForgetControllerDefinition<readonly any[]>>,
    optionsOrArg?: RSocketRequestOptions | unknown,
    ...args: unknown[]
  ): Mono<void> {
    if (isControllerInput(payloadOrController)) {
      return this.processControllerInput(
        "fireAndForget",
        payloadOrController,
        controllerArgs(arguments.length, optionsOrArg, args)
      ) as Mono<void>;
    }
    const payload = payloadOrController as RSocketPayloadInput<D, M>;
    const normalized = normalizeFacadeRequestOptions(optionsOrArg as RSocketRequestOptions | undefined);
    return this.withReadyClientMono((client) => client.fireAndForget(
      this.withClientMetadata(payload, normalized),
      this.withClientMetadataOptions(normalized)
    ));
  }

  /**
   * Sends a request-response request and emits the single decoded response.
   */
  requestResponse<C extends RequestResponseControllerDefinition<readonly any[], any>>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): Mono<RequestResponseResult<C>>;
  requestResponse<D = unknown, M = unknown>(
    payload: RSocketPayloadInput<D, M>,
    options?: RSocketRequestOptions
  ): Mono<RSocketPayloadFrame>;
  requestResponse<D = unknown, M = unknown>(
    payloadOrController: RSocketPayloadInput<D, M> | RSocketControllerInput<RequestResponseControllerDefinition<readonly any[], any>>,
    optionsOrArg?: RSocketRequestOptions | unknown,
    ...args: unknown[]
  ): Mono<RSocketPayloadFrame> | Mono<unknown> {
    if (isControllerInput(payloadOrController)) {
      return this.processControllerInput(
        "requestResponse",
        payloadOrController,
        controllerArgs(arguments.length, optionsOrArg, args)
      ) as Mono<unknown>;
    }
    const payload = payloadOrController as RSocketPayloadInput<D, M>;
    const normalized = normalizeFacadeRequestOptions(optionsOrArg as RSocketRequestOptions | undefined);
    return this.withReadyClientMono((client) => client.requestResponse(
      this.withClientMetadata(payload, normalized),
      this.withClientMetadataOptions(normalized)
    ));
  }

  /**
   * Sends a request-stream request and returns a demand-aware `Flux`.
   */
  requestStream<C extends RequestStreamControllerDefinition<readonly any[], any>>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): Flux<RequestStreamResult<C>>;
  requestStream<D = unknown, M = unknown>(
    payload: RSocketPayloadInput<D, M>,
    options?: RSocketStreamRequestOptions
  ): RSocketFlux;
  requestStream<D = unknown, M = unknown>(
    payloadOrController: RSocketPayloadInput<D, M> | RSocketControllerInput<RequestStreamControllerDefinition<readonly any[], any>>,
    optionsOrArg?: RSocketStreamRequestOptions | unknown,
    ...args: unknown[]
  ): RSocketFlux | Flux<unknown> {
    if (isControllerInput(payloadOrController)) {
      return this.processControllerInput(
        "requestStream",
        payloadOrController,
        controllerArgs(arguments.length, optionsOrArg, args)
      ) as Flux<unknown>;
    }
    const payload = payloadOrController as RSocketPayloadInput<D, M>;
    return this.requestStreamFlux(payload, normalizeFacadeRequestOptions(optionsOrArg as RSocketStreamRequestOptions | undefined));
  }

  /**
   * Starts request-channel from an existing outbound payload source.
   */
  requestChannel<C extends RequestChannelControllerDefinition<readonly any[], any>>(
    controllerDefinition: RSocketControllerInput<C>,
    ...args: ControllerArgs<C>
  ): Flux<RequestChannelResult<C>>;
  requestChannel<D = unknown, M = unknown>(
    payloads: RSocketChannelInput<D, M>,
    options?: RSocketStreamRequestOptions
  ): RSocketFlux;
  /**
   * Creates an imperative request-channel helper with a sink-like API.
   */
  requestChannel<D = unknown, M = unknown>(
    options?: RSocketStreamRequestOptions
  ): RSocketChannel<D, M>;
  /**
   * Starts request-channel either from an existing source or as an imperative
   * helper, depending on the first argument.
   */
  requestChannel<D = unknown, M = unknown>(
    payloadsOrControllerOrOptions?: RSocketChannelInput<D, M> | RSocketStreamRequestOptions | RSocketControllerInput<RequestChannelControllerDefinition<readonly any[], any>>,
    optionsOrArg?: RSocketStreamRequestOptions | unknown,
    ...args: unknown[]
  ): RSocketFlux | RSocketChannel<D, M> | Flux<unknown> {
    if (isControllerInput(payloadsOrControllerOrOptions)) {
      return this.processControllerInput(
        "requestChannel",
        payloadsOrControllerOrOptions,
        controllerArgs(arguments.length, optionsOrArg, args)
      ) as Flux<unknown>;
    }
    const payloadsOrOptions = payloadsOrControllerOrOptions as RSocketChannelInput<D, M> | RSocketStreamRequestOptions | undefined;
    if (payloadsOrOptions === undefined || isRequestOptions(payloadsOrOptions)) {
      const normalized = normalizeFacadeRequestOptions(payloadsOrOptions);
      return new RSocketChannel<D, M>(
        (payloads) => this.requestChannelFlux(payloads, normalized)
      );
    }
    return this.requestChannelFlux(payloadsOrOptions, normalizeFacadeRequestOptions(optionsOrArg as RSocketStreamRequestOptions | undefined));
  }

  /**
   * Sends a connection-level METADATA_PUSH frame.
   */
  metadataPush<M = unknown>(metadataPayload: M | Metadata<M>, options: RSocketRequestOptions = EMPTY_REQUEST_OPTIONS): Mono<void> {
    const normalized = normalizeFacadeRequestOptions(options);
    return this.withReadyClientMono((client) => client.metadataPush(
      this.withClientMetadataValue(metadataPayload, normalized),
      this.withClientMetadataOptions(normalized)
    ));
  }

  /**
   * Updates MIME-keyed defaults merged into subsequent outgoing metadata.
   */
  metadataUpdate(update: RSocketMetadataPatch | RSocketMetadataUpdater): RSocketMetadataMap {
    const snapshot = applyMetadataUpdate(this.metadataState, update, this.setupMetadataMimeType);
    this.metadataEntryList = metadataEntries(this.metadataState);
    this.metadataValue = clientMetadataValue(this.metadataEntryList, this.setupMetadataMimeType);
    this.metadataMergeCache = undefined;
    return snapshot;
  }

  /**
   * Executes a declarative controller through the matching request method.
   */
  private processControllerInput(
    expectedKind: AnyRSocketController["kind"],
    controllerDefinition: RSocketControllerInput<AnyRSocketController>,
    args: readonly unknown[]
  ): ControllerReturn<AnyRSocketController> {
    const controller = controllerInstance(controllerDefinition, this.controllerInstances);
    if (controller.kind !== expectedKind) {
      throw new RSocketConnectionError(`Controller kind ${controller.kind} cannot be used with ${expectedKind}.`);
    }
    return this.executeController(controller, args as ControllerArgs<AnyRSocketController>);
  }

  /**
   * Executes an already materialized controller against this socket facade.
   */
  private executeController<C extends AnyRSocketController>(
    controller: C,
    args: ControllerArgs<C>
  ): ControllerReturn<C> {
    return processController(this.controllerSurface, controller, args);
  }

  /**
   * Creates a request-stream Flux bound to the current connected client.
   */
  private requestStreamFlux<D, M>(
    payload: RSocketPayloadInput<D, M>,
    options: RSocketStreamRequestOptions
  ): RSocketFlux {
    return this.withReadyClientFlux((client) => client.requestStream(
      this.withClientMetadata(payload, options),
      this.withClientMetadataOptions(options)
    ));
  }

  /**
   * Creates a request-channel Flux bound to the current connected client.
   */
  private requestChannelFlux<D, M>(
    payloads: RSocketChannelInput<D, M>,
    options: RSocketStreamRequestOptions
  ): RSocketFlux {
    return this.withReadyClientFlux((client) => client.requestChannel(
      this.withClientMetadataInput(payloads, options),
      this.withClientMetadataOptions(options)
    ));
  }

  /**
   * Starts one physical WebSocket connection attempt.
   */
  private openConnection(reconnect: boolean, cause?: unknown): void {
    const token = ++this.connectToken;
    this.abortConnect();
    const abortController = new AbortController();
    this.connectAbortController = abortController;
    const attempt = reconnect ? this.reconnectAttempts : 0;
    this.connecting = true;
    this.emitLifecycle({
      type: "connecting",
      attempt,
      reconnect,
      error: cause,
      willReconnect: this.canReconnect()
    });

    const baseOptions = this.validatedClientOptions ?? this.clientOptions;
    const clientOptions = {
      ...baseOptions,
      abortSignal: abortController.signal
    };

    void this.openReadyConnection(clientOptions, reconnect, attempt).then(
      (client) => {
        if (!this.isCurrentConnect(token)) {
          this.closeClientQuietly(client, 1000, "Stale RSocket connection");
          return;
        }

        if (this.connectAbortController === abortController) this.connectAbortController = undefined;
        this.connecting = false;
        this.client = client;
        this.lastError = undefined;
        this.resumeDeadlineAt = 0;
        this.markConnectionStableAfterUptime(client, reconnect);
        client.onClose((error) => this.handleClientClose(client, error));
        if (this.client !== client || client.isClosed) return;
        this.readyDeferred.resolve(this);
        this.emitLifecycle({
          type: "connected",
          attempt,
          reconnect,
          willReconnect: false
        });
      },
      (error) => {
        if (!this.isCurrentConnect(token)) return;

        if (this.connectAbortController === abortController) this.connectAbortController = undefined;
        this.connecting = false;
        this.client = undefined;
        this.lastError = error;
        if (reconnect) {
          this.emitLifecycle({
            type: "reconnectFailed",
            attempt,
            reconnect: true,
            error,
            willReconnect: this.canReconnect()
          });
        }
        this.scheduleReconnect(error);
      }
    );
  }

  /**
   * Waits for browser network availability, then performs RESUME or SETUP.
   */
  private async openReadyConnection(
    clientOptions: RSocketClientOptions,
    reconnect: boolean,
    attempt: number
  ): Promise<BrowserRSocketClient> {
    if (!browserReconnectSignals.isOnline()) {
      await browserReconnectSignals.waitUntilOnline(clientOptions.abortSignal);
    }
    if (!reconnect || !this.canAttemptResume()) {
      return BrowserRSocketClient.connect(clientOptions);
    }

    try {
      return await BrowserRSocketClient.resume(clientOptions, { state: this.resumeState });
    } catch (error) {
      if (clientOptions.abortSignal?.aborted) throw error;
      if (error instanceof RSocketConnectionError) throw error;
      if (isRejectedResume(error)) {
        this.emitLifecycle({
          type: "resumeRejected",
          attempt,
          reconnect: true,
          error,
          willReconnect: true
        });
      }
      this.resumeDeadlineAt = 0;
      return BrowserRSocketClient.connect(clientOptions);
    }
  }

  /**
   * Handles closure of the currently active low-level session.
   */
  private handleClientClose(client: BrowserRSocketClient, error: unknown): void {
    if (client !== this.client) return;
    this.resumeState = client.resumeState();
    this.client = undefined;
    this.lastError = error;
    this.clearStableConnectionTimer();
    if (this.closeRequested) return;

    this.ensureReadyPending();
    this.startResumeWindow();
    this.emitLifecycle({
      type: "disconnect",
      attempt: this.reconnectAttempts,
      reconnect: false,
      error,
      willReconnect: this.canReconnect()
    });
    this.scheduleReconnect(error);
  }

  /**
   * Schedules the next connection attempt or closes the facade permanently.
   */
  private scheduleReconnect(error: unknown): void {
    this.clearReconnectTimer();
    if (!this.canReconnect()) {
      this.readyDeferred.reject(error);
      this.emitLifecycle({
        type: "closed",
        attempt: this.reconnectAttempts,
        reconnect: false,
        error,
        willReconnect: false
      });
      this.clearWakeListener();
      return;
    }

    this.ensureReadyPending();
    const attempt = this.reconnectAttempts + 1;
    const delayMs = this.nextReconnectDelay(attempt);
    this.reconnectAttempts = attempt;
    this.emitLifecycle({
      type: "reconnecting",
      attempt,
      reconnect: true,
      error,
      delayMs,
      willReconnect: true
    });
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.openConnection(true, error);
    }, delayMs);
  }

  /**
   * Checks reconnect policy against user intent and attempt budget.
   */
  private canReconnect(): boolean {
    return !this.closeRequested
      && this.reconnectOptions.enabled
      && (this.reconnectAttempts < this.reconnectOptions.maxAttempts || this.canAttemptResume());
  }

  /**
   * Starts the bounded protocol Resume window after a physical session drops.
   */
  private startResumeWindow(): void {
    if (!this.resumeOptions.enabled || this.resumeOptions.token === undefined) {
      this.resumeDeadlineAt = 0;
      return;
    }
    this.resumeDeadlineAt = Date.now() + this.resumeOptions.ttlMs;
  }

  /**
   * Checks whether the next reconnect attempt should send RESUME instead of SETUP.
   */
  private canAttemptResume(now = Date.now()): boolean {
    return this.resumeOptions.enabled
      && this.resumeOptions.token !== undefined
      && this.resumeDeadlineAt > now;
  }

  /**
   * Calculates the next reconnect delay while preserving the resume deadline.
   */
  private nextReconnectDelay(attempt: number): number {
    const delayMs = reconnectDelay(attempt, this.reconnectOptions);
    const now = Date.now();
    if (!this.canAttemptResume(now)) return delayMs;
    return Math.min(delayMs, Math.max(0, this.resumeDeadlineAt - now));
  }

  /**
   * Resets retry count only after the connection survives PartySocket-style min uptime.
   */
  private markConnectionStableAfterUptime(client: BrowserRSocketClient, reconnect: boolean): void {
    this.clearStableConnectionTimer();
    if (!reconnect || this.reconnectOptions.minUptimeMs <= 0) {
      this.reconnectAttempts = 0;
      return;
    }

    this.stableConnectionTimer = setTimeout(() => {
      if (client === this.client && !client.isClosed) this.reconnectAttempts = 0;
    }, this.reconnectOptions.minUptimeMs);
  }

  /**
   * Reacts to browser wake/network events after mobile sleep or offline periods.
   */
  private handleBrowserWake(): void {
    if (this.closeRequested) return;

    const client = this.currentClient();
    if (client !== undefined) {
      client.checkLifetime();
      return;
    }

    if (this.connecting || !this.canReconnect()) return;
    this.clearReconnectTimer();
    this.openConnection(true, this.lastError);
  }

  /**
   * Installs browser wake listeners while this facade is actively connected or reconnecting.
   */
  private ensureWakeListener(): void {
    this.disposeWakeListener ??= browserReconnectSignals.onWake(() => this.handleBrowserWake());
  }

  /**
   * Removes browser wake listeners after explicit disconnect or permanent close.
   */
  private clearWakeListener(): void {
    this.disposeWakeListener?.();
    this.disposeWakeListener = undefined;
  }

  /**
   * Replaces the ready promise after a previous ready promise settled.
   */
  private ensureReadyPending(): void {
    if (this.readyDeferred.settled) this.readyDeferred = deferred<this>();
  }

  /**
   * Cancels a pending reconnect timer when present.
   */
  private clearReconnectTimer(): void {
    if (this.reconnectTimer === undefined) return;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
  }

  /**
   * Cancels the pending stable-uptime timer.
   */
  private clearStableConnectionTimer(): void {
    if (this.stableConnectionTimer === undefined) return;
    clearTimeout(this.stableConnectionTimer);
    this.stableConnectionTimer = undefined;
  }

  /**
   * Registers constructor-time lifecycle event handlers.
   */
  private registerEventHandlers(handlers: RSocketConnectionEventHandlers): void {
    if (handlers.event !== undefined) (this.anyEventListeners ??= new Set<RSocketAnyConnectionEventListener>()).add(handlers.event);
    if (handlers.connecting !== undefined) this.eventHub.on("connecting", handlers.connecting);
    if (handlers.connected !== undefined) this.eventHub.on("connected", handlers.connected);
    if (handlers.disconnect !== undefined) this.eventHub.on("disconnect", handlers.disconnect);
    if (handlers.reconnecting !== undefined) this.eventHub.on("reconnecting", handlers.reconnecting);
    if (handlers.resumeRejected !== undefined) this.eventHub.on("resumeRejected", handlers.resumeRejected);
    if (handlers.reconnectFailed !== undefined) this.eventHub.on("reconnectFailed", handlers.reconnectFailed);
    if (handlers.closed !== undefined) this.eventHub.on("closed", handlers.closed);
  }

  /**
   * Aborts the in-flight WebSocket open attempt.
   */
  private abortConnect(): void {
    this.connectAbortController?.abort();
    this.connectAbortController = undefined;
  }

  /**
   * Closes a low-level session after facade state has already moved on.
   */
  private closeClientQuietly(client: BrowserRSocketClient, code: number | undefined, reason: string): void {
    try {
      client.close(code, reason);
    } catch (error) {
      this.lastError ??= error;
    }
  }

  /**
   * Guards async connect continuations against stale attempts.
   */
  private isCurrentConnect(token: number): boolean {
    return !this.closeRequested && token === this.connectToken;
  }

  /**
   * Emits lifecycle events to subscribers and optional lifecycle logs.
   */
  private emitLifecycle(draft: RSocketConnectionEventDraft): void {
    const lifecycleLogging = this.logOptions?.lifecycle === true;
    const anyListeners = this.anyEventListeners;
    if (!lifecycleLogging && (anyListeners === undefined || anyListeners.size === 0) && !this.eventHub.has(draft.type)) return;

    const event = connectionEvent(draft);
    this.eventHub.emit(event);
    if (anyListeners !== undefined) this.emitAnyLifecycle(event, anyListeners);
    if (lifecycleLogging) emitLog(this.logOptions, lifecycleLogEvent(event));
  }

  /**
   * Emits one lifecycle event to all-event listeners.
   */
  private emitAnyLifecycle(
    event: RSocketConnectionEvent,
    listeners: Set<RSocketAnyConnectionEventListener>
  ): void {
    for (const listener of listeners) {
      try {
        listener(event);
      } catch {
        // UI listeners must not break reconnect scheduling.
      }
    }
  }

  /**
   * Emits one frame activity log event when frame logging is enabled.
   */
  private logFrame(activity: RSocketFrameActivity): void {
    emitLog(this.logOptions, frameLogEvent(activity, this.logOptions?.payload === true));
  }

  /**
   * Defers a one-shot interaction until a low-level client is connected.
   */
  private withReadyClientMono<T>(factory: (client: BrowserRSocketClient) => Mono<T>): Mono<T> {
    return Mono.defer(() => {
      const client = this.currentClient();
      if (client !== undefined) return factory(client);
      return Mono.fromPromise(this.readyClient()).flatMap(factory);
    });
  }

  /**
   * Defers a streaming interaction until a low-level client is connected.
   */
  private withReadyClientFlux(factory: (client: BrowserRSocketClient) => RSocketFlux): RSocketFlux {
    return new RSocketFlux((subscriber) => {
      const client = this.currentClient();
      if (client !== undefined) return directRSocketFluxSubscription(factory(client), subscriber);
      return new DeferredRSocketFluxSubscription(
        subscriber,
        () => this.readyClient().then((readyClient) => ({ source: factory(readyClient) }))
      );
    });
  }

  /**
   * Resolves with a connected low-level client after initial connect or reconnect.
   */
  private async readyClient(): Promise<BrowserRSocketClient> {
    const client = this.currentClient();
    if (client !== undefined) return client;
    this.ensureReadyPending();
    await this.connect().block();
    return this.requireClient();
  }

  /**
   * Returns the active low-level client without changing connection state.
   */
  private currentClient(): BrowserRSocketClient | undefined {
    return this.client !== undefined && !this.client.isClosed ? this.client : undefined;
  }

  /**
   * Merges current client metadata into one outgoing request payload.
   */
  private withClientMetadata<D, M>(
    payload: RSocketPayloadInput<D, M>,
    options: RSocketRequestOptions
  ): RSocketPayloadInput<D, M> {
    if (this.metadataEntryList.length === 0) return payload;
    return withClientMetadata(
      payload,
      this.metadataEntryList,
      options.metadataMimeType,
      this.metadataValue,
      this.metadataMergeCache ??= new WeakMap(),
      this.setupMetadataMimeType
    );
  }

  /**
   * Merges current client metadata into each outgoing channel payload.
   */
  private withClientMetadataInput<D, M>(
    payloads: RSocketChannelInput<D, M>,
    options: RSocketRequestOptions
  ): RSocketChannelInput<D, M> {
    const entries = this.metadataEntryList;
    if (entries.length === 0) return payloads;
    return withClientMetadataInput(
      payloads,
      entries,
      options.metadataMimeType,
      this.metadataValue,
      this.metadataMergeCache ??= new WeakMap(),
      this.setupMetadataMimeType
    );
  }

  /**
   * Merges current client defaults into one connection-level metadata push.
   */
  private withClientMetadataValue<M>(
    metadata: M | Metadata<M>,
    options: RSocketRequestOptions
  ): M | Metadata<any> {
    const entries = this.metadataEntryList;
    if (entries.length === 0) return metadata;
    return mergeClientMetadata(
      entries,
      metadata,
      options.metadataMimeType,
      this.setupMetadataMimeType,
      this.metadataMergeCache ??= new WeakMap()
    );
  }

  /**
   * Forces composite metadata encoding when client metadata entries are present.
   */
  private withClientMetadataOptions(options: RSocketRequestOptions): RSocketRequestOptions {
    if (this.metadataEntryList.length === 0) return options;
    if (!isCompositeMetadataMimeType(this.setupMetadataMimeType)) return options;
    if (options.metadataMimeType === WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA) return options;
    if (options.dataMimeType === undefined && options.timeout === undefined) return COMPOSITE_METADATA_REQUEST_OPTIONS;
    return compositeMetadataOptions(options);
  }

  /**
   * Returns the current connected client or throws a user-facing error.
   */
  private requireClient(): BrowserRSocketClient {
    const client = this.currentClient();
    if (client !== undefined) return client;
    if (this.closeRequested) {
      throw new RSocketConnectionError("RSocket is disconnected. Subscribe to socket.connect() before starting interactions.", this.lastError);
    }
    throw new RSocketConnectionError(
      "RSocket is not connected. Subscribe to socket.connect() before starting interactions.",
      this.lastError
    );
  }

  /**
   * Validates immutable connection options before the facade starts reconnect state.
   */
  private validateConnectOptions(): void {
    if (this.validatedClientOptions !== undefined) return;
    const normalizedOptions = normalizeClientOptions(this.clientOptions);
    const webSocketEndpoint = normalizeWebSocketEndpoint(this.clientOptions.url, this.clientOptions.protocols);
    this.validatedClientOptions = {
      ...this.clientOptions,
      webSocketEndpoint,
      normalizedOptions
    } as PreparedRSocketClientOptions;
  }

  /**
   * Returns this instance narrowed to the connected state surface.
   */
  private connectedFacade(): ConnectedRSocket {
    return this.connectedSurface;
  }

  /**
   * Returns this instance narrowed to the disconnected state surface.
   */
  private disconnectedFacade(): DisconnectedRSocket {
    return this as unknown as DisconnectedRSocket;
  }
}

/**
 * Builds request options that force composite metadata without spreading.
 */
function compositeMetadataOptions(options: RSocketRequestOptions): RSocketRequestOptions {
  const next: RSocketRequestOptions = {
    metadataMimeType: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
  };
  if (options.dataMimeType !== undefined) {
    (next as { dataMimeType?: RSocketRequestOptions["dataMimeType"] }).dataMimeType = options.dataMimeType;
  }
  if (options.timeout !== undefined) {
    (next as { timeout?: RSocketRequestOptions["timeout"] }).timeout = options.timeout;
  }
  return next;
}

/**
 * Normalizes optional request options while keeping the no-options path allocation-free.
 */
function normalizeFacadeRequestOptions(options: RSocketRequestOptions | null | undefined): RSocketRequestOptions {
  return options == null || options === EMPTY_REQUEST_OPTIONS ? EMPTY_REQUEST_OPTIONS : normalizeRequestOptions(options);
}

/**
 * Detects a responder ERROR frame that explicitly rejects protocol Resume.
 */
function isRejectedResume(error: unknown): boolean {
  return typeof error === "object" &&
    error !== null &&
    (error as { readonly code?: unknown }).code === FrameErrorCode.REJECTED_RESUME;
}
