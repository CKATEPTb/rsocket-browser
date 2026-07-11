/**
 * Factory helpers for declarative RSocket controllers.
 *
 * These helpers keep route metadata, payload construction, response decoding,
 * request options, and per-controller logging in one reusable object.
 */
import type {
  FireAndForgetControllerDefinition,
  RequestChannelControllerDefinition,
  RequestResponseControllerDefinition,
  RequestStreamControllerDefinition,
  RSocketChannelFactory,
  RSocketControllerKind,
  RSocketControllerRoute,
  RSocketDataFactory,
  RSocketPayloadDecoder,
  RSocketPayloadFactory
} from "@/controllers/types.js";
import type {
  RSocketPayloadFrame,
  RSocketRequestOptions,
  RSocketStreamRequestOptions
} from "@/types/index.js";
import { routeChannelInputFactory, routePayloadFactory } from "@/controllers/route.js";
import {
  controllerLogDefaults,
  normalizeLogOptions,
  type NormalizedRSocketLogOptions,
  type RSocketLogInput
} from "@/logging/index.js";
import {
  FireAndForgetController,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController
} from "@/controllers/classes.js";
import { identityPayload } from "@/controllers/identity.js";

type DecodeOrOptions<Result> = RSocketPayloadDecoder<Result> | RSocketRequestOptions | undefined;
type StreamDecodeOrOptions<Result> = RSocketPayloadDecoder<Result> | RSocketStreamRequestOptions | undefined;

/**
 * Creates a fire-and-forget controller from a route plus data factory.
 */
export function fireAndForgetController<Args extends readonly unknown[]>(
  route: RSocketControllerRoute,
  data: RSocketDataFactory<Args>,
  options?: RSocketRequestOptions
): FireAndForgetControllerDefinition<Args>;
export function fireAndForgetController<Args extends readonly unknown[]>(
  payload: RSocketPayloadFactory<Args>,
  options?: RSocketRequestOptions
): FireAndForgetControllerDefinition<Args>;
export function fireAndForgetController<Args extends readonly unknown[]>(
  routeOrPayload: RSocketControllerRoute | RSocketPayloadFactory<Args>,
  dataOrOptions?: RSocketDataFactory<Args> | RSocketRequestOptions,
  maybeOptions?: RSocketRequestOptions
): FireAndForgetControllerDefinition<Args> {
  if (isRoute(routeOrPayload)) {
    const data = dataOrOptions as RSocketDataFactory<Args>;
    const routedPayload = routePayloadFactory(routeOrPayload);
    return addOptions(
      {
        kind: "fireAndForget",
        route: routeOrPayload,
        payload: (...args: Args) => routedPayload(data(...args))
      },
      maybeOptions
    );
  }

  return addOptions(
    {
      kind: "fireAndForget",
      payload: routeOrPayload
    },
    dataOrOptions as RSocketRequestOptions | undefined
  );
}

/**
 * Creates a request-response controller.
 *
 * When a decoder is omitted, `RSocket.requestResponse(...)` resolves with the raw
 * `RSocketPayloadFrame`; when a decoder is provided, the `Mono` emits the
 * decoder result.
 */
export function requestResponseController<Args extends readonly unknown[]>(
  route: RSocketControllerRoute,
  data: RSocketDataFactory<Args>,
  options?: RSocketRequestOptions
): RequestResponseControllerDefinition<Args, RSocketPayloadFrame>;
export function requestResponseController<Args extends readonly unknown[], Result>(
  route: RSocketControllerRoute,
  data: RSocketDataFactory<Args>,
  decode: RSocketPayloadDecoder<Result>,
  options?: RSocketRequestOptions
): RequestResponseControllerDefinition<Args, Result>;
export function requestResponseController<Args extends readonly unknown[]>(
  payload: RSocketPayloadFactory<Args>,
  options?: RSocketRequestOptions
): RequestResponseControllerDefinition<Args, RSocketPayloadFrame>;
export function requestResponseController<Args extends readonly unknown[], Result>(
  payload: RSocketPayloadFactory<Args>,
  decode: RSocketPayloadDecoder<Result>,
  options?: RSocketRequestOptions
): RequestResponseControllerDefinition<Args, Result>;
export function requestResponseController<Args extends readonly unknown[], Result = RSocketPayloadFrame>(
  routeOrPayload: RSocketControllerRoute | RSocketPayloadFactory<Args>,
  dataOrDecodeOrOptions?: RSocketDataFactory<Args> | DecodeOrOptions<Result>,
  decodeOrOptions?: DecodeOrOptions<Result>,
  maybeOptions?: RSocketRequestOptions
): RequestResponseControllerDefinition<Args, Result | RSocketPayloadFrame> {
  if (isRoute(routeOrPayload)) {
    const data = dataOrDecodeOrOptions as RSocketDataFactory<Args>;
    const { decode, options } = normalizeDecode(decodeOrOptions, maybeOptions);
    const routedPayload = routePayloadFactory(routeOrPayload);
    return addOptions(
      {
        kind: "requestResponse",
        route: routeOrPayload,
        payload: (...args: Args) => routedPayload(data(...args)),
        decode
      },
      options
    );
  }

  const { decode, options } = normalizeDecode(
    dataOrDecodeOrOptions as DecodeOrOptions<Result>,
    decodeOrOptions as RSocketRequestOptions | undefined
  );
  return addOptions(
    {
      kind: "requestResponse",
      payload: routeOrPayload,
      decode
    },
    options
  );
}

/**
 * Creates a request-stream controller.
 *
 * The controller delays execution until a subscriber requests demand from the
 * returned `Flux`, preserving RSocket backpressure semantics.
 */
export function requestStreamController<Args extends readonly unknown[]>(
  route: RSocketControllerRoute,
  data: RSocketDataFactory<Args>,
  options?: RSocketStreamRequestOptions
): RequestStreamControllerDefinition<Args, RSocketPayloadFrame>;
export function requestStreamController<Args extends readonly unknown[], Result>(
  route: RSocketControllerRoute,
  data: RSocketDataFactory<Args>,
  decode: RSocketPayloadDecoder<Result>,
  options?: RSocketStreamRequestOptions
): RequestStreamControllerDefinition<Args, Result>;
export function requestStreamController<Args extends readonly unknown[]>(
  payload: RSocketPayloadFactory<Args>,
  options?: RSocketStreamRequestOptions
): RequestStreamControllerDefinition<Args, RSocketPayloadFrame>;
export function requestStreamController<Args extends readonly unknown[], Result>(
  payload: RSocketPayloadFactory<Args>,
  decode: RSocketPayloadDecoder<Result>,
  options?: RSocketStreamRequestOptions
): RequestStreamControllerDefinition<Args, Result>;
export function requestStreamController<Args extends readonly unknown[], Result = RSocketPayloadFrame>(
  routeOrPayload: RSocketControllerRoute | RSocketPayloadFactory<Args>,
  dataOrDecodeOrOptions?: RSocketDataFactory<Args> | StreamDecodeOrOptions<Result>,
  decodeOrOptions?: StreamDecodeOrOptions<Result>,
  maybeOptions?: RSocketStreamRequestOptions
): RequestStreamControllerDefinition<Args, Result | RSocketPayloadFrame> {
  if (isRoute(routeOrPayload)) {
    const data = dataOrDecodeOrOptions as RSocketDataFactory<Args>;
    const { decode, options } = normalizeDecode(decodeOrOptions, maybeOptions);
    const routedPayload = routePayloadFactory(routeOrPayload);
    return addOptions(
      {
        kind: "requestStream",
        route: routeOrPayload,
        payload: (...args: Args) => routedPayload(data(...args)),
        decode
      },
      options
    );
  }

  const { decode, options } = normalizeDecode(
    dataOrDecodeOrOptions as StreamDecodeOrOptions<Result>,
    decodeOrOptions as RSocketStreamRequestOptions | undefined
  );
  return addOptions(
    {
      kind: "requestStream",
      payload: routeOrPayload,
      decode
    },
    options
  );
}

/**
 * Creates a request-channel controller.
 *
 * Route-based overloads automatically prepend an initial route-only payload to
 * the outbound channel, which matches common Spring RSocket controller routing.
 */
export function requestChannelController<Args extends readonly unknown[]>(
  route: RSocketControllerRoute,
  input: RSocketChannelFactory<Args>,
  options?: RSocketStreamRequestOptions
): RequestChannelControllerDefinition<Args, RSocketPayloadFrame>;
export function requestChannelController<Args extends readonly unknown[], Result>(
  route: RSocketControllerRoute,
  input: RSocketChannelFactory<Args>,
  decode: RSocketPayloadDecoder<Result>,
  options?: RSocketStreamRequestOptions
): RequestChannelControllerDefinition<Args, Result>;
export function requestChannelController<Args extends readonly unknown[]>(
  input: RSocketChannelFactory<Args>,
  options?: RSocketStreamRequestOptions
): RequestChannelControllerDefinition<Args, RSocketPayloadFrame>;
export function requestChannelController<Args extends readonly unknown[], Result>(
  input: RSocketChannelFactory<Args>,
  decode: RSocketPayloadDecoder<Result>,
  options?: RSocketStreamRequestOptions
): RequestChannelControllerDefinition<Args, Result>;
export function requestChannelController<Args extends readonly unknown[], Result = RSocketPayloadFrame>(
  routeOrInput: RSocketControllerRoute | RSocketChannelFactory<Args>,
  inputOrDecodeOrOptions?: RSocketChannelFactory<Args> | StreamDecodeOrOptions<Result>,
  decodeOrOptions?: StreamDecodeOrOptions<Result>,
  maybeOptions?: RSocketStreamRequestOptions
): RequestChannelControllerDefinition<Args, Result | RSocketPayloadFrame> {
  if (isRoute(routeOrInput)) {
    const input = inputOrDecodeOrOptions as RSocketChannelFactory<Args>;
    const { decode, options } = normalizeDecode(decodeOrOptions, maybeOptions);
    const routedInput = routeChannelInputFactory(routeOrInput);
    return addOptions(
      {
        kind: "requestChannel",
        route: routeOrInput,
        input: (...args: Args) => routedInput(input(...args)),
        decode
      },
      options
    );
  }

  const { decode, options } = normalizeDecode(
    inputOrDecodeOrOptions as StreamDecodeOrOptions<Result>,
    decodeOrOptions as RSocketStreamRequestOptions | undefined
  );
  return addOptions(
    {
      kind: "requestChannel",
      input: routeOrInput,
      decode
    },
    options
  );
}

/**
 * Grouped controller helpers for internal composition and advanced imports.
 */
export const controller = Object.freeze({
  FireAndForgetController,
  RequestResponseController,
  RequestStreamController,
  RequestChannelController,
  fireAndForget: fireAndForgetController,
  requestResponse: requestResponseController,
  requestStream: requestStreamController,
  requestChannel: requestChannelController
});

/**
 * Normalizes a decoder-or-options overload into explicit decoder and options.
 */
function normalizeDecode<Result, Options extends RSocketRequestOptions>(
  decodeOrOptions: RSocketPayloadDecoder<Result> | Options | undefined,
  options: Options | undefined
): { decode: RSocketPayloadDecoder<Result | RSocketPayloadFrame>; options?: Options } {
  if (typeof decodeOrOptions === "function") {
    return optionalOptions({ decode: decodeOrOptions }, options);
  }

  return optionalOptions({ decode: identityPayload }, decodeOrOptions);
}

/**
 * Attaches request options and the controller-local `.log(...)` method.
 */
function addOptions<T extends ControllerLogState, Options extends RSocketRequestOptions>(
  value: T,
  options: Options | undefined
): T & { readonly options?: Options; log(options?: RSocketLogInput): any } {
  return withControllerLog(optionalOptions(value, options)) as T & {
    readonly options?: Options;
    log(options?: RSocketLogInput): any;
  };
}

/**
 * Adds an `options` property only when the caller supplied one.
 */
function optionalOptions<T extends object, Options extends RSocketRequestOptions>(
  value: T,
  options: Options | undefined
): T & { readonly options?: Options } {
  const target = value as T & { options?: Options };
  if (options !== undefined) target.options = options;
  return target;
}

/**
 * Distinguishes route overloads from payload factory overloads.
 */
function isRoute(value: unknown): value is RSocketControllerRoute {
  return typeof value === "string" || Array.isArray(value);
}

/**
 * Shape used internally while cloning logged controllers.
 */
interface ControllerLogState {
  /** Controller interaction kind used to derive log categories. */
  readonly kind: RSocketControllerKind;
  /** Existing normalized logging options to merge with new options. */
  readonly logging?: NormalizedRSocketLogOptions;
  /** Optional existing log method omitted while cloning the base object. */
  log?: (options?: RSocketLogInput) => unknown;
}

/**
 * Attaches a `.log(...)` method and clones only when the caller changes logging.
 */
function withControllerLog<T extends ControllerLogState>(value: T): T {
  const target = value as T & { log(options?: RSocketLogInput): T };
  target.log = (options: RSocketLogInput = true): T => {
    const logging = normalizeLogOptions(
      options,
      controllerLogDefaults(target.kind),
      target.logging
    );
    return withControllerLog(controllerLogCopy(target, logging.enabled ? logging : undefined));
  };

  return target;
}

/**
 * Copies a controller while replacing or removing normalized logging state.
 */
function controllerLogCopy<T extends ControllerLogState>(
  value: T,
  logging: NormalizedRSocketLogOptions | undefined
): T {
  const next = { ...value } as T & ControllerLogState;
  delete next.log;
  if (logging === undefined) delete (next as { logging?: NormalizedRSocketLogOptions }).logging;
  else (next as { logging?: NormalizedRSocketLogOptions }).logging = logging;
  return next as T;
}
