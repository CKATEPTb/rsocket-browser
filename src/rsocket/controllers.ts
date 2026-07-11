/**
 * Controller overload helpers used by the public RSocket facade.
 */
import {
  FireAndForgetController,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController
} from "@/controllers/classes.js";
import type {
  AnyRSocketController,
  RequestChannelControllerDefinition,
  RequestResponseControllerDefinition,
  RequestStreamControllerDefinition,
  RSocketControllerConstructor
} from "@/controllers/index.js";

/**
 * Controller instance or zero-argument controller class accepted by request methods.
 * Class declarations are instantiated once per `RSocket`; pass an explicit
 * instance when application-controlled mutable controller state is required.
 */
export type RSocketControllerInput<C extends AnyRSocketController> = C | RSocketControllerConstructor<C>;

/**
 * Per-socket cache for declarative controller class instances.
 *
 * Class declarations are stateless definitions, like singleton Spring
 * controllers. Reusing their instance also reuses encoded route metadata.
 */
export type RSocketControllerInstanceCache = WeakMap<
  RSocketControllerConstructor<AnyRSocketController>,
  AnyRSocketController
>;

/**
 * Extracts a request-response controller result type.
 */
export type RequestResponseResult<C> =
  C extends RequestResponseControllerDefinition<readonly any[], infer Result> ? Result : never;

/**
 * Extracts a request-stream controller result type.
 */
export type RequestStreamResult<C> =
  C extends RequestStreamControllerDefinition<readonly any[], infer Result> ? Result : never;

/**
 * Extracts a request-channel controller result type.
 */
export type RequestChannelResult<C> =
  C extends RequestChannelControllerDefinition<readonly any[], infer Result> ? Result : never;

/**
 * Rebuilds controller arguments without injecting omitted request options.
 */
export function controllerArgs(argumentCount: number, first: unknown, rest: unknown[]): readonly unknown[] {
  if (argumentCount <= 1) return rest;
  rest.unshift(first);
  return rest;
}

/**
 * Detects controller instances and controller classes passed to request methods.
 */
export function isControllerInput(value: unknown): value is RSocketControllerInput<AnyRSocketController> {
  return isControllerConstructor(value) || isControllerInstance(value);
}

/**
 * Creates a controller instance when a request method receives a controller class.
 */
export function controllerInstance<C extends AnyRSocketController>(
  controllerDefinition: RSocketControllerInput<C>,
  cache?: RSocketControllerInstanceCache
): C {
  if (typeof controllerDefinition === "function") {
    const cached = cache?.get(controllerDefinition);
    if (cached !== undefined) return cached as C;
    const instance = new controllerDefinition();
    cache?.set(controllerDefinition, instance);
    return instance;
  }
  return controllerDefinition;
}

/**
 * Detects a concrete declarative controller instance.
 */
function isControllerInstance(value: unknown): value is AnyRSocketController {
  if (typeof value !== "object" || value === null || !("kind" in value)) return false;
  const controller = value as {
    readonly kind?: unknown;
    readonly payload?: unknown;
    readonly input?: unknown;
    readonly decode?: unknown;
  };
  switch (controller.kind) {
    case "fireAndForget":
      return typeof controller.payload === "function";
    case "requestResponse":
    case "requestStream":
      return typeof controller.payload === "function" && typeof controller.decode === "function";
    case "requestChannel":
      return typeof controller.input === "function" && typeof controller.decode === "function";
    default:
      return false;
  }
}

/**
 * Detects controller classes without treating function-valued application data as constructors.
 */
function isControllerConstructor(value: unknown): value is RSocketControllerConstructor<AnyRSocketController> {
  if (typeof value !== "function") return false;
  const prototype = value.prototype;
  return prototype instanceof FireAndForgetController ||
    prototype instanceof RequestResponseController ||
    prototype instanceof RequestStreamController ||
    prototype instanceof RequestChannelController;
}
