/**
 * Class-based declarative controller definitions.
 *
 * These classes support a Spring-like declaration style where an application
 * creates a small class, extends the interaction model, and assigns a protected
 * readonly `route` field with the server route name.
 */
import {
  controllerLogDefaults,
  normalizeLogOptions,
  type NormalizedRSocketLogOptions,
  type RSocketLogInput
} from "@/logging/index.js";
import {
  routeChannelInputFactory,
  routePayloadFactory,
  type RSocketRouteChannelInputFactory,
  type RSocketRoutePayloadFactory
} from "@/controllers/route.js";
import type {
  FireAndForgetControllerDefinition,
  RequestChannelControllerDefinition,
  RequestResponseControllerDefinition,
  RequestStreamControllerDefinition,
  RSocketControllerKind,
  RSocketControllerRoute,
  RSocketPayloadDecoder
} from "@/controllers/types.js";
import type {
  RSocketChannelInput,
  RSocketPayloadFrame,
  RSocketPayloadInput,
  RSocketRequestOptions,
  RSocketStreamRequestOptions
} from "@/types/index.js";

/**
 * Positional arguments expected by single-payload class controllers.
 */
export type ControllerPayloadArgs<Request> = [Request] extends [void]
  ? [] | [request: Request]
  : [request: Request];

/**
 * Positional arguments expected by request-channel class controllers.
 */
export type ControllerChannelArgs<Outbound> = [payloads: RSocketChannelInput<Outbound, any>];

/**
 * Union of class-based controller instances.
 */
export type AnyClassController =
  | FireAndForgetController<any>
  | RequestResponseController<any, any>
  | RequestStreamController<any, any>
  | RequestChannelController<any, any>;

/**
 * Shared runtime behavior for class-based controllers.
 */
abstract class RSocketRouteController<Request, Options extends RSocketRequestOptions = RSocketRequestOptions> {
  /** Immutable route metadata declared by each concrete controller class. */
  protected abstract readonly route: RSocketControllerRoute;

  private currentLogging: NormalizedRSocketLogOptions | undefined;
  private cachedChannelInput: RSocketRouteChannelInputFactory | undefined;
  private cachedPayload: RSocketRoutePayloadFactory | undefined;

  /**
   * Creates a route controller with optional per-request encoding options.
   */
  constructor(private readonly requestOptions?: Options) {}

  /**
   * Optional request options passed to the low-level interaction.
   */
  get options(): Options | undefined {
    return this.requestOptions;
  }

  /**
   * Optional normalized logging options used by `processController`.
   */
  get logging(): NormalizedRSocketLogOptions | undefined {
    return this.currentLogging;
  }

  /**
   * Enables, updates, or disables logging for this controller instance.
   */
  log(options: RSocketLogInput = true): this {
    const kind = this.resolveKind();
    const logging = normalizeLogOptions(
      options,
      controllerLogDefaults(kind),
      this.currentLogging
    );
    this.currentLogging = logging.enabled ? logging : undefined;
    return this;
  }

  /**
   * Converts a typed request value into the data part sent to the route.
   */
  protected data(request: Request): unknown {
    return request;
  }

  /**
   * Builds a routed payload from the first process argument.
   */
  protected routedPayload(args: ControllerPayloadArgs<Request>): RSocketPayloadInput<any, any> {
    const request = args.length === 0 ? undefined : this.data(args[0] as Request);
    return this.routePayload()(request);
  }

  /**
   * Prepends cached route metadata to a request-channel input source.
   */
  protected routedChannelInput(input: RSocketChannelInput<any, any>): RSocketChannelInput<any, any> {
    return this.routeChannelInput()(input);
  }

  /**
   * Validates and returns the route declared by the concrete controller.
   */
  protected resolveRoute(): RSocketControllerRoute {
    const route = this.route;
    if (route === undefined || route === "" || (Array.isArray(route) && route.length === 0)) {
      throw new Error(`${this.constructor.name} must define a non-empty protected route field`);
    }
    return route;
  }

  /**
   * Reads the concrete interaction kind for logging.
   */
  private resolveKind(): RSocketControllerKind {
    return (this as unknown as { kind: RSocketControllerKind }).kind;
  }

  /**
   * Returns a route payload builder cached for the current route field.
   */
  private routePayload(): RSocketRoutePayloadFactory {
    return this.cachedPayload ??= routePayloadFactory(this.resolveRoute());
  }

  /**
   * Returns a request-channel input builder cached for the current route field.
   */
  private routeChannelInput(): RSocketRouteChannelInputFactory {
    return this.cachedChannelInput ??= routeChannelInputFactory(this.resolveRoute());
  }
}

/**
 * Base class for fire-and-forget controllers.
 *
 * Extend it and set `protected readonly route = "routeName"` in the subclass.
 */
export abstract class FireAndForgetController<Request = void>
  extends RSocketRouteController<Request>
  implements FireAndForgetControllerDefinition<ControllerPayloadArgs<Request>> {
  /** Identifies the RSocket interaction model. */
  readonly kind = "fireAndForget" as const;

  /**
   * Builds the routed request payload from the typed request argument.
   */
  payload(...args: ControllerPayloadArgs<Request>): RSocketPayloadInput<any, any> {
    return this.routedPayload(args);
  }
}

/**
 * Base class for request-response controllers.
 *
 * The first generic is the request body type and the second generic is the
 * decoded response body type.
 */
export abstract class RequestResponseController<Request = void, Response = unknown>
  extends RSocketRouteController<Request>
  implements RequestResponseControllerDefinition<ControllerPayloadArgs<Request>, Response> {
  /** Identifies the RSocket interaction model. */
  readonly kind = "requestResponse" as const;

  /** Maps the low-level payload frame into the typed response value. */
  readonly decode: RSocketPayloadDecoder<Response> = (payload) => this.response(payload);

  /**
   * Builds the routed request payload from the typed request argument.
   */
  payload(...args: ControllerPayloadArgs<Request>): RSocketPayloadInput<any, any> {
    return this.routedPayload(args);
  }

  /**
   * Converts a response payload frame into the controller response type.
   */
  protected response(payload: RSocketPayloadFrame): Response {
    return payload.data as Response;
  }
}

/**
 * Base class for request-stream controllers.
 *
 * The first generic is the request body type and the second generic is the type
 * emitted for every response payload.
 */
export abstract class RequestStreamController<Request = void, Response = unknown>
  extends RSocketRouteController<Request, RSocketStreamRequestOptions>
  implements RequestStreamControllerDefinition<ControllerPayloadArgs<Request>, Response> {
  /** Identifies the RSocket interaction model. */
  readonly kind = "requestStream" as const;

  /** Maps each low-level payload frame into the typed stream value. */
  readonly decode: RSocketPayloadDecoder<Response> = (payload) => this.response(payload);

  /**
   * Builds the routed request payload from the typed request argument.
   */
  payload(...args: ControllerPayloadArgs<Request>): RSocketPayloadInput<any, any> {
    return this.routedPayload(args);
  }

  /**
   * Converts a response payload frame into the controller response type.
   */
  protected response(payload: RSocketPayloadFrame): Response {
    return payload.data as Response;
  }
}

/**
 * Base class for request-channel controllers.
 *
 * The first generic is the outbound item type and the second generic is the
 * typed response item emitted by the responder.
 */
export abstract class RequestChannelController<Outbound = unknown, Response = unknown>
  extends RSocketRouteController<RSocketChannelInput<Outbound, any>, RSocketStreamRequestOptions>
  implements RequestChannelControllerDefinition<ControllerChannelArgs<Outbound>, Response> {
  /** Identifies the RSocket interaction model. */
  readonly kind = "requestChannel" as const;

  /** Maps each low-level payload frame into the typed response item. */
  readonly decode: RSocketPayloadDecoder<Response> = (payload) => this.response(payload);

  /**
   * Prepends route metadata to the outbound channel publisher.
   */
  input(payloads: RSocketChannelInput<Outbound, any>): RSocketChannelInput<any, any> {
    return this.routedChannelInput(payloads);
  }

  /**
   * Converts a response payload frame into the controller response type.
   */
  protected response(payload: RSocketPayloadFrame): Response {
    return payload.data as Response;
  }
}
