/**
 * Type contracts for declarative controllers executed by `RSocket.process(...)`.
 */
import type {Flux, Mono} from "reactor-core-ts";
import type {RSocketFlux} from "@/stream/index.js";
import type {
    RSocketChannelInput,
    RSocketPayloadFrame,
    RSocketPayloadInput,
    RSocketRequestOptions,
    RSocketStreamRequestOptions
} from "@/types/index.js";
import type {NormalizedRSocketLogOptions, RSocketLogInput} from "@/logging/index.js";

/**
 * Interaction models that can be represented by a declarative controller.
 */
export type RSocketControllerKind =
    | "fireAndForget"
    | "requestResponse"
    | "requestStream"
    | "requestChannel";

/**
 * RSocket routing metadata accepted by class-based controllers.
 *
 * A string creates one `message/x.rsocket.routing.v0` route entry, while an
 * array creates a route from multiple path segments.
 */
export type RSocketControllerRoute = string | readonly string[];

/**
 * Builds a full RSocket payload from strongly typed controller arguments.
 */
export type RSocketPayloadFactory<Args extends readonly unknown[]> = (...args: Args) => RSocketPayloadInput<any, any>;

/**
 * Builds the outbound publisher used by request-channel controllers.
 */
export type RSocketChannelFactory<Args extends readonly unknown[]> = (...args: Args) => RSocketChannelInput<any, any>;

/**
 * Converts a decoded RSocket payload frame into an application-level value.
 */
export type RSocketPayloadDecoder<Result> = (payload: RSocketPayloadFrame) => Result;

/**
 * Declarative fire-and-forget endpoint.
 *
 * The controller produces one request payload and returns `Mono<void>` from
 * `RSocket.process(...)` after the frame is written to the WebSocket.
 */
export interface FireAndForgetControllerDefinition<Args extends readonly unknown[]> {
    /** Identifies the RSocket interaction model. */
    readonly kind: "fireAndForget";
    /** Creates the outbound payload from the arguments passed to the request method. */
    readonly payload: RSocketPayloadFactory<Args>;
    /** Optional MIME overrides or request metadata encoding settings. */
    readonly options?: RSocketRequestOptions | undefined;
    /** Optional per-controller logging configuration. */
    readonly logging?: NormalizedRSocketLogOptions | undefined;

    /** Returns a controller definition with Reactor-style logging enabled. */
    log(options?: RSocketLogInput): FireAndForgetControllerDefinition<Args>;
}

/**
 * Declarative request-response endpoint.
 *
 * The controller produces one request payload and maps the single responder
 * `PAYLOAD` frame into the declared `Result`.
 */
export interface RequestResponseControllerDefinition<Args extends readonly unknown[], Result> {
    /** Identifies the RSocket interaction model. */
    readonly kind: "requestResponse";
    /** Creates the outbound payload from the arguments passed to the request method. */
    readonly payload: RSocketPayloadFactory<Args>;
    /** Maps the responder payload frame into the application result. */
    readonly decode: RSocketPayloadDecoder<Result>;
    /** Optional MIME overrides or request metadata encoding settings. */
    readonly options?: RSocketRequestOptions | undefined;
    /** Optional per-controller logging configuration. */
    readonly logging?: NormalizedRSocketLogOptions | undefined;

    /** Returns a controller definition with Reactor-style logging enabled. */
    log(options?: RSocketLogInput): RequestResponseControllerDefinition<Args, Result>;
}

/**
 * Declarative request-stream endpoint.
 *
 * The controller produces one request payload and maps every responder
 * `PAYLOAD` frame into the declared `Result` stream.
 */
export interface RequestStreamControllerDefinition<Args extends readonly unknown[], Result> {
    /** Identifies the RSocket interaction model. */
    readonly kind: "requestStream";
    /** Creates the outbound payload from the arguments passed to the request method. */
    readonly payload: RSocketPayloadFactory<Args>;
    /** Maps each responder payload frame into an application value. */
    readonly decode: RSocketPayloadDecoder<Result>;
    /** Optional MIME overrides and initial request-n settings. */
    readonly options?: RSocketStreamRequestOptions | undefined;
    /** Optional per-controller logging configuration. */
    readonly logging?: NormalizedRSocketLogOptions | undefined;

    /** Returns a controller definition with Reactor-style logging enabled. */
    log(options?: RSocketLogInput): RequestStreamControllerDefinition<Args, Result>;
}

/**
 * Declarative request-channel endpoint.
 *
 * The controller produces the outbound publisher for the channel and maps every
 * responder `PAYLOAD` frame into the declared `Result` stream.
 */
export interface RequestChannelControllerDefinition<Args extends readonly unknown[], Result> {
    /** Identifies the RSocket interaction model. */
    readonly kind: "requestChannel";
    /** Creates the outbound channel payload source from request method arguments. */
    readonly input: RSocketChannelFactory<Args>;
    /** Maps each responder payload frame into an application value. */
    readonly decode: RSocketPayloadDecoder<Result>;
    /** Optional MIME overrides and initial request-n settings. */
    readonly options?: RSocketStreamRequestOptions | undefined;
    /** Optional per-controller logging configuration. */
    readonly logging?: NormalizedRSocketLogOptions | undefined;

    /** Returns a controller definition with Reactor-style logging enabled. */
    log(options?: RSocketLogInput): RequestChannelControllerDefinition<Args, Result>;
}

/**
 * Union accepted by controller-aware `RSocket` request methods.
 */
export type AnyRSocketController =
    | FireAndForgetControllerDefinition<readonly any[]>
    | RequestResponseControllerDefinition<readonly any[], any>
    | RequestStreamControllerDefinition<readonly any[], any>
    | RequestChannelControllerDefinition<readonly any[], any>;

/**
 * Zero-argument controller class accepted by controller-aware request methods.
 */
export type RSocketControllerConstructor<C extends AnyRSocketController = AnyRSocketController> = new () => C;

/**
 * Extracts the positional argument tuple expected by a controller.
 */
export type ControllerArgs<C> =
    C extends FireAndForgetControllerDefinition<infer Args>
        ? Args
        : C extends RequestResponseControllerDefinition<infer Args, unknown>
            ? Args
            : C extends RequestStreamControllerDefinition<infer Args, unknown>
                ? Args
                : C extends RequestChannelControllerDefinition<infer Args, unknown>
                    ? Args
                    : never;

/**
 * Resolves the exact Reactor return type produced by a declarative controller.
 */
export type ControllerReturn<C> =
    C extends FireAndForgetControllerDefinition<readonly any[]>
        ? Mono<void>
        : C extends RequestResponseControllerDefinition<readonly any[], infer Result>
            ? Mono<Result>
            : C extends RequestStreamControllerDefinition<readonly any[], infer Result>
                ? Flux<Result>
                : C extends RequestChannelControllerDefinition<readonly any[], infer Result>
                    ? Flux<Result>
                    : never;

/**
 * Minimal connection surface required to execute a declarative controller.
 */
export interface RSocketControllerConnection {
    /** Starts a fire-and-forget interaction. */
    fireAndForget(payload: RSocketPayloadInput<any, any>, options?: RSocketRequestOptions): Mono<void>;

    /** Starts a request-response interaction. */
    requestResponse(payload: RSocketPayloadInput<any, any>, options?: RSocketRequestOptions): Mono<RSocketPayloadFrame>;

    /** Starts a request-stream interaction. */
    requestStream(payload: RSocketPayloadInput<any, any>, options?: RSocketStreamRequestOptions): RSocketFlux;

    /** Starts a request-channel interaction. */
    requestChannel(payloads: RSocketChannelInput<any, any>, options?: RSocketStreamRequestOptions): RSocketFlux;
}
