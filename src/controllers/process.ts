/**
 * Runtime executor for declarative controllers.
 *
 * It converts a typed controller object into the matching low-level RSocket
 * interaction and wires optional controller-local Reactor-style logging.
 */
import {type Flux, Mono} from "reactor-core-ts";
import {emitLog, interactionLogEvent, type NormalizedRSocketLogOptions} from "@/logging/index.js";
import type {AnyClassController} from "@/controllers/classes.js";
import type {
    ControllerArgs,
    ControllerReturn,
    RSocketControllerConnection,
    RSocketPayloadDecoder
} from "@/controllers/types.js";
import type {RSocketChannelInput, RSocketPayloadFrame} from "@/types/index.js";
import {transformFluxPreservingDemand} from "@/controllers/flux.js";

/**
 * Executes a declarative controller against an RSocket connection.
 *
 * The return type is inferred from the controller kind: fire-and-forget returns
 * `Mono<void>`, request-response returns `Mono<Result>`, and streaming
 * interactions return `Flux<Result>`.
 */
export function processController<C extends AnyClassController>(
    connection: RSocketControllerConnection,
    controller: C,
    args: ControllerArgs<C>
): ControllerReturn<C> {
    switch (controller.kind) {
        case "fireAndForget": {
            const payload = controller.payload(...args);
            return logMono(
                connection.fireAndForget(payload, controller.options),
                controller.logging,
                controller.kind,
                payload
            ) as ControllerReturn<C>;
        }
        case "requestResponse": {
            const payload = controller.payload(...args);
            return logMono(
                decodeMono(connection.requestResponse(payload, controller.options), controller.decode),
                controller.logging,
                controller.kind,
                payload
            ) as ControllerReturn<C>;
        }
        case "requestStream": {
            const payload = controller.payload(...args);
            return logFlux(
                decodeFlux(connection.requestStream(payload, controller.options), controller.decode),
                controller.logging,
                controller.kind,
                payload
            ) as ControllerReturn<C>;
        }
        case "requestChannel": {
            const input = controller.input(args[0] as RSocketChannelInput<any, any>);
            return logFlux(
                decodeFlux(connection.requestChannel(input, controller.options), controller.decode),
                controller.logging,
                controller.kind,
                input
            ) as ControllerReturn<C>;
        }
    }
}

/**
 * Applies a request-response controller decoder.
 */
function decodeMono<Result>(
    source: Mono<RSocketPayloadFrame>,
    decode: RSocketPayloadDecoder<Result>
): Mono<Result> {
    return source.map(decode);
}

/**
 * Applies a streaming controller decoder.
 */
function decodeFlux<Result>(
    source: Flux<RSocketPayloadFrame>,
    decode: RSocketPayloadDecoder<Result>
): Flux<Result> {
    return transformFluxPreservingDemand(source, decode);
}

/**
 * Adds controller interaction logs around a `Mono` without subscribing early.
 */
function logMono<T>(
    source: Mono<T>,
    logging: NormalizedRSocketLogOptions | undefined,
    interaction: AnyClassController["kind"],
    payload: unknown
): Mono<T> {
    if (logging === undefined || !logging.enabled || !logging.interactions) return source;
    const loggedPayload = logging.payload ? payload : undefined;
    return source
        .doOnSubscribe(() => logInteraction(logging, interaction, "send", loggedPayload))
        .doOnNext((value) => logInteraction(logging, interaction, "receive", undefined, value))
        .doOnError((error) => logInteraction(logging, interaction, "error", undefined, undefined, error))
        .doFinally((signal) => {
            if (signal === "complete") logInteraction(logging, interaction, "complete");
        });
}

/**
 * Adds controller interaction logs to a `Flux` while preserving demand-driven
 * subscription behavior.
 */
function logFlux<T>(
    source: Flux<T>,
    logging: NormalizedRSocketLogOptions | undefined,
    interaction: AnyClassController["kind"],
    payload: unknown
): Flux<T> {
    if (logging === undefined || !logging.enabled || !logging.interactions) return source;
    const loggedPayload = logging.payload ? payload : undefined;
    return transformFluxPreservingDemand(source, (value) => value, {
        onSubscribe: () => logInteraction(logging, interaction, "send", loggedPayload),
        onNext: (value) => logInteraction(logging, interaction, "receive", undefined, value),
        onComplete: () => logInteraction(logging, interaction, "complete"),
        onError: (error) => logInteraction(logging, interaction, "error", undefined, undefined, error)
    });
}

/**
 * Emits one normalized controller interaction log event.
 */
function logInteraction(
    logging: NormalizedRSocketLogOptions | undefined,
    interaction: AnyClassController["kind"],
    stage: "send" | "receive" | "complete" | "error",
    payload?: unknown,
    value?: unknown,
    error?: unknown
): void {
    emitLog(
        logging,
        interactionLogEvent({
            interaction,
            stage,
            payload,
            value,
            error
        })
    );
}
