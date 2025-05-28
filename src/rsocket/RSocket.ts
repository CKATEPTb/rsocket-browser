import {Flux, Mono} from "reactor-core-ts";
import {Metadata} from "rsocket-frames-ts";

export interface RSocket<M, P> {
    fireAndForget(metadata?: Mono<M>, payload?: Mono<P>): void

    requestResponse<R>(metadata?: Mono<M>, payload?: Mono<P>): Mono<R>

    requestStream<R>(metadata?: Mono<M>, payload?: Mono<P>): Flux<R>

    requestChannel<R>(payloads: Flux<P>, metadata?: Mono<M>, payload?: Mono<P>): Flux<R>

    metadataPush(metadata: Mono<Metadata<any>>): void

    disconnect(): void
}