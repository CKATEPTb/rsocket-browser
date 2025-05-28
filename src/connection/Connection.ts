import {RSocket} from "@/rsocket/RSocket";
import {Flux, Mono, Schedulers, Sinks} from "reactor-core-ts";
import {
    CancelFrame,
    FireAndForgetFlag,
    Frame,
    FrameDeserializer,
    FrameType,
    KeepaliveFlag,
    KeepaliveFrame,
    Metadata,
    MetadataPushFrame,
    MimeType,
    Payload,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFlag,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFlag,
    RequestResponseFrame,
    RequestStreamFlag,
    RequestStreamFrame,
    SetupFlag,
    SetupFrame
} from "rsocket-frames-ts";
import {Emitter} from "@/emitter/Emitter";

export type ConnectionOptions<M, P> = {
    url: string,
    setup: {
        keepAlive: number
        lifetime: number
        mimetype: {
            metadata: MimeType<M>,
            payload: MimeType<P>
        }
        version?: {
            major?: number
            minor?: number
        }
    }
}

export class Connection<M, P> implements RSocket<M, P> {
    private _streamId = -1
    protected readonly requests = new Emitter<Frame>()
    protected readonly responses = new Emitter<Frame>()

    public constructor(protected readonly options: ConnectionOptions<M, P>, metadata?: Mono<M>, payload?: Mono<P>) {
        this.websocket()
        this.setup(metadata, payload)
        this.keepAlive()
    }

    protected websocket() {
        const websocket = new WebSocket(this.options.url)
        websocket.binaryType = 'arraybuffer'
        websocket.addEventListener("open", (_) => {
            console.debug('websocket open event', websocket)
            this.requests.addEmitHandler('next', (frame: Frame) => {
                console.debug('websocket send event', frame, frame.toUint8Array())
                websocket.send(frame.toUint8Array())
            })
            this.requests.addEmitHandler('complete', () => {
                websocket.close()
            })
            this.requests.request(Number.MAX_SAFE_INTEGER)
        })
        websocket.addEventListener("error", ev => {
            console.debug('websocket error event', websocket, ev)
        })
        websocket.addEventListener("close", ev => {
            console.debug('websocket close event', websocket, ev)
            this.responses.complete()
        })
        websocket.addEventListener("message", ev => {
            this.responses.next(FrameDeserializer.deserialize(
                new Uint8Array(ev.data),
                this.options.setup.mimetype.metadata,
                this.options.setup.mimetype.payload
            ))
        })
    }

    public fireAndForget(metadata?: Mono<M>, payload?: Mono<P>): void {
        this.normalize(metadata, payload)
            .subscribe({
                onNext: ({metadata, payload}) =>
                    this.requests.next(new RequestFireAndForgetFrame(this.streamId, FireAndForgetFlag.NONE, metadata, payload))
            })
            .request(1)
    }

    public requestResponse<R>(metadata?: Mono<M>, payload?: Mono<P>): Mono<R> {
        return this.normalize(metadata, payload)
            .flatMap(({metadata, payload}) => {
                const streamId = this.streamId
                this.requests.next(new RequestResponseFrame(streamId, RequestResponseFlag.NONE, metadata, payload))
                return Flux.from(this.responses)
                    .filter(frame => frame.header.streamId == streamId)
                    .mapNotNull(frame => frame.payload?.payload)
                    .first()
                    .cast<R>()
            })
    }

    public requestStream<R>(metadata?: Mono<M>, payload?: Mono<P>): Flux<R> {
        return this.normalize(metadata, payload)
            .flatMapMany(({metadata, payload}) => {
                const streamId = this.streamId
                this.requests.next(new RequestStreamFrame(streamId, RequestStreamFlag.NONE, 0, metadata, payload))
                const sink = Sinks.many().multicast<R>()
                let completed = false
                const server = Flux.from(this.responses)
                    .filter(frame => ((frame.type == FrameType.PAYLOAD) || (frame.type == FrameType.CANCEL)) &&
                        frame.header.streamId == streamId)
                    .cast<PayloadFrame>()
                    .subscribe({
                        onNext: (frame: PayloadFrame) => {
                            if (frame.type == FrameType.CANCEL) {
                                server.unsubscribe()
                                sink.complete()
                            } else {
                                if (frame.isNext()) {
                                    const payload = frame.payload?.payload
                                    if (payload != undefined) sink.next(payload)
                                }
                                if (frame.isComplete()) {
                                    completed = true
                                    sink.complete()
                                }
                            }
                        },
                        onComplete: () => {
                            sink.complete()
                        }
                    })
                return Flux.from(sink)
                    .doOnSubscribe(subscription => {
                        server.request(Number.MAX_SAFE_INTEGER)
                        const request = subscription.request
                        subscription.request = (count: number) => {
                            request(count)
                            this.requests.next(new RequestNFrame(streamId, count))
                        }
                        const unsubscribe = subscription.unsubscribe
                        subscription.unsubscribe = () => {
                            unsubscribe()
                            if (!completed) this.requests.next(new CancelFrame(streamId))
                            server.unsubscribe()
                        }
                    })
            })
    }

    public requestChannel<R>(payloads: Flux<P>, metadata?: Mono<M>, payload?: Mono<P>): Flux<R> {
        return this.normalize(metadata, payload)
            .flatMapMany(({metadata, payload}) => {
                const streamId = this.streamId
                this.requests.next(new RequestChannelFrame(streamId, RequestChannelFlag.NONE, 0, metadata, payload))
                // todo подумать как реализовать unsubscribe без complete
                const client = payloads.subscribe({
                    onNext: data => {
                        this.requests.next(new PayloadFrame(streamId, PayloadFlag.NEXT, undefined, this.options.setup.mimetype.payload.toPayload(data)))
                    },
                    onComplete: () => {
                        this.requests.next(new PayloadFrame(streamId, PayloadFlag.COMPLETE))
                    }
                })
                const sink = Sinks.many().multicast<R>()
                let completed = false
                const server = Flux.from(this.responses)
                    .filter(frame => ((frame.type == FrameType.CANCEL) ||
                            (frame.type == FrameType.PAYLOAD) ||
                            (frame.type == FrameType.REQUEST_N)) &&
                        frame.header.streamId == streamId)
                    .map(frame => {
                        const payload = frame?.payload?.payload
                        switch (frame.type) {
                            case FrameType.PAYLOAD: {
                                if (frame.isFlagSet(PayloadFlag.NEXT)) sink.next(payload)
                                if (frame.isFlagSet(PayloadFlag.COMPLETE)) {
                                    completed = true
                                    sink.complete()
                                }
                                break
                            }
                            case FrameType.CANCEL: {
                                client.unsubscribe()
                                break
                            }
                            case FrameType.REQUEST_N: {
                                client.request(Number(payload))
                                break
                            }
                        }
                    })
                    .subscribe()
                return Flux.from(sink)
                    .doOnSubscribe(subscription => {
                        server.request(Number.MAX_SAFE_INTEGER)
                        const request = subscription.request
                        subscription.request = (count: number) => {
                            request(count)
                            this.requests.next(new RequestNFrame(streamId, count))
                        }
                        const unsubscribe = subscription.unsubscribe
                        subscription.unsubscribe = () => {
                            unsubscribe()
                            if (!completed) this.requests.next(new CancelFrame(streamId))
                            server.unsubscribe()
                        }
                    })
            })
    }

    public metadataPush(metadata: Mono<Metadata<any>>): void {
        metadata.subscribe({
            onNext: value => this.requests.next(new MetadataPushFrame(value))
        }).request(1)
    }

    public disconnect(): void {
        this.requests.complete()
    }

    protected get streamId() {
        return this._streamId += 2
    }

    protected setup(metadata?: Mono<M>, payload?: Mono<P>): void {
        this.normalize(metadata, payload)
            .subscribe({
                onNext: ({metadata, payload}) => {
                    const setup = this.options.setup
                    const mimetype = setup.mimetype
                    const version = setup.version
                    this.requests.next(new SetupFrame(
                        setup.keepAlive, setup.lifetime,
                        mimetype.metadata, mimetype.payload,
                        undefined,
                        version?.major, version?.minor,
                        SetupFlag.NONE,
                        metadata, payload
                    ))
                }
            })
            .request(1)
    }

    protected keepAlive() {
        const request = new KeepaliveFrame(KeepaliveFlag.RESPOND)
        const respond = new KeepaliveFrame()
        const schedulers: Array<{ cancel: () => void }> = []
        Flux.from(this.responses)
            .filter(frame => frame.type == FrameType.KEEPALIVE)
            .cast<KeepaliveFrame>()
            .doOnNext(frame => {
                if (frame.isRequireRespond()) {
                    this.requests.next(respond)
                } else {
                    schedulers.pop()?.cancel?.()
                    schedulers.push(Schedulers.delay(this.options.setup.keepAlive).schedule(() => {
                        schedulers.pop()
                        schedulers.push(Schedulers.delay(this.options.setup.lifetime).schedule(() => {
                            schedulers.pop()
                            this.disconnect()
                        }))
                        this.requests.next(request)
                    }))
                }
            })
            .doFinally(() => schedulers.forEach(scheduler => scheduler.cancel()))
            .subscribe()
            .request(Number.MAX_SAFE_INTEGER)
        this.responses.next(respond)
    }

    private normalize(metadata?: Mono<M>, payload?: Mono<P>): Mono<{ metadata?: Metadata<M>, payload?: Payload<P> }> {
        return (metadata || Mono.empty()).switchIfEmpty(Mono.just(0).cast<M>())
            .zipWith((payload || Mono.empty()).switchIfEmpty(Mono.just(0).cast<P>()))
            .map(tuple => {
                const mimetype = this.options.setup.mimetype
                return {
                    metadata: tuple[0] ? mimetype.metadata.toMetadata(tuple[0]) : undefined,
                    payload: tuple[1] ? mimetype.payload.toPayload(tuple[1]) : undefined
                }
            })
    }
}