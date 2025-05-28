import {Flux, Publisher, Sink, Sinks, Subscriber, Subscription} from "reactor-core-ts";

type Handler = (value?: any) => void

export class Emitter<T> implements Sink<T>, Publisher<T>, Subscription {
    private readonly sink = Sinks.many().multicast<T>()
    private readonly handlers = new Map<'next' | 'error' | 'complete', Set<Handler>>()
    private readonly subscription = Flux.from(this.sink).subscribe({
        onNext: (value: T) => this.handlers.get('next')?.forEach?.(handler => handler(value)),
        onError: (error: Error) => this.handlers.get('error')?.forEach?.(handler => handler(error)),
        onComplete: () => this.handlers.get('complete')?.forEach?.(handler => handler())
    })

    public addEmitHandler(type: 'next', handler: (value: T) => void): void;
    public addEmitHandler(type: 'error', handler: (value: Error) => void): void;
    public addEmitHandler(type: 'complete', handler: () => void): void;
    public addEmitHandler(type: 'next' | 'error' | 'complete', handler: Handler): void {
        const handlers = this.handlers.get(type) || new Set<Handler>()
        handlers.add(handler)
        this.handlers.set(type, handlers)
    }

    public removeEmitHandler(type: 'next', handler: (value: T) => void): void;
    public removeEmitHandler(type: 'error', handler: (value: Error) => void): void;
    public removeEmitHandler(type: 'complete', handler: () => void): void;
    public removeEmitHandler(type: 'next' | 'error' | 'complete', handler: Handler): void {
        this.handlers.get(type)?.delete?.(handler)
    }

    public next(value: T) {
        this.sink.next(value)
    }

    public error(error: Error) {
        this.sink.error(error)
    }

    public complete(): void {
        this.sink.complete()
    }

    public subscribe(subscriber: Subscriber<T>): Subscription {
        return this.sink.subscribe(subscriber)
    }

    public unsubscribe() {
        this.subscription.unsubscribe()
    }

    public request(count: number) {
        this.subscription.request(count)
    }
}