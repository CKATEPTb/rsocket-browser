/**
 * Browser lifecycle signals used to make reconnect more resilient on mobile.
 */

/**
 * Listener invoked when the browser may have regained execution or network.
 */
export type RSocketBrowserWakeListener = () => void;

/**
 * Browser signal surface consumed by the high-level reconnect loop.
 */
export interface RSocketBrowserReconnectSignals {
    /** Returns `false` only when the browser explicitly reports offline state. */
    isOnline(): boolean;

    /** Resolves immediately when online, otherwise waits for a browser wake signal. */
    waitUntilOnline(signal?: AbortSignal): Promise<void>;

    /** Subscribes to browser wake/network events that should re-check the socket. */
    onWake(listener: RSocketBrowserWakeListener): () => void;
}

/**
 * Shared browser signal source.
 */
export const browserReconnectSignals: RSocketBrowserReconnectSignals = {
    isOnline,
    waitUntilOnline,
    onWake
};

/** Shared cleanup used when no browser wake targets exist. */
const NOOP = (): void => undefined;
/** Shared resolved promise for the common already-online reconnect path. */
const ONLINE_PROMISE = Promise.resolve();

/**
 * Checks the browser network hint without treating missing APIs as offline.
 */
function isOnline(): boolean {
    const navigatorLike = globalThis.navigator as ({ readonly onLine?: boolean } | undefined);
    return navigatorLike?.onLine !== false;
}

/**
 * Waits until browser events suggest the network can be tried again.
 */
function waitUntilOnline(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(reconnectAbortError());
    if (isOnline()) return ONLINE_PROMISE;
    return new Promise<void>((resolve, reject) => {
        let settled = false;
        let unsubscribe: (() => void) | undefined;

        const cleanup = (): void => {
            unsubscribe?.();
            signal?.removeEventListener("abort", onAbort);
        };
        const finish = (callback: () => void): void => {
            if (settled) return;
            settled = true;
            cleanup();
            callback();
        };
        const tryResolve = (): void => {
            if (isOnline()) finish(resolve);
        };
        const onAbort = (): void => {
            finish(() => reject(reconnectAbortError()));
        };

        unsubscribe = onWake(tryResolve);
        signal?.addEventListener("abort", onAbort, {once: true});
        if (signal?.aborted) onAbort();
        else tryResolve();
    });
}

/**
 * Creates a standard abort error for reconnect waits.
 */
function reconnectAbortError(): DOMException {
    return new DOMException("Reconnect wait aborted", "AbortError");
}

/**
 * Registers wake listeners on the browser globals that exist in this runtime.
 */
function onWake(listener: RSocketBrowserWakeListener): () => void {
    const globalTarget = eventTarget(globalThis);
    const documentTarget = eventTarget(globalThis.document);
    if (globalTarget === undefined && documentTarget === undefined) return NOOP;

    const cleanups: Array<() => void> = [];
    for (const type of ["online", "pageshow", "focus"] as const) {
        if (globalTarget !== undefined) cleanups.push(addListener(globalTarget, type, listener));
    }

    if (documentTarget !== undefined) {
        cleanups.push(addListener(documentTarget, "visibilitychange", () => {
            if (globalThis.document?.visibilityState !== "hidden") listener();
        }));
    }

    return () => {
        let cleanup: (() => void) | undefined;
        while ((cleanup = cleanups.pop()) !== undefined) cleanup();
    };
}

/**
 * Narrows browser-like globals to the event target operations we need.
 */
function eventTarget(value: unknown): EventTargetLike | undefined {
    if (
        value !== undefined
        && value !== null
        && typeof (value as EventTargetLike).addEventListener === "function"
        && typeof (value as EventTargetLike).removeEventListener === "function"
    ) {
        return value as EventTargetLike;
    }
    return undefined;
}

/**
 * Adds one listener and returns its cleanup callback.
 */
function addListener(
    target: EventTargetLike,
    type: string,
    listener: EventListener
): () => void {
    target.addEventListener(type, listener);
    return () => target.removeEventListener(type, listener);
}

/**
 * Minimal event target shape used to avoid DOM assumptions in tests and SSR.
 */
interface EventTargetLike {
    addEventListener(type: string, listener: EventListener): void;

    removeEventListener(type: string, listener: EventListener): void;
}
