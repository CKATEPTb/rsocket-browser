/**
 * Normalized reconnect configuration.
 */
export interface RSocketReconnectOptions {
    /** Whether automatic reconnect attempts are enabled. */
    readonly enabled: boolean;
    /** Delay before the first reconnect attempt after an unexpected close. */
    readonly initialDelayMs: number;
    /** Minimum delay used for later reconnect attempts. */
    readonly minDelayMs: number;
    /** Maximum delay between reconnect attempts. */
    readonly maxDelayMs: number;
    /** Exponential growth factor applied after the first reconnect attempt. */
    readonly growFactor: number;
    /** Uptime required before the reconnect retry counter is reset. */
    readonly minUptimeMs: number;
    /** Maximum number of reconnect attempts before closing permanently. */
    readonly maxAttempts: number;
    /** Randomization ratio applied to reconnect delays. */
    readonly jitter: number;
}

/**
 * Protocol Resume settings accepted by the public reconnect options.
 */
export type RSocketReconnectResumeInput = number | {
    /** Backend resume state lifetime, in milliseconds. */
    readonly ttl: number;
};

/**
 * Nested reconnect options accepted by `new RSocket(...)`.
 */
export interface RSocketReconnectPolicyInput {
    /** Enables protocol Resume for the given backend state lifetime. */
    readonly resume?: RSocketReconnectResumeInput | false;
}

/**
 * Public reconnect options accepted by `new RSocket(...)`.
 */
export interface RSocketReconnectOptionInput {
    /** Full reconnect settings object or a shortcut boolean. */
    readonly reconnect?: boolean | RSocketReconnectPolicyInput;
}

/**
 * Normalizes reconnect aliases and clamps unsafe values.
 */
export function normalizeReconnectOptions(input: RSocketReconnectOptionInput): RSocketReconnectOptions {
    const legacy = input as RSocketReconnectOptionInput & {
        readonly autoReconnect?: boolean;
        readonly reconnectDelay?: number;
        readonly reconnectDelayMs?: number;
        readonly reconnectMaxDelay?: number;
        readonly reconnectMaxDelayMs?: number;
        readonly reconnectMaxAttempts?: number;
        readonly reconnectJitter?: number;
        readonly reconnectGrowFactor?: number;
        readonly reconnectMinUptime?: number;
        readonly reconnectMinUptimeMs?: number;
    };
    const reconnectObject = typeof input.reconnect === "object" ? input.reconnect : undefined;
    const legacyReconnect = reconnectObject as (RSocketReconnectPolicyInput & Partial<RSocketReconnectOptions> & {
        readonly delay?: number;
        readonly minDelay?: number;
        readonly maxDelay?: number;
        readonly attempts?: number;
        readonly maxRetries?: number;
        readonly minUptime?: number;
        readonly growFactor?: number;
        readonly reconnectionDelayGrowFactor?: number;
    }) | undefined;
    const enabled = legacy.autoReconnect
        ?? (typeof input.reconnect === "boolean" ? input.reconnect : legacyReconnect?.enabled)
        ?? true;
    const initialDelayMs = legacy.reconnectDelayMs
        ?? legacy.reconnectDelay
        ?? legacyReconnect?.initialDelayMs
        ?? legacyReconnect?.delay
        ?? 0;
    const minDelayMs = legacyReconnect?.minDelayMs
        ?? legacyReconnect?.minDelay
        ?? (initialDelayMs > 0 ? initialDelayMs : undefined)
        ?? 3_000;
    const maxDelayMs = legacy.reconnectMaxDelayMs
        ?? legacy.reconnectMaxDelay
        ?? legacyReconnect?.maxDelayMs
        ?? legacyReconnect?.maxDelay
        ?? 10_000;
    const maxAttempts = legacy.reconnectMaxAttempts
        ?? legacyReconnect?.maxAttempts
        ?? legacyReconnect?.attempts
        ?? legacyReconnect?.maxRetries
        ?? Number.POSITIVE_INFINITY;
    const growFactor = legacy.reconnectGrowFactor
        ?? legacyReconnect?.growFactor
        ?? legacyReconnect?.reconnectionDelayGrowFactor
        ?? 1.3;
    const minUptimeMs = legacy.reconnectMinUptimeMs
        ?? legacy.reconnectMinUptime
        ?? legacyReconnect?.minUptimeMs
        ?? legacyReconnect?.minUptime
        ?? 5_000;
    const jitter = legacy.reconnectJitter ?? legacyReconnect?.jitter ?? 0;

    return {
        enabled,
        initialDelayMs: positiveDelay(initialDelayMs),
        minDelayMs: positiveDelay(minDelayMs),
        maxDelayMs: positiveDelay(maxDelayMs),
        growFactor: positiveGrowFactor(growFactor),
        minUptimeMs: positiveDelay(minUptimeMs),
        maxAttempts: positiveAttempts(maxAttempts),
        jitter: clamp(jitter, 0, 1)
    };
}

/**
 * Converts invalid delay values to zero and preserves valid positive delays.
 */
function positiveDelay(value: number): number {
    return Number.isFinite(value) && value > 0 ? value : 0;
}

/**
 * Converts invalid attempt limits to zero and floors finite positive limits.
 */
function positiveAttempts(value: number): number {
    if (value === Number.POSITIVE_INFINITY) return value;
    return Number.isFinite(value) && value > 0 ? Math.floor(value) : 0;
}

/**
 * Preserves PartySocket's growth default while preventing invalid exponents.
 */
function positiveGrowFactor(value: number): number {
    return Number.isFinite(value) && value >= 1 ? value : 1;
}

/**
 * Restricts a numeric value to a closed interval.
 */
function clamp(value: number, min: number, max: number): number {
    if (!Number.isFinite(value)) return min;
    return Math.max(min, Math.min(max, value));
}
