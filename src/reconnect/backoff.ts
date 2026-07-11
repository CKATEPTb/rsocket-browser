/**
 * Reconnect backoff calculation with optional jitter.
 */
import type {RSocketReconnectOptions} from "@/reconnect/options.js";

/**
 * Calculates the delay before a reconnect attempt.
 */
export function reconnectDelay(attempt: number, options: RSocketReconnectOptions): number {
    const baseDelay = attempt <= 1
        ? options.initialDelayMs
        : Math.min(options.maxDelayMs, options.minDelayMs * options.growFactor ** Math.max(0, attempt - 2));
    if (options.jitter <= 0 || baseDelay <= 0) return baseDelay;

    const spread = baseDelay * options.jitter;
    const min = Math.max(0, baseDelay - spread);
    const max = baseDelay + spread;
    return Math.round(min + Math.random() * (max - min));
}
