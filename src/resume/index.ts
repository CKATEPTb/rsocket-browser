/**
 * RSocket Resume option and state helpers.
 *
 * Resume is a protocol handshake that sends `RESUME` instead of a fresh `SETUP` when a
 * previous WebSocket drops and the server supports RSocket Resume.
 */

/**
 * Last known resumable byte positions for one physical RSocket session.
 */
export interface RSocketResumeState {
    /** Last implied client-to-server byte position sent by this requester. */
    readonly clientPosition: bigint;
    /** Earliest retained client position that can still be replayed. */
    readonly firstAvailableClientPosition: bigint;
    /** Last implied server-to-client byte position received by this requester. */
    readonly serverPosition: bigint;
}

/**
 * Normalized resume behavior used by the high-level socket facade.
 */
export interface RSocketResumeOptions {
    /** Whether protocol-level RSocket Resume should be attempted after reconnect. */
    readonly enabled: boolean;
    /** Opaque resume token sent in SETUP and later RESUME frames. */
    readonly token: string | undefined;
    /** Backend resume state lifetime, in milliseconds. */
    readonly ttlMs: number;
}

/**
 * Object form accepted by public `reconnect.resume`.
 */
export interface RSocketResumePolicyObjectInput {
    /** Backend resume state lifetime, in milliseconds. */
    readonly ttl: number;
}

/**
 * Shorthand or object form accepted by resume options.
 */
export type RSocketResumePolicyInput = number | RSocketResumePolicyObjectInput | false;

/**
 * Public constructor options related to protocol Resume.
 */
export interface RSocketResumeOptionInput {
    /** Reconnect options that may enable protocol Resume. */
    readonly reconnect?: boolean | {
        readonly resume?: RSocketResumePolicyInput;
    };
}

/**
 * Default protocol Resume TTL: five minutes.
 */
const DEFAULT_RESUME_TTL_MS = 300_000;

export {RSocketReplayBuffer} from "@/resume/replay.js";

/**
 * Normalizes reconnect Resume options and generates an opaque token when needed.
 */
export function normalizeResumeOptions(input: RSocketResumeOptionInput): RSocketResumeOptions {
    const reconnect = input.reconnect;
    const reconnectObject = typeof reconnect === "object" ? reconnect : undefined;
    const policy = reconnectObject?.resume;
    const enabled = policy !== undefined && policy !== false;
    const token = enabled ? createResumeToken() : undefined;
    const ttlMs = resumeTtl(policy);

    return {
        enabled: enabled && token !== undefined,
        token,
        ttlMs: positiveTtl(ttlMs)
    };
}

/**
 * Creates an opaque browser-safe token when resume is enabled without one.
 */
export function createResumeToken(): string {
    const crypto = globalThis.crypto;
    if (crypto?.randomUUID !== undefined) return crypto.randomUUID();
    if (crypto?.getRandomValues !== undefined) {
        const bytes = new Uint8Array(16);
        crypto.getRandomValues(bytes);
        let token = "";
        for (const byte of bytes) token += byte.toString(16).padStart(2, "0");
        return token;
    }
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

/**
 * Converts invalid TTL values to the default resume window.
 */
function positiveTtl(value: number): number {
    const ttl = Math.floor(value);
    return Number.isFinite(value) && ttl > 0 ? ttl : DEFAULT_RESUME_TTL_MS;
}

/**
 * Extracts the backend resume TTL from the public option shape.
 */
function resumeTtl(policy: RSocketResumePolicyInput | undefined): number {
    if (typeof policy === "number") return policy;
    if (typeof policy === "object" && policy !== null) {
        return policy.ttl;
    }
    return DEFAULT_RESUME_TTL_MS;
}
