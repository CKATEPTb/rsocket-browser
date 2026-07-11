/**
 * Client-wide metadata overlay helpers.
 *
 * The high-level socket uses these helpers to keep an immutable metadata map
 * that is merged into outgoing interactions using the metadata MIME negotiated
 * by SETUP.
 */
import {Metadata, MimeType, WellKnownMimeType} from "rsocket-frames-ts";
import {
  canPrefetchChannelInput,
  channelInputIterable,
  isChannelInputAsyncIterable,
  isChannelInputIterable,
  markPrefetchableChannelInput
} from "@/channel/input.js";
import {compositeMetadataEntries, encodeMetadataInput, metadata as encodeMetadataValue} from "@/payload/index.js";
import type {RSocketChannelInput, RSocketPayload, RSocketPayloadInput} from "@/types/index.js";

/** Shared immutable empty metadata list for request hot paths. */
const EMPTY_METADATA_ENTRIES: readonly Metadata<any>[] = [];

/**
 * Value accepted by `RSocket.metadataUpdate(...)` for one MIME entry.
 */
export type RSocketClientMetadataValue = unknown | Metadata<any> | null | undefined | false;

/**
 * Immutable view of metadata entries currently attached to outgoing requests.
 */
export type RSocketMetadataMap = ReadonlyMap<MimeType<any>, Metadata<any>>;

/**
 * One MIME-keyed metadata patch entry accepted by `RSocket.metadataUpdate(...)`.
 */
export type RSocketMetadataPatchEntry = readonly [MimeType<any>, RSocketClientMetadataValue];

/**
 * Patch object accepted by `RSocket.metadataUpdate(...)`.
 */
export type RSocketMetadataPatch =
    | Metadata<any>
    | ReadonlyMap<MimeType<any>, RSocketClientMetadataValue>
    | Iterable<Metadata<any> | RSocketMetadataPatchEntry>;

/**
 * Function form accepted by `RSocket.metadataUpdate(...)`.
 */
export type RSocketMetadataUpdater = (
    metadata: RSocketMetadataEditor
) => RSocketMetadataPatch | void;

/**
 * Transactional MIME-keyed metadata editor supplied to `metadataUpdate(...)`.
 */
export interface RSocketMetadataEditor {
    /** Number of metadata entries currently stored by the client. */
    readonly size: number;

    /** Returns the encoded entry stored for one MIME type. */
    get(mimeType: MimeType<any>): Metadata<any> | undefined;

    /** Returns whether one MIME type currently has an entry. */
    has(mimeType: MimeType<any>): boolean;

    /** Encodes and stores a value under its MIME type. */
    set(mimeType: MimeType<any>, value: RSocketClientMetadataValue): void;

    /** Removes the entry associated with one MIME type. */
    remove(mimeType: MimeType<any>): void;
}

/**
 * Mutable metadata storage owned by the high-level socket.
 */
export type RSocketMetadataState = Map<string, Metadata<any>>;

/**
 * Applies one metadata patch and returns the next immutable metadata view.
 */
export function applyMetadataUpdate(
    state: RSocketMetadataState,
    update: RSocketMetadataPatch | RSocketMetadataUpdater,
    setupMetadataMimeType?: MimeType<any>
): RSocketMetadataMap {
    const next = new Map(state);
    const patch = typeof update === "function"
        ? update(new MetadataEditor(next, setupMetadataMimeType))
        : update;
    if (patch !== undefined) applyMetadataPatch(next, patch, setupMetadataMimeType);
    replaceMetadataState(state, next);
    return metadataSnapshot(state);
}

/**
 * Mutable transaction view used only for the duration of one metadata update.
 */
class MetadataEditor implements RSocketMetadataEditor {
    /** Creates an editor over an isolated candidate state. */
    constructor(
        private readonly state: RSocketMetadataState,
        private readonly setupMetadataMimeType?: MimeType<any>
    ) {
    }

    /** Number of entries in the candidate state. */
    get size(): number {
        return this.state.size;
    }

    /** Returns one candidate entry by MIME type. */
    get(mimeType: MimeType<any>): Metadata<any> | undefined {
        return this.state.get(metadataKey(mimeType));
    }

    /** Checks one candidate entry by MIME type. */
    has(mimeType: MimeType<any>): boolean {
        return this.state.has(metadataKey(mimeType));
    }

    /** Encodes and stores one candidate entry. */
    set(mimeType: MimeType<any>, value: RSocketClientMetadataValue): void {
        applyMetadataValue(this.state, mimeType, value, this.setupMetadataMimeType);
    }

    /** Removes one candidate entry. */
    remove(mimeType: MimeType<any>): void {
        this.state.delete(metadataKey(mimeType));
    }
}

/**
 * Applies a direct patch to an isolated metadata state.
 */
function applyMetadataPatch(
    state: RSocketMetadataState,
    patch: RSocketMetadataPatch,
    setupMetadataMimeType?: MimeType<any>
): void {
    if (patch instanceof Metadata) {
        applyMetadataValue(state, patch.mimeType, patch, setupMetadataMimeType);
        return;
    }

    if (isMetadataPatchIterable(patch)) {
        for (const entry of patch) applyMetadataPatchEntry(state, entry, setupMetadataMimeType);
        return;
    }

    throw new TypeError(
        "RSocket.metadataUpdate expects Metadata, Map<MimeType, value>, or iterable Metadata/[MimeType, value] entries."
    );
}

/**
 * Commits a validated metadata patch without exposing partially applied state.
 */
function replaceMetadataState(state: RSocketMetadataState, next: RSocketMetadataState): void {
    state.clear();
    for (const [key, value] of next) state.set(key, value);
}

/**
 * Returns an immutable map view of the current metadata state.
 */
export function metadataSnapshot(state: RSocketMetadataState): RSocketMetadataMap {
    if (state.size === 0) return new Map();
    const snapshot = new Map<MimeType<any>, Metadata<any>>();
    for (const metadata of state.values()) snapshot.set(metadata.mimeType, metadata);
    return snapshot;
}

/**
 * Adds client-wide metadata entries to one outgoing payload input.
 */
export function withClientMetadata<D, M>(
    payload: RSocketPayloadInput<D, M>,
    entries: readonly Metadata<any>[],
    metadataMimeType?: MimeType<any>,
    clientMetadata?: Metadata<any>,
    mergeCache?: WeakMap<Metadata<any>, Metadata<any>>,
    targetMetadataMimeType: MimeType<any> = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
): RSocketPayloadInput<D, M> {
    if (entries.length === 0) return payload;
    if (payload instanceof Metadata) {
        return mergeClientMetadata(entries, payload, undefined, targetMetadataMimeType, mergeCache);
    }
    if (isPayloadEnvelope(payload)) {
        return envelopeWithMetadata(
            payload,
            entries,
            metadataMimeType,
            clientMetadata,
            mergeCache,
            targetMetadataMimeType
        );
    }
    const effectiveClientMetadata = clientMetadata ?? clientMetadataValue(entries, targetMetadataMimeType);
    return {
        data: payload,
        metadata: effectiveClientMetadata
    } as RSocketPayloadInput<D, M>;
}

/**
 * Merges client defaults with metadata supplied by one interaction.
 * Interaction metadata wins when both sources use the same MIME key.
 */
export function mergeClientMetadata(
    entries: readonly Metadata<any>[],
    value: unknown,
    metadataMimeType: MimeType<any> | undefined,
    targetMetadataMimeType: MimeType<any>,
    mergeCache?: WeakMap<Metadata<any>, Metadata<any>>
): Metadata<any> {
    return metadataWithEntries(entries, value, metadataMimeType, targetMetadataMimeType, mergeCache);
}

/**
 * Adds client-wide metadata entries to every item emitted by a channel source.
 */
export function withClientMetadataInput<D, M>(
    payloads: RSocketChannelInput<D, M>,
    entries: readonly Metadata<any>[],
    metadataMimeType?: MimeType<any>,
    clientMetadata?: Metadata<any>,
    mergeCache?: WeakMap<Metadata<any>, Metadata<any>>,
    targetMetadataMimeType: MimeType<any> = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
): RSocketChannelInput<D, M> {
    if (entries.length === 0) return payloads;
    const effectiveClientMetadata = clientMetadata ?? clientMetadataValue(entries, targetMetadataMimeType);
    if (!isChannelInputAsyncIterable(payloads) && isChannelInputIterable<RSocketPayloadInput<D, M>>(payloads)) {
        return withClientMetadataIterable(
            payloads,
            entries,
            metadataMimeType,
            effectiveClientMetadata,
            mergeCache,
            targetMetadataMimeType
        );
    }
    const mapped = withClientMetadataAsync(
        payloads,
        entries,
        metadataMimeType,
        effectiveClientMetadata,
        mergeCache,
        targetMetadataMimeType
    );
    return canPrefetchChannelInput(payloads) ? markPrefetchableChannelInput(mapped) : mapped;
}

/**
 * Adds client metadata to synchronous channel sources without leaving the sync fast path.
 */
function* withClientMetadataIterable<D, M>(
    payloads: Iterable<RSocketPayloadInput<D, M>>,
    entries: readonly Metadata<any>[],
    metadataMimeType: MimeType<any> | undefined,
    clientMetadata: Metadata<any> | undefined,
    mergeCache: WeakMap<Metadata<any>, Metadata<any>> | undefined,
    targetMetadataMimeType: MimeType<any>
): Iterable<RSocketPayloadInput<D, M>> {
    for (const payload of payloads) {
        yield withClientMetadata(
            payload,
            entries,
            metadataMimeType,
            clientMetadata,
            mergeCache,
            targetMetadataMimeType
        );
    }
}

/**
 * Adds client metadata to publisher, promise, and async iterable channel sources.
 */
async function* withClientMetadataAsync<D, M>(
    payloads: RSocketChannelInput<D, M>,
    entries: readonly Metadata<any>[],
    metadataMimeType: MimeType<any> | undefined,
    clientMetadata: Metadata<any> | undefined,
    mergeCache: WeakMap<Metadata<any>, Metadata<any>> | undefined,
    targetMetadataMimeType: MimeType<any>
): AsyncIterable<RSocketPayloadInput<D, M>> {
    for await (const payload of channelInputIterable(payloads)) {
        yield withClientMetadata(
            payload,
            entries,
            metadataMimeType,
            clientMetadata,
            mergeCache,
            targetMetadataMimeType
        );
    }
}

/**
 * Encodes current client metadata for the SETUP metadata MIME.
 */
export function clientMetadataValue(
    entries: readonly Metadata<any>[],
    targetMetadataMimeType: MimeType<any> = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
): Metadata<any> | undefined {
    if (entries.length === 0) return undefined;
    if (isCompositeMetadataMimeType(targetMetadataMimeType)) return compositeMetadataEntries(entries);
    const entry = entries[0] as Metadata<any>;
    if (entries.length === 1 && metadataKey(entry.mimeType) === metadataKey(targetMetadataMimeType)) return entry;
    throw unsupportedMetadataMimeType(targetMetadataMimeType);
}

/**
 * Returns whether one MIME is the RSocket composite metadata container.
 */
export function isCompositeMetadataMimeType(mimeType: MimeType<any>): boolean {
    return metadataKey(mimeType) === metadataKey(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA);
}

/**
 * Returns the metadata state as an ordered entry list for a single request.
 */
export function metadataEntries(state: RSocketMetadataState): readonly Metadata<any>[] {
    if (state.size === 0) return EMPTY_METADATA_ENTRIES;
    const entries = new Array<Metadata<any>>(state.size);
    let index = 0;
    for (const metadata of state.values()) {
        entries[index] = metadata;
        index += 1;
    }
    return entries;
}

/**
 * Returns a stable string key for one MIME type.
 */
function metadataKey(mimeType: MimeType<any>): string {
    return mimeType.mimeType;
}

/**
 * Applies one MIME-keyed metadata patch entry.
 */
function applyMetadataValue(
    state: RSocketMetadataState,
    mimeType: MimeType<any>,
    value: RSocketClientMetadataValue,
    setupMetadataMimeType?: MimeType<any>
): void {
    const key = metadataKey(mimeType);
    if (value === undefined || value === null || value === false) {
        state.delete(key);
        return;
    }

    if (value instanceof Metadata && metadataKey(value.mimeType) !== key) {
        throw new TypeError("RSocket.metadataUpdate Metadata value must match its MimeType map key.");
    }
    assertMetadataMimeTypeSupported(mimeType, setupMetadataMimeType);
    const metadata = value instanceof Metadata ? value : encodeMetadataValue(value as never, mimeType);
    state.set(metadataKey(metadata.mimeType), metadata);
}

/**
 * Applies one patch item from a metadata iterable.
 */
function applyMetadataPatchEntry(
    state: RSocketMetadataState,
    entry: Metadata<any> | RSocketMetadataPatchEntry,
    setupMetadataMimeType?: MimeType<any>
): void {
    if (entry instanceof Metadata) {
        applyMetadataValue(state, entry.mimeType, entry, setupMetadataMimeType);
        return;
    }

    if (isMetadataPatchEntry(entry)) {
        applyMetadataValue(state, entry[0], entry[1], setupMetadataMimeType);
        return;
    }

    throw new TypeError(
        "RSocket.metadataUpdate iterable entries must be Metadata or [MimeType, value] tuples."
    );
}

/**
 * Merges an object payload envelope with client-wide metadata entries.
 */
function envelopeWithMetadata<D, M>(
    payload: RSocketPayload<D, M>,
    entries: readonly Metadata<any>[],
    metadataMimeType: MimeType<any> | undefined,
    clientMetadata: Metadata<any> | undefined,
    mergeCache: WeakMap<Metadata<any>, Metadata<any>> | undefined,
    targetMetadataMimeType: MimeType<any>
): RSocketPayloadInput<D, M> {
    const metadata = "metadata" in payload && payload.metadata !== undefined
        ? metadataWithEntries(
            entries,
            payload.metadata,
            payload.metadataMimeType ?? metadataMimeType,
            targetMetadataMimeType,
            mergeCache
        )
        : clientMetadata ?? clientMetadataValue(entries, targetMetadataMimeType);
    const next: {
        data?: D;
        metadata?: Metadata<any>;
        dataMimeType?: MimeType<D>;
        metadataMimeType: MimeType<any>;
    } = {
        metadataMimeType: targetMetadataMimeType
    };
    if (metadata !== undefined) {
        next.metadata = metadata;
    }
    if ("data" in payload) {
        next.data = payload.data;
    }
    if ("dataMimeType" in payload) {
        next.dataMimeType = payload.dataMimeType;
    }
    return next as RSocketPayloadInput<D, M>;
}

/**
 * Merges client-wide metadata with one payload metadata value.
 */
function metadataWithEntries(
    entries: readonly Metadata<any>[],
    value: unknown,
    mimeType: MimeType<any> | undefined,
    targetMetadataMimeType: MimeType<any>,
    mergeCache?: WeakMap<Metadata<any>, Metadata<any>>
): Metadata<any> {
    if (!isCompositeMetadataMimeType(targetMetadataMimeType)) {
        const direct = value instanceof Metadata
            ? value
            : encodeMetadataValue(value as never, mimeType ?? targetMetadataMimeType);
        return encodeMetadataInput(direct, targetMetadataMimeType);
    }
    if (value instanceof Metadata) {
        const cached = mergeCache?.get(value);
        if (cached !== undefined) return cached;
        let merged: Metadata<any>;
        if (
            metadataKey(value.mimeType) === metadataKey(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA) &&
            Array.isArray(value.payload)
        ) {
            merged = compositeMetadataEntries(metadataEntriesWith(entries, value.payload));
        } else {
            merged = compositeMetadataEntries(metadataEntriesWith(entries, value));
        }
        mergeCache?.set(value, merged);
        return merged;
    }
    return compositeMetadataEntries(
        metadataEntriesWith(entries, encodeMetadataValue(value as never, mimeType ?? WellKnownMimeType.APPLICATION_JSON))
    );
}

/**
 * Returns metadata entries plus one or more payload entries using one allocation.
 */
function metadataEntriesWith(
    entries: readonly Metadata<any>[],
    extra: Metadata<any> | readonly Metadata<any>[]
): readonly Metadata<any>[] {
    const entryCount = entries.length;
    const extraList = Array.isArray(extra) ? extra : undefined;
    const extraLength = extraList?.length ?? 1;
    if (extraLength === 0) return entries;
    let retainedCount = 0;
    for (let index = 0; index < entryCount; index++) {
        if (!metadataListHasMimeType(extra, extraList, (entries[index] as Metadata<any>).mimeType)) retainedCount += 1;
    }
    const result = new Array<Metadata<any>>(retainedCount + extraLength);
    let resultIndex = 0;
    for (let index = 0; index < entryCount; index++) {
        const entry = entries[index] as Metadata<any>;
        if (metadataListHasMimeType(extra, extraList, entry.mimeType)) continue;
        result[resultIndex] = entry;
        resultIndex += 1;
    }
    if (extraList === undefined) {
        result[resultIndex] = extra as Metadata<any>;
    } else {
        for (let index = 0; index < extraLength; index++) result[resultIndex + index] = extraList[index] as Metadata<any>;
    }
    return result;
}

/**
 * Checks whether request metadata contains an entry with one MIME key.
 */
function metadataListHasMimeType(
    extra: Metadata<any> | readonly Metadata<any>[],
    extraList: readonly Metadata<any>[] | undefined,
    mimeType: MimeType<any>
): boolean {
    const key = metadataKey(mimeType);
    if (extraList === undefined) return metadataKey((extra as Metadata<any>).mimeType) === key;
    for (let index = 0; index < extraList.length; index++) {
        if (metadataKey((extraList[index] as Metadata<any>).mimeType) === key) return true;
    }
    return false;
}

/**
 * Validates one persistent entry against the connection metadata MIME.
 */
function assertMetadataMimeTypeSupported(
    mimeType: MimeType<any>,
    setupMetadataMimeType: MimeType<any> | undefined
): void {
    if (
        setupMetadataMimeType === undefined ||
        isCompositeMetadataMimeType(setupMetadataMimeType) ||
        metadataKey(mimeType) === metadataKey(setupMetadataMimeType)
    ) {
        return;
    }
    throw unsupportedMetadataMimeType(setupMetadataMimeType, mimeType);
}

/**
 * Creates the user-facing error for metadata unsupported by a direct SETUP MIME.
 */
function unsupportedMetadataMimeType(
    setupMetadataMimeType: MimeType<any>,
    attemptedMimeType?: MimeType<any>
): TypeError {
    const attempted = attemptedMimeType === undefined
        ? "multiple metadata entries"
        : `metadata MIME "${metadataKey(attemptedMimeType)}"`;
    return new TypeError(
        `RSocket SETUP metadata MIME "${metadataKey(setupMetadataMimeType)}" cannot store ${attempted}. ` +
        "Configure MESSAGE_RSOCKET_COMPOSITE_METADATA to use multiple metadata types."
    );
}

/**
 * Detects payload envelopes without treating codec metadata objects as envelopes.
 */
function isPayloadEnvelope(value: unknown): value is RSocketPayload<any, any> {
    if (typeof value !== "object" || value === null || value instanceof Metadata) return false;
    return "data" in value || "metadata" in value || "dataMimeType" in value || "metadataMimeType" in value;
}

/**
 * Detects iterable metadata patches while excluding codec metadata objects.
 */
function isMetadataPatchIterable(value: unknown): value is Iterable<Metadata<any> | RSocketMetadataPatchEntry> {
    return typeof value === "object"
        && value !== null
        && typeof (value as Partial<Iterable<Metadata<any>>>)[Symbol.iterator] === "function"
        && !(value instanceof Metadata);
}

/**
 * Detects a MIME-keyed tuple entry from a metadata update iterable.
 */
function isMetadataPatchEntry(value: unknown): value is RSocketMetadataPatchEntry {
    return Array.isArray(value)
        && value.length >= 2
        && value[0] instanceof MimeType;
}
