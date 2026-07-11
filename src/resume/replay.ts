/**
 * Outbound frame retention used by protocol Resume.
 *
 * Only frame types with implied Resume positions are stored. A head index keeps
 * acknowledgement and replay work linear without repeatedly shifting arrays.
 */
import type {Frame} from "rsocket-frames-ts";
import {RSocketProtocolError} from "@/errors/index.js";

/** One serialized frame retained until the peer acknowledges its end position. */
interface ReplayEntry {
    /** Inclusive implied position at which this frame starts. */
    readonly start: bigint;
    /** Exclusive implied position immediately after this frame. */
    readonly end: bigint;
    /** Frame retained only when diagnostic activity is enabled. */
    readonly frame: Frame | undefined;
    /** Immutable serialized bytes written again after RESUME_OK when required. */
    readonly bytes: Uint8Array;
}

/** Callback used to write one retained frame without advancing its position. */
type RSocketReplayEmitter = (frame: Frame | undefined, bytes: Uint8Array) => void;

/**
 * Position-aware replay buffer for one resumable logical RSocket session.
 */
export class RSocketReplayBuffer {
    private readonly entries: Array<ReplayEntry | undefined> = [];
    private head = 0;
    private acknowledgedPosition = 0n;

    /**
     * Returns the earliest implied client position still available for replay.
     */
    firstAvailablePosition(currentPosition: bigint): bigint {
        return this.entries[this.head]?.start ?? currentPosition;
    }

    /**
     * Retains one newly sent frame and returns the next implied position.
     */
    record(start: bigint, frame: Frame | undefined, bytes: Uint8Array): bigint {
        const end = start + BigInt(bytes.byteLength);
        this.entries.push({start, end, frame, bytes});
        return end;
    }

    /**
     * Discards frames ending at or before a monotonic KEEPALIVE acknowledgement.
     */
    acknowledge(position: bigint, currentPosition: bigint): void {
        if (position <= this.acknowledgedPosition) return;
        const nextHead = this.positionIndex(position, currentPosition);
        this.acknowledgedPosition = position;
        this.advanceHead(nextHead);
        this.compact();
    }

    /**
     * Replays every retained frame starting at the responder's RESUME_OK
     * position and records that position as acknowledged.
     */
    replayFrom(
        position: bigint,
        currentPosition: bigint,
        emit: RSocketReplayEmitter
    ): void {
        if (position < this.firstAvailablePosition(currentPosition)) {
            throw new RSocketProtocolError(
                "RSocket Resume responder requested client frames that are no longer available"
            );
        }

        const nextHead = this.positionIndex(position, currentPosition);
        this.acknowledgedPosition = position > this.acknowledgedPosition
            ? position
            : this.acknowledgedPosition;
        const replayEntries = this.entries.slice(nextHead) as ReplayEntry[];
        this.advanceHead(nextHead);
        this.compact();
        for (const entry of replayEntries) {
            emit(entry.frame, entry.bytes);
        }
    }

    /** Releases every retained frame when the logical session terminates. */
    clear(): void {
        this.entries.length = 0;
        this.head = 0;
        this.acknowledgedPosition = 0n;
    }

    /**
     * Resolves an implied position to the first frame beginning there.
     */
    private positionIndex(position: bigint, currentPosition: bigint): number {
        if (position < 0n || position > currentPosition) {
            throw new RSocketProtocolError("RSocket peer reported an impossible client position");
        }
        if (position === currentPosition) return this.entries.length;

        for (let index = this.head; index < this.entries.length; index += 1) {
            const entry = this.entries[index] as ReplayEntry;
            if (entry.start === position) return index;
            if (entry.end === position) return index + 1;
            if (entry.start > position || entry.end > position) break;
        }
        throw new RSocketProtocolError("RSocket peer reported a client position inside a frame");
    }

    /** Releases acknowledged references while retaining O(1) head movement. */
    private advanceHead(nextHead: number): void {
        for (let index = this.head; index < nextHead; index += 1) this.entries[index] = undefined;
        this.head = nextHead;
    }

    /**
     * Releases acknowledged array slots after enough entries have accumulated.
     */
    private compact(): void {
        if (this.head === this.entries.length) {
            this.entries.length = 0;
            this.head = 0;
            return;
        }
        if (this.head < 256 || this.head * 2 < this.entries.length) return;

        const remaining = this.entries.length - this.head;
        for (let index = 0; index < remaining; index += 1) {
            this.entries[index] = this.entries[index + this.head] as ReplayEntry;
        }
        this.entries.length = remaining;
        this.head = 0;
    }
}
