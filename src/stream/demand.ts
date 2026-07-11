/**
 * Shared Reactive Streams demand validation and saturation helpers.
 */

/**
 * Validates one downstream `request(n)` value.
 *
 * Positive infinity is retained as the JavaScript representation of unbounded
 * demand; finite demand must be a positive safe integer.
 */
export function normalizeReactiveDemand(value: number): number {
    if (value === Number.POSITIVE_INFINITY) return value;
    if (Number.isFinite(value) && Number.isInteger(value) && value > 0 && value <= Number.MAX_SAFE_INTEGER) {
        return value;
    }
    throw new RangeError("Reactive Streams request(n) expects a strictly positive integer");
}

/**
 * Adds demand without overflowing JavaScript's exact integer range.
 */
export function addReactiveDemand(current: number, next: number): number {
    if (current === Number.POSITIVE_INFINITY || next === Number.POSITIVE_INFINITY) {
        return Number.POSITIVE_INFINITY;
    }
    return next >= Number.MAX_SAFE_INTEGER - current
        ? Number.MAX_SAFE_INTEGER
        : current + next;
}
