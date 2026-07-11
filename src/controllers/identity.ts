/**
 * Identity decoder helpers for controller factories that return raw payload frames.
 */
import type { RSocketPayloadDecoder } from "@/controllers/types.js";
import type { RSocketPayloadFrame } from "@/types/index.js";

/** Internal marker used to skip Reactor map operators for raw-payload controllers. */
const IDENTITY_DECODER = Symbol("RSocket.identityDecoder");

/**
 * Payload decoder that returns the raw frame unchanged.
 */
export const identityPayload = Object.assign(
  (payload: RSocketPayloadFrame): RSocketPayloadFrame => payload,
  { [IDENTITY_DECODER]: true as const }
);

/**
 * Checks whether a controller decoder is the built-in raw-payload identity decoder.
 */
export function isIdentityDecoder(decoder: RSocketPayloadDecoder<unknown>): boolean {
  return (decoder as { readonly [IDENTITY_DECODER]?: true })[IDENTITY_DECODER] === true;
}
