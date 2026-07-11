/**
 * Route metadata helpers for route-based declarative controllers.
 */
import {
  canPrefetchChannelInput,
  channelInputIterable,
  isChannelInputAsyncIterable,
  isChannelInputIterable,
  markPrefetchableChannelInput
} from "@/channel/input.js";
import { route } from "@/payload/index.js";
import type { RSocketChannelInput, RSocketPayloadInput } from "@/types/index.js";
import type { RSocketControllerRoute } from "@/controllers/types.js";

/**
 * Cached builder for route-bearing payloads.
 */
export type RSocketRoutePayloadFactory = (payload?: unknown) => RSocketPayloadInput;

/**
 * Cached builder for route-prefixed request-channel inputs.
 */
export type RSocketRouteChannelInputFactory = (input: RSocketChannelInput<any, any>) => RSocketChannelInput;

/**
 * Creates an RSocket payload that carries Spring-compatible route metadata.
 *
 * When `payload` is omitted the returned payload contains only metadata, which
 * is useful as the first frame of a routed request-channel interaction.
 */
export function routePayload(controllerRoute: RSocketControllerRoute, payload?: unknown): RSocketPayloadInput {
  return routePayloadFactory(controllerRoute)(payload);
}

/**
 * Precomputes a routing entry that payload encoding adapts to direct or
 * composite metadata according to the configured metadata MIME.
 */
export function routePayloadFactory(controllerRoute: RSocketControllerRoute): RSocketRoutePayloadFactory {
  const metadata = routeEntry(controllerRoute);
  const metadataOnlyPayload = Object.freeze({ metadata });
  return (payload?: unknown) => {
    if (payload === undefined) return metadataOnlyPayload;
    return { data: payload, metadata };
  };
}

/**
 * Precomputes route metadata for repeated request-channel input creation.
 */
export function routeChannelInputFactory(controllerRoute: RSocketControllerRoute): RSocketRouteChannelInputFactory {
  const routedPayload = routePayloadFactory(controllerRoute);
  return (input) => prependRoutePayload(routedPayload, input);
}

/**
 * Creates the routing metadata entry for one configured controller route.
 */
function routeEntry(controllerRoute: RSocketControllerRoute) {
  return typeof controllerRoute === "string" ? route(controllerRoute) : route(...controllerRoute);
}

/**
 * Prepends route metadata to an outbound request-channel input source.
 */
export function routeChannelInput(
  controllerRoute: RSocketControllerRoute,
  input: RSocketChannelInput<any, any>
): RSocketChannelInput {
  return routeChannelInputFactory(controllerRoute)(input);
}

/**
 * Prepends a route-only payload while preserving synchronous iterables on the fast path.
 */
function prependRoutePayload(
  routedPayload: RSocketRoutePayloadFactory,
  input: RSocketChannelInput<any, any>
): RSocketChannelInput {
  if (!isChannelInputAsyncIterable(input) && isChannelInputIterable<RSocketPayloadInput<any, any>>(input)) {
    return prependRoutePayloadSync(routedPayload, input);
  }
  const routed = prependRoutePayloadAsync(routedPayload, input);
  return canPrefetchChannelInput(input) ? markPrefetchableChannelInput(routed) : routed;
}

/**
 * Synchronous route prepend used for arrays and other plain iterables.
 */
function* prependRoutePayloadSync(
  routedPayload: RSocketRoutePayloadFactory,
  input: Iterable<RSocketPayloadInput<any, any>>
): Iterable<RSocketPayloadInput<any, any>> {
  yield routedPayload();
  yield* input;
}

/**
 * Async route prepend used for publishers, promises, and async iterables.
 */
async function* prependRoutePayloadAsync(
  routedPayload: RSocketRoutePayloadFactory,
  input: RSocketChannelInput<any, any>
): AsyncIterable<RSocketPayloadInput<any, any>> {
  yield routedPayload();
  for await (const payload of channelInputIterable(input)) {
    yield payload;
  }
}
