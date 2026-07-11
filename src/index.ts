/**
 * Public package entry point.
 *
 * `RSocket` is the browser WebSocket client constructor. Declarative controller
 * base classes are exported as separate named symbols instead of static
 * properties on `RSocket`.
 */
export {RSocket} from "@/rsocket/index.js";
export {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController
} from "@/controllers/index.js";
export type {
    RSocketControllerRoute,
    RSocketPayloadDecoder
} from "@/controllers/index.js";
