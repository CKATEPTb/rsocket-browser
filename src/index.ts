/**
 * Public package entry point.
 *
 * `RSocket` is the browser WebSocket client constructor. Declarative controller
 * base classes and compact factory helpers are exported as separate named
 * symbols instead of static properties on `RSocket`.
 */
export { RSocket } from "@/rsocket/index.js";
export {
  FireAndForgetController,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController,
  fireAndForgetController,
  requestChannelController,
  requestResponseController,
  requestStreamController
} from "@/controllers/index.js";
export type {
  RSocketControllerRoute,
  RSocketPayloadDecoder
} from "@/controllers/index.js";
