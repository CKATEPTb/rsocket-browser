/**
 * Declarative controller public surface.
 *
 * This module groups class-based controllers, factory helpers, and the internal
 * processor used by controller-aware request methods.
 */
export {
  controller,
  fireAndForgetController,
  requestChannelController,
  requestResponseController,
  requestStreamController
} from "@/controllers/factory.js";
export { processController } from "@/controllers/process.js";
export {
  FireAndForgetController,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController
} from "@/controllers/classes.js";
export type {
  AnyClassController,
  ControllerChannelArgs,
  ControllerPayloadArgs
} from "@/controllers/classes.js";
export type {
  AnyRSocketController,
  ControllerArgs,
  ControllerReturn,
  FireAndForgetControllerDefinition,
  RequestChannelControllerDefinition,
  RequestResponseControllerDefinition,
  RequestStreamControllerDefinition,
  RSocketControllerConnection,
  RSocketControllerConstructor,
  RSocketControllerKind,
  RSocketControllerRoute,
  RSocketPayloadDecoder
} from "@/controllers/types.js";
