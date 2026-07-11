/**
 * Logging module public surface for internal socket and controller code.
 */
export {
  CONTROLLER_LOG_DEFAULTS,
  controllerLogDefaults,
  normalizeLogOptions,
  SOCKET_LOG_DEFAULTS
} from "@/logging/options.js";
export {
  emitLog,
  frameLogEvent,
  interactionLogEvent,
  lifecycleLogEvent
} from "@/logging/emit.js";
export type {
  NormalizedRSocketLogOptions,
  RSocketFrameLogEvent,
  RSocketInteractionLogEvent,
  RSocketInteractionLogStage,
  RSocketLogDefaults,
  RSocketLifecycleLogEvent,
  RSocketLogDirection,
  RSocketLogEvent,
  RSocketLogInput,
  RSocketLogOptions,
  RSocketLogSink
} from "@/logging/types.js";
