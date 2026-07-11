/**
 * Integration tests against a Spring Boot RSocket WebSocket responder.
 */
import { execFileSync, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { createServer } from "node:net";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Flux, type Subscription } from "reactor-core-ts";
import { WellKnownMimeType } from "rsocket-frames-ts";
import {
  RequestChannelController,
  RequestResponseController,
  RequestStreamController,
  RSocket
} from "@";
import type { RSocketPayloadFrame, RSocketWebSocket } from "@/types/index.js";

const projectRoot = fileURLToPath(new URL("../", import.meta.url));
const serverProject = resolve(projectRoot, "test/spring-boot-rsocket-server/pom.xml");
const serverSource = resolve(
  projectRoot,
  "test/spring-boot-rsocket-server/src/main/java/dev/ckateptb/rsocket/browser/tests/spring/SpringBootRSocketTestServer.java"
);
const serverJar = resolve(projectRoot, "test/spring-boot-rsocket-server/target/rsocket-browser-spring-boot-test-server-1.0.0.jar");
const dataMimeType = WellKnownMimeType.APPLICATION_JSON;
const metadataMimeType = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;

let serverProcess: ChildProcessWithoutNullStreams | undefined;
let serverPort = 0;

/**
 * Server-side counters exposed by the Spring Boot responder.
 */
interface SpringBootStats {
  /** Number of accepted SETUP frames. */
  readonly setups: number;
  /** Number of routed request-response interactions. */
  readonly requestResponse: number;
  /** Number of routed request-stream interactions. */
  readonly requestStream: number;
  /** Number of routed request-channel interactions. */
  readonly requestChannel: number;
}

/**
 * Response returned by the Spring echo controller.
 */
interface SpringEchoResponse {
  /** Response discriminator emitted by the Spring controller. */
  readonly kind: "spring-echo";
  /** Echoed request value. */
  readonly value: string;
  /** SETUP count observed when the handler ran. */
  readonly setups: number;
}

/**
 * Request-response controller bound to the Spring `echo` route.
 */
class SpringEchoController extends RequestResponseController<{ readonly value: string }, SpringEchoResponse> {
  /** Spring route encoded into RSocket routing metadata. */
  protected route = "echo";
}

/**
 * Request-response controller bound to the Spring `stats` route.
 */
class SpringStatsController extends RequestResponseController<void, SpringBootStats> {
  /** Spring route encoded into RSocket routing metadata. */
  protected route = "stats";
}

/**
 * Request-stream controller bound to the Spring `numbers` route.
 */
class SpringNumbersController extends RequestStreamController<{ readonly count: number }, { readonly n: number }> {
  /** Spring route encoded into RSocket routing metadata. */
  protected route = "numbers";
}

/**
 * Request-channel controller bound to the Spring `channel` route.
 */
class SpringChannelController extends RequestChannelController<{ readonly value: string }, {
  readonly kind: "spring-channel";
  readonly value: string;
}> {
  /** Spring route encoded into RSocket routing metadata. */
  protected route = "channel";
}

describe("RSocket Spring Boot WebSocket Resume integration", () => {
  beforeAll(async () => {
    buildSpringBootServer();
    serverPort = await freePort();
    serverProcess = startSpringBootServer(serverPort);
    await waitForServerReady(serverProcess);
  }, 240_000);

  afterAll(async () => {
    await stopSpringBootServer(serverProcess);
  });

  it("routes controller request-response calls through Spring MessageMapping", async () => {
    const socket = createSocket();
    const connected = await connectedSocket(socket);

    try {
      const before = await springStats(socket);
      const response = await connected.process(new SpringEchoController(), { value: "boot" }).block();
      const after = await springStats(socket);

      expect(response).toEqual({
        kind: "spring-echo",
        value: "boot",
        setups: before.setups
      });
      expect(after.setups).toBe(before.setups);
      expect(after.requestResponse).toBe(before.requestResponse + 2);
    } finally {
      connected.disconnect();
    }
  });

  it("keeps request-stream demand lazy against Spring MessageMapping", async () => {
    const socket = createSocket();
    const connected = await connectedSocket(socket);
    const received: unknown[] = [];
    let subscription: Subscription | undefined;

    try {
      connected.process(new SpringNumbersController(), { count: 4 }).subscribe({
        onSubscribe(next: Subscription) {
          subscription = next;
          next.request(1);
        },
        onNext(value: { readonly n: number }) {
          received.push(value);
        },
        onError(error: unknown) {
          throw error;
        },
        onComplete() {}
      });

      await waitFor(() => received.length === 1);
      await delay(100);
      expect(received).toEqual([{ n: 1 }]);

      subscription?.request(3);
      await waitFor(() => received.length === 4);
      expect(received).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }]);
    } finally {
      subscription?.cancel();
      connected.disconnect();
    }
  });

  it("runs request-channel through Spring MessageMapping", async () => {
    const socket = createSocket();
    const connected = await connectedSocket(socket);
    const received: unknown[] = [];
    let completed = false;
    let failure: unknown;

    try {
      connected
        .process(new SpringChannelController(), Flux.fromArray([
          { data: { value: "a" } },
          { data: { value: "b" } }
        ]))
        .subscribe({
          onSubscribe(subscription: Subscription) {
            subscription.request(2);
          },
          onNext(value: { readonly kind: "spring-channel"; readonly value: string }) {
            received.push(value);
          },
          onError(error: unknown) {
            failure = error;
          },
          onComplete() {
            completed = true;
          }
        });

      await waitFor(() => completed && received.length === 2);
      expect(received).toEqual([
        { kind: "spring-channel", value: "a" },
        { kind: "spring-channel", value: "b" }
      ]);
      expect(failure).toBeUndefined();
    } finally {
      connected.disconnect();
    }
  });

  it("resumes a dropped WebSocket session against Spring Boot without a second SETUP", async () => {
    const sockets: WebSocket[] = [];
    const events: Array<{ readonly type: string; readonly reconnect: boolean }> = [];
    const socket = createSocket({
      transport: (url: string | URL) => {
        const websocket = new WebSocket(url);
        sockets.push(websocket);
        return websocket as unknown as RSocketWebSocket;
      },
      reconnect: {
        resume: 30_000,
        delay: 100,
        minDelay: 100,
        maxDelay: 100,
        minUptime: 0
      },
      events: {
        event: (event: any) => events.push({ type: event.type, reconnect: event.reconnect })
      }
    });
    const connected = await connectedSocket(socket);

    try {
      const before = await springStats(socket);

      sockets[0]?.close(3001, "spring boot resume");
      await waitFor(() => events.some((event) => event.type === "reconnecting"));

      const queued = socket.process(new SpringEchoController(), { value: "after-resume" }).block();
      await waitFor(() => events.some((event) => event.type === "connected" && event.reconnect), 10_000);

      const response = await queued;
      const after = await springStats(socket);

      expect(sockets).toHaveLength(2);
      expect(events.filter((event) => event.type === "connected" && event.reconnect)).toHaveLength(1);
      expect(events.filter((event) => event.type === "reconnectFailed")).toHaveLength(0);
      expect(response).toEqual({
        kind: "spring-echo",
        value: "after-resume",
        setups: before.setups
      });
      expect(after.setups).toBe(before.setups);
      expect(after.requestResponse).toBe(before.requestResponse + 2);
    } finally {
      connected.disconnect();
    }
  }, 30_000);
});

/**
 * Creates a browser-client instance wired to the Spring Boot test server.
 */
function createSocket(overrides: Record<string, unknown> = {}): RSocket {
  const transport = overrides.transport as ((url: string | URL) => RSocketWebSocket) | undefined;
  const options = {
    setup: {
      keepAlive: 500,
      lifetime: 5_000,
      mimetype: {
        data: dataMimeType,
        metadata: metadataMimeType
      },
      transport: transport ?? ((url: string | URL) => new WebSocket(url) as unknown as RSocketWebSocket)
    },
    reconnect: false,
    ...without(overrides, "transport")
  };

  return new RSocket(`ws://127.0.0.1:${serverPort}/rsocket`, options as never);
}

/**
 * Opens the socket and fails clearly if the connected facade is missing.
 */
async function connectedSocket(socket: RSocket): Promise<any> {
  const connected = await socket.connect().block();
  if (connected === undefined) throw new Error("RSocket did not connect");
  return connected;
}

/**
 * Reads Spring Boot server-side interaction counters.
 */
async function springStats(socket: RSocket): Promise<SpringBootStats> {
  return await socket.process(new SpringStatsController()).block() as SpringBootStats;
}

/**
 * Builds the Spring Boot responder fixture.
 */
function buildSpringBootServer(): void {
  if (springBootServerJarIsFresh()) return;
  const command = mavenCommand();
  execFileSync(command.file, command.args, {
    cwd: projectRoot,
    stdio: "inherit"
  });
}

/**
 * Avoids rebuilding the Spring Boot jar when the fixture source is unchanged.
 */
function springBootServerJarIsFresh(): boolean {
  if (!existsSync(serverJar)) return false;
  const jarTime = statSync(serverJar).mtimeMs;
  return [serverProject, serverSource].every((file) => statSync(file).mtimeMs <= jarTime);
}

/**
 * Starts the Spring Boot jar on a fixed local port.
 */
function startSpringBootServer(port: number): ChildProcessWithoutNullStreams {
  return spawn(javaCommand(), [
    "-jar",
    serverJar,
    `--spring.main.banner-mode=off`,
    `--logging.level.root=WARN`,
    `--server.port=${port}`,
    `--spring.main.web-application-type=reactive`,
    `--spring.rsocket.server.mapping-path=/rsocket`,
    `--spring.rsocket.server.transport=websocket`,
    `--test.rsocket.resume-ttl-ms=30000`
  ], {
    cwd: projectRoot
  });
}

/**
 * Waits for the Spring Boot process to print its readiness line.
 */
function waitForServerReady(child: ChildProcessWithoutNullStreams): Promise<void> {
  return new Promise((resolveReady, rejectReady) => {
    let stdout = "";
    let stderr = "";
    const timeout = setTimeout(() => {
      cleanup();
      rejectReady(new Error(`Timed out waiting for Spring Boot RSocket test server. stderr: ${stderr}`));
    }, 45_000);

    const cleanup = (): void => {
      clearTimeout(timeout);
      child.stdout.off("data", onStdout);
      child.stderr.off("data", onStderr);
      child.off("exit", onExit);
      child.off("error", onError);
    };

    const onStdout = (chunk: Buffer): void => {
      stdout += chunk.toString("utf8");
      if (stdout.includes("SPRING_RSOCKET_TEST_SERVER_READY")) {
        cleanup();
        resolveReady();
      }
    };
    const onStderr = (chunk: Buffer): void => {
      stderr += chunk.toString("utf8");
    };
    const onExit = (code: number | null): void => {
      cleanup();
      rejectReady(new Error(`Spring Boot RSocket test server exited before ready with code ${code}. stderr: ${stderr}`));
    };
    const onError = (error: Error): void => {
      cleanup();
      rejectReady(error);
    };

    child.stdout.on("data", onStdout);
    child.stderr.on("data", onStderr);
    child.once("exit", onExit);
    child.once("error", onError);
  });
}

/**
 * Stops a Spring Boot responder process.
 */
async function stopSpringBootServer(child: ChildProcessWithoutNullStreams | undefined): Promise<void> {
  serverProcess = undefined;
  if (child === undefined || child.exitCode !== null) return;

  child.kill();
  await Promise.race([
    new Promise<void>((resolveStop) => child.once("exit", () => resolveStop())),
    delay(5_000).then(() => {
      if (child.exitCode === null) child.kill("SIGKILL");
    })
  ]);
}

/**
 * Finds a free local TCP port for the Spring Boot server.
 */
function freePort(): Promise<number> {
  return new Promise((resolvePort, rejectPort) => {
    const server = createServer();
    server.once("error", rejectPort);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() => {
        if (address === null || typeof address === "string") rejectPort(new Error("Unable to allocate a TCP port"));
        else resolvePort(address.port);
      });
    });
  });
}

/**
 * Waits until a condition becomes true.
 */
async function waitFor(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) throw new Error("Timed out waiting for Spring Boot integration condition");
    await delay(10);
  }
}

/**
 * Delays for a fixed number of milliseconds.
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}

/**
 * Returns a copy of an object without one key.
 */
function without(source: Record<string, unknown>, key: string): Record<string, unknown> {
  const copy = { ...source };
  delete copy[key];
  return copy;
}

/**
 * Platform-specific Maven command name.
 */
function mavenCommand(): { readonly file: string; readonly args: readonly string[] } {
  const args = ["-q", "-f", serverProject, "-DskipTests", "package"];
  return process.platform === "win32"
    ? { file: "cmd.exe", args: ["/d", "/s", "/c", ["mvn", ...args.map(shellQuote)].join(" ")] }
    : { file: "mvn", args };
}

/**
 * Platform-specific Java command name.
 */
function javaCommand(): string {
  return process.platform === "win32" ? "java.exe" : "java";
}

/**
 * Quotes one Windows shell argument.
 */
function shellQuote(value: string): string {
  return `"${value.replaceAll('"', '\\"')}"`;
}
