/**
 * Integration tests against the official rsocket-java WebSocket responder.
 */
import { execFileSync, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { createServer } from "node:net";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Flux, type Subscription } from "reactor-core-ts";
import { WellKnownMimeType } from "rsocket-frames-ts";
import { RequestResponseController, RSocket } from "@";
import type { RSocketPayloadFrame, RSocketWebSocket } from "@/types/index.js";

const projectRoot = fileURLToPath(new URL("../", import.meta.url));
const serverProject = resolve(projectRoot, "test/rsocket-java-server/pom.xml");
const serverSource = resolve(
  projectRoot,
  "test/rsocket-java-server/src/main/java/dev/ckateptb/rsocket/browser/tests/RSocketBrowserTestServer.java"
);
const serverJar = resolve(projectRoot, "test/rsocket-java-server/target/rsocket-browser-integration-server-1.0.0.jar");
const dataMimeType = WellKnownMimeType.APPLICATION_JSON;
const metadataMimeType = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;

let serverProcess: ChildProcessWithoutNullStreams | undefined;
let serverPort = 0;

/**
 * Server-side counters exposed by the Java responder.
 */
interface IntegrationStats {
  /** Number of accepted SETUP frames. */
  readonly setups: number;
  /** Number of fire-and-forget requests. */
  readonly fireAndForget: number;
  /** Number of metadata-push frames. */
  readonly metadataPush: number;
  /** Number of request-response interactions. */
  readonly requestResponse: number;
  /** Number of request-stream interactions. */
  readonly requestStream: number;
  /** Number of request-channel interactions. */
  readonly requestChannel: number;
}

/** Timed request used to verify requester cancellation against rsocket-java. */
class TimedJavaRequestController extends RequestResponseController<
  { cmd: string; ms: number },
  unknown
> {
  /** Metadata route ignored by the frame-level integration responder. */
  protected readonly route = "test.delay";

  /** Uses the short timeout required by the integration assertion. */
  constructor() {
    super({ timeout: 25 });
  }
}

describe("RSocket official rsocket-java WebSocket integration", () => {
  beforeAll(async () => {
    buildJavaServer();
    serverPort = await freePort();
    serverProcess = spawn(javaCommand(), ["-jar", serverJar, String(serverPort), "30000"], {
      cwd: projectRoot
    });
    await waitForServerReady(serverProcess);
  }, 240_000);

  afterAll(async () => {
    await stopJavaServer();
  });

  it("performs request-response, fire-and-forget, and metadata-push over a real WebSocket transport", async () => {
    const socket = createSocket();
    const connected = await connectedSocket(socket);

    try {
      const before = await stats(socket);
      const echo = await connected
        .requestResponse({ cmd: "echo", value: "hello" })
        .block();

      await connected.fireAndForget({ event: "clicked" }).block();
      await connected
        .metadataPush(WellKnownMimeType.TEXT_PLAIN.toMetadata("Bearer integration-token"))
        .block();

      const after = await stats(socket);

      expect(echo.data).toEqual({
        kind: "response",
        echo: "{\"cmd\":\"echo\",\"value\":\"hello\"}"
      });
      expect(after.fireAndForget).toBe(before.fireAndForget + 1);
      expect(after.metadataPush).toBe(before.metadataPush + 1);
      expect(after.requestResponse).toBe(before.requestResponse + 2);
    } finally {
      connected.disconnect();
    }
  });

  it("fragments a large request-response payload and rsocket-java reassembles it", async () => {
    const socket = createSocket({ maxFrameLength: 1_024 });
    const connected = await connectedSocket(socket);
    const request = {
      cmd: "size",
      value: "x".repeat(8_000)
    };

    try {
      const before = await stats(socket);
      const response = await connected.requestResponse(request).block();

      expect(response.data).toEqual({
        kind: "size",
        length: JSON.stringify(request).length
      });
      expect((await stats(socket)).requestResponse).toBe(before.requestResponse + 2);
    } finally {
      connected.disconnect();
    }
  });

  it("times out a delayed request-response without killing the Java connection", async () => {
    const socket = createSocket();
    const connected = await connectedSocket(socket);

    try {
      const delayed = connected
        .process(new TimedJavaRequestController(), { cmd: "delay", ms: 200 })
        .block();

      await expect(delayed).rejects.toThrow("timed out");
      await expect(connected.requestResponse({ cmd: "echo", value: "after-timeout" }).block())
        .resolves
        .toMatchObject({
          data: {
            kind: "response",
            echo: "{\"cmd\":\"echo\",\"value\":\"after-timeout\"}"
          }
        });
    } finally {
      connected.disconnect();
    }
  });

  it("maps requester demand to request-stream backpressure against rsocket-java", async () => {
    const socket = createSocket();
    const connected = await connectedSocket(socket);
    const received: unknown[] = [];
    let subscription: Subscription | undefined;
    let completed = false;
    let failure: unknown;

    try {
      const before = await stats(socket);

      connected.requestStream({ count: 5 }).subscribe({
        onSubscribe(next: Subscription) {
          subscription = next;
          next.request(2);
        },
        onNext(payload: RSocketPayloadFrame) {
          received.push(payload.data);
        },
        onError(error: unknown) {
          failure = error;
        },
        onComplete() {
          completed = true;
        }
      });

      await waitFor(() => received.length === 2);
      await delay(150);

      expect(received).toEqual([{ n: 1 }, { n: 2 }]);
      expect(completed).toBe(false);
      expect(failure).toBeUndefined();

      subscription?.request(3);
      await waitFor(() => completed && received.length === 5);

      expect(received).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 }]);
      expect(failure).toBeUndefined();
      expect((await stats(socket)).requestStream).toBe(before.requestStream + 1);
    } finally {
      connected.disconnect();
    }
  });

  it("propagates rsocket-java request-stream errors through the response publisher", async () => {
    const socket = createSocket();
    const connected = await connectedSocket(socket);
    const received: unknown[] = [];
    const errors: unknown[] = [];
    let subscription: Subscription | undefined;

    try {
      const before = await stats(socket);

      connected.requestStream({ error: true }).subscribe({
        onSubscribe(next: Subscription) {
          subscription = next;
          next.request(2);
        },
        onNext(payload: RSocketPayloadFrame) {
          received.push(payload.data);
        },
        onError(error: unknown) {
          errors.push(error);
        },
        onComplete() {}
      });

      await waitFor(() => errors.length === 1);

      expect(received).toEqual([{ n: 1 }]);
      expect((errors[0] as Error).message).toContain("stream boom");
      expect((await stats(socket)).requestStream).toBe(before.requestStream + 1);
      subscription?.cancel();
    } finally {
      connected.disconnect();
    }
  });

  it("runs bidirectional request-channel payloads through rsocket-java", async () => {
    const socket = createSocket();
    const connected = await connectedSocket(socket);
    const received: unknown[] = [];
    let completed = false;
    let failure: unknown;

    try {
      const before = await stats(socket);

      connected
        .requestChannel(Flux.fromArray([
          { data: { n: 1 } },
          { data: { n: 2 } },
          { data: { n: 3 } }
        ]))
        .subscribe({
          onSubscribe(subscription: Subscription) {
            subscription.request(3);
          },
          onNext(payload: RSocketPayloadFrame) {
            received.push(payload.data);
          },
          onError(error: unknown) {
            failure = error;
          },
          onComplete() {
            completed = true;
          }
        });

      await waitFor(() => completed && received.length === 3);

      expect(received).toEqual([
        { kind: "channel", echo: "{\"n\":1}" },
        { kind: "channel", echo: "{\"n\":2}" },
        { kind: "channel", echo: "{\"n\":3}" }
      ]);
      expect(failure).toBeUndefined();
      expect((await stats(socket)).requestChannel).toBe(before.requestChannel + 1);
    } finally {
      connected.disconnect();
    }
  });

  it("runs sink-style request-channel payloads through rsocket-java", async () => {
    const socket = createSocket();
    const connected = await connectedSocket(socket);
    const channel = connected.requestChannel();
    const received: unknown[] = [];
    let completed = false;
    let failure: unknown;

    try {
      const before = await stats(socket);

      channel.subscribe({
        onSubscribe(subscription: Subscription) {
          subscription.request(2);
        },
        onNext(payload: RSocketPayloadFrame) {
          received.push(payload.data);
        },
        onError(error: unknown) {
          failure = error;
        },
        onComplete() {
          completed = true;
        }
      });

      channel.next({ data: { sink: 1 } });
      channel.next({ data: { sink: 2 } });
      channel.complete();

      await waitFor(() => completed && received.length === 2);

      expect(received).toEqual([
        { kind: "channel", echo: "{\"sink\":1}" },
        { kind: "channel", echo: "{\"sink\":2}" }
      ]);
      expect(failure).toBeUndefined();
      expect((await stats(socket)).requestChannel).toBe(before.requestChannel + 1);
    } finally {
      connected.disconnect();
    }
  });

  it("keeps queued one-shot requests while reconnecting and resumes the rsocket-java session", async () => {
    const sockets: WebSocket[] = [];
    const events: Array<{ type: string; reconnect: boolean }> = [];
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
      const before = await stats(socket);

      sockets[0]?.close(3001, "integration reconnect");
      await waitFor(() => events.some((event) => event.type === "reconnecting"));

      const queued = socket
        .requestResponse({ cmd: "echo", value: "queued-during-reconnect" })
        .block();

      await waitFor(() => events.some((event) => event.type === "connected" && event.reconnect));
      const response = await queued;
      const after = await stats(socket);

      expect(sockets).toHaveLength(2);
      expect(events.filter((event) => event.type === "connected" && event.reconnect)).toHaveLength(1);
      expect(events.filter((event) => event.type === "reconnectFailed")).toHaveLength(0);
      expect(response?.data).toEqual({
        kind: "response",
        echo: "{\"cmd\":\"echo\",\"value\":\"queued-during-reconnect\"}"
      });
      expect(after.setups).toBe(before.setups);
      expect(after.requestResponse).toBe(before.requestResponse + 2);
    } finally {
      connected.disconnect();
    }
  }, 30_000);
});

/**
 * Creates a browser-client instance wired to the Java test server.
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

  return new RSocket(`ws://127.0.0.1:${serverPort}`, options as never);
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
 * Reads server-side interaction counters.
 */
async function stats(socket: RSocket): Promise<IntegrationStats> {
  const response = await socket.requestResponse({ cmd: "stats" }).block();
  return response?.data as IntegrationStats;
}

/**
 * Builds the official Java responder fixture.
 */
function buildJavaServer(): void {
  if (javaServerJarIsFresh()) return;
  const command = mavenCommand();
  execFileSync(command.file, command.args, {
    cwd: projectRoot,
    stdio: "inherit"
  });
}

/**
 * Avoids rebuilding the shaded jar when a previous test run already built the
 * current fixture source.
 */
function javaServerJarIsFresh(): boolean {
  if (!existsSync(serverJar)) return false;
  const jarTime = statSync(serverJar).mtimeMs;
  return [serverProject, serverSource].every((file) => statSync(file).mtimeMs <= jarTime);
}

/**
 * Waits for the Java process to print its readiness line.
 */
function waitForServerReady(child: ChildProcessWithoutNullStreams): Promise<void> {
  return new Promise((resolve, reject) => {
    let stdout = "";
    let stderr = "";
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for rsocket-java test server. stderr: ${stderr}`));
    }, 30_000);

    const cleanup = (): void => {
      clearTimeout(timeout);
      child.stdout.off("data", onStdout);
      child.stderr.off("data", onStderr);
      child.off("exit", onExit);
      child.off("error", onError);
    };

    const onStdout = (chunk: Buffer): void => {
      stdout += chunk.toString("utf8");
      if (stdout.includes("RSOCKET_TEST_SERVER_READY")) {
        cleanup();
        resolve();
      }
    };
    const onStderr = (chunk: Buffer): void => {
      stderr += chunk.toString("utf8");
    };
    const onExit = (code: number | null): void => {
      cleanup();
      reject(new Error(`rsocket-java test server exited before ready with code ${code}. stderr: ${stderr}`));
    };
    const onError = (error: Error): void => {
      cleanup();
      reject(error);
    };

    child.stdout.on("data", onStdout);
    child.stderr.on("data", onStderr);
    child.once("exit", onExit);
    child.once("error", onError);
  });
}

/**
 * Stops the Java responder process.
 */
async function stopJavaServer(): Promise<void> {
  const child = serverProcess;
  serverProcess = undefined;
  if (child === undefined || child.exitCode !== null) return;

  child.kill();
  await Promise.race([
    new Promise<void>((resolve) => child.once("exit", () => resolve())),
    delay(5_000).then(() => {
      if (child.exitCode === null) child.kill("SIGKILL");
    })
  ]);
}

/**
 * Finds a free local TCP port for the Java server.
 */
function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() => {
        if (address === null || typeof address === "string") reject(new Error("Unable to allocate a TCP port"));
        else resolve(address.port);
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
    if (Date.now() - startedAt > timeoutMs) throw new Error("Timed out waiting for integration condition");
    await delay(10);
  }
}

/**
 * Delays for a fixed number of milliseconds.
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
