package dev.ckateptb.rsocket.browser.tests;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.Resume;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Official rsocket-java WebSocket responder used by the TypeScript integration
 * tests. The production client talks to this process exactly as it would talk
 * to a JVM backend.
 */
public final class RSocketBrowserTestServer {
  private static final AtomicInteger SETUPS = new AtomicInteger();
  private static final AtomicInteger FIRE_AND_FORGET = new AtomicInteger();
  private static final AtomicInteger METADATA_PUSH = new AtomicInteger();
  private static final AtomicInteger REQUEST_RESPONSE = new AtomicInteger();
  private static final AtomicInteger REQUEST_STREAM = new AtomicInteger();
  private static final AtomicInteger REQUEST_CHANNEL = new AtomicInteger();

  private RSocketBrowserTestServer() {}

  /**
   * Starts the WebSocket RSocket responder on the requested port.
   */
  public static void main(String[] args) throws Exception {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : 0;
    long resumeTtlMs = args.length > 1 ? Long.parseLong(args[1]) : 30000L;
    CountDownLatch closed = new CountDownLatch(1);

    CloseableChannel server =
        RSocketServer.create(acceptor())
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .resume(new Resume().sessionDuration(Duration.ofMillis(resumeTtlMs)))
            .bindNow(WebsocketServerTransport.create("127.0.0.1", port));

    Runtime.getRuntime().addShutdownHook(new Thread(server::dispose));
    System.out.println("RSOCKET_TEST_SERVER_READY " + server.address());
    System.out.flush();

    server.onClose().doFinally(signal -> closed.countDown()).subscribe();
    closed.await();
  }

  /**
   * Creates one responder per RSocket SETUP frame.
   */
  private static SocketAcceptor acceptor() {
    return (ConnectionSetupPayload setupPayload, RSocket requester) -> {
      SETUPS.incrementAndGet();
      return Mono.just(new TestResponder());
    };
  }

  /**
   * Responder that exercises all requester interaction models.
   */
  private static final class TestResponder implements RSocket {
    /**
     * Records one-way messages.
     */
    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      FIRE_AND_FORGET.incrementAndGet();
      payload.release();
      return Mono.empty();
    }

    /**
     * Echoes JSON requests and exposes server-side counters.
     */
    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      REQUEST_RESPONSE.incrementAndGet();
      String data = payload.getDataUtf8();
      payload.release();

      if (data.contains("\"cmd\":\"stats\"")) {
        return Mono.just(json(statsJson()));
      }
      if (data.contains("\"cmd\":\"delay\"")) {
        int delayMs = intField(data, "ms", 100);
        return Mono.delay(Duration.ofMillis(delayMs))
            .thenReturn(json("{\"kind\":\"delay\",\"ms\":" + delayMs + "}"));
      }
      if (data.contains("\"cmd\":\"size\"")) {
        return Mono.just(json("{\"kind\":\"size\",\"length\":" + data.length() + "}"));
      }
      if (data.contains("\"cmd\":\"close\"")) {
        return Mono.error(new IllegalStateException("requested close"));
      }
      return Mono.just(json("{\"kind\":\"response\",\"echo\":" + jsonString(data) + "}"));
    }

    /**
     * Emits exactly the requested range and lets Reactive Streams demand control
     * when values are sent over the network.
     */
    @Override
    public Flux<Payload> requestStream(Payload payload) {
      REQUEST_STREAM.incrementAndGet();
      String data = payload.getDataUtf8();
      int count = intField(data, "count", 5);
      payload.release();
      if (data.contains("\"error\":true")) {
        return Flux.concat(
            Flux.just(json("{\"n\":1}")), Flux.error(new IllegalStateException("stream boom")));
      }
      return Flux.range(1, count).map(value -> json("{\"n\":" + value + "}"));
    }

    /**
     * Echoes every inbound channel payload back as a JSON object.
     */
    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      REQUEST_CHANNEL.incrementAndGet();
      return Flux.from(payloads)
          .map(
              payload -> {
                String data = payload.getDataUtf8();
                payload.release();
                return json("{\"kind\":\"channel\",\"echo\":" + jsonString(data) + "}");
              });
    }

    /**
     * Records connection-level metadata pushes.
     */
    @Override
    public Mono<Void> metadataPush(Payload payload) {
      METADATA_PUSH.incrementAndGet();
      payload.release();
      return Mono.empty();
    }
  }

  /**
   * Returns current interaction counters as JSON.
   */
  private static String statsJson() {
    return "{"
        + "\"setups\":"
        + SETUPS.get()
        + ",\"fireAndForget\":"
        + FIRE_AND_FORGET.get()
        + ",\"metadataPush\":"
        + METADATA_PUSH.get()
        + ",\"requestResponse\":"
        + REQUEST_RESPONSE.get()
        + ",\"requestStream\":"
        + REQUEST_STREAM.get()
        + ",\"requestChannel\":"
        + REQUEST_CHANNEL.get()
        + "}";
  }

  /**
   * Creates a JSON payload. The requester decodes it through the SETUP data MIME
   * type, so no per-frame MIME metadata is needed.
   */
  private static Payload json(String data) {
    return DefaultPayload.create(data);
  }

  /**
   * Extracts one positive integer from a tiny JSON request without adding a JSON
   * dependency to the test server.
   */
  private static int intField(String json, String field, int fallback) {
    String marker = "\"" + field + "\":";
    int start = json.indexOf(marker);
    if (start < 0) {
      return fallback;
    }
    int cursor = start + marker.length();
    while (cursor < json.length() && Character.isWhitespace(json.charAt(cursor))) {
      cursor += 1;
    }
    int end = cursor;
    while (end < json.length() && Character.isDigit(json.charAt(end))) {
      end += 1;
    }
    if (end == cursor) {
      return fallback;
    }
    return Integer.parseInt(json.substring(cursor, end));
  }

  /**
   * Escapes a Java string as a JSON string literal.
   */
  private static String jsonString(String value) {
    StringBuilder escaped = new StringBuilder(value.length() + 2);
    escaped.append('"');
    for (int index = 0; index < value.length(); index += 1) {
      char ch = value.charAt(index);
      switch (ch) {
        case '\\':
          escaped.append("\\\\");
          break;
        case '"':
          escaped.append("\\\"");
          break;
        case '\n':
          escaped.append("\\n");
          break;
        case '\r':
          escaped.append("\\r");
          break;
        case '\t':
          escaped.append("\\t");
          break;
        default:
          escaped.append(ch);
      }
    }
    escaped.append('"');
    return escaped.toString();
  }
}
