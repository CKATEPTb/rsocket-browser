package dev.ckateptb.rsocket.browser.tests.spring;

import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.rsocket.server.RSocketServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.annotation.ConnectMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Spring Boot RSocket responder used to verify browser Resume behavior against
 * the Spring messaging stack instead of an in-memory or hand-written responder.
 */
@SpringBootApplication
public class SpringBootRSocketTestServer {
  /**
   * Starts the Spring Boot application that owns the RSocket responder.
   */
  public static void main(String[] args) {
    SpringApplication.run(SpringBootRSocketTestServer.class, args);
  }

  /**
   * Provides counters visible to routed test interactions.
   */
  @Bean
  SpringBootRSocketStats springBootRSocketStats() {
    return new SpringBootRSocketStats();
  }

  /**
   * Enables RSocket Resume on Spring Boot's auto-configured server.
   */
  @Bean
  RSocketServerCustomizer springBootRSocketResume(
      @Value("${test.rsocket.resume-ttl-ms:30000}") long resumeTtlMs,
      @Value("${test.rsocket.fragment-mtu:1024}") int fragmentMtu) {
    return server ->
        server
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .fragment(fragmentMtu)
            .resume(new Resume().sessionDuration(Duration.ofMillis(resumeTtlMs)));
  }

  /**
   * Prints a deterministic readiness marker after the WebFlux RSocket endpoint
   * has been started by Spring Boot.
   */
  @EventListener(ApplicationReadyEvent.class)
  void ready(ApplicationReadyEvent event) {
    String port = event.getApplicationContext().getEnvironment().getProperty("local.server.port");
    System.out.println("SPRING_RSOCKET_TEST_SERVER_READY /127.0.0.1:" + port + "/rsocket");
    System.out.flush();
  }
}

/**
 * Mutable counters exposed through the Spring controller.
 */
final class SpringBootRSocketStats {
  /** Number of accepted SETUP frames. */
  final AtomicInteger setups = new AtomicInteger();
  /** Number of request-response interactions handled by Spring mappings. */
  final AtomicInteger requestResponse = new AtomicInteger();
  /** Number of request-stream interactions handled by Spring mappings. */
  final AtomicInteger requestStream = new AtomicInteger();
  /** Number of request-channel interactions handled by Spring mappings. */
  final AtomicInteger requestChannel = new AtomicInteger();

  /**
   * Returns an immutable JSON-friendly snapshot.
   */
  Map<String, Integer> snapshot() {
    return Map.of(
        "setups", setups.get(),
        "requestResponse", requestResponse.get(),
        "requestStream", requestStream.get(),
        "requestChannel", requestChannel.get());
  }
}

/**
 * Spring annotated RSocket handlers used by browser integration tests.
 */
@Controller
final class SpringBootRSocketController {
  private final SpringBootRSocketStats stats;

  /**
   * Creates a controller backed by shared counters.
   */
  SpringBootRSocketController(SpringBootRSocketStats stats) {
    this.stats = stats;
  }

  /**
   * Counts only fresh SETUP frames; a successful protocol RESUME bypasses this
   * mapping and must leave the counter unchanged.
   */
  @ConnectMapping
  Mono<Void> connect() {
    stats.setups.incrementAndGet();
    return Mono.empty();
  }

  /**
   * Echoes a JSON request through a real Spring {@link MessageMapping}.
   */
  @MessageMapping("echo")
  Mono<Map<String, Object>> echo(Map<String, Object> request) {
    stats.requestResponse.incrementAndGet();
    return Mono.just(
        Map.of(
            "kind", "spring-echo",
            "value", request.get("value"),
            "setups", stats.setups.get()));
  }

  /**
   * Exposes current counters through a routed request-response.
   */
  @MessageMapping("stats")
  Mono<Map<String, Integer>> stats() {
    stats.requestResponse.incrementAndGet();
    return Mono.just(stats.snapshot());
  }

  /**
   * Emits values only as downstream demand arrives from the browser requester.
   */
  @MessageMapping("numbers")
  Flux<Map<String, Integer>> numbers(Map<String, Integer> request) {
    stats.requestStream.incrementAndGet();
    int count = request.getOrDefault("count", 3);
    return Flux.range(1, count).map(value -> Map.of("n", value));
  }

  /**
   * Echoes large values as a demand-controlled stream so both the initial
   * request and every response item require protocol fragmentation.
   */
  @MessageMapping("fragmented-numbers")
  Flux<Map<String, Object>> fragmentedNumbers(Map<String, Object> request) {
    stats.requestStream.incrementAndGet();
    int count = (Integer) request.getOrDefault("count", 2);
    Object value = request.get("value");
    return Flux.range(1, count).map(index -> Map.of("n", index, "value", value));
  }

  /**
   * Echoes every channel input item through the Spring request-channel path.
   */
  @MessageMapping("channel")
  Flux<Map<String, Object>> channel(Publisher<Map<String, Object>> payloads) {
    stats.requestChannel.incrementAndGet();
    return Flux.from(payloads)
        .map(payload -> Map.of("kind", "spring-channel", "value", payload.get("value")));
  }
}
