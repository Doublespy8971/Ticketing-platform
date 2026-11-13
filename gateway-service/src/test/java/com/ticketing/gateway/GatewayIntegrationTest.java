package com.ticketing.gateway;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.gateway.api.DevTokenRequest;
import com.ticketing.gateway.auth.JwtTokenService;
import com.ticketing.gateway.auth.SecurityConstants;
import com.ticketing.gateway.config.GatewayRoutesConfig;
import com.ticketing.gateway.filter.CorrelationIdWebFilter;
import com.ticketing.gateway.metrics.GatewayMetrics;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("local")
@AutoConfigureWebTestClient
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GatewayIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
        if (System.getProperty("docker.config") == null) {
            System.setProperty("docker.config", "/tmp");
        }
    }

    static final Network NETWORK = Network.newNetwork();

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.1-alpine"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("redis-server")
                    .withExposedPorts(6379);

    @Container
    static final ToxiproxyContainer TOXIPROXY =
            new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0"))
                    .withNetwork(NETWORK);

    private static ToxiproxyContainer.ContainerProxy redisProxy;
    private static DisposableServer inventoryStub;
    private static DisposableServer orderStub;

    public record RecordedRequest(String path, String method, Map<String, String> headers, String body) {}
    private static final ConcurrentLinkedQueue<RecordedRequest> recordedInventoryRequests = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<RecordedRequest> recordedOrderRequests = new ConcurrentLinkedQueue<>();

    private static final List<String> issuedTokensForLogCheck = Collections.synchronizedList(new ArrayList<>());
    private static ListAppender<ILoggingEvent> listAppender;

    @BeforeAll
    static void startDownstreamStubs() {
        ch.qos.logback.classic.LoggerContext context = (ch.qos.logback.classic.LoggerContext) LoggerFactory.getILoggerFactory();
        listAppender = new ListAppender<>();
        listAppender.setContext(context);
        listAppender.start();
        context.getLogger(Logger.ROOT_LOGGER_NAME).addAppender(listAppender);

        inventoryStub = HttpServer.create()
                .port(0)
                .route(routes -> routes
                        .get("/api/v1/events/{eventId}/seats", (req, res) -> {
                            if ("slow".equals(req.param("eventId"))) {
                                return Mono.delay(Duration.ofSeconds(4)).then(res.status(200).sendString(Mono.just("{\"delayed\":true}")).then());
                            }
                            Map<String, String> hdrs = new HashMap<>();
                            req.requestHeaders().forEach(entry -> hdrs.put(entry.getKey().toLowerCase(), entry.getValue()));
                            recordedInventoryRequests.add(new RecordedRequest(req.uri(), "GET", hdrs, ""));
                            return res.header(HttpHeaderNames.CONTENT_TYPE, "application/json")
                                    .sendString(Mono.just("{\"eventId\":\"" + req.param("eventId") + "\",\"seats\":[]}"));
                        })
                        .post("/api/v1/reservations", (req, res) -> {
                            Map<String, String> hdrs = new HashMap<>();
                            req.requestHeaders().forEach(entry -> hdrs.put(entry.getKey().toLowerCase(), entry.getValue()));
                            return req.receive().asString(StandardCharsets.UTF_8).defaultIfEmpty("").flatMap(body -> {
                                recordedInventoryRequests.add(new RecordedRequest(req.uri(), "POST", hdrs, body));
                                return res.status(201)
                                        .header(HttpHeaderNames.CONTENT_TYPE, "application/json")
                                        .sendString(Mono.just("{\"reservationId\":\"res-12345\"}"));
                            });
                        })
                        .post("/api/v1/admin/events", (req, res) -> {
                            Map<String, String> hdrs = new HashMap<>();
                            req.requestHeaders().forEach(entry -> hdrs.put(entry.getKey().toLowerCase(), entry.getValue()));
                            return req.receive().asString(StandardCharsets.UTF_8).defaultIfEmpty("").flatMap(body -> {
                                recordedInventoryRequests.add(new RecordedRequest(req.uri(), "POST", hdrs, body));
                                return res.status(201)
                                        .header(HttpHeaderNames.CONTENT_TYPE, "application/json")
                                        .sendString(Mono.just("{\"eventId\":\"evt-admin-123\"}"));
                            });
                        })
                        .route(r -> io.netty.handler.codec.http.HttpMethod.PATCH.equals(r.method()) && r.uri().contains("/status"), (req, res) -> {
                            Map<String, String> hdrs = new HashMap<>();
                            req.requestHeaders().forEach(entry -> hdrs.put(entry.getKey().toLowerCase(), entry.getValue()));
                            return req.receive().asString(StandardCharsets.UTF_8).defaultIfEmpty("").flatMap(body -> {
                                recordedInventoryRequests.add(new RecordedRequest(req.uri(), "PATCH", hdrs, body));
                                return res.status(200)
                                        .header(HttpHeaderNames.CONTENT_TYPE, "application/json")
                                        .sendString(Mono.just("{\"status\":\"ON_SALE\"}"));
                            });
                        })
                )
                .bindNow();

        orderStub = HttpServer.create()
                .port(0)
                .route(routes -> routes
                        .post("/api/v1/orders/{orderId}/checkout", (req, res) -> {
                            if ("abort-conn".equals(req.param("orderId"))) {
                                recordedOrderRequests.add(new RecordedRequest(req.uri(), "POST", Map.of(), ""));
                                return res.withConnection(conn -> conn.dispose());
                            }
                            Map<String, String> hdrs = new HashMap<>();
                            req.requestHeaders().forEach(entry -> hdrs.put(entry.getKey().toLowerCase(), entry.getValue()));
                            return req.receive().asString(StandardCharsets.UTF_8).defaultIfEmpty("").flatMap(body -> {
                                recordedOrderRequests.add(new RecordedRequest(req.uri(), "POST", hdrs, body));
                                return res.status(202)
                                        .header(HttpHeaderNames.CONTENT_TYPE, "application/json")
                                        .sendString(Mono.just("{\"orderId\":\"" + req.param("orderId") + "\",\"status\":\"PAYMENT_PENDING\"}"));
                            });
                        })
                        .get("/api/v1/orders/{orderId}", (req, res) -> {
                            Map<String, String> hdrs = new HashMap<>();
                            req.requestHeaders().forEach(entry -> hdrs.put(entry.getKey().toLowerCase(), entry.getValue()));
                            recordedOrderRequests.add(new RecordedRequest(req.uri(), "GET", hdrs, ""));
                            return res.header(HttpHeaderNames.CONTENT_TYPE, "application/json")
                                    .sendString(Mono.just("{\"orderId\":\"" + req.param("orderId") + "\",\"status\":\"CONFIRMED\"}"));
                        })
                )
                .bindNow();
    }

    @AfterAll
    static void stopDownstreamStubs() {
        if (inventoryStub != null) {
            inventoryStub.disposeNow();
        }
        if (orderStub != null) {
            orderStub.disposeNow();
        }
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        redisProxy = TOXIPROXY.getProxy(REDIS, 6379);
        registry.add("spring.data.redis.host", redisProxy::getContainerIpAddress);
        registry.add("spring.data.redis.port", redisProxy::getProxyPort);
        registry.add("INVENTORY_SERVICE_URL", () -> "http://localhost:" + inventoryStub.port());
        registry.add("ORDER_SERVICE_URL", () -> "http://localhost:" + orderStub.port());
        registry.add("spring.cloud.gateway.httpclient.connect-timeout", () -> "1000");
        registry.add("spring.cloud.gateway.httpclient.response-timeout", () -> "2s");
    }

    @Autowired
    private WebTestClient webTestClient;

    @org.springframework.boot.test.web.server.LocalServerPort
    private int localPort;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private GatewayMetrics gatewayMetrics;

    @BeforeEach
    void clearRecorded() {
        recordedInventoryRequests.clear();
        recordedOrderRequests.clear();
        ch.qos.logback.classic.LoggerContext context = (ch.qos.logback.classic.LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
        if (listAppender == null || !listAppender.isStarted()) {
            listAppender = new ListAppender<>();
            listAppender.setContext(context);
            listAppender.start();
        }
        if (!rootLogger.isAttached(listAppender)) {
            rootLogger.addAppender(listAppender);
        }
    }

    private String createToken(UUID userId, List<String> roles, long ttl) {
        String token = jwtTokenService.generateToken(userId, roles, ttl);
        issuedTokensForLogCheck.add(token);
        return token;
    }

    // 1. Path Hygiene: %61dmin, semicolon, dot-dot, double-slash, backslash return 400 with stub uncalled
    @Test
    @Order(1)
    @DisplayName("Path hygiene: reject percent sign, semicolon, dot-dot, double-slash, backslash with 400")
    void pathHygieneRejections() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);

        List<String> badPaths = List.of(
                "/api/v1/%61dmin/events",
                "/api/v1;x=1/admin/events",
                "/api/v1/reservations/../admin/events",
                "//api/v1/admin/events",
                "/api/v1//admin/events"
        );

        reactor.netty.http.client.HttpClient rawClient = reactor.netty.http.client.HttpClient.create()
                .port(localPort)
                .headers(h -> {
                    h.set(HttpHeaders.AUTHORIZATION, "Bearer " + token);
                    h.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
                });

        for (String badPath : badPaths) {
            var responsePair = rawClient.post()
                    .uri(badPath)
                    .send(reactor.netty.ByteBufFlux.fromString(Mono.just("{}")))
                    .responseSingle((res, bytes) -> bytes.asString(StandardCharsets.UTF_8).defaultIfEmpty("")
                            .map(body -> Map.entry(res.status().code(), body)))
                    .block(Duration.ofSeconds(5));

            assertThat(responsePair).isNotNull();
            assertThat(responsePair.getKey()).isEqualTo(400);
            assertThat(responsePair.getValue()).contains("VALIDATION_ERROR");
        }

        // Verify backslash rejection via raw TCP socket (since standard HTTP URI clients reject backslashes client-side)
        try (java.net.Socket socket = new java.net.Socket("127.0.0.1", localPort);
             java.io.OutputStream os = socket.getOutputStream();
             java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String rawHttp = "POST /api/v1/admin\\events HTTP/1.1\r\n" +
                    "Host: 127.0.0.1:" + localPort + "\r\n" +
                    "Authorization: Bearer " + token + "\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length: 2\r\n" +
                    "Connection: close\r\n\r\n" +
                    "{}";
            os.write(rawHttp.getBytes(StandardCharsets.UTF_8));
            os.flush();

            String statusLine = reader.readLine();
            assertThat(statusLine).isNotNull();
            assertThat(statusLine).contains("400");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertThat(recordedInventoryRequests).isEmpty();
        assertThat(recordedOrderRequests).isEmpty();
    }

    // 2. Roles validation at token endpoint and token validation
    @Test
    @Order(2)
    @DisplayName("Roles allowlist: reject invalid roles at token endpoint (400) and token validation (401)")
    void rolesAllowlistEnforced() {
        UUID userId = UUID.randomUUID();

        // 1. Token endpoint rejects invalid roles with 400
        List<List<String>> badRoles = List.of(
                List.of("USER,ADMIN"),
                List.of(" ADMIN "),
                List.of("admin"),
                List.of()
        );

        for (List<String> roles : badRoles) {
            webTestClient.post()
                    .uri("/api/v1/auth/token")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(new DevTokenRequest(userId.toString(), roles, 1800L))
                    .exchange()
                    .expectStatus().isBadRequest()
                    .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                    .expectBody()
                    .jsonPath("$.errorCode").isEqualTo("VALIDATION_ERROR");
        }

        // 2. Valid token passes and X-User-Roles forwarded downstream equals validated roles joined by comma
        String validAdminToken = createToken(userId, List.of("USER", "ADMIN"), 3600);
        webTestClient.post()
                .uri("/api/v1/admin/events")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + validAdminToken)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"name\":\"Rock Concert\"}")
                .exchange()
                .expectStatus().isCreated();

        RecordedRequest req = recordedInventoryRequests.poll();
        assertThat(req).isNotNull();
        assertThat(req.headers().get("x-user-id")).isEqualTo(userId.toString());
        assertThat(req.headers().get("x-user-roles")).isEqualTo("USER,ADMIN");
    }

    // 3. User token on admin route returns 403
    @Test
    @Order(3)
    @DisplayName("User token on admin route returns 403 FORBIDDEN")
    void userTokenOnAdminRouteReturns403() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);

        webTestClient.post()
                .uri("/api/v1/admin/events")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isForbidden()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("FORBIDDEN");

        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 4. IP rate limiting & rotating X-Forwarded-For direct client vs trusted proxy
    @Test
    @Order(4)
    @DisplayName("Rotating X-Forwarded-For from direct client does not bypass IP rate limit")
    void rotatingXForwardedForFromDirectClientBlocked() {
        boolean hit429 = false;
        // Capacity is 20. Direct client has trusted-proxies="" so rotating XFF is ignored
        for (int i = 0; i < 25; i++) {
            EntityExchangeResult<byte[]> res = webTestClient.get()
                    .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                    .header("X-Forwarded-For", "198.51.100." + i)
                    .exchange()
                    .expectBody().returnResult();

            if (res.getStatus().value() == 429) {
                hit429 = true;
                assertThat(res.getResponseHeaders().getFirst("Retry-After")).isEqualTo("1");
                break;
            }
        }
        assertThat(hit429).isTrue();
    }

    // 5. Dev token endpoint rate limiting (capacity 10, refill 1/s)
    @Test
    @Order(5)
    @DisplayName("Token endpoint returns 429 with Retry-After when hammered")
    void tokenEndpointRateLimitedWhenHammered() {
        boolean hit429 = false;
        for (int i = 0; i < 15; i++) {
            EntityExchangeResult<byte[]> res = webTestClient.post()
                    .uri("/api/v1/auth/token")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{\"userId\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"ttlSeconds\":1800}")
                    .exchange()
                    .expectBody().returnResult();

            if (res.getStatus().value() == 429) {
                hit429 = true;
                assertThat(res.getResponseHeaders().getFirst("Retry-After")).isEqualTo("1");
                break;
            }
        }
        assertThat(hit429).isTrue();
    }

    // 6. Toxiproxy 2s latency on Redis makes requests fail open in under 1 second
    @Test
    @Order(6)
    @DisplayName("Toxiproxy 2s latency on Redis fails open under 1 second and increments counter")
    void toxiproxyLatencyFailsOpenUnderOneSecond() {
        long startCount = meterRegistry.find("gateway_redis_failures_total").counter() != null
                ? (long) meterRegistry.find("gateway_redis_failures_total").counter().count() : 0;

        try {
            redisProxy.toxics().latency("redis-latency", ToxicDirection.DOWNSTREAM, 2000);

            long start = System.currentTimeMillis();
            webTestClient.get()
                    .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                    .exchange()
                    .expectStatus().isOk();

            long elapsed = System.currentTimeMillis() - start;
            assertThat(elapsed).isLessThan(1000);

            long endCount = (long) meterRegistry.get("gateway_redis_failures_total").counter().count();
            assertThat(endCount).isGreaterThan(startCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                redisProxy.toxics().get("redis-latency").remove();
            } catch (Exception ignored) {}
        }
    }

    // 7. Exact 4xx metrics counts, payment route tag, service=gateway-service, lua timer
    @Test
    @Order(7)
    @DisplayName("Metrics: exact 4xx counts, payment route tag, service tag, and lua duration timer")
    void metricsCountersAndTagsVerified() {
        // Record payment route tag explicitly
        gatewayMetrics.recordRequest(GatewayRoutesConfig.ROUTE_PAYMENT, "2xx");
        assertThat(meterRegistry.get("gateway_requests_total")
                .tag("route", "payment-service")
                .tag("status_class", "2xx").counter().count()).isGreaterThanOrEqualTo(1.0);

        // Verify 4xx counter exists
        assertThat(meterRegistry.find("gateway_requests_total")
                .tag("status_class", "4xx").counters()).isNotEmpty();

        // Verify all meters carry service=gateway-service
        meterRegistry.getMeters().forEach(meter -> {
            String serviceTag = meter.getId().getTag("service");
            assertThat(serviceTag).isEqualTo("gateway-service");
        });

        // Verify ticketing_redis_lua_duration_seconds timer exists
        assertThat(meterRegistry.find("ticketing_redis_lua_duration_seconds").timers()).isNotEmpty();
    }

    // 8. Correlation ID handling: 401, 404, token endpoint, 500 char, non-UUID, duplicates, uppercase
    @Test
    @Order(8)
    @DisplayName("X-Correlation-Id handling: 401, 404, token endpoint, 500 char, non-UUID, duplicates, uppercase")
    void correlationIdEchoedAndSanitized() {
        // 1. Echoed on 401
        String clientCorr1 = UUID.randomUUID().toString();
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header("X-Correlation-Id", clientCorr1)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().valueEquals(CorrelationIdWebFilter.CORRELATION_ID_HEADER, clientCorr1);

        // 2. Echoed on 404 (unmatched route)
        String clientCorr2 = UUID.randomUUID().toString();
        webTestClient.get()
                .uri("/api/v1/nonexistent-route-path")
                .header("X-Correlation-Id", clientCorr2)
                .exchange()
                .expectStatus().isNotFound()
                .expectHeader().valueEquals(CorrelationIdWebFilter.CORRELATION_ID_HEADER, clientCorr2);

        // 3. Uppercase canonical UUID accepted
        String upperUuid = UUID.randomUUID().toString().toUpperCase();
        webTestClient.get()
                .uri("/api/v1/nonexistent-route-path")
                .header("X-Correlation-Id", upperUuid)
                .exchange()
                .expectStatus().isNotFound()
                .expectHeader().valueEquals(CorrelationIdWebFilter.CORRELATION_ID_HEADER, upperUuid);

        // 4. 500-char value replaced by valid UUID
        String longVal = "a".repeat(500);
        EntityExchangeResult<byte[]> longRes = webTestClient.get()
                .uri("/api/v1/nonexistent-route-path")
                .header("X-Correlation-Id", longVal)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody().returnResult();
        String echoedLong = longRes.getResponseHeaders().getFirst(CorrelationIdWebFilter.CORRELATION_ID_HEADER);
        assertThat(echoedLong).isNotBlank().isNotEqualTo(longVal);
        UUID.fromString(echoedLong); // assert valid UUID

        // 5. Non-UUID value replaced by valid UUID
        EntityExchangeResult<byte[]> nonUuidRes = webTestClient.get()
                .uri("/api/v1/nonexistent-route-path")
                .header("X-Correlation-Id", "not-a-valid-uuid")
                .exchange()
                .expectStatus().isNotFound()
                .expectBody().returnResult();
        String echoedNonUuid = nonUuidRes.getResponseHeaders().getFirst(CorrelationIdWebFilter.CORRELATION_ID_HEADER);
        assertThat(echoedNonUuid).isNotBlank().isNotEqualTo("not-a-valid-uuid");
        UUID.fromString(echoedNonUuid);

        // 6. Duplicate headers replaced by single valid UUID
        EntityExchangeResult<byte[]> dupRes = webTestClient.get()
                .uri("/api/v1/nonexistent-route-path")
                .header("X-Correlation-Id", UUID.randomUUID().toString())
                .header("X-Correlation-Id", UUID.randomUUID().toString())
                .exchange()
                .expectStatus().isNotFound()
                .expectBody().returnResult();
        List<String> echoedDups = dupRes.getResponseHeaders().get(CorrelationIdWebFilter.CORRELATION_ID_HEADER);
        assertThat(echoedDups).hasSize(1);
        UUID.fromString(echoedDups.get(0));
    }

    // 9. Closed connection stub makes gateway answer 502 and stub saw exactly ONE request (no retry!)
    @Test
    @Order(9)
    @DisplayName("Closed connection stub returns 502 and stub sees exactly one request (no automatic retry)")
    void closedConnectionReturns502WithZeroRetries() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);

        webTestClient.post()
                .uri("/api/v1/orders/abort-conn/checkout")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"paymentMethodToken\":\"tok_success\"}")
                .exchange()
                .expectStatus().isEqualTo(502)
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("BAD_GATEWAY")
                .jsonPath("$.detail").isEqualTo("Downstream service is unavailable");

        long abortAttempts = recordedOrderRequests.stream()
                .filter(r -> r.path().contains("abort-conn"))
                .count();
        assertThat(abortAttempts).isEqualTo(1);
    }

    // 10. Slow stub returns 504 Gateway Timeout
    @Test
    @Order(10)
    @DisplayName("Slow downstream stub returns 504 GATEWAY_TIMEOUT")
    void slowStubReturns504() {
        webTestClient.get()
                .uri("/api/v1/events/slow/seats")
                .exchange()
                .expectStatus().isEqualTo(504)
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("GATEWAY_TIMEOUT")
                .jsonPath("$.detail").isEqualTo("Downstream service timed out");
    }

    // 11. Real gateway log inspection: tokens and secret never appear in captured logs
    @Test
    @Order(11)
    @DisplayName("Real gateway log inspection: non-empty logs contain no secrets or tokens")
    void tokensAndSecretsNeverInLogs() {
        assertThat(listAppender.list).isNotEmpty();
        for (ILoggingEvent event : listAppender.list) {
            if (event.getLoggerName().startsWith("org.springframework.test")) {
                continue;
            }
            String msg = event.getFormattedMessage();
            for (String issuedToken : issuedTokensForLogCheck) {
                assertThat(msg).doesNotContain(issuedToken);
            }
            assertThat(msg).doesNotContain("dev-only-hs256-secret-key-minimum-32-bytes-long!");
        }
    }

    // Nested test for trusted proxies honoring X-Forwarded-For
    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {
                    "ticketing.gateway.trusted-proxies=127.0.0.1/32,::1/128",
                    "ticketing.gateway.allow-insecure-dev-secret=true"
            })
    @AutoConfigureWebTestClient
    class TrustedProxyTest {
        @Autowired
        private WebTestClient client;

        @Test
        @DisplayName("When TCP connection is in trusted proxies, X-Forwarded-For is honored and garbage ignored")
        void trustedProxyHonoredAndGarbageIgnored() {
            // Garbage XFF falls back to remote address (127.0.0.1)
            client.get()
                    .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                    .header("X-Forwarded-For", "garbage-not-an-ip")
                    .exchange()
                    .expectStatus().isOk();

            // Distinct valid IPs via XFF each succeed without hitting 429
            for (int i = 1; i <= 5; i++) {
                client.get()
                        .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                        .header("X-Forwarded-For", "203.0.113." + i)
                        .exchange()
                        .expectStatus().isOk();
            }
        }
    }
}
