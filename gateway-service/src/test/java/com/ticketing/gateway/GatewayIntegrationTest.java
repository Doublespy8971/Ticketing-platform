package com.ticketing.gateway;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.gateway.auth.JwtTokenService;
import com.ticketing.gateway.filter.CorrelationIdGlobalFilter;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
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

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.1-alpine"))
                    .withExposedPorts(6379);

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
                )
                .bindNow();

        orderStub = HttpServer.create()
                .port(0)
                .route(routes -> routes
                        .post("/api/v1/orders/{orderId}/checkout", (req, res) -> {
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
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("INVENTORY_SERVICE_URL", () -> "http://localhost:" + inventoryStub.port());
        registry.add("ORDER_SERVICE_URL", () -> "http://localhost:" + orderStub.port());
        registry.add("PAYMENT_SERVICE_URL", () -> "http://localhost:59999"); // closed port for 502 test
        registry.add("ticketing.gateway.allow-insecure-dev-secret", () -> "true");
        registry.add("ticketing.gateway.dev-token.enabled", () -> "true");
        registry.add("ticketing.gateway.dev-token.allow-admin", () -> "false");
        registry.add("spring.cloud.gateway.httpclient.connect-timeout", () -> "1000");
        registry.add("spring.cloud.gateway.httpclient.response-timeout", () -> "2s");
    }

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void clearRecorded() {
        recordedInventoryRequests.clear();
        recordedOrderRequests.clear();
        ch.qos.logback.classic.LoggerContext context = (ch.qos.logback.classic.LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
        ch.qos.logback.classic.Logger appLogger = context.getLogger("com.ticketing");
        if (listAppender == null || !listAppender.isStarted()) {
            listAppender = new ListAppender<>();
            listAppender.setContext(context);
            listAppender.start();
        }
        if (!rootLogger.isAttached(listAppender)) {
            rootLogger.addAppender(listAppender);
        }
        if (!appLogger.isAttached(listAppender)) {
            appLogger.addAppender(listAppender);
        }
    }

    private String createToken(UUID userId, List<String> roles, long ttl) {
        String token = jwtTokenService.generateToken(userId, roles, ttl);
        issuedTokensForLogCheck.add(token);
        return token;
    }

    // 1. Valid user token reaches stub and stub receives X-User-Id and X-User-Roles derived from token
    @Test
    @Order(1)
    @DisplayName("Valid user token reaches stub with X-User-Id and X-User-Roles")
    void validUserTokenReachesStubWithHeaders() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"eventId\":\"" + UUID.randomUUID() + "\",\"seatIds\":[\"" + UUID.randomUUID() + "\"]}")
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().value("X-Correlation-Id", val -> assertThat(val).isNotBlank());

        RecordedRequest req = recordedInventoryRequests.poll();
        assertThat(req).isNotNull();
        assertThat(req.headers().get("x-user-id")).isEqualTo(userId.toString());
        assertThat(req.headers().get("x-user-roles")).isEqualTo("USER");
    }

    // 2. Request with valid user token that ALSO sends spoofed headers reaches stub with token-derived values only
    @Test
    @Order(2)
    @DisplayName("Valid user token + spoofed headers arrives with token-derived values only")
    void validUserTokenWithSpoofedHeadersSanitized() {
        UUID validUserId = UUID.randomUUID();
        UUID spoofedUserId = UUID.randomUUID();
        String token = createToken(validUserId, List.of("USER"), 3600);

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header("X-User-Id", spoofedUserId.toString())
                .header("X-User-Roles", "ADMIN")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();

        RecordedRequest req = recordedInventoryRequests.poll();
        assertThat(req).isNotNull();
        assertThat(req.headers().get("x-user-id")).isEqualTo(validUserId.toString());
        assertThat(req.headers().get("x-user-roles")).isEqualTo("USER");
    }

    // 3. Spoofed headers without any token return 401 and never reach the stub
    @Test
    @Order(3)
    @DisplayName("Spoofed headers without token return 401 and stub is never called")
    void spoofedHeadersWithoutTokenReturns401() {
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header("X-User-Id", UUID.randomUUID().toString())
                .header("X-User-Roles", "ADMIN")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("UNAUTHORIZED")
                .jsonPath("$.status").isEqualTo(401);

        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 4. Expired token returns 401 with documented RFC 7807 body
    @Test
    @Order(4)
    @DisplayName("Expired token returns 401")
    void expiredTokenReturns401() {
        UUID userId = UUID.randomUUID();
        String payload = "{\"sub\":\"" + userId + "\",\"roles\":[\"USER\"],\"iat\":1000,\"exp\":1001}";
        String header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        String b64Payload = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        String token = sign(header + "." + b64Payload, "dev-only-hs256-secret-key-minimum-32-bytes-long!");
        issuedTokensForLogCheck.add(token);

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("UNAUTHORIZED")
                .jsonPath("$.status").isEqualTo(401)
                .jsonPath("$.detail").isEqualTo("Token has expired");

        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 5. Wrongly signed token returns 401
    @Test
    @Order(5)
    @DisplayName("Wrongly signed token returns 401")
    void wronglySignedTokenReturns401() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);
        String[] parts = token.split("\\.");
        String wrongToken = parts[0] + "." + parts[1] + ".invalidSignatureFake123456";
        issuedTokensForLogCheck.add(wrongToken);

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + wrongToken)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("UNAUTHORIZED")
                .jsonPath("$.status").isEqualTo(401)
                .jsonPath("$.detail").isEqualTo("Invalid token signature");

        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 6. Malformed token returns 401
    @Test
    @Order(6)
    @DisplayName("Malformed token returns 401")
    void malformedTokenReturns401() {
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer not.a.valid.jwt")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("UNAUTHORIZED")
                .jsonPath("$.status").isEqualTo(401);

        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 7. Missing-claim token returns 401
    @Test
    @Order(7)
    @DisplayName("Missing-claim token returns 401")
    void missingClaimTokenReturns401() {
        String noSub = "{\"roles\":[\"USER\"],\"iat\":1000,\"exp\":2000000000}";
        String header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        String b64Payload = Base64.getUrlEncoder().withoutPadding().encodeToString(noSub.getBytes(StandardCharsets.UTF_8));
        String token = sign(header + "." + b64Payload, "dev-only-hs256-secret-key-minimum-32-bytes-long!");
        issuedTokensForLogCheck.add(token);

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("UNAUTHORIZED")
                .jsonPath("$.status").isEqualTo(401);

        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 8. User token on admin route returns 403 and stub is not called
    @Test
    @Order(8)
    @DisplayName("User token on admin route returns 403 and stub is not called")
    void userTokenOnAdminRouteReturns403() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);

        webTestClient.post()
                .uri("/api/v1/admin/events")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"name\":\"Test Event\"}")
                .exchange()
                .expectStatus().isForbidden()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("FORBIDDEN")
                .jsonPath("$.status").isEqualTo(403);

        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 9. Admin token on admin route is forwarded
    @Test
    @Order(9)
    @DisplayName("Admin token on admin route is forwarded")
    void adminTokenOnAdminRouteIsForwarded() {
        UUID adminId = UUID.randomUUID();
        String token = createToken(adminId, List.of("ADMIN"), 3600);

        webTestClient.post()
                .uri("/api/v1/admin/events")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"name\":\"Test Event\"}")
                .exchange()
                .expectStatus().isCreated();

        RecordedRequest req = recordedInventoryRequests.poll();
        assertThat(req).isNotNull();
        assertThat(req.headers().get("x-user-id")).isEqualTo(adminId.toString());
        assertThat(req.headers().get("x-user-roles")).isEqualTo("ADMIN");
    }

    // 10. Authorization header is NOT forwarded downstream
    @Test
    @Order(10)
    @DisplayName("Authorization header is NOT forwarded downstream")
    void authorizationHeaderNotForwarded() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();

        RecordedRequest req = recordedInventoryRequests.poll();
        assertThat(req).isNotNull();
        assertThat(req.headers().get("authorization")).isNull();
        assertThat(req.headers().get("Authorization")).isNull();
    }

    // 11. Idempotency-Key and X-Correlation-Id are forwarded; correlation id generated and echoed when absent
    @Test
    @Order(11)
    @DisplayName("Idempotency-Key and X-Correlation-Id forwarded; correlation id generated and echoed")
    void idempotencyKeyAndCorrelationIdForwarded() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);
        String idempotencyKey = UUID.randomUUID().toString();
        String clientCorrelationId = UUID.randomUUID().toString();

        // With client correlation id
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header("Idempotency-Key", idempotencyKey)
                .header("X-Correlation-Id", clientCorrelationId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().valueEquals("X-Correlation-Id", clientCorrelationId);

        RecordedRequest req1 = recordedInventoryRequests.poll();
        assertThat(req1).isNotNull();
        assertThat(req1.headers().get("idempotency-key")).isEqualTo(idempotencyKey);
        assertThat(req1.headers().get("x-correlation-id")).isEqualTo(clientCorrelationId);

        // Without client correlation id -> generated and echoed
        EntityExchangeResult<byte[]> result = webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated()
                .expectBody().returnResult();

        String echoedCorrelationId = result.getResponseHeaders().getFirst("X-Correlation-Id");
        assertThat(echoedCorrelationId).isNotBlank();
        UUID.fromString(echoedCorrelationId); // assert valid UUID

        RecordedRequest req2 = recordedInventoryRequests.poll();
        assertThat(req2).isNotNull();
        assertThat(req2.headers().get("x-correlation-id")).isEqualTo(echoedCorrelationId);
    }

    // 12. Token endpoint POST /api/v1/auth/token issues token accepted by gateway; admin rejected when allow-admin=false
    @Test
    @Order(12)
    @DisplayName("Token endpoint issues token accepted by gateway; admin rejected when allow-admin is false")
    void tokenEndpointWorkflow() {
        UUID userId = UUID.randomUUID();

        // 1. Request user token
        EntityExchangeResult<byte[]> tokenResult = webTestClient.post()
                .uri("/api/v1/auth/token")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"userId\":\"" + userId + "\",\"roles\":[\"USER\"],\"ttlSeconds\":1800}")
                .exchange()
                .expectStatus().isOk()
                .expectBody().returnResult();

        String responseBody = new String(tokenResult.getResponseBody(), StandardCharsets.UTF_8);
        try {
            JsonNode json = objectMapper.readTree(responseBody);
            String accessToken = json.path("accessToken").asText();
            assertThat(accessToken).isNotBlank();
            issuedTokensForLogCheck.add(accessToken);

            // Use the minted token on a protected route
            webTestClient.post()
                    .uri("/api/v1/reservations")
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{}")
                    .exchange()
                    .expectStatus().isCreated();

            RecordedRequest req = recordedInventoryRequests.poll();
            assertThat(req).isNotNull();
            assertThat(req.headers().get("x-user-id")).isEqualTo(userId.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 2. Request admin token when allowAdmin is false -> 403 Forbidden
        webTestClient.post()
                .uri("/api/v1/auth/token")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"userId\":\"" + userId + "\",\"roles\":[\"ADMIN\"],\"ttlSeconds\":1800}")
                .exchange()
                .expectStatus().isForbidden()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("FORBIDDEN");
    }

    // 13. Burst above configured burst capacity returns 429 with Retry-After for same user while another user is unaffected
    @Test
    @Order(13)
    @DisplayName("Rate limiting burst above capacity returns 429 for same user; other user unaffected")
    void rateLimitingBurstReturns429() {
        UUID userA = UUID.randomUUID();
        UUID userB = UUID.randomUUID();
        String tokenA = createToken(userA, List.of("USER"), 3600);
        String tokenB = createToken(userB, List.of("USER"), 3600);

        boolean hit429 = false;
        // User capacity is 10. Fire 15 requests rapidly
        for (int i = 0; i < 15; i++) {
            EntityExchangeResult<byte[]> exchangeResult = webTestClient.post()
                    .uri("/api/v1/reservations")
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenA)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{}")
                    .exchange()
                    .expectBody().returnResult();

            int statusCode = exchangeResult.getStatus().value();
            if (statusCode == 429) {
                hit429 = true;
                assertThat(exchangeResult.getResponseHeaders().getFirst("Retry-After")).isEqualTo("1");
                assertThat(exchangeResult.getResponseHeaders().getContentType().toString()).contains("application/problem+json");
                break;
            }
        }
        assertThat(hit429).isTrue();

        // User B must be unaffected!
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenB)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();
    }

    // 14. Downstream timeout returns 504 and connection refused returns 502 with RFC 7807 body
    @Test
    @Order(14)
    @DisplayName("Downstream connection failure returns 502 and timeout returns 504")
    void downstreamErrorsProduceProblemDetails() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);

        // 1. Connection failure (payment-service is on closed port 59999)
        webTestClient.get()
                .uri("/api/v1/payments/test")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .exchange()
                .expectStatus().isEqualTo(502)
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("BAD_GATEWAY")
                .jsonPath("$.status").isEqualTo(502)
                .jsonPath("$.detail").isEqualTo("Downstream service is unavailable");

        // 2. Downstream timeout (slow path on inventory: delay 4s > response-timeout 2s)
        webTestClient.get()
                .uri("/api/v1/events/slow/seats")
                .exchange()
                .expectStatus().isEqualTo(504)
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("GATEWAY_TIMEOUT")
                .jsonPath("$.status").isEqualTo(504)
                .jsonPath("$.detail").isEqualTo("Downstream service timed out");
    }

    // 15. All metrics exist with bounded tags
    @Test
    @Order(15)
    @DisplayName("All metrics exist with bounded tags")
    void allMetricsExistWithBoundedTags() {
        // Assert gateway_requests_total exists and has valid tags
        assertThat(meterRegistry.find("gateway_requests_total").counters()).isNotEmpty();
        meterRegistry.find("gateway_requests_total").counters().forEach(counter -> {
            String routeTag = counter.getId().getTag("route");
            String statusClassTag = counter.getId().getTag("status_class");
            assertThat(routeTag).isIn("inventory-seats", "inventory-reservations", "inventory-admin",
                    "order-checkout", "order-get", "auth-token", "actuator", "unknown");
            assertThat(statusClassTag).isIn("2xx", "4xx", "5xx", "other");
        });

        // Assert gateway_rate_limited_total exists
        assertThat(meterRegistry.find("gateway_rate_limited_total").counters()).isNotEmpty();
        meterRegistry.find("gateway_rate_limited_total").counters().forEach(counter -> {
            String routeTag = counter.getId().getTag("route");
            assertThat(routeTag).isNotBlank();
        });

        // Assert gateway_auth_failures_total exists
        assertThat(meterRegistry.find("gateway_auth_failures_total").counters()).isNotEmpty();
        meterRegistry.find("gateway_auth_failures_total").counters().forEach(counter -> {
            String reasonTag = counter.getId().getTag("reason");
            assertThat(reasonTag).isIn("missing", "invalid", "expired", "forbidden");
        });
    }

    // 16. Tokens never appear in captured logs
    @Test
    @Order(16)
    @DisplayName("Tokens never appear in captured logs")
    void tokensNeverAppearInCapturedLogs() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);
        org.slf4j.LoggerFactory.getLogger("com.ticketing.gateway").info("Processing test request for user verification");

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();

        assertThat(listAppender.list).isNotEmpty();
        assertThat(issuedTokensForLogCheck).isNotEmpty();

        for (ILoggingEvent event : listAppender.list) {
            String msg = event.getFormattedMessage();
            for (String issuedToken : issuedTokensForLogCheck) {
                assertThat(msg)
                        .withFailMessage("Security violation: Token was logged in log event: " + msg)
                        .doesNotContain(issuedToken);
            }
        }
    }

    // 17. Redis stopped makes requests behave per policy (fail open) and does not crash gateway
    @Test
    @Order(17)
    @DisplayName("Redis stopped fails open and does not crash gateway")
    void redisStoppedFailsOpen() {
        // Stop Redis container
        REDIS.stop();

        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);

        // Protected request should still succeed (fail-open)
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();

        // Public seat query should also succeed (fail-open)
        webTestClient.get()
                .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                .exchange()
                .expectStatus().isOk();

        assertThat(meterRegistry.get("gateway_redis_failures_total").counter().count()).isGreaterThanOrEqualTo(1.0);
    }

    private static String sign(String signingInput, String secret) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] sig = mac.doFinal(signingInput.getBytes(StandardCharsets.US_ASCII));
            return signingInput + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(sig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
