package com.ticketing.gateway;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.gateway.api.DevTokenRequest;
import com.ticketing.gateway.auth.JwtTokenService;
import com.ticketing.gateway.auth.SecurityConstants;
import com.ticketing.gateway.config.DevTokenProperties;
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
import org.springframework.http.HttpMethod;
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
import java.util.Base64;
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
                        .post("/api/v1/admin/events/{eventId}/seats/bulk", (req, res) -> {
                            Map<String, String> hdrs = new HashMap<>();
                            req.requestHeaders().forEach(entry -> hdrs.put(entry.getKey().toLowerCase(), entry.getValue()));
                            return req.receive().asString(StandardCharsets.UTF_8).defaultIfEmpty("").flatMap(body -> {
                                recordedInventoryRequests.add(new RecordedRequest(req.uri(), "POST", hdrs, body));
                                return res.status(201)
                                        .header(HttpHeaderNames.CONTENT_TYPE, "application/json")
                                        .sendString(Mono.just("{\"created\":true}"));
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

    @Autowired
    private DevTokenProperties devTokenProperties;

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

    private double getAuthFailureCount(String reason) {
        var counter = meterRegistry.find("gateway_auth_failures_total")
                .tag("reason", reason)
                .counter();
        return counter != null ? counter.count() : 0.0;
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

    // 4. Admin token on admin route is forwarded
    @Test
    @Order(4)
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

    // 5. Valid user token reaches stub with X-User-Id and X-User-Roles
    @Test
    @Order(5)
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

    // 6. Valid user token + spoofed headers arrives with token-derived values only on protected route and none on public route
    @Test
    @Order(6)
    @DisplayName("Valid user token + spoofed headers arrives with token-derived values only on protected route and none on public route")
    void validUserTokenWithSpoofedHeadersSanitized() {
        UUID validUserId = UUID.randomUUID();
        UUID spoofedUserId = UUID.randomUUID();
        String token = createToken(validUserId, List.of("USER"), 3600);

        // Protected route
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header("X-User-Id", spoofedUserId.toString())
                .header("x-user-id", spoofedUserId.toString())
                .header("X-USER-ID", spoofedUserId.toString())
                .header("X-User-Roles", "ADMIN")
                .header("x-user-roles", "ADMIN")
                .header("X-USER-ROLES", "ADMIN")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();

        RecordedRequest reqProtected = recordedInventoryRequests.poll();
        assertThat(reqProtected).isNotNull();
        assertThat(reqProtected.headers().get("x-user-id")).isEqualTo(validUserId.toString());
        assertThat(reqProtected.headers().get("x-user-roles")).isEqualTo("USER");

        // Public route
        webTestClient.get()
                .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header("X-User-Id", spoofedUserId.toString())
                .header("x-user-id", spoofedUserId.toString())
                .header("X-USER-ID", spoofedUserId.toString())
                .header("X-User-Roles", "ADMIN")
                .header("x-user-roles", "ADMIN")
                .header("X-USER-ROLES", "ADMIN")
                .exchange()
                .expectStatus().isOk();

        RecordedRequest reqPublic = recordedInventoryRequests.poll();
        assertThat(reqPublic).isNotNull();
        assertThat(reqPublic.headers().get("x-user-id")).isNull();
        assertThat(reqPublic.headers().get("x-user-roles")).isNull();
    }

    // 7. Spoofed headers without token return 401 and stub is never called
    @Test
    @Order(7)
    @DisplayName("Spoofed headers without token return 401 and stub is never called")
    void spoofedHeadersWithoutTokenReturns401() {
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header("X-User-Id", UUID.randomUUID().toString())
                .header("x-user-id", UUID.randomUUID().toString())
                .header("X-USER-ID", UUID.randomUUID().toString())
                .header("X-User-Roles", "ADMIN")
                .header("x-user-roles", "ADMIN")
                .header("X-USER-ROLES", "ADMIN")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("UNAUTHORIZED")
                .jsonPath("$.status").isEqualTo(401)
                .jsonPath("$.detail").isEqualTo("Authentication required");

        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 8. Expired token beyond clock skew returns 401 and records expired metric
    @Test
    @Order(8)
    @DisplayName("Expired token beyond clock skew returns 401 and records expired metric")
    void expiredTokenReturns401() {
        UUID userId = UUID.randomUUID();
        long nowSec = java.time.Instant.now().getEpochSecond();
        long expSec = nowSec - 45;
        long iatSec = nowSec - 1000;
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + userId + "\",\"roles\":[\"USER\"],\"iat\":" + iatSec + ",\"exp\":" + expSec + "}";
        String hB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(header.getBytes(StandardCharsets.UTF_8));
        String pB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        String token = sign(hB64 + "." + pB64, "dev-only-hs256-secret-key-minimum-32-bytes-long!");
        issuedTokensForLogCheck.add(token);

        double countBefore = getAuthFailureCount("expired");

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
                .jsonPath("$.detail").isEqualTo("Authentication required");

        assertThat(getAuthFailureCount("expired")).isGreaterThan(countBefore);
        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 9. Wrongly signed token returns 401 and records invalid metric
    @Test
    @Order(9)
    @DisplayName("Wrongly signed token returns 401 and records invalid metric")
    void wronglySignedTokenReturns401() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);
        String[] parts = token.split("\\.");
        String wrongToken = parts[0] + "." + parts[1] + ".invalidSignatureFake123456";
        issuedTokensForLogCheck.add(wrongToken);

        double countBefore = getAuthFailureCount("invalid");

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
                .jsonPath("$.detail").isEqualTo("Authentication required");

        assertThat(getAuthFailureCount("invalid")).isGreaterThan(countBefore);
        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 10. Token signed with different secret returns 401 and records invalid metric
    @Test
    @Order(10)
    @DisplayName("Token signed with different secret returns 401 and records invalid metric")
    void differentSecretTokenReturns401() {
        UUID userId = UUID.randomUUID();
        long nowSec = java.time.Instant.now().getEpochSecond();
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + userId + "\",\"roles\":[\"USER\"],\"iat\":" + nowSec + ",\"exp\":" + (nowSec + 3600) + "}";
        String hB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(header.getBytes(StandardCharsets.UTF_8));
        String pB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        String token = sign(hB64 + "." + pB64, "different-secret-key-at-least-32-bytes!");
        issuedTokensForLogCheck.add(token);

        double countBefore = getAuthFailureCount("invalid");

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
                .jsonPath("$.detail").isEqualTo("Authentication required");

        assertThat(getAuthFailureCount("invalid")).isGreaterThan(countBefore);
        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 11. Malformed tokens return 401 and record invalid metric
    @Test
    @Order(11)
    @DisplayName("Malformed tokens return 401 and record invalid metric")
    void malformedTokenReturns401() {
        UUID userId = UUID.randomUUID();
        String validToken = createToken(userId, List.of("USER"), 3600);

        List<String> malformedAuthHeaders = List.of(
                "Bearer not.a.valid.jwt.token",
                "Bearer justonestring",
                "Bearer ",
                "Bearer " + validToken + "."
        );

        for (String authHeader : malformedAuthHeaders) {
            double countBefore = getAuthFailureCount("invalid");

            webTestClient.post()
                    .uri("/api/v1/reservations")
                    .header(HttpHeaders.AUTHORIZATION, authHeader)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{}")
                    .exchange()
                    .expectStatus().isUnauthorized()
                    .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                    .expectBody()
                    .jsonPath("$.errorCode").isEqualTo("UNAUTHORIZED")
                    .jsonPath("$.status").isEqualTo(401)
                    .jsonPath("$.detail").isEqualTo("Authentication required");

            assertThat(getAuthFailureCount("invalid")).isGreaterThan(countBefore);
        }

        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 12. Token without sub claim returns 401 and records invalid metric
    @Test
    @Order(12)
    @DisplayName("Token without sub claim returns 401 and records invalid metric")
    void missingClaimTokenReturns401() {
        long nowSec = java.time.Instant.now().getEpochSecond();
        String noSub = "{\"roles\":[\"USER\"],\"iat\":" + nowSec + ",\"exp\":" + (nowSec + 3600) + "}";
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String hB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(header.getBytes(StandardCharsets.UTF_8));
        String pB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(noSub.getBytes(StandardCharsets.UTF_8));
        String token = sign(hB64 + "." + pB64, "dev-only-hs256-secret-key-minimum-32-bytes-long!");
        issuedTokensForLogCheck.add(token);

        double countBefore = getAuthFailureCount("invalid");

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
                .jsonPath("$.detail").isEqualTo("Authentication required");

        assertThat(getAuthFailureCount("invalid")).isGreaterThan(countBefore);
        assertThat(recordedInventoryRequests).isEmpty();
    }

    // 13. Authorization header is NOT forwarded downstream
    @Test
    @Order(13)
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

    // 14. Idempotency-Key and X-Correlation-Id reach downstream stub unchanged
    @Test
    @Order(14)
    @DisplayName("Idempotency-Key and X-Correlation-Id reach downstream stub unchanged")
    void idempotencyKeyAndCorrelationIdForwarded() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);
        String idempotencyKey = UUID.randomUUID().toString();
        String clientCorrelationId = UUID.randomUUID().toString();

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

        RecordedRequest req = recordedInventoryRequests.poll();
        assertThat(req).isNotNull();
        assertThat(req.headers().get("idempotency-key")).isEqualTo(idempotencyKey);
        assertThat(req.headers().get("x-correlation-id")).isEqualTo(clientCorrelationId);
    }

    // 15. Token endpoint issues token accepted by gateway; admin rejected when allow-admin is false
    @Test
    @Order(15)
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
        boolean prevAllowAdmin = devTokenProperties.isAllowAdmin();
        try {
            devTokenProperties.setAllowAdmin(false);
            webTestClient.post()
                    .uri("/api/v1/auth/token")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{\"userId\":\"" + userId + "\",\"roles\":[\"ADMIN\"],\"ttlSeconds\":1800}")
                    .exchange()
                    .expectStatus().isForbidden()
                    .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                    .expectBody()
                    .jsonPath("$.errorCode").isEqualTo("FORBIDDEN");
        } finally {
            devTokenProperties.setAllowAdmin(prevAllowAdmin);
        }
    }

    // 16. Rate limiting burst above capacity returns 429 for same user; other user unaffected
    @Test
    @Order(16)
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

    // 17. IP rate limiting & rotating X-Forwarded-For direct client vs trusted proxy
    @Test
    @Order(17)
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

    // 18. Dev token endpoint rate limiting (capacity 10, refill 1/s)
    @Test
    @Order(18)
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

    // 19. Toxiproxy 2s latency on Redis makes requests fail open in under 1 second
    @Test
    @Order(19)
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

    // 20. Correlation ID handling: 401, 404, token endpoint, 500 char, non-UUID, duplicates, uppercase
    @Test
    @Order(20)
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

    // 21. Closed connection stub makes gateway answer 502 and stub saw exactly ONE request (no retry!)
    @Test
    @Order(21)
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

    // 22. Slow stub returns 504 Gateway Timeout
    @Test
    @Order(22)
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

    // 23. Metrics: exact counts, bounded tags, rate limit metric, auth failure reasons, and timer
    @Test
    @Order(23)
    @DisplayName("Metrics: exact counts, bounded tags, rate limit metric, auth failure reasons, and timer")
    void metricsCountersAndTagsVerified() {
        double start2xx = meterRegistry.find("gateway_requests_total").tag("status_class", "2xx").counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
        double start4xx = meterRegistry.find("gateway_requests_total").tag("status_class", "4xx").counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
        double start5xx = meterRegistry.find("gateway_requests_total").tag("status_class", "5xx").counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();

        UUID testUser = UUID.randomUUID();
        String testToken = createToken(testUser, List.of("USER"), 3600);

        // Exactly 2 requests -> 2xx
        webTestClient.get()
                .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                .exchange()
                .expectStatus().isOk();

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testToken)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();

        // Exactly 3 requests -> 4xx (404 unmatched, 401 unauth, 403 forbidden)
        webTestClient.get()
                .uri("/api/v1/unmatched-route-metrics-check")
                .exchange()
                .expectStatus().isNotFound();

        webTestClient.post()
                .uri("/api/v1/reservations")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isUnauthorized();

        webTestClient.post()
                .uri("/api/v1/admin/events")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testToken)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isForbidden();

        // Exactly 1 request -> 5xx (502 abort-conn)
        webTestClient.post()
                .uri("/api/v1/orders/abort-conn/checkout")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testToken)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isEqualTo(502);

        double end2xx = meterRegistry.find("gateway_requests_total").tag("status_class", "2xx").counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
        double end4xx = meterRegistry.find("gateway_requests_total").tag("status_class", "4xx").counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
        double end5xx = meterRegistry.find("gateway_requests_total").tag("status_class", "5xx").counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();

        assertThat(end2xx - start2xx).isEqualTo(2.0);
        assertThat(end4xx - start4xx).isEqualTo(3.0);
        assertThat(end5xx - start5xx).isEqualTo(1.0);

        // Assert gateway_rate_limited_total >= 1 for inventory-reservations route
        assertThat(meterRegistry.get("gateway_rate_limited_total")
                .tag("route", "inventory-reservations").counter().count()).isGreaterThanOrEqualTo(1.0);

        // Assert gateway_auth_failures_total has non-zero counts for each reason value used
        for (String reason : List.of("missing", "invalid", "expired", "forbidden")) {
            assertThat(meterRegistry.get("gateway_auth_failures_total")
                    .tag("reason", reason).counter().count()).isGreaterThanOrEqualTo(1.0);
        }

        // Assert route tags across gateway_* meters are strictly within ALLOWED_ROUTES
        meterRegistry.getMeters().forEach(meter -> {
            String name = meter.getId().getName();
            if (name.startsWith("gateway_")) {
                String routeTag = meter.getId().getTag("route");
                if (routeTag != null) {
                    assertThat(GatewayMetrics.ALLOWED_ROUTES)
                            .withFailMessage("Meter %s has disallowed route tag: %s", name, routeTag)
                            .contains(routeTag);
                }
            }
        });

        // Verify all meters carry service=gateway-service
        meterRegistry.getMeters().forEach(meter -> {
            String serviceTag = meter.getId().getTag("service");
            assertThat(serviceTag).isEqualTo("gateway-service");
        });

        // Verify ticketing_redis_lua_duration_seconds timer exists
        assertThat(meterRegistry.find("ticketing_redis_lua_duration_seconds").timers()).isNotEmpty();
    }

    // 24. Real gateway log inspection: tokens and secret never appear in captured logs
    @Test
    @Order(24)
    @DisplayName("Real gateway log inspection: non-empty logs contain no secrets or tokens")
    void tokensAndSecretsNeverInLogs() {
        List<ILoggingEvent> gatewayLogs = listAppender.list.stream()
                .filter(e -> e.getLoggerName().startsWith("com.ticketing.gateway"))
                .toList();

        assertThat(gatewayLogs).isNotEmpty();
        for (ILoggingEvent event : gatewayLogs) {
            String msg = event.getFormattedMessage();
            for (String issuedToken : issuedTokensForLogCheck) {
                assertThat(msg)
                        .withFailMessage("Security violation: Token was logged in gateway log event: " + msg)
                        .doesNotContain(issuedToken);
            }
            assertThat(msg)
                    .withFailMessage("Security violation: Authorization header was logged in gateway log event: " + msg)
                    .doesNotContain("Bearer ")
                    .doesNotContain("bearer ");
            assertThat(msg)
                    .withFailMessage("Security violation: JWT secret was logged in gateway log event: " + msg)
                    .doesNotContain("dev-only-hs256-secret-key-minimum-32-bytes-long!");
        }
    }

    // 25. Underscore headers and method override headers never reach stub
    @Test
    @Order(25)
    @DisplayName("Underscore headers and method override headers never reach stub on protected and public routes")
    void underscoreAndMethodOverrideHeadersNeverReachStub() {
        UUID validUserId = UUID.randomUUID();
        UUID spoofedUserId = UUID.randomUUID();
        String token = createToken(validUserId, List.of("USER"), 3600);

        // Protected route
        recordedInventoryRequests.clear();
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header("X_User_Id", spoofedUserId.toString())
                .header("X_USER_ID", spoofedUserId.toString())
                .header("x_user_id", spoofedUserId.toString())
                .header("X_User_Roles", "ADMIN")
                .header("X_USER_ROLES", "ADMIN")
                .header("x_user_roles", "ADMIN")
                .header("X-HTTP-Method-Override", "DELETE")
                .header("X_HTTP_Method_Override", "DELETE")
                .header("X-HTTP-Method", "DELETE")
                .header("X_HTTP_Method", "DELETE")
                .header("X-Method-Override", "DELETE")
                .header("X_Method_Override", "DELETE")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();

        RecordedRequest reqProtected = recordedInventoryRequests.poll();
        assertThat(reqProtected).isNotNull();
        assertThat(reqProtected.headers().get("x-user-id")).isEqualTo(validUserId.toString());
        assertThat(reqProtected.headers().get("x-user-roles")).isEqualTo("USER");
        assertThat(reqProtected.headers()).doesNotContainKey("x_user_id");
        assertThat(reqProtected.headers()).doesNotContainKey("x_user_roles");
        assertThat(reqProtected.headers()).doesNotContainKey("x-http-method-override");
        assertThat(reqProtected.headers()).doesNotContainKey("x_http_method_override");
        assertThat(reqProtected.headers()).doesNotContainKey("x-http-method");
        assertThat(reqProtected.headers()).doesNotContainKey("x_http_method");
        assertThat(reqProtected.headers()).doesNotContainKey("x-method-override");
        assertThat(reqProtected.headers()).doesNotContainKey("x_method_override");

        // Public route
        recordedInventoryRequests.clear();
        webTestClient.get()
                .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header("X_User_Id", spoofedUserId.toString())
                .header("X_USER_ID", spoofedUserId.toString())
                .header("x_user_id", spoofedUserId.toString())
                .header("X_User_Roles", "ADMIN")
                .header("X_USER_ROLES", "ADMIN")
                .header("x_user_roles", "ADMIN")
                .header("X-HTTP-Method-Override", "DELETE")
                .header("X_HTTP_Method_Override", "DELETE")
                .header("X-HTTP-Method", "DELETE")
                .header("X_HTTP_Method", "DELETE")
                .header("X-Method-Override", "DELETE")
                .header("X_Method_Override", "DELETE")
                .exchange()
                .expectStatus().isOk();

        RecordedRequest reqPublic = recordedInventoryRequests.poll();
        assertThat(reqPublic).isNotNull();
        assertThat(reqPublic.headers().get("x-user-id")).isNull();
        assertThat(reqPublic.headers().get("x-user-roles")).isNull();
        assertThat(reqPublic.headers()).doesNotContainKey("x_user_id");
        assertThat(reqPublic.headers()).doesNotContainKey("x_user_roles");
        assertThat(reqPublic.headers()).doesNotContainKey("x-http-method-override");
        assertThat(reqPublic.headers()).doesNotContainKey("x_http_method_override");
        assertThat(reqPublic.headers()).doesNotContainKey("x-http-method");
        assertThat(reqPublic.headers()).doesNotContainKey("x_http_method");
        assertThat(reqPublic.headers()).doesNotContainKey("x-method-override");
        assertThat(reqPublic.headers()).doesNotContainKey("x_method_override");
    }

    // 26. Bearer prefix case-insensitivity
    @Test
    @Order(26)
    @DisplayName("Bearer prefix case-insensitive: BEARER and bearer both succeed")
    void bearerPrefixCaseInsensitive() {
        UUID userId = UUID.randomUUID();
        String token = createToken(userId, List.of("USER"), 3600);

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "BEARER " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();

        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isCreated();
    }

    // 27. Request body limits enforced
    @Test
    @Order(27)
    @DisplayName("Request body limits enforced: GET no body (400), POST chunked (411), oversized body (413)")
    void requestBodyLimitsEnforced() {
        UUID userId = UUID.randomUUID();
        String userToken = createToken(userId, List.of("USER"), 3600);
        String adminToken = createToken(userId, List.of("ADMIN"), 3600);

        // 1. GET with body returns 400 VALIDATION_ERROR
        webTestClient.method(HttpMethod.GET)
                .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                .header(HttpHeaders.CONTENT_LENGTH, "10")
                .bodyValue("0123456789")
                .exchange()
                .expectStatus().isBadRequest()
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("VALIDATION_ERROR");

        // 2. POST with chunked body returns 411 LENGTH_REQUIRED
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken)
                .header(HttpHeaders.TRANSFER_ENCODING, "chunked")
                .body(org.springframework.web.reactive.function.BodyInserters.fromPublisher(
                        reactor.core.publisher.Flux.just("{\"test\":1}"), String.class))
                .exchange()
                .expectStatus().isEqualTo(411)
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("LENGTH_REQUIRED");

        // 3. POST with body > 64KB returns 413 PAYLOAD_TOO_LARGE and zero stub requests
        recordedInventoryRequests.clear();
        String bigBody65k = "{\"data\":\"" + "a".repeat(65 * 1024) + "\"}";
        webTestClient.post()
                .uri("/api/v1/reservations")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(bigBody65k)
                .exchange()
                .expectStatus().isEqualTo(413)
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("PAYLOAD_TOO_LARGE");

        assertThat(recordedInventoryRequests).isEmpty();

        // 4. Bulk seats route accepts larger body (100KB <= 2MB)
        String bulk100k = "{\"seats\":[],\"data\":\"" + "x".repeat(100 * 1024) + "\"}";
        webTestClient.post()
                .uri("/api/v1/admin/events/" + UUID.randomUUID() + "/seats/bulk")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(bulk100k)
                .exchange()
                .expectStatus().isCreated();

        // 5. Bulk seats route rejects body > 2MB
        String bulkOver2M = "{\"seats\":[],\"data\":\"" + "x".repeat(2 * 1024 * 1024 + 1024) + "\"}";
        webTestClient.post()
                .uri("/api/v1/admin/events/" + UUID.randomUUID() + "/seats/bulk")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(bulkOver2M)
                .exchange()
                .expectStatus().isEqualTo(413)
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("PAYLOAD_TOO_LARGE");
    }

    // 28. Path hygiene rejection counted in metrics with unmatched route
    @Test
    @Order(28)
    @DisplayName("Path hygiene rejection is counted in gateway_requests_total with route unmatched")
    void pathHygieneRejectionCountedInMetricsWithUnmatchedRoute() {
        double beforeCount = meterRegistry.find("gateway_requests_total")
                .tag("route", "unmatched")
                .tag("status_class", "4xx")
                .counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();

        webTestClient.get()
                .uri("/api/v1/%61dmin/events")
                .exchange()
                .expectStatus().isBadRequest();

        double afterCount = meterRegistry.find("gateway_requests_total")
                .tag("route", "unmatched")
                .tag("status_class", "4xx")
                .counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();

        assertThat(afterCount - beforeCount).isGreaterThanOrEqualTo(1.0);
    }

    // 29. Aborted client request counted once as cancelled
    @Test
    @Order(29)
    @DisplayName("Aborted client request is counted once with status_class cancelled")
    void abortedClientRequestCountedOnceAsCancelled() throws Exception {
        double beforeCancelled = meterRegistry.find("gateway_requests_total")
                .tag("status_class", "cancelled")
                .counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();

        try (java.net.Socket socket = new java.net.Socket("127.0.0.1", localPort)) {
            java.io.OutputStream out = socket.getOutputStream();
            out.write("GET /api/v1/events/slow/seats HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            Thread.sleep(150);
            socket.setSoLinger(true, 0);
        }

        Thread.sleep(300);

        double afterCancelled = meterRegistry.find("gateway_requests_total")
                .tag("status_class", "cancelled")
                .counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();

        assertThat(afterCancelled - beforeCancelled).isEqualTo(1.0);
    }

    // 30. Unmatched auth and actuator routes tagged unmatched
    @Test
    @Order(30)
    @DisplayName("GET /api/v1/auth/foo and /actuator/env are tagged unmatched")
    void unmatchedAuthAndActuatorRoutesTaggedUnmatched() {
        double beforeUnmatched = meterRegistry.find("gateway_requests_total")
                .tag("route", "unmatched")
                .counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();

        webTestClient.get()
                .uri("/api/v1/auth/foo")
                .exchange()
                .expectStatus().isNotFound();

        webTestClient.get()
                .uri("/actuator/env")
                .exchange()
                .expectStatus().isNotFound();

        double afterUnmatched = meterRegistry.find("gateway_requests_total")
                .tag("route", "unmatched")
                .counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();

        assertThat(afterUnmatched - beforeUnmatched).isGreaterThanOrEqualTo(2.0);
    }

    // 31. Error bodies never contain marker role or path segment
    @Test
    @Order(31)
    @DisplayName("Error bodies never contain request-derived marker role or marker path segment")
    void errorBodiesNeverContainMarkerRoleOrPathSegment() {
        // 1. Role validation error
        String markerRole = "MARKER_ROLE_FORBIDDEN_XYZ_999";
        webTestClient.post()
                .uri("/api/v1/auth/token")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new DevTokenRequest(UUID.randomUUID().toString(), List.of(markerRole), 3600L))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class)
                .value(body -> {
                    assertThat(body).contains("role is not allowlisted");
                    assertThat(body).doesNotContain(markerRole);
                });

        // 2. Path hygiene rejection
        String markerPathSegment = "marker_hygiene_test_path_segment_123";
        webTestClient.get()
                .uri("/api/v1/%61dmin/" + markerPathSegment)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class)
                .value(body -> {
                    assertThat(body).contains("\"instance\":\"/\"");
                    assertThat(body).doesNotContain(markerPathSegment);
                });

        // 3. Unmatched route 404
        String markerUnmatchedSegment = "marker_unmatched_route_path_segment_789";
        webTestClient.get()
                .uri("/api/v1/" + markerUnmatchedSegment)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody(String.class)
                .value(body -> {
                    assertThat(body).contains("\"instance\":\"/\"");
                    assertThat(body).doesNotContain(markerUnmatchedSegment);
                });
    }

    // 32. Redis stopped makes requests behave per policy (fail open) and does not crash gateway
    @Test
    @Order(32)
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

        @Test
        @DisplayName("X-Forwarded-For: 203.0.113.5, unknown from trusted proxy resolves to proxy address")
        void xffWithUnknownEntryStopsAndUsesProxyAddress() {
            client.get()
                    .uri("/api/v1/events/" + UUID.randomUUID() + "/seats")
                    .header("X-Forwarded-For", "203.0.113.5, unknown")
                    .exchange()
                    .expectStatus().isOk();

            com.ticketing.gateway.ratelimit.ClientAddressResolver resolver =
                    new com.ticketing.gateway.ratelimit.ClientAddressResolver("127.0.0.1/32");
            org.springframework.mock.http.server.reactive.MockServerHttpRequest request =
                    org.springframework.mock.http.server.reactive.MockServerHttpRequest.get("/api/v1/events/123/seats")
                            .remoteAddress(new java.net.InetSocketAddress("127.0.0.1", 12345))
                            .header("X-Forwarded-For", "203.0.113.5, unknown")
                            .build();
            org.springframework.mock.web.server.MockServerWebExchange exchange =
                    org.springframework.mock.web.server.MockServerWebExchange.from(request);

            String resolved = resolver.resolveClientIp(exchange);
            assertThat(resolved).isEqualTo("127.0.0.1");
        }
    }
}
