package com.ticketing.order.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.events.PaymentRequestedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.order.api.dto.CheckoutRequest;
import com.ticketing.order.domain.OrderItemRecord;
import com.ticketing.order.domain.OrderRecord;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.idempotency.IdempotencyKeyRecord;
import com.ticketing.order.idempotency.IdempotencyKeyRepository;
import com.ticketing.order.service.CheckoutMetrics;
import com.ticketing.order.service.CheckoutService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class CheckoutControllerIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
    }

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4-alpine"))
                    .withDatabaseName("order_test_db")
                    .withUsername("ticketing")
                    .withPassword("ticketing_secret");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("ticketing.outbox.enabled", () -> "false");
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private IdempotencyKeyRepository idempotencyKeyRepository;

    @Autowired
    private CheckoutService checkoutService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setupAndCleanTables() {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();

        jdbcTemplate.execute("TRUNCATE TABLE order_items, orders, outbox_events, idempotency_keys CASCADE");
        checkoutService.setAfterIdempotencyClaimHook(null);
        checkoutService.setInTransactionHook(null);
    }

    private OrderRecord insertOrder(UUID orderId, UUID userId, String status, Instant expiresAt) {
        OrderRecord order = new OrderRecord(
                orderId,
                orderId, // reservationId == orderId
                userId,
                UUID.randomUUID(), // eventId
                status,
                30000L,
                "USD",
                null,
                null,
                null,
                0L,
                Instant.now().minus(1, ChronoUnit.MINUTES),
                expiresAt,
                null,
                null,
                null,
                null,
                null,
                Instant.now(),
                Instant.now()
        );
        OrderItemRecord item1 = new OrderItemRecord(
                UUID.randomUUID(),
                orderId,
                UUID.randomUUID(),
                "FLOOR-A",
                "1",
                "101",
                15000L
        );
        OrderItemRecord item2 = new OrderItemRecord(
                UUID.randomUUID(),
                orderId,
                UUID.randomUUID(),
                "FLOOR-A",
                "1",
                "102",
                15000L
        );
        transactionTemplate.executeWithoutResult(s -> orderRepository.insert(order, List.of(item1, item2)));
        return order;
    }

    @Test
    @DisplayName("1. Happy path: 202 Accepted, order updated to PAYMENT_PENDING, outbox row written, idempotency COMPLETED")
    void checkoutHappyPath() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();
        OrderRecord order = insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));

        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        MvcResult result = mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header(CorrelationConstants.HTTP_HEADER, correlationId)
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(header().string(CorrelationConstants.HTTP_HEADER, correlationId))
                .andExpect(jsonPath("$.orderId", is(orderId.toString())))
                .andExpect(jsonPath("$.reservationId", is(orderId.toString())))
                .andExpect(jsonPath("$.userId", is(userId.toString())))
                .andExpect(jsonPath("$.eventId", is(order.eventId().toString())))
                .andExpect(jsonPath("$.status", is("PAYMENT_PENDING")))
                .andExpect(jsonPath("$.totalAmountCents", is(30000)))
                .andExpect(jsonPath("$.currency", is("USD")))
                .andExpect(jsonPath("$.checkoutAt").isNotEmpty())
                .andExpect(jsonPath("$.expiresAt").isNotEmpty())
                .andReturn();

        // Verify database order updated
        OrderRecord updated = orderRepository.findById(orderId).orElseThrow();
        assertThat(updated.status()).isEqualTo("PAYMENT_PENDING");
        assertThat(updated.paymentMethodToken()).isEqualTo("pm_tok_visa_success");
        assertThat(updated.checkoutAt()).isNotNull();
        assertThat(updated.version()).isEqualTo(1L);

        // Verify outbox row written
        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                Integer.class,
                orderId,
                EventTypes.ORDER_PAYMENT_REQUESTED
        );
        assertThat(outboxCount).isEqualTo(1);

        // Verify idempotency completed
        var idempRecord = idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey).orElseThrow();
        assertThat(idempRecord.status()).isEqualTo("COMPLETED");
        assertThat(idempRecord.responseStatus()).isEqualTo(202);

        // Verify metrics
        Counter counter = meterRegistry.find(CheckoutMetrics.COUNTER_NAME)
                .tag(CheckoutMetrics.TAG_OUTCOME, CheckoutMetrics.OUTCOME_ACCEPTED)
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isGreaterThanOrEqualTo(1.0);

        Timer timer = meterRegistry.find(CheckoutMetrics.TIMER_NAME).timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
    }

    @Test
    @DisplayName("2. Missing user header returns 401 Unauthorized")
    void checkoutMissingUserHeader() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.UNAUTHORIZED.name())))
                .andExpect(jsonPath("$.status", is(401)));
    }

    @Test
    @DisplayName("3. Mismatched user returns 409 ORDER_NOT_READY with Retry-After: 1")
    void checkoutMismatchedUser() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID ownerUserId = UUID.randomUUID();
        UUID callerUserId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        insertOrder(orderId, ownerUserId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));

        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, callerUserId.toString())
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.ORDER_NOT_READY.name())))
                .andExpect(jsonPath("$.status", is(409)));
    }

    @Test
    @DisplayName("4. Non-existent order returns 409 ORDER_NOT_READY with Retry-After: 1")
    void checkoutNonExistentOrder() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.ORDER_NOT_READY.name())))
                .andExpect(jsonPath("$.status", is(409)));
    }

    @Test
    @DisplayName("5. Expired order (< 15s remaining) returns 409 RESERVATION_EXPIRED")
    void checkoutExpiredOrder() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        // 10 seconds remaining is < 15 seconds grace buffer
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(10, ChronoUnit.SECONDS));

        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.RESERVATION_EXPIRED.name())))
                .andExpect(jsonPath("$.status", is(409)));
    }

    @Test
    @DisplayName("6. Invalid order status (CONFIRMED, CANCELLED) returns 409 INVALID_ORDER_STATE even when expires_at is in past")
    void checkoutInvalidOrderStatus() throws Exception {
        UUID orderId1 = UUID.randomUUID();
        UUID orderId2 = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        insertOrder(orderId1, userId, "CONFIRMED", Instant.now().minus(60, ChronoUnit.SECONDS));
        insertOrder(orderId2, userId, "CANCELLED_PAYMENT_FAILED", Instant.now().minus(60, ChronoUnit.SECONDS));

        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId1)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.INVALID_ORDER_STATE.name())));

        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId2)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.INVALID_ORDER_STATE.name())));
    }

    @Test
    @DisplayName("7. Already PAYMENT_PENDING: idempotency replay returns 202; retry with new key returns 409 INVALID_ORDER_STATE without second outbox row")
    void checkoutAlreadyPaymentPendingReplayAndNewKeyRetry() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        // 1. First checkout succeeds -> 202
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", key1.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted());

        assertThat(countOutboxEvents(orderId)).isEqualTo(1);

        // 2. Replay with SAME key1 -> 202 Accepted with X-Idempotent-Replay: true
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", key1.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(header().string("X-Idempotent-Replay", "true"));

        assertThat(countOutboxEvents(orderId)).isEqualTo(1);

        // 3. Retry with NEW key2 -> 409 INVALID_ORDER_STATE, no second outbox row
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", key2.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.INVALID_ORDER_STATE.name())));

        assertThat(countOutboxEvents(orderId)).isEqualTo(1);

        // Verify key2 was cleaned up / deleted from idempotency_keys
        assertThat(idempotencyKeyRepository.findByUserAndKey(userId, key2)).isEmpty();
    }

    @Test
    @DisplayName("8. Idempotency replay: same key and body returns 202, preserves headers, no duplicate outbox row")
    void checkoutIdempotencyReplay() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        String correlationId1 = UUID.randomUUID().toString();
        String correlationId2 = UUID.randomUUID().toString();
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        // First call
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header(CorrelationConstants.HTTP_HEADER, correlationId1)
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(header().string(CorrelationConstants.HTTP_HEADER, correlationId1));

        // Replay call
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header(CorrelationConstants.HTTP_HEADER, correlationId2)
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(header().string("X-Idempotent-Replay", "true"))
                .andExpect(header().string(CorrelationConstants.HTTP_HEADER, correlationId2));

        assertThat(countOutboxEvents(orderId)).isEqualTo(1);
    }

    @Test
    @DisplayName("9. Idempotency payload mismatch returns 422 Unprocessable Entity")
    void checkoutIdempotencyPayloadMismatch() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));

        CheckoutRequest request1 = new CheckoutRequest("pm_tok_visa_success");
        CheckoutRequest request2 = new CheckoutRequest("pm_tok_different_card");

        // First call
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request1)))
                .andExpect(status().isAccepted());

        // Second call with different body
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request2)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.IDEMPOTENCY_KEY_PAYLOAD_MISMATCH.name())));
    }

    @Test
    @DisplayName("10. Concurrent request with same key returns 409 IDEMPOTENCY_REQUEST_IN_PROGRESS with Retry-After: 1")
    void checkoutConcurrentRequestWithSameKey() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        CountDownLatch inProgressLatch = new CountDownLatch(1);
        CountDownLatch resumeLatch = new CountDownLatch(1);

        checkoutService.setAfterIdempotencyClaimHook(() -> {
            inProgressLatch.countDown();
            try {
                resumeLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<MvcResult> thread1 = executor.submit(() -> mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                            .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                            .header("Idempotency-Key", idempotencyKey.toString())
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(request)))
                    .andReturn());

            // Wait until thread 1 has claimed the key in IN_PROGRESS
            assertThat(inProgressLatch.await(5, TimeUnit.SECONDS)).isTrue();

            // Thread 2 performs request with same key while Thread 1 is in progress
            mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                            .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                            .header("Idempotency-Key", idempotencyKey.toString())
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(request)))
                    .andExpect(status().isConflict())
                    .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                    .andExpect(jsonPath("$.errorCode", is(ErrorCode.IDEMPOTENCY_REQUEST_IN_PROGRESS.name())));

            // Let thread 1 resume
            resumeLatch.countDown();
            MvcResult res1 = thread1.get(5, TimeUnit.SECONDS);
            assertThat(res1.getResponse().getStatus()).isEqualTo(202);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("11. Business rejection releases key so retry with same key can re-execute")
    void checkoutBusinessRejectionReleasesKey() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        // 1. Call checkout on non-existent order -> 409 ORDER_NOT_READY
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.ORDER_NOT_READY.name())));

        // Verify key was released (deleted)
        assertThat(idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey)).isEmpty();

        // 2. Order is now materialized / created
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));

        // 3. Retry with SAME key now succeeds!
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted());

        // Verify key is now COMPLETED
        var idempRecord = idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey).orElseThrow();
        assertThat(idempRecord.status()).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("12. Outbox event schema and payload validation")
    void checkoutOutboxEventValidation() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();
        OrderRecord order = insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header(CorrelationConstants.HTTP_HEADER, correlationId)
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted());

        // Query outbox row
        String sql = """
                SELECT event_id, aggregate_type, aggregate_id, topic, event_type,
                       correlation_id, schema_version, payload::text AS payload, created_at, published_at
                FROM outbox_events
                WHERE aggregate_id = ?
                """;
        var outboxRows = jdbcTemplate.queryForList(sql, orderId);
        assertThat(outboxRows).hasSize(1);
        var row = outboxRows.get(0);

        assertThat(row.get("aggregate_type")).isEqualTo("ORDER");
        assertThat(row.get("aggregate_id")).isEqualTo(orderId);
        assertThat(row.get("topic")).isEqualTo(Topics.ORDER_EVENTS);
        assertThat(row.get("event_type")).isEqualTo(EventTypes.ORDER_PAYMENT_REQUESTED);
        assertThat(row.get("schema_version")).isEqualTo(1);
        assertThat(row.get("correlation_id")).isEqualTo(UUID.fromString(correlationId));
        assertThat(row.get("published_at")).isNull();

        String payloadJson = (String) row.get("payload");
        PaymentRequestedPayload payload = objectMapper.readValue(payloadJson, PaymentRequestedPayload.class);
        assertThat(payload.orderId()).isEqualTo(orderId);
        assertThat(payload.reservationId()).isEqualTo(orderId);
        assertThat(payload.userId()).isEqualTo(userId);
        assertThat(payload.eventId()).isEqualTo(order.eventId());
        assertThat(payload.amountCents()).isEqualTo(30000L);
        assertThat(payload.currency()).isEqualTo("USD");
        assertThat(payload.paymentMethodToken()).isEqualTo("pm_tok_visa_success");
        assertThat(payload.requestedAt()).isNotNull();
        assertThat(payload.reservationExpiresAt()).isNotNull();
    }

    @Test
    @DisplayName("13. Payment method token is NEVER present in logs during checkout")
    void checkoutPaymentMethodTokenNotPresentInLogs() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));

        String secretToken = "pm_secret_sensitive_tok_xyz123";
        CheckoutRequest request = new CheckoutRequest(secretToken);

        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        rootLogger.addAppender(listAppender);

        try {
            mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                            .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                            .header("Idempotency-Key", idempotencyKey.toString())
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(request)))
                    .andExpect(status().isAccepted());

            assertThat(listAppender.list).isNotEmpty();
            assertThat(listAppender.list)
                    .anyMatch(event -> event.getFormattedMessage().contains("Order checkout accepted"));

            // Scan all captured log events
            for (ILoggingEvent event : listAppender.list) {
                String formatted = event.getFormattedMessage();
                assertThat(formatted).doesNotContain(secretToken);
                if (event.getArgumentArray() != null) {
                    for (Object arg : event.getArgumentArray()) {
                        if (arg != null) {
                            assertThat(arg.toString()).doesNotContain(secretToken);
                        }
                    }
                }
            }
        } finally {
            rootLogger.detachAppender(listAppender);
        }
    }

    @Test
    @DisplayName("GET /api/v1/orders/{orderId} returns 200 with order and seats")
    void getOrderHappyPath() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));

        mockMvc.perform(get("/api/v1/orders/{orderId}", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId", is(orderId.toString())))
                .andExpect(jsonPath("$.userId", is(userId.toString())))
                .andExpect(jsonPath("$.status", is("RESERVED")))
                .andExpect(jsonPath("$.seats").isArray())
                .andExpect(jsonPath("$.seats.length()", is(2)))
                .andExpect(jsonPath("$.totalAmountCents", is(30000)));
    }

    @Test
    @DisplayName("GET /api/v1/orders/{orderId} with mismatched user returns 403 Forbidden")
    void getOrderMismatchedUser() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        UUID callerId = UUID.randomUUID();
        insertOrder(orderId, ownerId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));

        mockMvc.perform(get("/api/v1/orders/{orderId}", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, callerId.toString()))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.FORBIDDEN.name())));
    }

    @Test
    @DisplayName("GET /api/v1/orders/{orderId} with non-existent order returns 404 Not Found")
    void getOrderNotFound() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        mockMvc.perform(get("/api/v1/orders/{orderId}", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.RESOURCE_NOT_FOUND.name())));
    }

    @Test
    @DisplayName("Thread 1 holds row lock on User A order; Thread 2 checks out as User B, finishes within 500ms without blocking, returns 409 ORDER_NOT_READY")
    void checkoutUserBDoesNotBlockOnUserALock() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userA = UUID.randomUUID();
        UUID userB = UUID.randomUUID();
        UUID keyA = UUID.randomUUID();
        UUID keyB = UUID.randomUUID();
        insertOrder(orderId, userA, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        CountDownLatch t1LockedLatch = new CountDownLatch(1);
        CountDownLatch t2DoneLatch = new CountDownLatch(1);

        checkoutService.setInTransactionHook(() -> {
            t1LockedLatch.countDown();
            try {
                boolean done = t2DoneLatch.await(5, TimeUnit.SECONDS);
                if (!done) {
                    throw new RuntimeException("Timed out waiting for Thread 2");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<MvcResult> t1Future = executor.submit(() ->
                    mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                                    .header(CorrelationConstants.USER_ID_HEADER, userA.toString())
                                    .header("Idempotency-Key", keyA.toString())
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .content(objectMapper.writeValueAsString(request)))
                            .andReturn()
            );

            // Wait until Thread 1 has acquired the row lock inside transaction
            assertThat(t1LockedLatch.await(5, TimeUnit.SECONDS)).isTrue();

            // Thread 2 checks out the same order as User B
            long start = System.currentTimeMillis();
            mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                            .header(CorrelationConstants.USER_ID_HEADER, userB.toString())
                            .header("Idempotency-Key", keyB.toString())
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(request)))
                    .andExpect(status().isConflict())
                    .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                    .andExpect(jsonPath("$.errorCode", is(ErrorCode.ORDER_NOT_READY.name())))
                    .andExpect(jsonPath("$.status", is(409)));
            long elapsed = System.currentTimeMillis() - start;
            assertThat(elapsed).isLessThan(500);

            // Thread 2 done, release Thread 1
            t2DoneLatch.countDown();

            MvcResult t1Result = t1Future.get(5, TimeUnit.SECONDS);
            assertThat(t1Result.getResponse().getStatus()).isEqualTo(202);

            // Verify order state is PAYMENT_PENDING and 1 outbox event written
            OrderRecord updated = orderRepository.findById(orderId).orElseThrow();
            assertThat(updated.status()).isEqualTo("PAYMENT_PENDING");
            assertThat(countOutboxEvents(orderId)).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("Two concurrent checkouts for same RESERVED order with different keys: exactly one 202 and one 409 INVALID_ORDER_STATE, one outbox row, losing key IN_PROGRESS removed")
    void checkoutConcurrentDifferentKeysOnlyOneSucceeds() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        try {
            Future<MvcResult> f1 = executor.submit(() -> {
                startLatch.await(5, TimeUnit.SECONDS);
                return mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header("Idempotency-Key", key1.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                        .andReturn();
            });

            Future<MvcResult> f2 = executor.submit(() -> {
                startLatch.await(5, TimeUnit.SECONDS);
                return mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header("Idempotency-Key", key2.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                        .andReturn();
            });

            startLatch.countDown();

            MvcResult r1 = f1.get(10, TimeUnit.SECONDS);
            MvcResult r2 = f2.get(10, TimeUnit.SECONDS);

            int status1 = r1.getResponse().getStatus();
            int status2 = r2.getResponse().getStatus();

            // Exactly one 202 and one 409
            assertThat(List.of(status1, status2)).containsExactlyInAnyOrder(202, 409);

            MvcResult conflictResult = (status1 == 409) ? r1 : r2;
            UUID losingKey = (status1 == 409) ? key1 : key2;
            UUID winningKey = (status1 == 202) ? key1 : key2;

            JsonNode conflictJson = objectMapper.readTree(conflictResult.getResponse().getContentAsString());
            assertThat(conflictJson.get("errorCode").asText()).isEqualTo(ErrorCode.INVALID_ORDER_STATE.name());

            // Exactly one outbox row
            assertThat(countOutboxEvents(orderId)).isEqualTo(1);

            // Winning key is COMPLETED
            var winRecord = idempotencyKeyRepository.findByUserAndKey(userId, winningKey).orElseThrow();
            assertThat(winRecord.status()).isEqualTo("COMPLETED");

            // Losing key's IN_PROGRESS row was removed
            assertThat(idempotencyKeyRepository.findByUserAndKey(userId, losingKey)).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("Pre-seeded IN_PROGRESS idempotency row older than 35 seconds is reclaimed with fresh fencing timestamp and succeeds with 202")
    void checkoutStaleInProgressReclaimedAfter35Seconds() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));

        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");
        String endpoint = "/api/v1/orders/" + orderId + "/checkout";
        String canonicalJson = objectMapper.writeValueAsString(request);
        String hashInput = "POST:" + endpoint + ":" + canonicalJson;
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        String requestHash = HexFormat.of().formatHex(digest.digest(hashInput.getBytes(StandardCharsets.UTF_8)));

        // Pre-seed an IN_PROGRESS idempotency row older than 35 seconds
        jdbcTemplate.update("""
                INSERT INTO idempotency_keys (user_id, idempotency_key, endpoint, request_hash, status, created_at, expires_at)
                VALUES (?, ?, ?, ?, 'IN_PROGRESS', NOW() - INTERVAL '35 seconds', NOW() + INTERVAL '24 hours')
                """,
                userId, idempotencyKey, endpoint, requestHash);

        var preSeeded = idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey).orElseThrow();
        Instant seededCreatedAt = preSeeded.createdAt();

        // Perform checkout with the same key
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId", is(orderId.toString())))
                .andExpect(jsonPath("$.status", is("PAYMENT_PENDING")));

        // Verify key reclaimed, fresh fencing timestamp, completed
        var completed = idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey).orElseThrow();
        assertThat(completed.status()).isEqualTo("COMPLETED");
        assertThat(completed.responseStatus()).isEqualTo(202);
        assertThat(completed.createdAt()).isAfter(seededCreatedAt);
        assertThat(completed.completedAt()).isNotNull();

        assertThat(countOutboxEvents(orderId)).isEqualTo(1);
    }

    @Test
    @DisplayName("Documented counter exists with documented tag values and order_checkout_requests_total does not exist")
    void checkoutMetricsConformToConventionsDoc() throws Exception {
        // Assert old counter does not exist
        assertThat(meterRegistry.find("order_checkout_requests_total").counter()).isNull();

        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID key1 = UUID.randomUUID();
        insertOrder(orderId, userId, "RESERVED", Instant.now().plus(600, ChronoUnit.SECONDS));
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");

        // 1. ACCEPTED
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", key1.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted());

        // 2. REPLAY
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", key1.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted());

        // 3. INVALID_ORDER_STATE (retry with new key while PAYMENT_PENDING)
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict());

        // 4. ORDER_NOT_READY (unknown order)
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", UUID.randomUUID())
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict());

        // 5. RESERVATION_EXPIRED
        UUID expiredOrderId = UUID.randomUUID();
        insertOrder(expiredOrderId, userId, "RESERVED", Instant.now().minus(60, ChronoUnit.SECONDS));
        mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", expiredOrderId)
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict());

        // Assert all documented outcome tags exist and have count >= 1
        for (String outcome : CheckoutMetrics.ALLOWED_OUTCOMES) {
            Counter counter = meterRegistry.find(CheckoutMetrics.COUNTER_NAME)
                    .tag(CheckoutMetrics.TAG_OUTCOME, outcome)
                    .counter();
            assertThat(counter)
                    .as("Counter for outcome %s should exist", outcome)
                    .isNotNull();
            assertThat(counter.count())
                    .as("Count for outcome %s should be >= 1", outcome)
                    .isGreaterThanOrEqualTo(1.0);
        }

        // Verify timer exists
        Timer timer = meterRegistry.find(CheckoutMetrics.TIMER_NAME).timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isGreaterThanOrEqualTo(5L);

        // Ensure legacy counter is still absent
        assertThat(meterRegistry.find("order_checkout_requests_total").counter()).isNull();
    }

    private int countOutboxEvents(UUID orderId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ?",
                Integer.class,
                orderId
        );
        return count != null ? count : 0;
    }
}

