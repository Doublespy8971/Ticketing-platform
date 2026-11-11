package com.ticketing.order.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.events.OrderCancelledPayload;
import com.ticketing.common.events.OrderConfirmedPayload;
import com.ticketing.common.events.PaymentFailedPayload;
import com.ticketing.common.events.PaymentRefundedPayload;
import com.ticketing.common.events.PaymentRequestedPayload;
import com.ticketing.common.events.PaymentSucceededPayload;
import com.ticketing.common.events.RefundRequestedPayload;
import com.ticketing.common.events.ReservationExpiredPayload;
import com.ticketing.common.events.SeatConfirmationRejectedPayload;
import com.ticketing.common.events.SeatConfirmedPayload;
import com.ticketing.common.events.SeatLine;
import com.ticketing.common.events.SeatReleasedPayload;
import com.ticketing.common.events.SeatReservedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import com.ticketing.order.api.dto.CheckoutRequest;
import com.ticketing.order.domain.OrderItemRecord;
import com.ticketing.order.domain.OrderRecord;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.idempotency.IdempotencyKeyRepository;
import com.ticketing.order.service.CheckoutService;
import com.ticketing.order.service.OrderSagaService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class OrderSagaKafkaIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
    }

    private static final String INVENTORY_CG = TicketingRetryTopicNamesProviderFactory.ORDER_SERVICE_INVENTORY_CG;
    private static final String PAYMENT_CG = TicketingRetryTopicNamesProviderFactory.ORDER_SERVICE_PAYMENT_CG;

    private static final String INVENTORY_RETRY_0 = Topics.INVENTORY_EVENTS + "-" + INVENTORY_CG + "-retry-0";
    private static final String INVENTORY_RETRY_1 = Topics.INVENTORY_EVENTS + "-" + INVENTORY_CG + "-retry-1";
    private static final String INVENTORY_DLT = Topics.INVENTORY_EVENTS + "-" + INVENTORY_CG + "-dlt";

    private static final String PAYMENT_RETRY_0 = Topics.PAYMENT_EVENTS + "-" + PAYMENT_CG + "-retry-0";
    private static final String PAYMENT_RETRY_1 = Topics.PAYMENT_EVENTS + "-" + PAYMENT_CG + "-retry-1";
    private static final String PAYMENT_DLT = Topics.PAYMENT_EVENTS + "-" + PAYMENT_CG + "-dlt";

    private static final AtomicBoolean TOPICS_CREATED = new AtomicBoolean(false);
    private static final AtomicBoolean SCHEMA_MIGRATED = new AtomicBoolean(false);

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4-alpine"))
                    .withDatabaseName("order_saga_test_db")
                    .withUsername("ticketing")
                    .withPassword("ticketing_secret");

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        ensureTopicsCreated();
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("ticketing.outbox.enabled", () -> "true");
        registry.add("ticketing.outbox.poll-interval-ms", () -> "50");
        registry.add("ticketing.consumer.retry.initial-interval-ms", () -> "400");
        registry.add("ticketing.consumer.retry.multiplier", () -> "2.0");
        registry.add("ticketing.consumer.retry.max-interval-ms", () -> "1500");
        registry.add("spring.kafka.listener.auto-startup", () -> "true");
    }

    private static void ensureTopicsCreated() {
        if (!TOPICS_CREATED.compareAndSet(false, true)) {
            return;
        }
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = List.of(
                    new NewTopic(Topics.INVENTORY_EVENTS, 6, (short) 1),
                    new NewTopic(INVENTORY_RETRY_0, 6, (short) 1),
                    new NewTopic(INVENTORY_RETRY_1, 6, (short) 1),
                    new NewTopic(INVENTORY_DLT, 3, (short) 1),
                    new NewTopic(Topics.PAYMENT_EVENTS, 6, (short) 1),
                    new NewTopic(PAYMENT_RETRY_0, 6, (short) 1),
                    new NewTopic(PAYMENT_RETRY_1, 6, (short) 1),
                    new NewTopic(PAYMENT_DLT, 3, (short) 1),
                    new NewTopic(Topics.ORDER_EVENTS, 6, (short) 1)
            );
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to create Kafka topics for test", ex);
        }
    }

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
    private OrderSagaService orderSagaService;

    @Autowired
    private CheckoutService checkoutService;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private MockMvc mockMvc;

    @BeforeAll
    static void beforeAll() {
        ensureTopicsCreated();
    }

    @BeforeEach
    void setUp() throws Exception {
        if (SCHEMA_MIGRATED.compareAndSet(false, true)) {
            Flyway flyway = Flyway.configure()
                    .dataSource(dataSource)
                    .locations("classpath:db/migration")
                    .load();
            flyway.migrate();
        }
        orderSagaService.setBeforeOrderLockHook(null);
        orderSagaService.setAfterOrderLockHook(null);
        checkoutService.setBeforeOrderLockHook(null);
        checkoutService.setAfterIdempotencyClaimHook(null);
        checkoutService.setInTransactionHook(null);
        waitForListenerContainersAssignment();
    }

    private void waitForListenerContainersAssignment() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L;
        for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
            if (!container.isRunning()) {
                container.start();
            }
            while ((container.getAssignedPartitions() == null || container.getAssignedPartitions().isEmpty())
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(25L);
            }
        }
    }

    // =========================================================================
    // 1. Parameterized test covering every (order status, event type) pair
    // =========================================================================

    static Stream<Arguments> stateTransitionMatrix() {
        // Order state descriptors:
        // "NONE", "RESERVED", "PAYMENT_PENDING", "PAYMENT_PENDING_PAID", "CONFIRMED",
        // "CANCELLED_PAYMENT_FAILED", "CANCELLED_EXPIRED", "REFUND_PENDING", "CANCELLED_REFUNDED"
        List<String> states = List.of(
                "NONE",
                OrderSagaService.STATUS_RESERVED,
                OrderSagaService.STATUS_PAYMENT_PENDING,
                "PAYMENT_PENDING_PAID",
                OrderSagaService.STATUS_CONFIRMED,
                OrderSagaService.STATUS_CANCELLED_PAYMENT_FAILED,
                OrderSagaService.STATUS_CANCELLED_EXPIRED,
                OrderSagaService.STATUS_REFUND_PENDING,
                OrderSagaService.STATUS_CANCELLED_REFUNDED
        );

        List<String> events = List.of(
                EventTypes.INVENTORY_SEAT_RESERVED,
                EventTypes.INVENTORY_SEAT_RELEASED,
                EventTypes.INVENTORY_RESERVATION_EXPIRED,
                EventTypes.INVENTORY_SEAT_CONFIRMED,
                EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED,
                EventTypes.PAYMENT_SUCCEEDED,
                EventTypes.PAYMENT_FAILED,
                EventTypes.PAYMENT_REFUNDED,
                EventTypes.ORDER_PAYMENT_REQUESTED // unrelated event type
        );

        List<Arguments> args = new ArrayList<>();
        for (String state : states) {
            String initialDbStatus = "PAYMENT_PENDING_PAID".equals(state)
                    ? OrderSagaService.STATUS_PAYMENT_PENDING
                    : ("NONE".equals(state) ? null : state);
            boolean presetPaymentId = "PAYMENT_PENDING_PAID".equals(state)
                    || OrderSagaService.STATUS_CONFIRMED.equals(state)
                    || OrderSagaService.STATUS_REFUND_PENDING.equals(state)
                    || OrderSagaService.STATUS_CANCELLED_REFUNDED.equals(state);

            for (String eventType : events) {
                if ("NONE".equals(state)) {
                    String expectedStatus;
                    String expectedOutboxEventType = null;
                    String expectedOutcome;
                    if (EventTypes.INVENTORY_SEAT_RESERVED.equals(eventType)) {
                        expectedStatus = OrderSagaService.STATUS_RESERVED;
                        expectedOutcome = OrderSagaService.OUTCOME_APPLIED;
                    } else if (EventTypes.INVENTORY_RESERVATION_EXPIRED.equals(eventType)
                            || EventTypes.INVENTORY_SEAT_CONFIRMED.equals(eventType)
                            || EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED.equals(eventType)) {
                        expectedStatus = "NONE";
                        expectedOutcome = "retried";
                    } else if (EventTypes.PAYMENT_SUCCEEDED.equals(eventType)
                            || EventTypes.PAYMENT_FAILED.equals(eventType)
                            || EventTypes.PAYMENT_REFUNDED.equals(eventType)) {
                        expectedStatus = "NONE";
                        expectedOutcome = "dlt";
                    } else {
                        expectedStatus = "NONE";
                        expectedOutcome = OrderSagaService.OUTCOME_IGNORED;
                    }
                    args.add(Arguments.of(
                            state,
                            initialDbStatus,
                            presetPaymentId,
                            eventType,
                            expectedStatus,
                            expectedOutboxEventType,
                            expectedOutcome
                    ));
                    continue;
                }

                String expectedStatus = initialDbStatus;
                String expectedOutboxEventType = null;
                String expectedOutcome = OrderSagaService.OUTCOME_NOOP;

                if (EventTypes.INVENTORY_SEAT_RELEASED.equals(eventType)
                        || EventTypes.ORDER_PAYMENT_REQUESTED.equals(eventType)) {
                    expectedOutcome = OrderSagaService.OUTCOME_IGNORED;
                } else if (EventTypes.INVENTORY_RESERVATION_EXPIRED.equals(eventType)) {
                    if (OrderSagaService.STATUS_RESERVED.equals(state)
                            || OrderSagaService.STATUS_PAYMENT_PENDING.equals(state)) {
                        expectedStatus = OrderSagaService.STATUS_CANCELLED_EXPIRED;
                        expectedOutboxEventType = EventTypes.ORDER_CANCELLED;
                        expectedOutcome = OrderSagaService.OUTCOME_APPLIED;
                    }
                } else if (EventTypes.INVENTORY_SEAT_CONFIRMED.equals(eventType)) {
                    if (OrderSagaService.STATUS_PAYMENT_PENDING.equals(state)
                            || "PAYMENT_PENDING_PAID".equals(state)) {
                        expectedStatus = OrderSagaService.STATUS_CONFIRMED;
                        expectedOutboxEventType = EventTypes.ORDER_CONFIRMED;
                        expectedOutcome = OrderSagaService.OUTCOME_APPLIED;
                    }
                } else if (EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED.equals(eventType)) {
                    if (OrderSagaService.STATUS_PAYMENT_PENDING.equals(state)
                            || "PAYMENT_PENDING_PAID".equals(state)
                            || OrderSagaService.STATUS_CANCELLED_EXPIRED.equals(state)
                            || OrderSagaService.STATUS_CANCELLED_PAYMENT_FAILED.equals(state)) {
                        expectedStatus = OrderSagaService.STATUS_REFUND_PENDING;
                        expectedOutboxEventType = EventTypes.ORDER_REFUND_REQUESTED;
                        expectedOutcome = OrderSagaService.OUTCOME_REFUND_REQUESTED;
                    }
                } else if (EventTypes.PAYMENT_SUCCEEDED.equals(eventType)) {
                    if (OrderSagaService.STATUS_PAYMENT_PENDING.equals(state)
                            || "PAYMENT_PENDING_PAID".equals(state)
                            || OrderSagaService.STATUS_CONFIRMED.equals(state)
                            || OrderSagaService.STATUS_REFUND_PENDING.equals(state)) {
                        expectedStatus = initialDbStatus;
                        expectedOutboxEventType = null;
                        expectedOutcome = OrderSagaService.OUTCOME_APPLIED;
                    } else if (OrderSagaService.STATUS_CANCELLED_EXPIRED.equals(state)
                            || OrderSagaService.STATUS_CANCELLED_PAYMENT_FAILED.equals(state)) {
                        expectedStatus = OrderSagaService.STATUS_REFUND_PENDING;
                        expectedOutboxEventType = EventTypes.ORDER_REFUND_REQUESTED;
                        expectedOutcome = OrderSagaService.OUTCOME_REFUND_REQUESTED;
                    }
                } else if (EventTypes.PAYMENT_FAILED.equals(eventType)) {
                    if (OrderSagaService.STATUS_PAYMENT_PENDING.equals(state)
                            || "PAYMENT_PENDING_PAID".equals(state)) {
                        expectedStatus = OrderSagaService.STATUS_CANCELLED_PAYMENT_FAILED;
                        expectedOutboxEventType = EventTypes.ORDER_CANCELLED;
                        expectedOutcome = OrderSagaService.OUTCOME_APPLIED;
                    }
                } else if (EventTypes.PAYMENT_REFUNDED.equals(eventType)) {
                    if (OrderSagaService.STATUS_REFUND_PENDING.equals(state)) {
                        expectedStatus = OrderSagaService.STATUS_CANCELLED_REFUNDED;
                        expectedOutboxEventType = EventTypes.ORDER_CANCELLED;
                        expectedOutcome = OrderSagaService.OUTCOME_APPLIED;
                    }
                }

                args.add(Arguments.of(
                        state,
                        initialDbStatus,
                        presetPaymentId,
                        eventType,
                        expectedStatus,
                        expectedOutboxEventType,
                        expectedOutcome
                ));
            }
        }
        return args.stream();
    }

    @ParameterizedTest(name = "state={0}, event={3} -> finalStatus={4}, outbox={5}, outcome={6}")
    @MethodSource("stateTransitionMatrix")
    @DisplayName("1. Parameterized test covering every (order status, event type) pair")
    void everyOrderStatusAndEventTypePairFollowsStateMachine(
            String stateLabel,
            String initialDbStatus,
            boolean presetPaymentId,
            String eventType,
            String expectedFinalStatus,
            String expectedOutboxEventType,
            String expectedOutcome
    ) throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID paymentId = presetPaymentId ? UUID.randomUUID() : null;

        if ("NONE".equals(stateLabel)) {
            handleNoneStateMatrixCase(orderId, userId, concertEventId, seatId, eventType);
            return;
        }

        insertOrderInStatus(orderId, userId, concertEventId, seatId, initialDbStatus, paymentId);

        double beforeMetric = sagaMetricCount(eventType, expectedOutcome);
        UUID kafkaEventId = publishEventForOrder(eventType, orderId, userId, concertEventId, seatId,
                paymentId != null ? paymentId : UUID.randomUUID());

        waitForCondition(() -> isEventProcessed(kafkaEventId), Duration.ofSeconds(10),
                "Timed out waiting for event " + kafkaEventId + " (" + eventType + ") on state " + stateLabel);

        OrderRecord updated = orderRepository.findById(orderId).orElseThrow();
        assertThat(updated.status()).isEqualTo(expectedFinalStatus);

        List<Map<String, Object>> outboxRows = findOutboxRowsForAggregate(orderId);
        if (expectedOutboxEventType == null) {
            assertThat(outboxRows).isEmpty();
        } else {
            assertThat(outboxRows).hasSize(1);
            assertThat(outboxRows.get(0).get("event_type")).isEqualTo(expectedOutboxEventType);
        }

        waitForCondition(() -> sagaMetricCount(eventType, expectedOutcome) >= beforeMetric + 1.0,
                Duration.ofSeconds(5),
                "Expected metric order_saga_events_handled_total for " + eventType + " / " + expectedOutcome);
    }

    private void handleNoneStateMatrixCase(
            UUID orderId,
            UUID userId,
            UUID concertEventId,
            UUID seatId,
            String eventType
    ) throws Exception {
        if (EventTypes.INVENTORY_SEAT_RESERVED.equals(eventType)) {
            double beforeMetric = sagaMetricCount(eventType, OrderSagaService.OUTCOME_APPLIED);
            UUID kafkaEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);
            waitForCondition(() -> isEventProcessed(kafkaEventId), Duration.ofSeconds(10), "seat.reserved on NONE");

            OrderRecord created = orderRepository.findById(orderId).orElseThrow();
            assertThat(created.status()).isEqualTo(OrderSagaService.STATUS_RESERVED);
            assertThat(findOutboxRowsForAggregate(orderId)).isEmpty();
            waitForCondition(() -> sagaMetricCount(eventType, OrderSagaService.OUTCOME_APPLIED) >= beforeMetric + 1.0,
                    Duration.ofSeconds(5), "metric applied for seat.reserved on NONE");
        } else if (EventTypes.INVENTORY_RESERVATION_EXPIRED.equals(eventType)
                || EventTypes.INVENTORY_SEAT_CONFIRMED.equals(eventType)
                || EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED.equals(eventType)) {
            UUID paymentId = UUID.randomUUID();
            UUID kafkaEventId = publishEventForOrder(eventType, orderId, userId, concertEventId, seatId, paymentId);

            ConsumerRecord<String, String> retryRecord = pollSingleRecordFromTopic(
                    INVENTORY_RETRY_0,
                    orderId.toString(),
                    Duration.ofSeconds(10)
            );
            assertThat(retryRecord).as("Event %s on unknown order must appear on first retry topic", eventType).isNotNull();

            ConsumerRecord<String, String> dltRecord = pollSingleRecordFromTopic(
                    INVENTORY_DLT,
                    orderId.toString(),
                    Duration.ofMillis(500)
            );
            assertThat(dltRecord).as("Event %s on unknown order must not appear in DLT at first", eventType).isNull();
            assertThat(orderRepository.findById(orderId)).isEmpty();
            assertThat(isEventProcessed(kafkaEventId)).isFalse();
        } else if (EventTypes.PAYMENT_SUCCEEDED.equals(eventType)
                || EventTypes.PAYMENT_FAILED.equals(eventType)
                || EventTypes.PAYMENT_REFUNDED.equals(eventType)) {
            UUID paymentId = UUID.randomUUID();
            UUID kafkaEventId = publishEventForOrder(eventType, orderId, userId, concertEventId, seatId, paymentId);

            ConsumerRecord<String, String> dltRecord = pollSingleRecordFromTopic(
                    PAYMENT_DLT,
                    orderId.toString(),
                    Duration.ofSeconds(10)
            );
            assertThat(dltRecord).as("Event %s on unknown order must appear directly in DLT", eventType).isNotNull();

            ConsumerRecord<String, String> retryRecord = pollSingleRecordFromTopics(
                    List.of(PAYMENT_RETRY_0, PAYMENT_RETRY_1),
                    orderId.toString(),
                    Duration.ofMillis(500)
            );
            assertThat(retryRecord).as("Event %s on unknown order must not appear on retry topics", eventType).isNull();
            assertThat(orderRepository.findById(orderId)).isEmpty();
            assertThat(isEventProcessed(kafkaEventId)).isFalse();
        } else {
            // Unrelated event type
            double beforeMetric = sagaMetricCount(eventType, OrderSagaService.OUTCOME_IGNORED);
            UUID kafkaEventId = publishEventForOrder(eventType, orderId, userId, concertEventId, seatId, UUID.randomUUID());
            waitForCondition(() -> isEventProcessed(kafkaEventId), Duration.ofSeconds(10), "unrelated event processed on NONE");
            assertThat(orderRepository.findById(orderId)).isEmpty();
            assertThat(findOutboxRowsForAggregate(orderId)).isEmpty();
            waitForCondition(() -> sagaMetricCount(eventType, OrderSagaService.OUTCOME_IGNORED) >= beforeMetric + 1.0,
                    Duration.ofSeconds(5), "metric ignored for unrelated event on NONE");
        }
    }

    // =========================================================================
    // 2. Full happy path: seat.reserved -> checkout -> payment.succeeded -> seat.confirmed
    // =========================================================================

    @Test
    @DisplayName("2. Full happy path: seat.reserved -> checkout -> payment.succeeded -> seat.confirmed ends in CONFIRMED with 1 order.confirmed row")
    void fullHappyPath() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        // 1. inventory.seat.reserved
        UUID reservedEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(reservedEventId), Duration.ofSeconds(10), "seat.reserved processed");

        OrderRecord reservedOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(reservedOrder.status()).isEqualTo(OrderSagaService.STATUS_RESERVED);

        Timer lagTimer = meterRegistry.find(OrderSagaService.METRIC_MATERIALIZATION_LAG).timer();
        assertThat(lagTimer).isNotNull();
        assertThat(lagTimer.count()).isGreaterThanOrEqualTo(1L);

        // 2. Checkout
        performCheckout(orderId, userId, UUID.randomUUID())
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status", is(OrderSagaService.STATUS_PAYMENT_PENDING)));

        // 3. payment.succeeded
        UUID paidEventId = publishPaymentSucceeded(orderId, userId, paymentId);
        waitForCondition(() -> isEventProcessed(paidEventId), Duration.ofSeconds(10), "payment.succeeded processed");

        OrderRecord afterPayment = orderRepository.findById(orderId).orElseThrow();
        assertThat(afterPayment.status()).isEqualTo(OrderSagaService.STATUS_PAYMENT_PENDING);
        assertThat(afterPayment.paymentId()).isEqualTo(paymentId);
        assertThat(afterPayment.paidAt()).isNotNull();

        // 4. inventory.seat.confirmed
        UUID confirmedEventId = publishSeatConfirmed(orderId, concertEventId, seatId, paymentId);
        waitForCondition(() -> isEventProcessed(confirmedEventId), Duration.ofSeconds(10), "seat.confirmed processed");

        OrderRecord confirmedOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(confirmedOrder.status()).isEqualTo(OrderSagaService.STATUS_CONFIRMED);
        assertThat(confirmedOrder.paymentId()).isEqualTo(paymentId);
        assertThat(confirmedOrder.paidAt()).isNotNull();
        assertThat(confirmedOrder.confirmedAt()).isNotNull();

        List<Map<String, Object>> confirmedOutbox = findOutboxRowsByType(orderId, EventTypes.ORDER_CONFIRMED);
        assertThat(confirmedOutbox).hasSize(1);

        OrderConfirmedPayload confirmedPayload = objectMapper.readValue(
                (String) confirmedOutbox.get(0).get("payload"),
                OrderConfirmedPayload.class
        );
        assertThat(confirmedPayload.orderId()).isEqualTo(orderId);
        assertThat(confirmedPayload.paymentId()).isEqualTo(paymentId);
        assertThat(confirmedPayload.seats()).hasSize(1);
        assertThat(confirmedPayload.seats().get(0).seatId()).isEqualTo(seatId);
    }

    // =========================================================================
    // 3. Out-of-order happy path: seat.confirmed BEFORE payment.succeeded
    // =========================================================================

    @Test
    @DisplayName("3. Happy path with seat.confirmed arriving BEFORE payment.succeeded gives CONFIRMED with payment_id and paid_at populated and 1 order.confirmed row")
    void happyPathWithSeatConfirmedBeforePaymentSucceeded() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        // 1. inventory.seat.reserved
        UUID reservedEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(reservedEventId), Duration.ofSeconds(10), "seat.reserved processed");

        // 2. Checkout -> PAYMENT_PENDING
        performCheckout(orderId, userId, UUID.randomUUID())
                .andExpect(status().isAccepted());

        // 3. inventory.seat.confirmed arrives FIRST
        UUID confirmedEventId = publishSeatConfirmed(orderId, concertEventId, seatId, paymentId);
        waitForCondition(() -> isEventProcessed(confirmedEventId), Duration.ofSeconds(10), "seat.confirmed processed");

        OrderRecord afterConfirm = orderRepository.findById(orderId).orElseThrow();
        assertThat(afterConfirm.status()).isEqualTo(OrderSagaService.STATUS_CONFIRMED);
        assertThat(afterConfirm.paymentId()).isEqualTo(paymentId);
        assertThat(afterConfirm.paidAt()).isNull();
        assertThat(afterConfirm.confirmedAt()).isNotNull();

        List<Map<String, Object>> confirmedOutboxAfterConfirm = findOutboxRowsByType(orderId, EventTypes.ORDER_CONFIRMED);
        assertThat(confirmedOutboxAfterConfirm).hasSize(1);
        long versionAfterConfirm = afterConfirm.version();

        // 4. payment.succeeded arrives SECOND
        Instant paymentPaidAt = Instant.now().minus(3, ChronoUnit.SECONDS).truncatedTo(ChronoUnit.MICROS);
        UUID paidEventId = publishPaymentSucceeded(orderId, userId, paymentId, paymentPaidAt);
        waitForCondition(() -> isEventProcessed(paidEventId), Duration.ofSeconds(10), "payment.succeeded processed");

        OrderRecord finalOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(finalOrder.status()).isEqualTo(OrderSagaService.STATUS_CONFIRMED);
        assertThat(finalOrder.paymentId()).isEqualTo(paymentId);
        assertThat(finalOrder.paidAt()).isNotNull();
        assertThat(finalOrder.paidAt().truncatedTo(ChronoUnit.MICROS)).isEqualTo(paymentPaidAt);
        assertThat(finalOrder.confirmedAt()).isEqualTo(afterConfirm.confirmedAt());

        List<Map<String, Object>> confirmedOutbox = findOutboxRowsByType(orderId, EventTypes.ORDER_CONFIRMED);
        assertThat(confirmedOutbox).hasSize(1);
        assertThat(finalOrder.version()).isEqualTo(versionAfterConfirm + 1);

        // 5. A second payment.succeeded with a different eventId and a different paidAt does not change recorded paid_at
        Instant secondPaidAt = paymentPaidAt.plus(10, ChronoUnit.SECONDS);
        long versionBeforeSecond = finalOrder.version();
        UUID secondPaidEventId = publishPaymentSucceeded(orderId, userId, paymentId, secondPaidAt);
        waitForCondition(() -> isEventProcessed(secondPaidEventId), Duration.ofSeconds(10), "second payment.succeeded processed");

        OrderRecord afterSecondPayment = orderRepository.findById(orderId).orElseThrow();
        assertThat(afterSecondPayment.status()).isEqualTo(OrderSagaService.STATUS_CONFIRMED);
        assertThat(afterSecondPayment.paidAt().truncatedTo(ChronoUnit.MICROS)).isEqualTo(paymentPaidAt);
        assertThat(afterSecondPayment.version()).isEqualTo(versionBeforeSecond);
        assertThat(findOutboxRowsByType(orderId, EventTypes.ORDER_CONFIRMED)).hasSize(1);
    }

    // =========================================================================
    // 4. payment.failed path
    // =========================================================================

    @Test
    @DisplayName("4. payment.failed path ends CANCELLED_PAYMENT_FAILED with 1 order.cancelled")
    void paymentFailedPath() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        UUID reservedEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(reservedEventId), Duration.ofSeconds(10), "seat.reserved processed");

        performCheckout(orderId, userId, UUID.randomUUID())
                .andExpect(status().isAccepted());

        UUID failedEventId = publishPaymentFailed(orderId, userId, paymentId);
        waitForCondition(() -> isEventProcessed(failedEventId), Duration.ofSeconds(10), "payment.failed processed");

        OrderRecord cancelledOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(cancelledOrder.status()).isEqualTo(OrderSagaService.STATUS_CANCELLED_PAYMENT_FAILED);
        assertThat(cancelledOrder.cancellationReason()).isEqualTo(OrderSagaService.REASON_PAYMENT_FAILED);
        assertThat(cancelledOrder.cancelledAt()).isNotNull();

        List<Map<String, Object>> cancelledOutbox = findOutboxRowsByType(orderId, EventTypes.ORDER_CANCELLED);
        assertThat(cancelledOutbox).hasSize(1);

        OrderCancelledPayload payload = objectMapper.readValue(
                (String) cancelledOutbox.get(0).get("payload"),
                OrderCancelledPayload.class
        );
        assertThat(payload.finalStatus()).isEqualTo(OrderSagaService.STATUS_CANCELLED_PAYMENT_FAILED);
        assertThat(payload.cancellationReason()).isEqualTo(OrderSagaService.REASON_PAYMENT_FAILED);
    }

    // =========================================================================
    // 5. Expiry path: RESERVED -> CANCELLED_EXPIRED
    // =========================================================================

    @Test
    @DisplayName("5. Expiry path: RESERVED -> CANCELLED_EXPIRED with 1 order.cancelled")
    void expiryPathFromReserved() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        UUID reservedEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(reservedEventId), Duration.ofSeconds(10), "seat.reserved processed");

        UUID expiredEventId = publishReservationExpired(orderId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(expiredEventId), Duration.ofSeconds(10), "reservation.expired processed");

        OrderRecord expiredOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(expiredOrder.status()).isEqualTo(OrderSagaService.STATUS_CANCELLED_EXPIRED);
        assertThat(expiredOrder.cancellationReason()).isEqualTo(OrderSagaService.REASON_RESERVATION_EXPIRED);
        assertThat(expiredOrder.cancelledAt()).isNotNull();

        List<Map<String, Object>> cancelledOutbox = findOutboxRowsByType(orderId, EventTypes.ORDER_CANCELLED);
        assertThat(cancelledOutbox).hasSize(1);

        OrderCancelledPayload payload = objectMapper.readValue(
                (String) cancelledOutbox.get(0).get("payload"),
                OrderCancelledPayload.class
        );
        assertThat(payload.finalStatus()).isEqualTo(OrderSagaService.STATUS_CANCELLED_EXPIRED);
        assertThat(payload.cancellationReason()).isEqualTo(OrderSagaService.REASON_RESERVATION_EXPIRED);
    }

    // =========================================================================
    // 6. Late payment path: reservation.expired -> payment.succeeded -> confirmation_rejected -> payment.refunded
    // =========================================================================

    @Test
    @DisplayName("6. Late payment path: reservation.expired -> payment.succeeded -> confirmation_rejected -> REFUND_PENDING (1 order.refund.requested) -> payment.refunded -> CANCELLED_REFUNDED")
    void latePaymentPathPaymentSucceededFirst() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        UUID reservedEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(reservedEventId), Duration.ofSeconds(10), "seat.reserved processed");

        performCheckout(orderId, userId, UUID.randomUUID())
                .andExpect(status().isAccepted());

        // 1. reservation.expired -> CANCELLED_EXPIRED
        UUID expiredEventId = publishReservationExpired(orderId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(expiredEventId), Duration.ofSeconds(10), "reservation.expired processed");
        assertThat(orderRepository.findById(orderId).orElseThrow().status())
                .isEqualTo(OrderSagaService.STATUS_CANCELLED_EXPIRED);

        // 2. payment.succeeded arrives first -> moves to REFUND_PENDING, writes 1 order.refund.requested
        UUID paidEventId = publishPaymentSucceeded(orderId, userId, paymentId);
        waitForCondition(() -> isEventProcessed(paidEventId), Duration.ofSeconds(10), "payment.succeeded processed");

        OrderRecord refundPendingOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(refundPendingOrder.status()).isEqualTo(OrderSagaService.STATUS_REFUND_PENDING);
        assertThat(refundPendingOrder.paymentId()).isEqualTo(paymentId);
        assertThat(findOutboxRowsByType(orderId, EventTypes.ORDER_REFUND_REQUESTED)).hasSize(1);

        // 3. inventory.seat.confirmation_rejected arrives second -> no-op in REFUND_PENDING, still 1 order.refund.requested
        UUID rejectedEventId = publishSeatConfirmationRejected(orderId, paymentId);
        waitForCondition(() -> isEventProcessed(rejectedEventId), Duration.ofSeconds(10), "confirmation_rejected processed");

        assertThat(orderRepository.findById(orderId).orElseThrow().status())
                .isEqualTo(OrderSagaService.STATUS_REFUND_PENDING);
        assertThat(findOutboxRowsByType(orderId, EventTypes.ORDER_REFUND_REQUESTED)).hasSize(1);

        // 4. payment.refunded -> moves to CANCELLED_REFUNDED, writes order.cancelled (CANCELLED_REFUNDED)
        UUID refundedEventId = publishPaymentRefunded(orderId, userId, paymentId);
        waitForCondition(() -> isEventProcessed(refundedEventId), Duration.ofSeconds(10), "payment.refunded processed");

        OrderRecord finalOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(finalOrder.status()).isEqualTo(OrderSagaService.STATUS_CANCELLED_REFUNDED);
        assertThat(finalOrder.refundedAt()).isNotNull();

        List<Map<String, Object>> cancelledOutbox = findOutboxRowsByType(orderId, EventTypes.ORDER_CANCELLED);
        assertThat(cancelledOutbox).hasSize(2); // 1 from CANCELLED_EXPIRED, 1 from CANCELLED_REFUNDED

        OrderCancelledPayload finalCancelledPayload = objectMapper.readValue(
                (String) cancelledOutbox.get(1).get("payload"),
                OrderCancelledPayload.class
        );
        assertThat(finalCancelledPayload.finalStatus()).isEqualTo(OrderSagaService.STATUS_CANCELLED_REFUNDED);
        assertThat(finalCancelledPayload.cancellationReason()).isEqualTo(OrderSagaService.REASON_LATE_PAYMENT_EXPIRED_HOLD);
    }

    // =========================================================================
    // 7. Late payment path with confirmation_rejected arriving BEFORE payment.succeeded
    // =========================================================================

    @Test
    @DisplayName("7. Late payment path with confirmation_rejected arriving BEFORE payment.succeeded -> REFUND_PENDING (1 order.refund.requested) -> payment.refunded -> CANCELLED_REFUNDED")
    void latePaymentPathConfirmationRejectedFirst() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        UUID reservedEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(reservedEventId), Duration.ofSeconds(10), "seat.reserved processed");

        performCheckout(orderId, userId, UUID.randomUUID())
                .andExpect(status().isAccepted());

        // 1. reservation.expired -> CANCELLED_EXPIRED
        UUID expiredEventId = publishReservationExpired(orderId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(expiredEventId), Duration.ofSeconds(10), "reservation.expired processed");

        // 2. inventory.seat.confirmation_rejected arrives FIRST -> moves to REFUND_PENDING, writes 1 order.refund.requested
        UUID rejectedEventId = publishSeatConfirmationRejected(orderId, paymentId);
        waitForCondition(() -> isEventProcessed(rejectedEventId), Duration.ofSeconds(10), "confirmation_rejected processed");

        OrderRecord refundPendingOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(refundPendingOrder.status()).isEqualTo(OrderSagaService.STATUS_REFUND_PENDING);
        assertThat(refundPendingOrder.paymentId()).isEqualTo(paymentId);
        assertThat(refundPendingOrder.paidAt()).isNull();
        assertThat(findOutboxRowsByType(orderId, EventTypes.ORDER_REFUND_REQUESTED)).hasSize(1);
        long versionAfterReject = refundPendingOrder.version();

        // 3. payment.succeeded arrives SECOND -> backfills paid_at in REFUND_PENDING, still 1 order.refund.requested
        Instant paymentPaidAt = Instant.now().minus(2, ChronoUnit.SECONDS).truncatedTo(ChronoUnit.MICROS);
        UUID paidEventId = publishPaymentSucceeded(orderId, userId, paymentId, paymentPaidAt);
        waitForCondition(() -> isEventProcessed(paidEventId), Duration.ofSeconds(10), "payment.succeeded processed");

        OrderRecord afterPayment = orderRepository.findById(orderId).orElseThrow();
        assertThat(afterPayment.status()).isEqualTo(OrderSagaService.STATUS_REFUND_PENDING);
        assertThat(afterPayment.paymentId()).isEqualTo(paymentId);
        assertThat(afterPayment.paidAt()).isNotNull();
        assertThat(afterPayment.paidAt().truncatedTo(ChronoUnit.MICROS)).isEqualTo(paymentPaidAt);
        assertThat(findOutboxRowsByType(orderId, EventTypes.ORDER_REFUND_REQUESTED)).hasSize(1);
        assertThat(afterPayment.version()).isEqualTo(versionAfterReject + 1);

        // 3b. A second payment.succeeded with a different eventId and different paidAt does not change recorded paid_at
        Instant secondPaidAt = paymentPaidAt.plus(5, ChronoUnit.SECONDS);
        long versionBeforeSecond = afterPayment.version();
        UUID secondPaidEventId = publishPaymentSucceeded(orderId, userId, paymentId, secondPaidAt);
        waitForCondition(() -> isEventProcessed(secondPaidEventId), Duration.ofSeconds(10), "second payment.succeeded processed");

        OrderRecord afterSecondPayment = orderRepository.findById(orderId).orElseThrow();
        assertThat(afterSecondPayment.status()).isEqualTo(OrderSagaService.STATUS_REFUND_PENDING);
        assertThat(afterSecondPayment.paidAt().truncatedTo(ChronoUnit.MICROS)).isEqualTo(paymentPaidAt);
        assertThat(afterSecondPayment.version()).isEqualTo(versionBeforeSecond);
        assertThat(findOutboxRowsByType(orderId, EventTypes.ORDER_REFUND_REQUESTED)).hasSize(1);

        // 4. payment.refunded -> moves to CANCELLED_REFUNDED
        UUID refundedEventId = publishPaymentRefunded(orderId, userId, paymentId);
        waitForCondition(() -> isEventProcessed(refundedEventId), Duration.ofSeconds(10), "payment.refunded processed");

        OrderRecord finalOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(finalOrder.status()).isEqualTo(OrderSagaService.STATUS_CANCELLED_REFUNDED);
        assertThat(finalOrder.refundedAt()).isNotNull();
    }

    // =========================================================================
    // 8. Duplicate eventId delivered twice writes 1 outbox row
    // =========================================================================

    @Test
    @DisplayName("8. Duplicate eventId delivered twice writes 1 outbox row")
    void duplicateEventIdDeliveredTwiceWritesOneOutboxRow() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        UUID reservedEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(reservedEventId), Duration.ofSeconds(10), "seat.reserved processed");

        UUID duplicateEventId = UUID.randomUUID();
        ReservationExpiredPayload expiredPayload = new ReservationExpiredPayload(
                orderId, orderId, userId, concertEventId, List.of(seatId), Instant.now()
        );
        EventEnvelope<ReservationExpiredPayload> envelope = createEnvelope(
                duplicateEventId,
                EventTypes.INVENTORY_RESERVATION_EXPIRED,
                UUID.randomUUID().toString(),
                expiredPayload
        );
        String rawJson = objectMapper.writeValueAsString(envelope);

        Counter dedupCounter = meterRegistry.find(com.ticketing.consumer.ConsumerMetrics.DUPLICATE_TOTAL_METRIC)
                .tag("event_type", EventTypes.INVENTORY_RESERVATION_EXPIRED)
                .tag("consumer_group", INVENTORY_CG)
                .counter();
        double beforeDedup = dedupCounter != null ? dedupCounter.count() : 0.0;

        // Send twice with the exact same eventId
        kafkaTemplate.send(Topics.INVENTORY_EVENTS, orderId.toString(), rawJson).get(10, TimeUnit.SECONDS);
        waitForCondition(() -> isEventProcessed(duplicateEventId), Duration.ofSeconds(10), "first delivery processed");

        kafkaTemplate.send(Topics.INVENTORY_EVENTS, orderId.toString(), rawJson).get(10, TimeUnit.SECONDS);
        waitForCondition(() -> {
            Counter c = meterRegistry.find(com.ticketing.consumer.ConsumerMetrics.DUPLICATE_TOTAL_METRIC)
                    .tag("event_type", EventTypes.INVENTORY_RESERVATION_EXPIRED)
                    .tag("consumer_group", INVENTORY_CG)
                    .counter();
            return c != null && c.count() >= beforeDedup + 1.0;
        }, Duration.ofSeconds(10), "second delivery deduplicated");

        assertThat(findOutboxRowsByType(orderId, EventTypes.ORDER_CANCELLED)).hasSize(1);
    }

    // =========================================================================
    // 9. seat.reserved twice with different eventIds creates 1 order
    // =========================================================================

    @Test
    @DisplayName("9. seat.reserved twice with different eventIds creates 1 order")
    void seatReservedTwiceWithDifferentEventIdsCreatesOneOrder() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        double beforeNoop = sagaMetricCount(EventTypes.INVENTORY_SEAT_RESERVED, OrderSagaService.OUTCOME_NOOP);

        UUID eventId1 = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(eventId1), Duration.ofSeconds(10), "first seat.reserved processed");

        UUID eventId2 = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(eventId2), Duration.ofSeconds(10), "second seat.reserved processed");

        Integer orderCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM orders WHERE order_id = ?",
                Integer.class,
                orderId
        );
        assertThat(orderCount).isEqualTo(1);
        assertThat(orderRepository.findItemsByOrderId(orderId)).hasSize(1);

        waitForCondition(
                () -> sagaMetricCount(EventTypes.INVENTORY_SEAT_RESERVED, OrderSagaService.OUTCOME_NOOP) >= beforeNoop + 1.0,
                Duration.ofSeconds(5),
                "second seat.reserved recorded as noop"
        );
    }

    // =========================================================================
    // 10. Checkout racing reservation.expired on the same order
    // =========================================================================

    @Test
    @DisplayName("10. Checkout racing reservation.expired on the same order ends with 1 winner, consistent state, and deterministic coordination for both orderings")
    void checkoutRacingReservationExpiredEndsWithOneWinner() throws Exception {
        // ---------------------------------------------------------------------
        // Ordering 1: Expiry wins the lock first
        // ---------------------------------------------------------------------
        UUID orderIdA = UUID.randomUUID();
        UUID userIdA = UUID.randomUUID();
        UUID concertEventIdA = UUID.randomUUID();
        UUID seatIdA = UUID.randomUUID();
        UUID idempotencyKeyA = UUID.randomUUID();

        UUID resA = publishSeatReserved(orderIdA, userIdA, concertEventIdA, seatIdA);
        waitForCondition(() -> isEventProcessed(resA), Duration.ofSeconds(10), "orderA reserved");

        CountDownLatch expiryLockedLatch = new CountDownLatch(1);
        CountDownLatch releaseExpiryLatch = new CountDownLatch(1);
        CountDownLatch checkoutAboutToLockLatch = new CountDownLatch(1);

        orderSagaService.setAfterOrderLockHook(() -> {
            expiryLockedLatch.countDown();
            try {
                releaseExpiryLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        checkoutService.setBeforeOrderLockHook(() -> {
            checkoutAboutToLockLatch.countDown();
        });

        ExecutorService executorA = Executors.newSingleThreadExecutor();
        try {
            // 1. Publish reservation.expired; consumer acquires SELECT ... FOR UPDATE on orderIdA and pauses
            UUID expA = publishReservationExpired(orderIdA, concertEventIdA, seatIdA);
            assertThat(expiryLockedLatch.await(10, TimeUnit.SECONDS)).isTrue();

            // 2. Start HTTP checkout concurrently; it will block waiting for row lock
            Future<MvcResult> checkoutFutureA = executorA.submit(() ->
                    performCheckout(orderIdA, userIdA, idempotencyKeyA).andReturn()
            );

            // 3. Wait until checkout reaches the locking read (it signals before taking the lock, then blocks on it)
            assertThat(checkoutAboutToLockLatch.await(10, TimeUnit.SECONDS)).isTrue();

            // 4. Release expiry handler only after checkout signaled it is about to take the lock
            releaseExpiryLatch.countDown();

            waitForCondition(() -> isEventProcessed(expA), Duration.ofSeconds(10), "orderA expired finished");
            MvcResult checkoutResultA = checkoutFutureA.get(10, TimeUnit.SECONDS);

            // Expiry won the lock -> checkout must fail with 409 RESERVATION_EXPIRED
            assertThat(checkoutResultA.getResponse().getStatus()).isEqualTo(409);
            JsonNode errBody = objectMapper.readTree(checkoutResultA.getResponse().getContentAsString());
            assertThat(errBody.get("errorCode").asText()).isEqualTo(ErrorCode.RESERVATION_EXPIRED.name());

            // Consistent state: CANCELLED_EXPIRED, 1 order.cancelled, 0 order.payment.requested, idempotency claim cleaned up
            OrderRecord finalOrderA = orderRepository.findById(orderIdA).orElseThrow();
            assertThat(finalOrderA.status()).isEqualTo(OrderSagaService.STATUS_CANCELLED_EXPIRED);
            assertThat(findOutboxRowsByType(orderIdA, EventTypes.ORDER_CANCELLED)).hasSize(1);
            assertThat(findOutboxRowsByType(orderIdA, EventTypes.ORDER_PAYMENT_REQUESTED)).isEmpty();
            assertThat(idempotencyKeyRepository.findByUserAndKey(userIdA, idempotencyKeyA)).isEmpty();
        } finally {
            orderSagaService.setAfterOrderLockHook(null);
            checkoutService.setBeforeOrderLockHook(null);
            executorA.shutdownNow();
        }

        // ---------------------------------------------------------------------
        // Ordering 2: Checkout wins the lock first
        // ---------------------------------------------------------------------
        UUID orderIdB = UUID.randomUUID();
        UUID userIdB = UUID.randomUUID();
        UUID concertEventIdB = UUID.randomUUID();
        UUID seatIdB = UUID.randomUUID();
        UUID idempotencyKeyB = UUID.randomUUID();

        UUID resB = publishSeatReserved(orderIdB, userIdB, concertEventIdB, seatIdB);
        waitForCondition(() -> isEventProcessed(resB), Duration.ofSeconds(10), "orderB reserved");

        CountDownLatch checkoutLockedLatch = new CountDownLatch(1);
        CountDownLatch releaseCheckoutLatch = new CountDownLatch(1);
        CountDownLatch expiryAboutToLockLatch = new CountDownLatch(1);

        checkoutService.setInTransactionHook(() -> {
            checkoutLockedLatch.countDown();
            try {
                releaseCheckoutLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        orderSagaService.setBeforeOrderLockHook(() -> {
            expiryAboutToLockLatch.countDown();
        });

        ExecutorService executorB = Executors.newSingleThreadExecutor();
        try {
            // 1. Start checkout; it acquires row lock in RESERVED and pauses in tx hook
            Future<MvcResult> checkoutFutureB = executorB.submit(() ->
                    performCheckout(orderIdB, userIdB, idempotencyKeyB).andReturn()
            );
            assertThat(checkoutLockedLatch.await(10, TimeUnit.SECONDS)).isTrue();

            // 2. Publish reservation.expired; consumer reaches findByIdForUpdate and signals before taking lock
            UUID expB = publishReservationExpired(orderIdB, concertEventIdB, seatIdB);
            assertThat(expiryAboutToLockLatch.await(10, TimeUnit.SECONDS)).isTrue();

            // 3. Release checkout to finish and commit
            releaseCheckoutLatch.countDown();

            MvcResult checkoutResultB = checkoutFutureB.get(10, TimeUnit.SECONDS);
            assertThat(checkoutResultB.getResponse().getStatus()).isEqualTo(202);

            // 4. Wait for expiry to process after checkout released lock
            waitForCondition(() -> isEventProcessed(expB), Duration.ofSeconds(10), "orderB expired processed");

            // Checkout won first -> order transitioned to PAYMENT_PENDING (emitted order.payment.requested),
            // then expiry processed PAYMENT_PENDING without payment -> CANCELLED_EXPIRED (emitted order.cancelled).
            OrderRecord finalOrderB = orderRepository.findById(orderIdB).orElseThrow();
            assertThat(finalOrderB.status()).isEqualTo(OrderSagaService.STATUS_CANCELLED_EXPIRED);
            assertThat(findOutboxRowsByType(orderIdB, EventTypes.ORDER_PAYMENT_REQUESTED)).hasSize(1);
            assertThat(findOutboxRowsByType(orderIdB, EventTypes.ORDER_CANCELLED)).hasSize(1);
            assertThat(idempotencyKeyRepository.findByUserAndKey(userIdB, idempotencyKeyB)).isPresent();
        } finally {
            checkoutService.setInTransactionHook(null);
            orderSagaService.setBeforeOrderLockHook(null);
            executorB.shutdownNow();
        }
    }

    // =========================================================================
    // 11. payment.succeeded for an unknown order lands directly in -dlt
    // =========================================================================

    @Test
    @DisplayName("11. payment.succeeded for an unknown order lands directly in payment.events-order-service-payment-cg-dlt")
    void paymentSucceededForUnknownOrderLandsDirectlyInDlt() throws Exception {
        UUID unknownOrderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        UUID eventId = publishPaymentSucceeded(unknownOrderId, userId, paymentId);

        ConsumerRecord<String, String> dltRecord = pollSingleRecordFromTopic(
                PAYMENT_DLT,
                unknownOrderId.toString(),
                Duration.ofSeconds(10)
        );
        assertThat(dltRecord).isNotNull();
        JsonNode envelope = objectMapper.readTree(dltRecord.value());
        assertThat(envelope.get("eventId").asText()).isEqualTo(eventId.toString());
        assertThat(isEventProcessed(eventId)).isFalse();

        ConsumerRecord<String, String> retryRecord = pollSingleRecordFromTopics(
                List.of(PAYMENT_RETRY_0, PAYMENT_RETRY_1),
                unknownOrderId.toString(),
                Duration.ofMillis(500)
        );
        assertThat(retryRecord).isNull();
    }

    // =========================================================================
    // 12. inventory.seat.confirmed for an unknown order is retried via -retry-0 and succeeds once seat.reserved is delivered
    // =========================================================================

    @Test
    @DisplayName("12. inventory.seat.confirmed for an unknown order is retried via -retry-0 and succeeds once seat.reserved + checkout are delivered")
    void seatConfirmedForUnknownOrderRetriedAndSucceedsAfterOrderMaterialized() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        // 1. Publish inventory.seat.confirmed BEFORE the order exists
        UUID confirmedEventId = publishSeatConfirmed(orderId, concertEventId, seatId, paymentId);

        // 2. Verify it lands in inventory.events-order-service-inventory-cg-retry-0
        ConsumerRecord<String, String> retry0Record = pollSingleRecordFromTopic(
                INVENTORY_RETRY_0,
                orderId.toString(),
                Duration.ofSeconds(10)
        );
        assertThat(retry0Record).isNotNull();

        // 3. Now deliver inventory.seat.reserved and checkout so the order is in PAYMENT_PENDING
        UUID reservedEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);
        waitForCondition(() -> isEventProcessed(reservedEventId), Duration.ofSeconds(10), "seat.reserved processed");

        performCheckout(orderId, userId, UUID.randomUUID())
                .andExpect(status().isAccepted());

        // 4. Wait for the retry of inventory.seat.confirmed to succeed
        waitForCondition(() -> isEventProcessed(confirmedEventId), Duration.ofSeconds(10),
                "retried seat.confirmed processed after order materialized");

        OrderRecord finalOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(finalOrder.status()).isEqualTo(OrderSagaService.STATUS_CONFIRMED);
        assertThat(finalOrder.paymentId()).isEqualTo(paymentId);
        assertThat(findOutboxRowsByType(orderId, EventTypes.ORDER_CONFIRMED)).hasSize(1);
    }

    // =========================================================================
    // 13. Unrelated event types are ignored and the next record is processed
    // =========================================================================

    @Test
    @DisplayName("13. Unrelated event types are ignored and the next record is processed")
    void unrelatedEventTypesAreIgnoredAndNextRecordIsProcessed() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID concertEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        // 1. Publish inventory.seat.released (documented in EventTypes, ignored by order-service)
        UUID releasedEventId = UUID.randomUUID();
        SeatReleasedPayload releasedPayload = new SeatReleasedPayload(
                orderId, orderId, concertEventId, List.of(seatId), "PAYMENT_FAILED", Instant.now()
        );
        EventEnvelope<SeatReleasedPayload> releasedEnvelope = createEnvelope(
                releasedEventId,
                EventTypes.INVENTORY_SEAT_RELEASED,
                UUID.randomUUID().toString(),
                releasedPayload
        );
        kafkaTemplate.send(Topics.INVENTORY_EVENTS, orderId.toString(), objectMapper.writeValueAsString(releasedEnvelope))
                .get(10, TimeUnit.SECONDS);

        // 2. Publish completely unknown event type
        UUID customEventId = UUID.randomUUID();
        String customEnvelope = """
                {
                  "eventId": "%s",
                  "eventType": "custom.unknown.event",
                  "occurredAt": "%s",
                  "correlationId": "%s",
                  "schemaVersion": 1,
                  "payload": {"hello": "world"}
                }
                """.formatted(customEventId, Instant.now(), UUID.randomUUID());
        kafkaTemplate.send(Topics.INVENTORY_EVENTS, orderId.toString(), customEnvelope)
                .get(10, TimeUnit.SECONDS);

        // 3. Publish valid inventory.seat.reserved right after
        UUID reservedEventId = publishSeatReserved(orderId, userId, concertEventId, seatId);

        waitForCondition(() -> isEventProcessed(releasedEventId)
                        && isEventProcessed(customEventId)
                        && isEventProcessed(reservedEventId),
                Duration.ofSeconds(10),
                "unrelated events and subsequent seat.reserved processed");

        OrderRecord created = orderRepository.findById(orderId).orElseThrow();
        assertThat(created.status()).isEqualTo(OrderSagaService.STATUS_RESERVED);
    }

    // =========================================================================
    // 14. Outbox poller publishes order.confirmed, order.cancelled, and order.refund.requested to order.events
    // =========================================================================

    @Test
    @DisplayName("14. Outbox poller publishes order.confirmed, order.cancelled, and order.refund.requested to order.events with orderId key and headers in order")
    void outboxPollerPublishesOrderEventsWithKeyAndHeadersInOrder() throws Exception {
        // Order A: seat.reserved -> checkout (order.payment.requested) -> payment.succeeded -> seat.confirmed (order.confirmed)
        UUID orderA = UUID.randomUUID();
        UUID userA = UUID.randomUUID();
        UUID eventA = UUID.randomUUID();
        UUID seatA = UUID.randomUUID();
        UUID paymentA = UUID.randomUUID();

        UUID resA = publishSeatReserved(orderA, userA, eventA, seatA);
        waitForCondition(() -> isEventProcessed(resA), Duration.ofSeconds(10), "orderA reserved");
        performCheckout(orderA, userA, UUID.randomUUID()).andExpect(status().isAccepted());
        UUID paidA = publishPaymentSucceeded(orderA, userA, paymentA);
        UUID confA = publishSeatConfirmed(orderA, eventA, seatA, paymentA);
        waitForCondition(() -> isEventProcessed(paidA) && isEventProcessed(confA), Duration.ofSeconds(10), "orderA confirmed");

        // Order B: seat.reserved -> reservation.expired (order.cancelled) -> payment.succeeded (order.refund.requested) -> payment.refunded (order.cancelled)
        UUID orderB = UUID.randomUUID();
        UUID userB = UUID.randomUUID();
        UUID eventB = UUID.randomUUID();
        UUID seatB = UUID.randomUUID();
        UUID paymentB = UUID.randomUUID();

        UUID resB = publishSeatReserved(orderB, userB, eventB, seatB);
        waitForCondition(() -> isEventProcessed(resB), Duration.ofSeconds(10), "orderB reserved");
        UUID expB = publishReservationExpired(orderB, eventB, seatB);
        waitForCondition(() -> isEventProcessed(expB), Duration.ofSeconds(10), "orderB expired");
        UUID paidB = publishPaymentSucceeded(orderB, userB, paymentB);
        waitForCondition(() -> isEventProcessed(paidB), Duration.ofSeconds(10), "orderB late payment");
        UUID refB = publishPaymentRefunded(orderB, userB, paymentB);
        waitForCondition(() -> isEventProcessed(refB), Duration.ofSeconds(10), "orderB refunded");

        // Wait until all outbox rows for orderA and orderB are published by OutboxPoller
        waitForCondition(() -> countUnpublishedOutboxRows(orderA) == 0 && countUnpublishedOutboxRows(orderB) == 0,
                Duration.ofSeconds(10),
                "Outbox poller published all events for orderA and orderB");

        List<ConsumerRecord<String, String>> orderARecords = pollRecordsForKey(Topics.ORDER_EVENTS, orderA.toString(), 2, Duration.ofSeconds(10));
        List<ConsumerRecord<String, String>> orderBRecords = pollRecordsForKey(Topics.ORDER_EVENTS, orderB.toString(), 3, Duration.ofSeconds(10));

        assertThat(orderARecords).hasSize(2);
        assertOrderEventRecord(orderARecords.get(0), orderA, EventTypes.ORDER_PAYMENT_REQUESTED);
        assertOrderEventRecord(orderARecords.get(1), orderA, EventTypes.ORDER_CONFIRMED);

        assertThat(orderBRecords).hasSize(3);
        assertOrderEventRecord(orderBRecords.get(0), orderB, EventTypes.ORDER_CANCELLED);
        assertOrderEventRecord(orderBRecords.get(1), orderB, EventTypes.ORDER_REFUND_REQUESTED);
        assertOrderEventRecord(orderBRecords.get(2), orderB, EventTypes.ORDER_CANCELLED);
    }

    // =========================================================================
    // 15. Application's default KafkaTemplate still has acks=all and enable.idempotence=true
    // =========================================================================

    @Test
    @DisplayName("15. Application's default KafkaTemplate retains acks=all and enable.idempotence=true")
    void defaultKafkaTemplateRetainsOutboxProducerGuarantees() {
        Map<String, Object> configs = kafkaTemplate.getProducerFactory().getConfigurationProperties();
        assertThat(configs.get(ProducerConfig.ACKS_CONFIG)).asString().isEqualTo("all");
        assertThat(configs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).asString().isEqualTo("true");
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    private void insertOrderInStatus(
            UUID orderId,
            UUID userId,
            UUID concertEventId,
            UUID seatId,
            String status,
            UUID paymentId
    ) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        OrderRecord order = new OrderRecord(
                orderId,
                orderId,
                userId,
                concertEventId,
                status,
                15000L,
                "USD",
                OrderSagaService.STATUS_RESERVED.equals(status) ? null : "pm_tok_visa",
                paymentId,
                null,
                0L,
                now.minus(60, ChronoUnit.SECONDS),
                now.plus(600, ChronoUnit.SECONDS),
                OrderSagaService.STATUS_RESERVED.equals(status) ? null : now.minus(30, ChronoUnit.SECONDS),
                paymentId != null ? now.minus(20, ChronoUnit.SECONDS) : null,
                OrderSagaService.STATUS_CONFIRMED.equals(status) ? now.minus(10, ChronoUnit.SECONDS) : null,
                status.startsWith("CANCELLED") ? now.minus(10, ChronoUnit.SECONDS) : null,
                OrderSagaService.STATUS_CANCELLED_REFUNDED.equals(status) ? now.minus(5, ChronoUnit.SECONDS) : null,
                now,
                now
        );
        OrderItemRecord item = new OrderItemRecord(
                UUID.randomUUID(),
                orderId,
                seatId,
                "SEC-A",
                "1",
                "10",
                15000L
        );
        transactionTemplate.executeWithoutResult(s -> orderRepository.insert(order, List.of(item)));
    }

    private UUID publishEventForOrder(
            String eventType,
            UUID orderId,
            UUID userId,
            UUID concertEventId,
            UUID seatId,
            UUID paymentId
    ) throws Exception {
        return switch (eventType) {
            case EventTypes.INVENTORY_SEAT_RESERVED -> publishSeatReserved(orderId, userId, concertEventId, seatId);
            case EventTypes.INVENTORY_SEAT_RELEASED -> {
                UUID eventId = UUID.randomUUID();
                SeatReleasedPayload payload = new SeatReleasedPayload(
                        orderId, orderId, concertEventId, List.of(seatId), "PAYMENT_FAILED", Instant.now()
                );
                sendEnvelope(Topics.INVENTORY_EVENTS, orderId.toString(),
                        createEnvelope(eventId, EventTypes.INVENTORY_SEAT_RELEASED, UUID.randomUUID().toString(), payload));
                yield eventId;
            }
            case EventTypes.INVENTORY_RESERVATION_EXPIRED -> publishReservationExpired(orderId, concertEventId, seatId);
            case EventTypes.INVENTORY_SEAT_CONFIRMED -> publishSeatConfirmed(orderId, concertEventId, seatId, paymentId);
            case EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED -> publishSeatConfirmationRejected(orderId, paymentId);
            case EventTypes.PAYMENT_SUCCEEDED -> publishPaymentSucceeded(orderId, userId, paymentId);
            case EventTypes.PAYMENT_FAILED -> publishPaymentFailed(orderId, userId, paymentId);
            case EventTypes.PAYMENT_REFUNDED -> publishPaymentRefunded(orderId, userId, paymentId);
            case EventTypes.ORDER_PAYMENT_REQUESTED -> {
                UUID eventId = UUID.randomUUID();
                PaymentRequestedPayload payload = new PaymentRequestedPayload(
                        orderId, orderId, userId, concertEventId, 15000L, "USD", "pm_tok_test",
                        Instant.now(), Instant.now().plus(600, ChronoUnit.SECONDS)
                );
                sendEnvelope(Topics.INVENTORY_EVENTS, orderId.toString(),
                        createEnvelope(eventId, EventTypes.ORDER_PAYMENT_REQUESTED, UUID.randomUUID().toString(), payload));
                yield eventId;
            }
            default -> throw new IllegalArgumentException("Unsupported eventType: " + eventType);
        };
    }

    private UUID publishSeatReserved(UUID orderId, UUID userId, UUID concertEventId, UUID seatId) throws Exception {
        UUID kafkaEventId = UUID.randomUUID();
        Instant now = Instant.now();
        SeatReservedPayload payload = new SeatReservedPayload(
                orderId,
                orderId,
                userId,
                concertEventId,
                List.of(new SeatLine(seatId, "SEC-A", "1", "10", 15000L)),
                15000L,
                "USD",
                now,
                now.plus(600, ChronoUnit.SECONDS)
        );
        sendEnvelope(Topics.INVENTORY_EVENTS, orderId.toString(),
                createEnvelope(kafkaEventId, EventTypes.INVENTORY_SEAT_RESERVED, UUID.randomUUID().toString(), payload));
        return kafkaEventId;
    }

    private UUID publishReservationExpired(UUID orderId, UUID concertEventId, UUID seatId) throws Exception {
        UUID kafkaEventId = UUID.randomUUID();
        ReservationExpiredPayload payload = new ReservationExpiredPayload(
                orderId,
                orderId,
                UUID.randomUUID(),
                concertEventId,
                List.of(seatId),
                Instant.now()
        );
        sendEnvelope(Topics.INVENTORY_EVENTS, orderId.toString(),
                createEnvelope(kafkaEventId, EventTypes.INVENTORY_RESERVATION_EXPIRED, UUID.randomUUID().toString(), payload));
        return kafkaEventId;
    }

    private UUID publishSeatConfirmed(UUID orderId, UUID concertEventId, UUID seatId, UUID paymentId) throws Exception {
        UUID kafkaEventId = UUID.randomUUID();
        SeatConfirmedPayload payload = new SeatConfirmedPayload(
                orderId,
                orderId,
                paymentId,
                concertEventId,
                List.of(seatId),
                Instant.now()
        );
        sendEnvelope(Topics.INVENTORY_EVENTS, orderId.toString(),
                createEnvelope(kafkaEventId, EventTypes.INVENTORY_SEAT_CONFIRMED, UUID.randomUUID().toString(), payload));
        return kafkaEventId;
    }

    private UUID publishSeatConfirmationRejected(UUID orderId, UUID paymentId) throws Exception {
        UUID kafkaEventId = UUID.randomUUID();
        SeatConfirmationRejectedPayload payload = new SeatConfirmationRejectedPayload(
                orderId,
                orderId,
                paymentId,
                UUID.randomUUID(),
                UUID.randomUUID(),
                "RESERVATION_ALREADY_EXPIRED",
                Instant.now()
        );
        sendEnvelope(Topics.INVENTORY_EVENTS, orderId.toString(),
                createEnvelope(kafkaEventId, EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED, UUID.randomUUID().toString(), payload));
        return kafkaEventId;
    }

    private UUID publishPaymentSucceeded(UUID orderId, UUID userId, UUID paymentId) throws Exception {
        return publishPaymentSucceeded(orderId, userId, paymentId, Instant.now());
    }

    private UUID publishPaymentSucceeded(UUID orderId, UUID userId, UUID paymentId, Instant paidAt) throws Exception {
        UUID kafkaEventId = UUID.randomUUID();
        PaymentSucceededPayload payload = new PaymentSucceededPayload(
                paymentId,
                orderId,
                orderId,
                userId,
                15000L,
                "USD",
                "psp_ref_" + paymentId,
                paidAt
        );
        sendEnvelope(Topics.PAYMENT_EVENTS, orderId.toString(),
                createEnvelope(kafkaEventId, EventTypes.PAYMENT_SUCCEEDED, UUID.randomUUID().toString(), payload));
        return kafkaEventId;
    }

    private UUID publishPaymentFailed(UUID orderId, UUID userId, UUID paymentId) throws Exception {
        UUID kafkaEventId = UUID.randomUUID();
        PaymentFailedPayload payload = new PaymentFailedPayload(
                paymentId,
                orderId,
                orderId,
                userId,
                15000L,
                "USD",
                "CARD_DECLINED",
                "Insufficient funds",
                Instant.now()
        );
        sendEnvelope(Topics.PAYMENT_EVENTS, orderId.toString(),
                createEnvelope(kafkaEventId, EventTypes.PAYMENT_FAILED, UUID.randomUUID().toString(), payload));
        return kafkaEventId;
    }

    private UUID publishPaymentRefunded(UUID orderId, UUID userId, UUID paymentId) throws Exception {
        UUID kafkaEventId = UUID.randomUUID();
        UUID refundId = UUID.randomUUID();
        PaymentRefundedPayload payload = new PaymentRefundedPayload(
                refundId,
                paymentId,
                orderId,
                orderId,
                userId,
                15000L,
                "USD",
                "LATE_PAYMENT_EXPIRED_HOLD",
                Instant.now()
        );
        sendEnvelope(Topics.PAYMENT_EVENTS, orderId.toString(),
                createEnvelope(kafkaEventId, EventTypes.PAYMENT_REFUNDED, UUID.randomUUID().toString(), payload));
        return kafkaEventId;
    }

    private static <T> EventEnvelope<T> createEnvelope(UUID eventId, String eventType, String correlationId, T payload) {
        return new EventEnvelope<>(eventId, eventType, Instant.now().truncatedTo(ChronoUnit.MICROS), correlationId, 1, payload);
    }

    private void sendEnvelope(String topic, String key, EventEnvelope<?> envelope) throws Exception {
        String json = objectMapper.writeValueAsString(envelope);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json);
        record.headers().add(CorrelationConstants.KAFKA_HEADER,
                envelope.correlationId().toString().getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
    }

    private org.springframework.test.web.servlet.ResultActions performCheckout(
            UUID orderId,
            UUID userId,
            UUID idempotencyKey
    ) throws Exception {
        CheckoutRequest request = new CheckoutRequest("pm_tok_visa_success");
        return mockMvc.perform(post("/api/v1/orders/{orderId}/checkout", orderId)
                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                .header(CorrelationConstants.HTTP_HEADER, UUID.randomUUID().toString())
                .header("Idempotency-Key", idempotencyKey.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)));
    }

    private boolean isEventProcessed(UUID eventId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = ?",
                Integer.class,
                eventId
        );
        return count != null && count > 0;
    }

    private List<Map<String, Object>> findOutboxRowsForAggregate(UUID orderId) {
        return jdbcTemplate.queryForList(
                """
                SELECT event_id, event_type, topic, correlation_id, schema_version, payload::text AS payload, published_at
                FROM outbox_events
                WHERE aggregate_id = ?
                ORDER BY created_at ASC, event_id ASC
                """,
                orderId
        );
    }

    private List<Map<String, Object>> findOutboxRowsByType(UUID orderId, String eventType) {
        return jdbcTemplate.queryForList(
                """
                SELECT event_id, event_type, topic, correlation_id, schema_version, payload::text AS payload, published_at
                FROM outbox_events
                WHERE aggregate_id = ? AND event_type = ?
                ORDER BY created_at ASC, event_id ASC
                """,
                orderId,
                eventType
        );
    }

    private int countUnpublishedOutboxRows(UUID orderId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NULL",
                Integer.class,
                orderId
        );
        return count != null ? count : 0;
    }

    private double sagaMetricCount(String eventType, String outcome) {
        Counter counter = meterRegistry.find(OrderSagaService.METRIC_SAGA_EVENTS_HANDLED)
                .tag("event_type", eventType)
                .tag("outcome", outcome)
                .counter();
        return counter != null ? counter.count() : 0.0;
    }

    private void assertOrderEventRecord(ConsumerRecord<String, String> record, UUID expectedOrderId, String expectedEventType) throws Exception {
        assertThat(record.key()).isEqualTo(expectedOrderId.toString());
        assertThat(headerValue(record, CorrelationConstants.KAFKA_HEADER)).isNotBlank();
        assertThat(headerValue(record, "event-type")).isEqualTo(expectedEventType);
        assertThat(headerValue(record, "event-id")).isNotBlank();
        assertThat(headerValue(record, "schema-version")).isEqualTo("1");

        JsonNode root = objectMapper.readTree(record.value());
        assertThat(root.get("eventType").asText()).isEqualTo(expectedEventType);
        assertThat(root.get("schemaVersion").asInt()).isEqualTo(1);
    }

    private static String headerValue(ConsumerRecord<String, String> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header != null && header.value() != null
                ? new String(header.value(), StandardCharsets.UTF_8)
                : null;
    }

    private ConsumerRecord<String, String> pollSingleRecordFromTopic(String topic, String expectedKey, Duration timeout) {
        List<ConsumerRecord<String, String>> records = pollRecordsForKey(topic, expectedKey, 1, timeout);
        return records.isEmpty() ? null : records.get(0);
    }

    private ConsumerRecord<String, String> pollSingleRecordFromTopics(List<String> topics, String expectedKey, Duration timeout) {
        List<ConsumerRecord<String, String>> records = pollRecordsForTopics(topics, expectedKey, 1, timeout);
        return records.isEmpty() ? null : records.get(0);
    }

    private List<ConsumerRecord<String, String>> pollRecordsForKey(String topic, String expectedKey, int expectedCount, Duration timeout) {
        return pollRecordsForTopics(List.of(topic), expectedKey, expectedCount, timeout);
    }

    private List<ConsumerRecord<String, String>> pollRecordsForTopics(List<String> topics, String expectedKey, int expectedCount, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-verifier-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(topics);
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline && matched.size() < expectedCount) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : polled) {
                    if (expectedKey.equals(record.key())) {
                        matched.add(record);
                    }
                }
            }
        }
        return matched;
    }

    private static void waitForCondition(BooleanSupplier condition, Duration timeout, String description) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20L);
        }
        assertThat(condition.getAsBoolean()).as(description).isTrue();
    }
}

