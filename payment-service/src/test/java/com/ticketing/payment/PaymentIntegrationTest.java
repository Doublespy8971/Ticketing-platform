package com.ticketing.payment;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.events.PaymentFailedPayload;
import com.ticketing.common.events.PaymentRefundedPayload;
import com.ticketing.common.events.PaymentRequestedPayload;
import com.ticketing.common.events.PaymentSucceededPayload;
import com.ticketing.common.events.RefundRequestedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import com.ticketing.payment.domain.PaymentRecord;
import com.ticketing.payment.domain.PaymentRepository;
import com.ticketing.payment.domain.PaymentStatus;
import com.ticketing.payment.service.PaymentService;
import com.ticketing.payment.service.provider.MockPaymentProvider;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
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

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class PaymentIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
    }

    private static final String CONSUMER_GROUP = TicketingRetryTopicNamesProviderFactory.PAYMENT_SERVICE_ORDER_CG;

    private static final String ORDER_RETRY_0 = Topics.ORDER_EVENTS + "-" + CONSUMER_GROUP + "-retry-0";
    private static final String ORDER_RETRY_1 = Topics.ORDER_EVENTS + "-" + CONSUMER_GROUP + "-retry-1";
    private static final String ORDER_DLT = Topics.ORDER_EVENTS + "-" + CONSUMER_GROUP + "-dlt";

    private static final AtomicBoolean TOPICS_CREATED = new AtomicBoolean(false);

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4-alpine"))
                    .withDatabaseName("payment_test_db")
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
        registry.add("ticketing.payment.provider.mock.decline-rate", () -> "0.0");
        registry.add("ticketing.payment.provider.mock.min-latency-ms", () -> "0");
        registry.add("ticketing.payment.provider.mock.max-latency-ms", () -> "0");
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
                    new NewTopic(Topics.ORDER_EVENTS, 6, (short) 1),
                    new NewTopic(ORDER_RETRY_0, 6, (short) 1),
                    new NewTopic(ORDER_RETRY_1, 6, (short) 1),
                    new NewTopic(ORDER_DLT, 3, (short) 1),
                    new NewTopic(Topics.PAYMENT_EVENTS, 6, (short) 1)
            );
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to create Kafka topics for test", ex);
        }
    }

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private MockPaymentProvider mockPaymentProvider;

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        mockPaymentProvider.reset();
        paymentService.setInsideTransactionHook(null);

        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
        logAppender = new ListAppender<>();
        logAppender.start();
        rootLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        if (logAppender != null) {
            logAppender.stop();
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
            rootLogger.detachAppender(logAppender);
        }
    }

    // =========================================================================
    // 1. Successful Charge
    // =========================================================================
    @Test
    @DisplayName("1. Successful charge writes one SUCCEEDED payment and exactly one payment.succeeded outbox row")
    void successfulCharge() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        publishOrderPaymentRequested(orderId, userId, "pm_tok_visa_success", 25000);

        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        PaymentRecord payment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(payment.amountCents()).isEqualTo(25000L);
        assertThat(payment.currency()).isEqualTo("USD");
        assertThat(payment.providerReference()).isNotBlank();
        assertThat(payment.paidAt()).isNotNull();
        assertThat(payment.failedAt()).isNull();

        List<Map<String, Object>> outboxRows = jdbcTemplate.queryForList(
                "SELECT * FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                orderId, EventTypes.PAYMENT_SUCCEEDED
        );
        assertThat(outboxRows).hasSize(1);
        String payloadJson = outboxRows.get(0).get("payload").toString();
        PaymentSucceededPayload outboxPayload = objectMapper.readValue(payloadJson, PaymentSucceededPayload.class);
        assertThat(outboxPayload.orderId()).isEqualTo(orderId);
        assertThat(outboxPayload.paymentId()).isEqualTo(payment.paymentId());
        assertThat(outboxPayload.amountCents()).isEqualTo(25000L);
        assertThat(outboxPayload.providerReference()).isEqualTo(payment.providerReference());
        assertThat(outboxPayload.paidAt()).isNotNull();
    }

    // =========================================================================
    // 2. tok_decline gives FAILED payment and payment.failed
    // =========================================================================
    @Test
    @DisplayName("2. tok_decline gives a FAILED payment and one payment.failed row with failureCode and failureMessage")
    void declinedPayment() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        publishOrderPaymentRequested(orderId, userId, MockPaymentProvider.MAGIC_TOKEN_DECLINE, 15000);

        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        PaymentRecord payment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.failureCode()).isEqualTo(MockPaymentProvider.FAILURE_CODE_DECLINED);
        assertThat(payment.failureMessage()).isEqualTo(MockPaymentProvider.FAILURE_MESSAGE_DECLINED);
        assertThat(payment.failedAt()).isNotNull();
        assertThat(payment.paidAt()).isNull();

        List<Map<String, Object>> outboxRows = jdbcTemplate.queryForList(
                "SELECT * FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                orderId, EventTypes.PAYMENT_FAILED
        );
        assertThat(outboxRows).hasSize(1);
        PaymentFailedPayload outboxPayload = objectMapper.readValue(
                outboxRows.get(0).get("payload").toString(), PaymentFailedPayload.class
        );
        assertThat(outboxPayload.orderId()).isEqualTo(orderId);
        assertThat(outboxPayload.failureCode()).isEqualTo(MockPaymentProvider.FAILURE_CODE_DECLINED);
        assertThat(outboxPayload.failureMessage()).isEqualTo(MockPaymentProvider.FAILURE_MESSAGE_DECLINED);
    }

    // =========================================================================
    // 3. Same event ID delivered twice gives 1 payment, 1 outbox row, 1 charge
    // =========================================================================
    @Test
    @DisplayName("3. Same event ID delivered twice gives one payment, one outbox row and one provider charge")
    void sameEventIdTwice() {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        String rawJson = buildPaymentRequestedEnvelopeJson(eventId, orderId, userId, "pm_tok_visa", 12000);

        // First delivery
        sendToKafka(Topics.ORDER_EVENTS, orderId.toString(), rawJson);
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        // Second delivery of exact same message
        sendToKafka(Topics.ORDER_EVENTS, orderId.toString(), rawJson);

        // Allow some time for Kafka consumer to process duplicate
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        Integer paymentCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM payments WHERE order_id = ?", Integer.class, orderId
        );
        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ?", Integer.class, orderId
        );
        assertThat(paymentCount).isEqualTo(1);
        assertThat(outboxCount).isEqualTo(1);
        assertThat(mockPaymentProvider.getEffectiveChargeCount()).isEqualTo(1);
    }

    // =========================================================================
    // 4. Two different event IDs for the same order give 1 payment and 1 outbox row
    // =========================================================================
    @Test
    @DisplayName("4. Two different event IDs for the same order give one payment and one outbox row")
    void twoDifferentEventIdsSameOrder() {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        String event1 = buildPaymentRequestedEnvelopeJson(UUID.randomUUID(), orderId, userId, "pm_tok_visa", 10000);
        String event2 = buildPaymentRequestedEnvelopeJson(UUID.randomUUID(), orderId, userId, "pm_tok_visa", 10000);

        sendToKafka(Topics.ORDER_EVENTS, orderId.toString(), event1);
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        sendToKafka(Topics.ORDER_EVENTS, orderId.toString(), event2);

        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        Integer paymentCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM payments WHERE order_id = ?", Integer.class, orderId
        );
        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ?", Integer.class, orderId
        );
        assertThat(paymentCount).isEqualTo(1);
        assertThat(outboxCount).isEqualTo(1);
        assertThat(mockPaymentProvider.getEffectiveChargeCount()).isEqualTo(1);
    }

    // =========================================================================
    // 5. 20 concurrent order.payment.requested events for same order
    // =========================================================================
    @Test
    @DisplayName("5. 20 concurrent order.payment.requested events for the same order give one payment row and one outbox row")
    void concurrentPaymentRequests() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        int threads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch readyLatch = new CountDownLatch(threads);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            UUID eventId = UUID.randomUUID();
            String rawJson = buildPaymentRequestedEnvelopeJson(eventId, orderId, userId, "pm_tok_visa", 18000);
            futures.add(executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    paymentService.processOrderEvent(rawJson);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        readyLatch.await(5, TimeUnit.SECONDS);
        startLatch.countDown();

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();

        Integer paymentCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM payments WHERE order_id = ?", Integer.class, orderId
        );
        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ?", Integer.class, orderId
        );
        assertThat(paymentCount).isEqualTo(1);
        assertThat(outboxCount).isEqualTo(1);
    }

    // =========================================================================
    // 6. tok_error retry topic flow
    // =========================================================================
    @Test
    @DisplayName("6. tok_error makes the first delivery fail, record appears on first retry topic, retry succeeds")
    void tokErrorRetryFlow() {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        publishOrderPaymentRequested(orderId, userId, MockPaymentProvider.MAGIC_TOKEN_ERROR, 20000);

        // Await payment reaching SUCCEEDED after retry
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(15)))
                .isTrue();

        PaymentRecord payment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.SUCCEEDED);

        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                Integer.class, orderId, EventTypes.PAYMENT_SUCCEEDED
        );
        assertThat(outboxCount).isEqualTo(1);
        assertThat(mockPaymentProvider.getEffectiveChargeCount()).isEqualTo(1);
    }

    // =========================================================================
    // 7. Simulated failure inside processor transaction
    // =========================================================================
    @Test
    @DisplayName("7. Simulated failure inside processor transaction after successful provider call redelivers and succeeds")
    void failureInsideTransactionRedelivers() {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        AtomicBoolean failedOnce = new AtomicBoolean(false);
        paymentService.setInsideTransactionHook(() -> {
            if (failedOnce.compareAndSet(false, true)) {
                throw new RuntimeException("Simulated transient failure inside processor transaction");
            }
        });

        publishOrderPaymentRequested(orderId, userId, "pm_tok_visa", 30000);

        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(15)))
                .isTrue();

        PaymentRecord payment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(mockPaymentProvider.getEffectiveChargeCount()).isEqualTo(1);

        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                Integer.class, orderId, EventTypes.PAYMENT_SUCCEEDED
        );
        assertThat(outboxCount).isEqualTo(1);
    }

    // =========================================================================
    // 8. Refund request after successful payment gives REFUNDED, outbox, 1 refund
    // =========================================================================
    @Test
    @DisplayName("8. Refund request after successful payment gives REFUNDED, one payment.refunded row and one effective refund")
    void refundSuccessfulPayment() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        publishOrderPaymentRequested(orderId, userId, "pm_tok_visa", 15000);
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        PaymentRecord payment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.SUCCEEDED);

        publishOrderRefundRequested(orderId, payment.paymentId(), userId, 15000, "LATE_PAYMENT_EXPIRED_HOLD");

        assertThat(awaitCondition(() -> {
            Optional<PaymentRecord> p = paymentRepository.findByOrderId(orderId);
            return p.isPresent() && p.get().status() == PaymentStatus.REFUNDED;
        }, Duration.ofSeconds(10))).isTrue();

        PaymentRecord refundedPayment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertThat(refundedPayment.refundId()).isNotNull();
        assertThat(refundedPayment.refundReason()).isEqualTo("LATE_PAYMENT_EXPIRED_HOLD");
        assertThat(refundedPayment.refundedAt()).isNotNull();

        List<Map<String, Object>> outboxRows = jdbcTemplate.queryForList(
                "SELECT * FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                orderId, EventTypes.PAYMENT_REFUNDED
        );
        assertThat(outboxRows).hasSize(1);
        PaymentRefundedPayload payload = objectMapper.readValue(
                outboxRows.get(0).get("payload").toString(), PaymentRefundedPayload.class
        );
        assertThat(payload.refundId()).isEqualTo(refundedPayment.refundId());
        assertThat(payload.paymentId()).isEqualTo(payment.paymentId());
        assertThat(payload.orderId()).isEqualTo(orderId);
        assertThat(payload.amountCents()).isEqualTo(15000L);
        assertThat(payload.reason()).isEqualTo("LATE_PAYMENT_EXPIRED_HOLD");
        assertThat(payload.refundedAt()).isNotNull();

        assertThat(mockPaymentProvider.getEffectiveRefundCount()).isEqualTo(1);
    }

    // =========================================================================
    // 9. Second refund request with different eventId changes nothing
    // =========================================================================
    @Test
    @DisplayName("9. Second refund request with a different event id changes nothing")
    void secondRefundRequestDifferentEventId() {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        publishOrderPaymentRequested(orderId, userId, "pm_tok_visa", 15000);
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        PaymentRecord payment = paymentRepository.findByOrderId(orderId).orElseThrow();

        publishOrderRefundRequested(orderId, payment.paymentId(), userId, 15000, "LATE_PAYMENT_EXPIRED_HOLD");
        assertThat(awaitCondition(() -> {
            Optional<PaymentRecord> p = paymentRepository.findByOrderId(orderId);
            return p.isPresent() && p.get().status() == PaymentStatus.REFUNDED;
        }, Duration.ofSeconds(10))).isTrue();

        // Second refund with a fresh eventId
        publishOrderRefundRequested(orderId, payment.paymentId(), userId, 15000, "LATE_PAYMENT_EXPIRED_HOLD");

        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        Integer refundOutboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                Integer.class, orderId, EventTypes.PAYMENT_REFUNDED
        );
        assertThat(refundOutboxCount).isEqualTo(1);
        assertThat(mockPaymentProvider.getEffectiveRefundCount()).isEqualTo(1);
    }

    // =========================================================================
    // 10. Refund for FAILED payment is a no-op
    // =========================================================================
    @Test
    @DisplayName("10. A refund for a FAILED payment is a no-op")
    void refundForFailedPaymentIsNoOp() {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        publishOrderPaymentRequested(orderId, userId, MockPaymentProvider.MAGIC_TOKEN_DECLINE, 15000);
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        PaymentRecord payment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.FAILED);

        publishOrderRefundRequested(orderId, payment.paymentId(), userId, 15000, "LATE_PAYMENT_EXPIRED_HOLD");

        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        PaymentRecord current = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertThat(current.status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(current.refundId()).isNull();

        Integer refundOutboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                Integer.class, orderId, EventTypes.PAYMENT_REFUNDED
        );
        assertThat(refundOutboxCount).isEqualTo(0);
        assertThat(mockPaymentProvider.getEffectiveRefundCount()).isEqualTo(0);
    }

    // =========================================================================
    // 11. Refund for unknown order or wrong amount lands in DLT without retrying
    // =========================================================================
    @Test
    @DisplayName("11. Refund for unknown order or with a wrong amount lands in DLT without touching retry topics")
    void refundUnknownOrderOrWrongAmountLandsInDlt() {
        long initialRetry0 = getTopicTotalEndOffset(ORDER_RETRY_0);
        long initialRetry1 = getTopicTotalEndOffset(ORDER_RETRY_1);
        long initialDlt = getTopicTotalEndOffset(ORDER_DLT);

        // Case A: Unknown order
        UUID unknownOrderId = UUID.randomUUID();
        publishOrderRefundRequested(unknownOrderId, UUID.randomUUID(), UUID.randomUUID(), 15000, "LATE_PAYMENT_EXPIRED_HOLD");

        assertThat(awaitCondition(() -> getTopicTotalEndOffset(ORDER_DLT) >= initialDlt + 1, Duration.ofSeconds(10)))
                .isTrue();

        assertThat(getTopicTotalEndOffset(ORDER_RETRY_0)).isEqualTo(initialRetry0);
        assertThat(getTopicTotalEndOffset(ORDER_RETRY_1)).isEqualTo(initialRetry1);

        // Case B: Wrong amount
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        publishOrderPaymentRequested(orderId, userId, "pm_tok_visa", 15000);
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();
        PaymentRecord payment = paymentRepository.findByOrderId(orderId).orElseThrow();

        long dltBeforeWrongAmount = getTopicTotalEndOffset(ORDER_DLT);

        // Request refund with wrong amount (9999 instead of 15000)
        publishOrderRefundRequested(orderId, payment.paymentId(), userId, 9999, "LATE_PAYMENT_EXPIRED_HOLD");

        assertThat(awaitCondition(() -> getTopicTotalEndOffset(ORDER_DLT) >= dltBeforeWrongAmount + 1, Duration.ofSeconds(10)))
                .isTrue();

        assertThat(getTopicTotalEndOffset(ORDER_RETRY_0)).isEqualTo(initialRetry0);
        assertThat(getTopicTotalEndOffset(ORDER_RETRY_1)).isEqualTo(initialRetry1);
    }

    // =========================================================================
    // 12. Unrelated event type is ignored and next record is processed
    // =========================================================================
    @Test
    @DisplayName("12. An unrelated event type is ignored and the next record is processed")
    void unrelatedEventIgnoredAndNextProcessed() {
        UUID unrelatedOrderId = UUID.randomUUID();
        String unrelatedEvent = String.format(
                "{\"eventId\":\"%s\",\"eventType\":\"order.confirmed\",\"occurredAt\":\"%s\",\"correlationId\":\"%s\",\"schemaVersion\":1,\"payload\":{\"orderId\":\"%s\"}}",
                UUID.randomUUID(), Instant.now(), UUID.randomUUID(), unrelatedOrderId
        );

        sendToKafka(Topics.ORDER_EVENTS, unrelatedOrderId.toString(), unrelatedEvent);

        // Process a valid payment right after
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        publishOrderPaymentRequested(orderId, userId, "pm_tok_visa", 16000);

        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        // Unrelated order has no payment row
        assertThat(paymentRepository.findByOrderId(unrelatedOrderId)).isEmpty();
    }

    // =========================================================================
    // 13. Outbox poller publishes to payment.events with key and headers in order
    // =========================================================================
    @Test
    @DisplayName("13. Outbox poller publishes payment.succeeded, payment.failed and payment.refunded to payment.events in order")
    void outboxPollerPublishesToKafkaInOrder() {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        publishOrderPaymentRequested(orderId, userId, "pm_tok_visa", 15000);
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        PaymentRecord payment = paymentRepository.findByOrderId(orderId).orElseThrow();

        publishOrderRefundRequested(orderId, payment.paymentId(), userId, 15000, "LATE_PAYMENT_EXPIRED_HOLD");
        assertThat(awaitCondition(() -> {
            Optional<PaymentRecord> p = paymentRepository.findByOrderId(orderId);
            return p.isPresent() && p.get().status() == PaymentStatus.REFUNDED;
        }, Duration.ofSeconds(10))).isTrue();

        // Also create a separate order with payment.failed
        UUID failedOrderId = UUID.randomUUID();
        publishOrderPaymentRequested(failedOrderId, userId, MockPaymentProvider.MAGIC_TOKEN_DECLINE, 15000);
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(failedOrderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        // Poll payment.events and verify records
        List<ConsumerRecord<String, String>> received = pollRecordsFromTopic(Topics.PAYMENT_EVENTS, Duration.ofSeconds(10));

        List<ConsumerRecord<String, String>> orderRecords = received.stream()
                .filter(r -> orderId.toString().equals(r.key()))
                .toList();
        assertThat(orderRecords).hasSize(2);
        assertThat(getHeader(orderRecords.get(0), "event-type")).isEqualTo(EventTypes.PAYMENT_SUCCEEDED);
        assertThat(getHeader(orderRecords.get(1), "event-type")).isEqualTo(EventTypes.PAYMENT_REFUNDED);
        assertThat(getHeader(orderRecords.get(0), CorrelationConstants.KAFKA_HEADER)).isNotNull();

        List<ConsumerRecord<String, String>> failedRecords = received.stream()
                .filter(r -> failedOrderId.toString().equals(r.key()))
                .toList();
        assertThat(failedRecords).hasSize(1);
        assertThat(getHeader(failedRecords.get(0), "event-type")).isEqualTo(EventTypes.PAYMENT_FAILED);
    }

    // =========================================================================
    // 14. Application KafkaTemplate has acks=all and idempotence enabled
    // =========================================================================
    @Test
    @DisplayName("14. The application KafkaTemplate still has acks all and idempotence enabled")
    void applicationKafkaTemplateHasIdempotentSettings() {
        Map<String, Object> configs = kafkaTemplate.getProducerFactory().getConfigurationProperties();
        assertThat(configs.get(ProducerConfig.ACKS_CONFIG)).asString().isEqualTo("all");
        assertThat(configs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).asString().isEqualTo("true");
    }

    // =========================================================================
    // 15. Payment token never appears in captured logs
    // =========================================================================
    @Test
    @DisplayName("15. The payment method token never appears in captured log output")
    void paymentMethodTokenNeverLogged() {
        String secretToken = "pm_tok_super_secret_token_123456789";
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        publishOrderPaymentRequested(orderId, userId, secretToken, 20000);
        assertThat(awaitCondition(() -> paymentRepository.findByOrderId(orderId).isPresent(), Duration.ofSeconds(10)))
                .isTrue();

        List<ILoggingEvent> logs = logAppender.list;
        assertThat(logs).isNotEmpty();

        for (ILoggingEvent logEvent : logs) {
            assertThat(logEvent.getFormattedMessage()).doesNotContain(secretToken);
        }
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    private void publishOrderPaymentRequested(UUID orderId, UUID userId, String token, long amountCents) {
        String json = buildPaymentRequestedEnvelopeJson(UUID.randomUUID(), orderId, userId, token, amountCents);
        sendToKafka(Topics.ORDER_EVENTS, orderId.toString(), json);
    }

    private void publishOrderRefundRequested(UUID orderId, UUID paymentId, UUID userId, long amountCents, String reason) {
        String json = buildRefundRequestedEnvelopeJson(UUID.randomUUID(), orderId, paymentId, userId, amountCents, reason);
        sendToKafka(Topics.ORDER_EVENTS, orderId.toString(), json);
    }

    private String buildPaymentRequestedEnvelopeJson(UUID eventId, UUID orderId, UUID userId, String token, long amountCents) {
        PaymentRequestedPayload payload = new PaymentRequestedPayload(
                orderId,
                orderId,
                userId,
                UUID.randomUUID(),
                amountCents,
                "USD",
                token,
                Instant.now(),
                Instant.now().plusSeconds(600)
        );
        EventEnvelope<PaymentRequestedPayload> envelope = new EventEnvelope<>(
                eventId,
                EventTypes.ORDER_PAYMENT_REQUESTED,
                Instant.now(),
                UUID.randomUUID().toString(),
                1,
                payload
        );
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String buildRefundRequestedEnvelopeJson(UUID eventId, UUID orderId, UUID paymentId, UUID userId, long amountCents, String reason) {
        RefundRequestedPayload payload = new RefundRequestedPayload(
                orderId,
                orderId,
                paymentId,
                userId,
                amountCents,
                "USD",
                reason,
                Instant.now()
        );
        EventEnvelope<RefundRequestedPayload> envelope = new EventEnvelope<>(
                eventId,
                EventTypes.ORDER_REFUND_REQUESTED,
                Instant.now(),
                UUID.randomUUID().toString(),
                1,
                payload
        );
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void sendToKafka(String topic, String key, String json) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json);
        record.headers().add(CorrelationConstants.KAFKA_HEADER, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        try {
            kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long getTopicTotalEndOffset(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "offset-checker-" + UUID.randomUUID());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            return endOffsets.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    private List<ConsumerRecord<String, String>> pollRecordsFromTopic(String topic, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-poll-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> r : records) {
                    result.add(r);
                }
                if (result.size() >= 3) {
                    break;
                }
            }
        }
        return result;
    }

    private static String getHeader(ConsumerRecord<String, String> record, String headerName) {
        if (record.headers() == null) return null;
        var header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    private boolean awaitCondition(BooleanSupplier supplier, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (supplier.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return supplier.getAsBoolean();
    }
}

