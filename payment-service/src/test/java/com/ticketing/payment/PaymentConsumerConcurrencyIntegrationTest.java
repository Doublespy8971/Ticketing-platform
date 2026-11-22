package com.ticketing.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.events.PaymentRequestedPayload;
import com.ticketing.common.events.RefundRequestedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import com.ticketing.payment.domain.PaymentRecord;
import com.ticketing.payment.domain.PaymentRepository;
import com.ticketing.payment.domain.PaymentStatus;
import com.ticketing.payment.service.provider.MockPaymentProvider;
import com.ticketing.payment.service.provider.PaymentProvider;
import com.ticketing.payment.service.provider.PaymentResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies parallel Kafka consumption with {@code ticketing.consumer.concurrency=4} on
 * {@code payment-service-order-cg}: multiple consumer threads process distinct order keys
 * concurrently with exactly-once payment and outbox persistence, while sequential events
 * sharing one order key remain strictly ordered on the same partition/thread.
 */
@SpringBootTest
@Testcontainers
class PaymentConsumerConcurrencyIntegrationTest {

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
                    .withDatabaseName("payment_concurrency_test_db")
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
        registry.add("ticketing.consumer.concurrency", () -> "4");
        registry.add("ticketing.outbox.enabled", () -> "false");
        registry.add("ticketing.payment.provider.mock.decline-rate", () -> "0.0");
        registry.add("ticketing.payment.provider.mock.min-latency-ms", () -> "15");
        registry.add("ticketing.payment.provider.mock.max-latency-ms", () -> "25");
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
            throw new IllegalStateException("Failed to create Kafka topics for concurrency test", ex);
        }
    }

    static final class RecordingPaymentProvider implements PaymentProvider {
        private final PaymentProvider delegate;
        final Set<String> chargeThreads = ConcurrentHashMap.newKeySet();
        final Map<UUID, List<String>> operationsByOrder = new ConcurrentHashMap<>();
        final Map<UUID, List<String>> threadsByOrder = new ConcurrentHashMap<>();

        RecordingPaymentProvider(PaymentProvider delegate) {
            this.delegate = delegate;
        }

        void reset() {
            chargeThreads.clear();
            operationsByOrder.clear();
            threadsByOrder.clear();
        }

        @Override
        public PaymentResult charge(UUID idempotencyKey, long amountCents, String currency, String paymentMethodToken) {
            String threadName = Thread.currentThread().getName();
            chargeThreads.add(threadName);
            operationsByOrder.computeIfAbsent(idempotencyKey, k -> new CopyOnWriteArrayList<>()).add("charge:start");
            threadsByOrder.computeIfAbsent(idempotencyKey, k -> new CopyOnWriteArrayList<>()).add(threadName);
            PaymentResult res = delegate.charge(idempotencyKey, amountCents, currency, paymentMethodToken);
            operationsByOrder.computeIfAbsent(idempotencyKey, k -> new CopyOnWriteArrayList<>()).add("charge:end");
            return res;
        }

        @Override
        public PaymentResult refund(UUID idempotencyKey, String providerReference, long amountCents, String currency) {
            String threadName = Thread.currentThread().getName();
            PaymentResult res = delegate.refund(idempotencyKey, providerReference, amountCents, currency);
            chargeThreads.add(threadName);
            return res;
        }

        @Override
        public PaymentResult refund(UUID idempotencyKey, String providerReference, long amountCents, String currency, String paymentMethodToken) {
            String threadName = Thread.currentThread().getName();
            PaymentResult res = delegate.refund(idempotencyKey, providerReference, amountCents, currency, paymentMethodToken);
            chargeThreads.add(threadName);
            return res;
        }
    }

    @TestConfiguration
    static class RecordingProviderConfig {
        @Bean
        @Primary
        RecordingPaymentProvider recordingPaymentProvider(MockPaymentProvider mockPaymentProvider) {
            return new RecordingPaymentProvider(mockPaymentProvider);
        }
    }

    @Autowired
    private RecordingPaymentProvider recordingProvider;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("TRUNCATE TABLE payments, outbox_events, processed_events");
        recordingProvider.reset();
    }

    @Test
    @DisplayName("Concurrency 4 processes 60 distinct orders across multiple threads and preserves per-key ordering")
    void concurrentConsumptionAcrossPartitionsWithPerKeyOrdering() throws Exception {
        int orderCount = 60;
        List<UUID> orderIds = new ArrayList<>(orderCount);
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now();

        for (int i = 0; i < orderCount; i++) {
            UUID orderId = UUID.randomUUID();
            orderIds.add(orderId);
            PaymentRequestedPayload payload = new PaymentRequestedPayload(
                    orderId,
                    orderId,
                    UUID.randomUUID(),
                    eventId,
                    15000L,
                    "USD",
                    "tok_success",
                    now,
                    now.plusSeconds(600)
            );
            sendOrderEvent(orderId, EventTypes.ORDER_PAYMENT_REQUESTED, payload);
        }
        kafkaTemplate.flush();

        // Wait until all 60 orders have been persisted
        awaitTrue(30_000, () -> {
            Integer count = jdbcTemplate.queryForObject("SELECT count(*) FROM payments", Integer.class);
            return count != null && count == orderCount;
        });

        // 1. Processed by more than one distinct consumer thread
        assertThat(recordingProvider.chargeThreads.size())
                .as("Expected concurrency=4 on 6-partition topic to utilize >1 distinct consumer thread, saw: %s",
                        recordingProvider.chargeThreads)
                .isGreaterThan(1);

        // 2. Every order has exactly 1 SUCCEEDED payment and 1 payment.succeeded outbox row
        for (UUID orderId : orderIds) {
            Integer payCount = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM payments WHERE order_id = ? AND status = 'SUCCEEDED'",
                    Integer.class, orderId);
            assertThat(payCount).isEqualTo(1);

            Integer outboxCount = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                    Integer.class, orderId, EventTypes.PAYMENT_SUCCEEDED);
            assertThat(outboxCount).isEqualTo(1);
        }

        // 3. Per-key ordering: produce two sequential events (charge then refund) for the SAME orderId key
        UUID orderedOrderId = UUID.randomUUID();
        UUID orderedUserId = UUID.randomUUID();
        PaymentRequestedPayload chargePayload = new PaymentRequestedPayload(
                orderedOrderId,
                orderedOrderId,
                orderedUserId,
                eventId,
                20000L,
                "USD",
                "tok_success",
                now,
                now.plusSeconds(600)
        );
        RefundRequestedPayload refundPayload = new RefundRequestedPayload(
                orderedOrderId,
                orderedOrderId,
                UUID.randomUUID(),
                orderedUserId,
                20000L,
                "USD",
                "LATE_PAYMENT_COMPENSATION",
                now
        );

        // Send sequentially with identical partition key (orderedOrderId) so Kafka places both on the exact same partition
        sendOrderEventSync(orderedOrderId, EventTypes.ORDER_PAYMENT_REQUESTED, chargePayload);
        sendOrderEventSync(orderedOrderId, EventTypes.ORDER_REFUND_REQUESTED, refundPayload);

        // Refund can only succeed if charge completed first; if out-of-order, refund would fail with NonRetryableEventException
        awaitTrue(20_000, () -> {
            var recOpt = paymentRepository.findByOrderId(orderedOrderId);
            return recOpt.isPresent() && recOpt.get().status() == PaymentStatus.REFUNDED;
        });

        PaymentRecord refunded = paymentRepository.findByOrderId(orderedOrderId).orElseThrow();
        assertThat(refunded.status()).isEqualTo(PaymentStatus.REFUNDED);
        assertThat(refunded.refundId()).isNotNull();

        List<String> outboxEventTypesInOrder = jdbcTemplate.queryForList(
                "SELECT event_type FROM outbox_events WHERE aggregate_id = ? ORDER BY created_at ASC, event_id ASC",
                String.class, orderedOrderId);
        assertThat(outboxEventTypesInOrder)
                .containsExactly(EventTypes.PAYMENT_SUCCEEDED, EventTypes.PAYMENT_REFUNDED);
    }

    private void sendOrderEvent(UUID orderId, String eventType, Object payload) throws Exception {
        String corrId = UUID.randomUUID().toString();
        EventEnvelope<Object> env = new EventEnvelope<>(UUID.randomUUID(), eventType, Instant.now(), corrId, 1, payload);
        String json = objectMapper.writeValueAsString(env);
        ProducerRecord<String, String> rec = new ProducerRecord<>(Topics.ORDER_EVENTS, orderId.toString(), json);
        rec.headers().add(CorrelationConstants.KAFKA_HEADER, corrId.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(rec);
    }

    private void sendOrderEventSync(UUID orderId, String eventType, Object payload) throws Exception {
        String corrId = UUID.randomUUID().toString();
        EventEnvelope<Object> env = new EventEnvelope<>(UUID.randomUUID(), eventType, Instant.now(), corrId, 1, payload);
        String json = objectMapper.writeValueAsString(env);
        ProducerRecord<String, String> rec = new ProducerRecord<>(Topics.ORDER_EVENTS, orderId.toString(), json);
        rec.headers().add(CorrelationConstants.KAFKA_HEADER, corrId.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(rec).get(10, TimeUnit.SECONDS);
    }

    private static void awaitTrue(long timeoutMs, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        assertThat(condition.getAsBoolean()).as("Condition was not met within %d ms", timeoutMs).isTrue();
    }
}
