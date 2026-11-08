package com.ticketing.inventory.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.events.PaymentFailedPayload;
import com.ticketing.common.events.PaymentSucceededPayload;
import com.ticketing.common.events.SeatConfirmationRejectedPayload;
import com.ticketing.common.events.SeatConfirmedPayload;
import com.ticketing.common.events.SeatReleasedPayload;
import com.ticketing.common.events.SeatReservedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import com.ticketing.inventory.api.dto.CreateReservationRequest;
import com.ticketing.inventory.api.dto.ReservationResponse;
import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.ReservationItemRecord;
import com.ticketing.inventory.domain.ReservationRecord;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.redis.RedisKeys;
import com.ticketing.inventory.redis.RedisSeatGate;
import com.ticketing.inventory.redis.RedisUnavailableException;
import com.ticketing.inventory.service.ExpirySweeper;
import com.ticketing.inventory.service.PaymentEventService;
import com.ticketing.outbox.OutboxPoller;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class PaymentEventKafkaIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
    }

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String PAYMENT_RETRY_0_TOPIC = TicketingRetryTopicNamesProviderFactory.retryTopicName(
            Topics.PAYMENT_EVENTS, PaymentEventService.CONSUMER_GROUP, 0);
    private static final String PAYMENT_RETRY_1_TOPIC = TicketingRetryTopicNamesProviderFactory.retryTopicName(
            Topics.PAYMENT_EVENTS, PaymentEventService.CONSUMER_GROUP, 1);
    private static final String PAYMENT_DLT_TOPIC = TicketingRetryTopicNamesProviderFactory.dltTopicName(
            Topics.PAYMENT_EVENTS, PaymentEventService.CONSUMER_GROUP);

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4-alpine"))
                    .withDatabaseName("inventory_kafka_test_db")
                    .withUsername("ticketing")
                    .withPassword("ticketing_secret");

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.1-alpine"))
                    .withExposedPorts(6379);

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private static KafkaProducer<String, String> testProducer;
    private static KafkaListenerEndpointRegistry staticListenerRegistry;
    private static OutboxPoller staticOutboxPoller;

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        provisionTopics(KAFKA.getBootstrapServers());
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "20");
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("ticketing.outbox.enabled", () -> "true");
        registry.add("ticketing.outbox.poll-interval-ms", () -> "100");
        registry.add("ticketing.sweeper.enabled", () -> "false");
    }

    private static void provisionTopics(String bootstrapServers) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            List<NewTopic> topics = List.of(
                    new NewTopic(Topics.INVENTORY_EVENTS, 3, (short) 1),
                    new NewTopic(Topics.ORDER_EVENTS, 3, (short) 1),
                    new NewTopic(Topics.PAYMENT_EVENTS, 3, (short) 1),
                    new NewTopic(PAYMENT_RETRY_0_TOPIC, 3, (short) 1),
                    new NewTopic(PAYMENT_RETRY_1_TOPIC, 3, (short) 1),
                    new NewTopic(PAYMENT_DLT_TOPIC, 3, (short) 1)
            );
            admin.createTopics(topics).all().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to provision Kafka topics for integration test", e);
        }
    }

    @BeforeAll
    static void initProducer() {
        testProducer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG, "all"
        ));
    }

    @AfterAll
    static void closeProducer() {
        if (staticOutboxPoller != null) {
            staticOutboxPoller.destroy();
        }
        if (staticListenerRegistry != null && staticListenerRegistry.isRunning()) {
            staticListenerRegistry.stop();
        }
        if (testProducer != null) {
            testProducer.close(Duration.ofSeconds(5));
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private ExpirySweeper expirySweeper;

    @Autowired
    private PaymentEventService paymentEventService;

    @SpyBean
    private RedisSeatGate redisSeatGate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private KafkaListenerEndpointRegistry listenerEndpointRegistry;

    @Autowired
    private OutboxPoller outboxPoller;

    @BeforeEach
    void setUp() throws Exception {
        staticListenerRegistry = this.listenerEndpointRegistry;
        staticOutboxPoller = this.outboxPoller;
        Mockito.reset(redisSeatGate);
        paymentEventService.setRedisConfirmRetryPauseMs(50L);
        paymentEventService.setAfterReservationLockHook(null);
        expirySweeper.setAfterReservationLockHook(null);
        reservationRepository.setTransientLockFailureCount(0);
        MDC.remove(CorrelationConstants.MDC_KEY);

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        jdbcTemplate.execute(
                "TRUNCATE TABLE idempotency_keys, processed_events, outbox_events, reservation_items, reservations, seats, events CASCADE");

        stringRedisTemplate.execute((RedisCallback<Void>) (RedisConnection connection) -> {
            connection.serverCommands().flushDb();
            return null;
        });

        awaitListenerAssignment();
    }

    @AfterEach
    void tearDown() {
        Mockito.reset(redisSeatGate);
        paymentEventService.setRedisConfirmRetryPauseMs(50L);
        paymentEventService.setAfterReservationLockHook(null);
        expirySweeper.setAfterReservationLockHook(null);
        reservationRepository.setTransientLockFailureCount(0);
        MDC.remove(CorrelationConstants.MDC_KEY);
    }

    // =========================================================================
    // 1. Hold creation via HTTP -> outbox poller publishes inventory.seat.reserved
    // =========================================================================

    @Test
    void holdCreationViaHttp_outboxPollerPublishesSeatReservedWithKeyAndCorrelationHeaderAndSetsPublishedAt() throws Exception {
        UUID eventId = insertOnSaleEvent("Taylor Swift Eras Tour");
        UUID seat1 = insertSeat(eventId, "A", "1", "1", 15000L, "AVAILABLE");
        UUID seat2 = insertSeat(eventId, "A", "1", "2", 15000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();

        Map<TopicPartition, Long> startOffsets = currentEndOffsets(Topics.INVENTORY_EVENTS);

        ReservationResponse response = createHoldViaHttp(eventId, userId, List.of(seat1, seat2), correlationId);
        UUID reservationId = response.reservationId();

        ConsumerRecord<String, String> record = awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                startOffsets,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_RESERVED),
                TIMEOUT
        );

        assertThat(record.key()).isEqualTo(reservationId.toString());
        assertThat(headerValue(record, CorrelationConstants.KAFKA_HEADER)).isEqualTo(correlationId);

        JsonNode envelope = objectMapper.readTree(record.value());
        assertThat(envelope.get("eventType").asText()).isEqualTo(EventTypes.INVENTORY_SEAT_RESERVED);
        assertThat(envelope.get("correlationId").asText()).isEqualTo(correlationId);
        assertThat(envelope.get("schemaVersion").asInt()).isEqualTo(1);

        SeatReservedPayload payload = objectMapper.treeToValue(envelope.get("payload"), SeatReservedPayload.class);
        assertThat(payload.reservationId()).isEqualTo(reservationId);
        assertThat(payload.orderId()).isEqualTo(reservationId);
        assertThat(payload.userId()).isEqualTo(userId);
        assertThat(payload.eventId()).isEqualTo(eventId);
        assertThat(payload.totalAmountCents()).isEqualTo(30000L);
        assertThat(payload.seats()).hasSize(2);

        awaitCondition(TIMEOUT, () -> {
            Integer publishedCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ? AND published_at IS NOT NULL",
                    Integer.class,
                    reservationId,
                    EventTypes.INVENTORY_SEAT_RESERVED
            );
            return publishedCount != null && publishedCount == 1;
        });
    }

    // =========================================================================
    // 2. payment.succeeded for HELD reservation + 3. duplicate eventId + 4. new eventId when CONFIRMED
    // =========================================================================

    @Test
    void paymentSucceededForHeldReservation_confirmsDbAndRedisAndPublishesSeatConfirmed_andHandlesDuplicatesAndAlreadyConfirmed() throws Exception {
        UUID eventId = insertOnSaleEvent("Coldplay World Tour");
        UUID seat1 = insertSeat(eventId, "B", "2", "10", 12000L, "AVAILABLE");
        UUID seat2 = insertSeat(eventId, "B", "2", "11", 12000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1, seat2), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1)))
                .isEqualTo(reservationId.toString());
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat2)))
                .isEqualTo(reservationId.toString());

        Map<TopicPartition, Long> inventoryOffsets = currentEndOffsets(Topics.INVENTORY_EVENTS);

        UUID paymentEventId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        String incomingCorrelationId = UUID.randomUUID().toString();

        publishPaymentSucceeded(paymentEventId, paymentId, reservationId, userId, 24000L, incomingCorrelationId);

        ConsumerRecord<String, String> confirmedRecord = awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                inventoryOffsets,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_CONFIRMED),
                TIMEOUT
        );

        assertThat(headerValue(confirmedRecord, CorrelationConstants.KAFKA_HEADER)).isEqualTo(incomingCorrelationId);
        JsonNode envelope = objectMapper.readTree(confirmedRecord.value());
        assertThat(envelope.get("eventType").asText()).isEqualTo(EventTypes.INVENTORY_SEAT_CONFIRMED);
        assertThat(envelope.get("correlationId").asText()).isEqualTo(incomingCorrelationId);

        SeatConfirmedPayload confirmedPayload = objectMapper.treeToValue(envelope.get("payload"), SeatConfirmedPayload.class);
        assertThat(confirmedPayload.reservationId()).isEqualTo(reservationId);
        assertThat(confirmedPayload.orderId()).isEqualTo(reservationId);
        assertThat(confirmedPayload.paymentId()).isEqualTo(paymentId);
        assertThat(confirmedPayload.eventId()).isEqualTo(eventId);
        assertThat(confirmedPayload.seatIds()).containsExactlyInAnyOrder(seat1, seat2);
        assertThat(confirmedPayload.confirmedAt()).isNotNull();

        // Verify PostgreSQL state: reservation CONFIRMED, items CONFIRMED, seats SOLD with version = 2
        ReservationRecord reservation = reservationRepository.findById(reservationId).orElseThrow();
        assertThat(reservation.status()).isEqualTo("CONFIRMED");
        assertThat(reservation.confirmedAt()).isNotNull();

        List<ReservationItemRecord> items = reservationRepository.findItemsByReservationId(reservationId);
        assertThat(items).hasSize(2).allSatisfy(item -> assertThat(item.status()).isEqualTo("CONFIRMED"));

        SeatRecord s1 = seatRepository.findById(seat1).orElseThrow();
        SeatRecord s2 = seatRepository.findById(seat2).orElseThrow();
        assertThat(s1.status()).isEqualTo("SOLD");
        assertThat(s2.status()).isEqualTo("SOLD");
        assertThat(s1.version()).isEqualTo(2L);
        assertThat(s2.version()).isEqualTo(2L);

        // Verify Redis state: seat keys marked SOLD permanently (no TTL) and no longer hold reservationId lock
        awaitCondition(TIMEOUT, () -> {
            String val1 = stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1));
            String val2 = stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat2));
            Long ttl1 = stringRedisTemplate.getExpire(RedisKeys.seatKey(eventId, seat1));
            Long ttl2 = stringRedisTemplate.getExpire(RedisKeys.seatKey(eventId, seat2));
            return RedisKeys.SOLD_MARKER.equals(val1)
                    && RedisKeys.SOLD_MARKER.equals(val2)
                    && Long.valueOf(-1L).equals(ttl1)
                    && Long.valueOf(-1L).equals(ttl2);
        });

        // Verify processed_events has the row
        Integer processedCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = ? AND consumer_group = ?",
                Integer.class,
                paymentEventId,
                PaymentEventService.CONSUMER_GROUP
        );
        assertThat(processedCount).isEqualTo(1);

        // 3. Duplicate payment.succeeded with the SAME eventId -> no-op
        double duplicatesBefore = counterValue("consumer_events_duplicate_total",
                "consumer_group", PaymentEventService.CONSUMER_GROUP,
                "event_type", EventTypes.PAYMENT_SUCCEEDED);
        double confirmedOutcomeBefore = counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_SUCCEEDED,
                "outcome", PaymentEventService.OUTCOME_CONFIRMED);

        publishPaymentSucceeded(paymentEventId, paymentId, reservationId, userId, 24000L, incomingCorrelationId);

        awaitCondition(TIMEOUT, () -> counterValue("consumer_events_duplicate_total",
                "consumer_group", PaymentEventService.CONSUMER_GROUP,
                "event_type", EventTypes.PAYMENT_SUCCEEDED) >= duplicatesBefore + 1.0);

        assertThat(counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_SUCCEEDED,
                "outcome", PaymentEventService.OUTCOME_CONFIRMED)).isEqualTo(confirmedOutcomeBefore);
        assertThat(seatRepository.findById(seat1).orElseThrow().version()).isEqualTo(2L);
        assertThat(seatRepository.findById(seat2).orElseThrow().version()).isEqualTo(2L);
        assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMED)).isEqualTo(1);

        // 4. Second payment.succeeded with a NEW eventId for an already CONFIRMED reservation -> no-op
        UUID secondPaymentEventId = UUID.randomUUID();
        double noopBefore = counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_SUCCEEDED,
                "outcome", PaymentEventService.OUTCOME_NOOP);

        publishPaymentSucceeded(secondPaymentEventId, UUID.randomUUID(), reservationId, userId, 24000L, UUID.randomUUID().toString());

        awaitCondition(TIMEOUT, () -> counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_SUCCEEDED,
                "outcome", PaymentEventService.OUTCOME_NOOP) >= noopBefore + 1.0);

        assertThat(seatRepository.findById(seat1).orElseThrow().version()).isEqualTo(2L);
        assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMED)).isEqualTo(1);
    }

    // =========================================================================
    // 5. Late payment.succeeded for EXPIRED reservation -> confirmation_rejected (RESERVATION_ALREADY_EXPIRED)
    // =========================================================================

    @Test
    void latePaymentSucceededForExpiredReservation_leavesDbAndRedisUnchangedAndPublishesConfirmationRejected() throws Exception {
        UUID eventId = insertOnSaleEvent("Adele Residency");
        UUID seat1 = insertSeat(eventId, "C", "1", "1", 20000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        // Force reservation to expire via ExpirySweeper
        jdbcTemplate.update(
                "UPDATE reservations SET created_at = clock_timestamp() - INTERVAL '1 hour', expires_at = clock_timestamp() - INTERVAL '10 seconds' WHERE reservation_id = ?",
                reservationId
        );
        boolean expired = expirySweeper.expireReservation(reservationId);
        assertThat(expired).isTrue();
        assertThat(reservationRepository.findById(reservationId).orElseThrow().status()).isEqualTo("EXPIRED");
        assertThat(seatRepository.findById(seat1).orElseThrow().status()).isEqualTo("AVAILABLE");
        long seatVersionAfterExpire = seatRepository.findById(seat1).orElseThrow().version();

        Map<TopicPartition, Long> inventoryOffsets = currentEndOffsets(Topics.INVENTORY_EVENTS);
        UUID paymentId = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();

        publishPaymentSucceeded(UUID.randomUUID(), paymentId, reservationId, userId, 20000L, correlationId);

        ConsumerRecord<String, String> rejectedRecord = awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                inventoryOffsets,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED),
                TIMEOUT
        );

        assertThat(headerValue(rejectedRecord, CorrelationConstants.KAFKA_HEADER)).isEqualTo(correlationId);
        JsonNode envelope = objectMapper.readTree(rejectedRecord.value());
        assertThat(envelope.get("eventType").asText()).isEqualTo(EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED);
        SeatConfirmationRejectedPayload payload = objectMapper.treeToValue(
                envelope.get("payload"), SeatConfirmationRejectedPayload.class);

        assertThat(payload.reservationId()).isEqualTo(reservationId);
        assertThat(payload.orderId()).isEqualTo(reservationId);
        assertThat(payload.paymentId()).isEqualTo(paymentId);
        assertThat(payload.userId()).isEqualTo(userId);
        assertThat(payload.eventId()).isEqualTo(eventId);
        assertThat(payload.reason()).isEqualTo(PaymentEventService.REASON_RESERVATION_ALREADY_EXPIRED);
        assertThat(payload.rejectedAt()).isNotNull();

        // DB and Redis remain unchanged
        assertThat(reservationRepository.findById(reservationId).orElseThrow().status()).isEqualTo("EXPIRED");
        SeatRecord seatAfter = seatRepository.findById(seat1).orElseThrow();
        assertThat(seatAfter.status()).isEqualTo("AVAILABLE");
        assertThat(seatAfter.version()).isEqualTo(seatVersionAfterExpire);
        assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seat1)))
                .isFalse();
    }

    // =========================================================================
    // 6. payment.succeeded for RELEASED reservation -> confirmation_rejected (RESERVATION_ALREADY_RELEASED)
    // =========================================================================

    @Test
    void paymentSucceededForReleasedReservation_publishesConfirmationRejectedWithAlreadyReleasedReason() throws Exception {
        UUID eventId = insertOnSaleEvent("Ed Sheeran Mathematics");
        UUID seat1 = insertSeat(eventId, "D", "3", "5", 9500L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        // First transition to RELEASED via payment.failed
        Map<TopicPartition, Long> offsetsBeforeFail = currentEndOffsets(Topics.INVENTORY_EVENTS);
        publishPaymentFailed(UUID.randomUUID(), UUID.randomUUID(), reservationId, userId, 9500L, UUID.randomUUID().toString());
        awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                offsetsBeforeFail,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_RELEASED),
                TIMEOUT
        );
        assertThat(reservationRepository.findById(reservationId).orElseThrow().status()).isEqualTo("RELEASED");

        // Now send payment.succeeded for the RELEASED reservation
        Map<TopicPartition, Long> offsetsBeforeSucceeded = currentEndOffsets(Topics.INVENTORY_EVENTS);
        UUID paymentId = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();
        publishPaymentSucceeded(UUID.randomUUID(), paymentId, reservationId, userId, 9500L, correlationId);

        ConsumerRecord<String, String> rejectedRecord = awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                offsetsBeforeSucceeded,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED),
                TIMEOUT
        );

        JsonNode envelope = objectMapper.readTree(rejectedRecord.value());
        SeatConfirmationRejectedPayload payload = objectMapper.treeToValue(
                envelope.get("payload"), SeatConfirmationRejectedPayload.class);
        assertThat(payload.reservationId()).isEqualTo(reservationId);
        assertThat(payload.paymentId()).isEqualTo(paymentId);
        assertThat(payload.reason()).isEqualTo(PaymentEventService.REASON_RESERVATION_ALREADY_RELEASED);
        assertThat(reservationRepository.findById(reservationId).orElseThrow().status()).isEqualTo("RELEASED");
    }

    // =========================================================================
    // 7. payment.failed for HELD reservation -> RELEASED / AVAILABLE, Redis locks deleted, re-holdable
    // =========================================================================

    @Test
    void paymentFailedForHeldReservation_marksReleasedAndAvailable_deletesRedisLocksSoSeatsCanBeHeldAgain_andPublishesSeatReleased() throws Exception {
        UUID eventId = insertOnSaleEvent("Oasis Reunion");
        UUID seat1 = insertSeat(eventId, "E", "1", "1", 18000L, "AVAILABLE");
        UUID seat2 = insertSeat(eventId, "E", "1", "2", 18000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1, seat2), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        Map<TopicPartition, Long> inventoryOffsets = currentEndOffsets(Topics.INVENTORY_EVENTS);
        String correlationId = UUID.randomUUID().toString();

        publishPaymentFailed(UUID.randomUUID(), UUID.randomUUID(), reservationId, userId, 36000L, correlationId);

        ConsumerRecord<String, String> releasedRecord = awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                inventoryOffsets,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_RELEASED),
                TIMEOUT
        );

        assertThat(headerValue(releasedRecord, CorrelationConstants.KAFKA_HEADER)).isEqualTo(correlationId);
        JsonNode envelope = objectMapper.readTree(releasedRecord.value());
        SeatReleasedPayload payload = objectMapper.treeToValue(envelope.get("payload"), SeatReleasedPayload.class);

        assertThat(payload.reservationId()).isEqualTo(reservationId);
        assertThat(payload.orderId()).isEqualTo(reservationId);
        assertThat(payload.eventId()).isEqualTo(eventId);
        assertThat(payload.seatIds()).containsExactlyInAnyOrder(seat1, seat2);
        assertThat(payload.reason()).isEqualTo(PaymentEventService.REASON_PAYMENT_FAILED);
        assertThat(payload.releasedAt()).isNotNull();

        ReservationRecord reservation = reservationRepository.findById(reservationId).orElseThrow();
        assertThat(reservation.status()).isEqualTo("RELEASED");
        assertThat(reservation.releasedAt()).isNotNull();

        List<ReservationItemRecord> items = reservationRepository.findItemsByReservationId(reservationId);
        assertThat(items).hasSize(2).allSatisfy(item -> assertThat(item.status()).isEqualTo("RELEASED"));

        SeatRecord s1 = seatRepository.findById(seat1).orElseThrow();
        SeatRecord s2 = seatRepository.findById(seat2).orElseThrow();
        assertThat(s1.status()).isEqualTo("AVAILABLE");
        assertThat(s2.status()).isEqualTo("AVAILABLE");
        assertThat(s1.version()).isEqualTo(2L);
        assertThat(s2.version()).isEqualTo(2L);

        awaitCondition(TIMEOUT, () ->
                Boolean.FALSE.equals(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seat1)))
                        && Boolean.FALSE.equals(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seat2)))
        );

        // Verify the released seats can immediately be held again by another user
        ReservationResponse secondHold = createHoldViaHttp(
                eventId, UUID.randomUUID(), List.of(seat1, seat2), UUID.randomUUID().toString());
        assertThat(secondHold.status()).isEqualTo("HELD");
        assertThat(secondHold.reservationId()).isNotEqualTo(reservationId);
    }

    // =========================================================================
    // 8. payment.failed for already CONFIRMED or EXPIRED reservation is a no-op
    // =========================================================================

    @Test
    void paymentFailedForAlreadyConfirmedOrExpiredReservation_isNoOp() throws Exception {
        UUID eventId = insertOnSaleEvent("Metallica M72");
        UUID seatConfirmed = insertSeat(eventId, "F", "1", "1", 11000L, "AVAILABLE");
        UUID seatExpired = insertSeat(eventId, "F", "1", "2", 11000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        // Reservation 1: CONFIRMED
        ReservationResponse hold1 = createHoldViaHttp(eventId, userId, List.of(seatConfirmed), UUID.randomUUID().toString());
        UUID confirmedResId = hold1.reservationId();
        Map<TopicPartition, Long> offsets1 = currentEndOffsets(Topics.INVENTORY_EVENTS);
        publishPaymentSucceeded(UUID.randomUUID(), UUID.randomUUID(), confirmedResId, userId, 11000L, UUID.randomUUID().toString());
        awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                offsets1,
                rec -> confirmedResId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_CONFIRMED),
                TIMEOUT
        );

        // Reservation 2: EXPIRED
        ReservationResponse hold2 = createHoldViaHttp(eventId, userId, List.of(seatExpired), UUID.randomUUID().toString());
        UUID expiredResId = hold2.reservationId();
        jdbcTemplate.update(
                "UPDATE reservations SET created_at = clock_timestamp() - INTERVAL '1 hour', expires_at = clock_timestamp() - INTERVAL '10 seconds' WHERE reservation_id = ?",
                expiredResId
        );
        assertThat(expirySweeper.expireReservation(expiredResId)).isTrue();

        double noopBefore = counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_FAILED,
                "outcome", PaymentEventService.OUTCOME_NOOP);

        publishPaymentFailed(UUID.randomUUID(), UUID.randomUUID(), confirmedResId, userId, 11000L, UUID.randomUUID().toString());
        publishPaymentFailed(UUID.randomUUID(), UUID.randomUUID(), expiredResId, userId, 11000L, UUID.randomUUID().toString());

        awaitCondition(TIMEOUT, () -> counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_FAILED,
                "outcome", PaymentEventService.OUTCOME_NOOP) >= noopBefore + 2.0);

        assertThat(reservationRepository.findById(confirmedResId).orElseThrow().status()).isEqualTo("CONFIRMED");
        assertThat(seatRepository.findById(seatConfirmed).orElseThrow().status()).isEqualTo("SOLD");
        assertThat(countOutboxEvents(confirmedResId, EventTypes.INVENTORY_SEAT_RELEASED)).isZero();

        assertThat(reservationRepository.findById(expiredResId).orElseThrow().status()).isEqualTo("EXPIRED");
        assertThat(seatRepository.findById(seatExpired).orElseThrow().status()).isEqualTo("AVAILABLE");
        assertThat(countOutboxEvents(expiredResId, EventTypes.INVENTORY_SEAT_RELEASED)).isZero();
    }

    // =========================================================================
    // 9. payment.succeeded or payment.failed for non-existent reservationId -> payment.events.dlt without retry
    // =========================================================================

    @Test
    void paymentEventsForNonExistentReservationId_routeDirectlyToDltWithoutRetry() throws Exception {
        UUID missingRes1 = UUID.randomUUID();
        UUID missingRes2 = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        Map<TopicPartition, Long> retry0Offsets = currentEndOffsets(PAYMENT_RETRY_0_TOPIC);
        Map<TopicPartition, Long> dltOffsets = currentEndOffsets(PAYMENT_DLT_TOPIC);

        publishPaymentSucceeded(UUID.randomUUID(), UUID.randomUUID(), missingRes1, userId, 10000L, UUID.randomUUID().toString());
        publishPaymentFailed(UUID.randomUUID(), UUID.randomUUID(), missingRes2, userId, 10000L, UUID.randomUUID().toString());

        ConsumerRecord<String, String> dlt1 = awaitSingleRecord(
                PAYMENT_DLT_TOPIC,
                dltOffsets,
                rec -> missingRes1.toString().equals(rec.key()),
                TIMEOUT
        );
        ConsumerRecord<String, String> dlt2 = awaitSingleRecord(
                PAYMENT_DLT_TOPIC,
                dltOffsets,
                rec -> missingRes2.toString().equals(rec.key()),
                TIMEOUT
        );

        assertThat(dlt1.value()).contains(missingRes1.toString());
        assertThat(dlt2.value()).contains(missingRes2.toString());

        assertNoNewRecordsMatching(
                PAYMENT_RETRY_0_TOPIC,
                retry0Offsets,
                rec -> missingRes1.toString().equals(rec.key()) || missingRes2.toString().equals(rec.key())
        );
    }

    // =========================================================================
    // 10. Malformed payload goes straight to payment.events.dlt without hitting retry topics
    // =========================================================================

    @Test
    void malformedPayload_goesStraightToDltWithoutHittingRetryTopics() throws Exception {
        String key = UUID.randomUUID().toString();
        String malformedJson = "{\"notValidEnvelope\": true, broken";

        Map<TopicPartition, Long> retry0Offsets = currentEndOffsets(PAYMENT_RETRY_0_TOPIC);
        Map<TopicPartition, Long> dltOffsets = currentEndOffsets(PAYMENT_DLT_TOPIC);

        testProducer.send(new ProducerRecord<>(Topics.PAYMENT_EVENTS, key, malformedJson)).get(5, TimeUnit.SECONDS);

        ConsumerRecord<String, String> dltRecord = awaitSingleRecord(
                PAYMENT_DLT_TOPIC,
                dltOffsets,
                rec -> key.equals(rec.key()),
                TIMEOUT
        );
        assertThat(dltRecord.value()).isEqualTo(malformedJson);

        assertNoNewRecordsMatching(
                PAYMENT_RETRY_0_TOPIC,
                retry0Offsets,
                rec -> key.equals(rec.key())
        );
    }

    // =========================================================================
    // 11. Unrelated event type on payment.events is ignored and not dead-lettered
    // =========================================================================

    @Test
    void unrelatedEventTypeOnPaymentEvents_isIgnoredAndNotDeadLettered() throws Exception {
        UUID unrelatedEventId = UUID.randomUUID();
        String key = UUID.randomUUID().toString();
        String correlationId = UUID.randomUUID().toString();

        String envelopeJson = objectMapper.writeValueAsString(new EventEnvelope<>(
                unrelatedEventId,
                EventTypes.PAYMENT_REFUNDED,
                Instant.now().truncatedTo(ChronoUnit.MICROS),
                correlationId,
                1,
                Map.of("paymentId", UUID.randomUUID().toString())
        ));

        Map<TopicPartition, Long> dltOffsets = currentEndOffsets(PAYMENT_DLT_TOPIC);
        double ignoredBefore = counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_REFUNDED,
                "outcome", PaymentEventService.OUTCOME_IGNORED);

        testProducer.send(new ProducerRecord<>(Topics.PAYMENT_EVENTS, key, envelopeJson)).get(5, TimeUnit.SECONDS);

        awaitCondition(TIMEOUT, () -> counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_REFUNDED,
                "outcome", PaymentEventService.OUTCOME_IGNORED) >= ignoredBefore + 1.0);

        Integer processed = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = ? AND consumer_group = ?",
                Integer.class,
                unrelatedEventId,
                PaymentEventService.CONSUMER_GROUP
        );
        assertThat(processed).isEqualTo(1);

        assertNoNewRecordsMatching(
                PAYMENT_DLT_TOPIC,
                dltOffsets,
                rec -> key.equals(rec.key())
        );
    }

    // =========================================================================
    // 12. Redis unavailable during post-commit confirm -> DB and outbox still commit, failure counter increments
    // =========================================================================

    @Test
    void whenRedisUnavailableDuringPostCommitConfirm_dbAndOutboxStillCommitAndFailureMetricIncrements() throws Exception {
        UUID eventId = insertOnSaleEvent("Radiohead Live");
        UUID seat1 = insertSeat(eventId, "G", "1", "1", 14000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        paymentEventService.setRedisConfirmRetryPauseMs(5L);
        doThrow(new RedisUnavailableException("Simulated Redis outage during confirm"))
                .when(redisSeatGate).confirm(any(), any(), any());

        double redisFailuresBefore = counterValue(PaymentEventService.METRIC_REDIS_CONFIRM_FAILURES);
        Map<TopicPartition, Long> inventoryOffsets = currentEndOffsets(Topics.INVENTORY_EVENTS);

        UUID paymentEventId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        publishPaymentSucceeded(paymentEventId, paymentId, reservationId, userId, 14000L, UUID.randomUUID().toString());

        ConsumerRecord<String, String> confirmedRecord = awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                inventoryOffsets,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_CONFIRMED),
                TIMEOUT
        );
        assertThat(confirmedRecord).isNotNull();

        awaitCondition(TIMEOUT, () ->
                counterValue(PaymentEventService.METRIC_REDIS_CONFIRM_FAILURES) >= redisFailuresBefore + 1.0);

        assertThat(reservationRepository.findById(reservationId).orElseThrow().status()).isEqualTo("CONFIRMED");
        assertThat(seatRepository.findById(seat1).orElseThrow().status()).isEqualTo("SOLD");
        assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMED)).isEqualTo(1);
    }

    // =========================================================================
    // 13. Race test: payment.succeeded vs ExpirySweeper.expireReservation on overdue HELD reservation
    // =========================================================================

    @Test
    void raceBetweenPaymentSucceededAndExpirySweeper_exactlyOneWinsCleanly() throws Exception {
        UUID eventId = insertOnSaleEvent("Daft Punk Reunion");
        UUID userId = UUID.randomUUID();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 5; i++) {
                UUID seatId = insertSeat(eventId, "R", "1", String.valueOf(i + 1), 10000L, "AVAILABLE");
                ReservationResponse hold = createHoldViaHttp(
                        eventId, userId, List.of(seatId), UUID.randomUUID().toString());
                UUID reservationId = hold.reservationId();

                // Make the HELD reservation overdue in PostgreSQL
                jdbcTemplate.update(
                        "UPDATE reservations SET created_at = clock_timestamp() - INTERVAL '1 hour', expires_at = clock_timestamp() - INTERVAL '5 seconds' WHERE reservation_id = ?",
                        reservationId
                );

                UUID paymentEventId = UUID.randomUUID();
                UUID paymentId = UUID.randomUUID();
                String correlationId = UUID.randomUUID().toString();
                String paymentSucceededJson = objectMapper.writeValueAsString(new EventEnvelope<>(
                        paymentEventId,
                        EventTypes.PAYMENT_SUCCEEDED,
                        Instant.now().truncatedTo(ChronoUnit.MICROS),
                        correlationId,
                        1,
                        new PaymentSucceededPayload(
                                paymentId,
                                reservationId,
                                reservationId,
                                userId,
                                10000L,
                                "USD",
                                "pi_race_" + i,
                                Instant.now().truncatedTo(ChronoUnit.MICROS)
                        )
                ));

                CyclicBarrier barrier = new CyclicBarrier(2);
                Future<Boolean> confirmFuture = pool.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return paymentEventService.handlePaymentEventRecord(paymentSucceededJson);
                });
                Future<Boolean> expireFuture = pool.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return expirySweeper.expireReservation(reservationId);
                });

                boolean confirmProcessed = confirmFuture.get(10, TimeUnit.SECONDS);
                boolean sweeperExpired = expireFuture.get(10, TimeUnit.SECONDS);

                assertThat(confirmProcessed).isTrue();

                ReservationRecord finalRes = reservationRepository.findById(reservationId).orElseThrow();
                SeatRecord finalSeat = seatRepository.findById(seatId).orElseThrow();

                if (!sweeperExpired) {
                    // payment.succeeded won the lock
                    assertThat(finalRes.status()).isEqualTo("CONFIRMED");
                    assertThat(finalRes.confirmedAt()).isNotNull();
                    assertThat(finalRes.expiredAt()).isNull();
                    assertThat(finalSeat.status()).isEqualTo("SOLD");
                    assertThat(finalSeat.version()).isEqualTo(2L);
                    assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMED)).isEqualTo(1);
                    assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_RESERVATION_EXPIRED)).isZero();
                    assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED)).isZero();
                    assertThat(stringRedisTemplate.opsForValue()
                            .get(RedisKeys.seatKey(eventId, seatId))).isEqualTo(RedisKeys.SOLD_MARKER);
                } else {
                    // ExpirySweeper won the lock first; payment.succeeded observed EXPIRED and emitted confirmation_rejected
                    assertThat(finalRes.status()).isEqualTo("EXPIRED");
                    assertThat(finalRes.expiredAt()).isNotNull();
                    assertThat(finalRes.confirmedAt()).isNull();
                    assertThat(finalSeat.status()).isEqualTo("AVAILABLE");
                    assertThat(finalSeat.version()).isEqualTo(2L);
                    assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_RESERVATION_EXPIRED)).isEqualTo(1);
                    assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED)).isEqualTo(1);
                    assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMED)).isZero();
                    assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seatId))).isFalse();
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // =========================================================================
    // 14. payment.succeeded for overdue HELD reservation when sweeper not running confirms it
    // =========================================================================

    @Test
    void paymentSucceededForOverdueHeldReservationWhenSweeperNotRunning_confirmsDbAndRedis() throws Exception {
        UUID eventId = insertOnSaleEvent("Bruce Springsteen Tour");
        UUID seat1 = insertSeat(eventId, "H", "1", "1", 25000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        // Make the reservation overdue in PostgreSQL
        jdbcTemplate.update(
                "UPDATE reservations SET created_at = clock_timestamp() - INTERVAL '1 hour', expires_at = clock_timestamp() - INTERVAL '15 seconds' WHERE reservation_id = ?",
                reservationId
        );

        Map<TopicPartition, Long> inventoryOffsets = currentEndOffsets(Topics.INVENTORY_EVENTS);
        UUID paymentEventId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();

        publishPaymentSucceeded(paymentEventId, paymentId, reservationId, userId, 25000L, correlationId);

        ConsumerRecord<String, String> confirmedRecord = awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                inventoryOffsets,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_CONFIRMED),
                TIMEOUT
        );
        assertThat(confirmedRecord).isNotNull();

        ReservationRecord res = reservationRepository.findById(reservationId).orElseThrow();
        assertThat(res.status()).isEqualTo("CONFIRMED");
        assertThat(res.confirmedAt()).isNotNull();

        SeatRecord seat = seatRepository.findById(seat1).orElseThrow();
        assertThat(seat.status()).isEqualTo("SOLD");
        assertThat(seat.version()).isEqualTo(2L);

        awaitCondition(TIMEOUT, () -> {
            String val = stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1));
            Long ttl = stringRedisTemplate.getExpire(RedisKeys.seatKey(eventId, seat1));
            return RedisKeys.SOLD_MARKER.equals(val) && Long.valueOf(-1L).equals(ttl);
        });

        assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMED)).isEqualTo(1);
    }

    // =========================================================================
    // 15. Transient DataAccessException on repository lock retries via retry-0 and succeeds
    // =========================================================================

    @Test
    void transientDataAccessExceptionOnFirstDelivery_retriesThroughRetry0AndSucceeds() throws Exception {
        UUID eventId = insertOnSaleEvent("Pink Floyd Experience");
        UUID seat1 = insertSeat(eventId, "I", "2", "3", 19000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        Map<TopicPartition, Long> retry0Offsets = currentEndOffsets(PAYMENT_RETRY_0_TOPIC);
        Map<TopicPartition, Long> inventoryOffsets = currentEndOffsets(Topics.INVENTORY_EVENTS);

        UUID paymentEventId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();

        // Cause the first repository lock call to throw a transient DataAccessException
        reservationRepository.setTransientLockFailureCount(1);

        publishPaymentSucceeded(paymentEventId, paymentId, reservationId, userId, 19000L, correlationId);

        // Await delivery into payment retry-0 topic
        ConsumerRecord<String, String> retryRecord = awaitSingleRecord(
                PAYMENT_RETRY_0_TOPIC,
                retry0Offsets,
                rec -> reservationId.toString().equals(rec.key()),
                TIMEOUT
        );
        assertThat(retryRecord).isNotNull();

        // Await successful confirmation on inventory.events
        ConsumerRecord<String, String> confirmedRecord = awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                inventoryOffsets,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_CONFIRMED),
                TIMEOUT
        );
        assertThat(confirmedRecord).isNotNull();

        Integer processedCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = ? AND consumer_group = ?",
                Integer.class,
                paymentEventId,
                PaymentEventService.CONSUMER_GROUP
        );
        assertThat(processedCount).isEqualTo(1);
        assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMED)).isEqualTo(1);
        assertThat(reservationRepository.findById(reservationId).orElseThrow().status()).isEqualTo("CONFIRMED");
    }

    // =========================================================================
    // 16. Deterministic lock ordering between payment.succeeded and ExpirySweeper
    // =========================================================================

    @Test
    void deterministicLockOrderingBetweenPaymentSucceededAndExpirySweeper_forcesBothOutcomes() throws Exception {
        UUID eventId = insertOnSaleEvent("Hans Zimmer Live");
        UUID userId = UUID.randomUUID();
        ExecutorService pool = Executors.newFixedThreadPool(2);

        try {
            // -------------------------------------------------------------------------
            // Ordering 1: Payment lock first -> Confirm wins, sweeper gets SKIP LOCKED false
            // -------------------------------------------------------------------------
            UUID seat1 = insertSeat(eventId, "K", "1", "1", 16000L, "AVAILABLE");
            ReservationResponse hold1 = createHoldViaHttp(eventId, userId, List.of(seat1), UUID.randomUUID().toString());
            UUID res1 = hold1.reservationId();
            jdbcTemplate.update(
                    "UPDATE reservations SET created_at = clock_timestamp() - INTERVAL '1 hour', expires_at = clock_timestamp() - INTERVAL '10 seconds' WHERE reservation_id = ?",
                    res1
            );

            UUID payEvent1 = UUID.randomUUID();
            UUID payId1 = UUID.randomUUID();
            String payJson1 = objectMapper.writeValueAsString(new EventEnvelope<>(
                    payEvent1,
                    EventTypes.PAYMENT_SUCCEEDED,
                    Instant.now().truncatedTo(ChronoUnit.MICROS),
                    UUID.randomUUID().toString(),
                    1,
                    new PaymentSucceededPayload(payId1, res1, res1, userId, 16000L, "USD", "pi_det_1", Instant.now().truncatedTo(ChronoUnit.MICROS))
            ));

            CountDownLatch pay1LockAcquired = new CountDownLatch(1);
            CountDownLatch sweeper1Attempted = new CountDownLatch(1);

            paymentEventService.setAfterReservationLockHook(() -> {
                pay1LockAcquired.countDown();
                try {
                    sweeper1Attempted.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            Future<Boolean> payFuture1 = pool.submit(() -> paymentEventService.handlePaymentEventRecord(payJson1));
            assertThat(pay1LockAcquired.await(5, TimeUnit.SECONDS)).isTrue();

            boolean sweeperResult1 = expirySweeper.expireReservation(res1);
            sweeper1Attempted.countDown();
            boolean payResult1 = payFuture1.get(5, TimeUnit.SECONDS);

            assertThat(sweeperResult1).isFalse();
            assertThat(payResult1).isTrue();

            ReservationRecord r1 = reservationRepository.findById(res1).orElseThrow();
            assertThat(r1.status()).isEqualTo("CONFIRMED");
            assertThat(seatRepository.findById(seat1).orElseThrow().status()).isEqualTo("SOLD");
            awaitCondition(TIMEOUT, () -> RedisKeys.SOLD_MARKER.equals(
                    stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1))));
            assertThat(countOutboxEvents(res1, EventTypes.INVENTORY_SEAT_CONFIRMED)).isEqualTo(1);
            assertThat(countOutboxEvents(res1, EventTypes.INVENTORY_RESERVATION_EXPIRED)).isZero();
            assertThat(countOutboxEvents(res1, EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED)).isZero();

            // -------------------------------------------------------------------------
            // Ordering 2: Sweeper lock first -> Sweeper wins, payment blocks and emits confirmation_rejected
            // -------------------------------------------------------------------------
            paymentEventService.setAfterReservationLockHook(null);
            expirySweeper.setAfterReservationLockHook(null);

            UUID seat2 = insertSeat(eventId, "K", "1", "2", 16000L, "AVAILABLE");
            ReservationResponse hold2 = createHoldViaHttp(eventId, userId, List.of(seat2), UUID.randomUUID().toString());
            UUID res2 = hold2.reservationId();
            jdbcTemplate.update(
                    "UPDATE reservations SET created_at = clock_timestamp() - INTERVAL '1 hour', expires_at = clock_timestamp() - INTERVAL '10 seconds' WHERE reservation_id = ?",
                    res2
            );

            UUID payEvent2 = UUID.randomUUID();
            UUID payId2 = UUID.randomUUID();
            String payJson2 = objectMapper.writeValueAsString(new EventEnvelope<>(
                    payEvent2,
                    EventTypes.PAYMENT_SUCCEEDED,
                    Instant.now().truncatedTo(ChronoUnit.MICROS),
                    UUID.randomUUID().toString(),
                    1,
                    new PaymentSucceededPayload(payId2, res2, res2, userId, 16000L, "USD", "pi_det_2", Instant.now().truncatedTo(ChronoUnit.MICROS))
            ));

            CountDownLatch sweeper2LockAcquired = new CountDownLatch(1);
            CountDownLatch pay2Started = new CountDownLatch(1);

            expirySweeper.setAfterReservationLockHook(() -> {
                sweeper2LockAcquired.countDown();
                try {
                    pay2Started.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            Future<Boolean> sweeperFuture2 = pool.submit(() -> expirySweeper.expireReservation(res2));
            assertThat(sweeper2LockAcquired.await(5, TimeUnit.SECONDS)).isTrue();

            Future<Boolean> payFuture2 = pool.submit(() -> paymentEventService.handlePaymentEventRecord(payJson2));
            pay2Started.countDown();

            boolean sweeperResult2 = sweeperFuture2.get(5, TimeUnit.SECONDS);
            boolean payResult2 = payFuture2.get(5, TimeUnit.SECONDS);

            assertThat(sweeperResult2).isTrue();
            assertThat(payResult2).isTrue();

            ReservationRecord r2 = reservationRepository.findById(res2).orElseThrow();
            assertThat(r2.status()).isEqualTo("EXPIRED");
            assertThat(seatRepository.findById(seat2).orElseThrow().status()).isEqualTo("AVAILABLE");
            assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seat2))).isFalse();
            assertThat(countOutboxEvents(res2, EventTypes.INVENTORY_RESERVATION_EXPIRED)).isEqualTo(1);
            assertThat(countOutboxEvents(res2, EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED)).isEqualTo(1);
            assertThat(countOutboxEvents(res2, EventTypes.INVENTORY_SEAT_CONFIRMED)).isZero();
        } finally {
            paymentEventService.setAfterReservationLockHook(null);
            expirySweeper.setAfterReservationLockHook(null);
            pool.shutdownNow();
        }
    }

    // =========================================================================
    // 17. Duplicate and new payment.failed on RELEASED reservation
    // =========================================================================

    @Test
    void duplicatePaymentFailedAndNewPaymentFailedOnReleasedReservation_incrementsMetricsAndLeavesOneReleasedEvent() throws Exception {
        UUID eventId = insertOnSaleEvent("Rolling Stones Hackney Diamonds");
        UUID seat1 = insertSeat(eventId, "L", "1", "1", 22000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        Map<TopicPartition, Long> inventoryOffsets = currentEndOffsets(Topics.INVENTORY_EVENTS);
        UUID paymentEventId1 = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        publishPaymentFailed(paymentEventId1, paymentId, reservationId, userId, 22000L, UUID.randomUUID().toString());

        awaitSingleRecord(
                Topics.INVENTORY_EVENTS,
                inventoryOffsets,
                rec -> reservationId.toString().equals(rec.key())
                        && rec.value().contains(EventTypes.INVENTORY_SEAT_RELEASED),
                TIMEOUT
        );
        assertThat(reservationRepository.findById(reservationId).orElseThrow().status()).isEqualTo("RELEASED");
        assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_RELEASED)).isEqualTo(1);

        double duplicateBefore = counterValue("consumer_events_duplicate_total",
                "consumer_group", PaymentEventService.CONSUMER_GROUP,
                "event_type", EventTypes.PAYMENT_FAILED);
        double noopBefore = counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_FAILED,
                "outcome", PaymentEventService.OUTCOME_NOOP);

        // 1. Duplicate payment.failed with SAME eventId
        publishPaymentFailed(paymentEventId1, paymentId, reservationId, userId, 22000L, UUID.randomUUID().toString());
        awaitCondition(TIMEOUT, () -> counterValue("consumer_events_duplicate_total",
                "consumer_group", PaymentEventService.CONSUMER_GROUP,
                "event_type", EventTypes.PAYMENT_FAILED) >= duplicateBefore + 1.0);

        // 2. Second payment.failed with NEW eventId on already RELEASED reservation
        UUID paymentEventId2 = UUID.randomUUID();
        publishPaymentFailed(paymentEventId2, UUID.randomUUID(), reservationId, userId, 22000L, UUID.randomUUID().toString());
        awaitCondition(TIMEOUT, () -> counterValue(PaymentEventService.METRIC_PAYMENT_EVENTS_HANDLED,
                "event_type", EventTypes.PAYMENT_FAILED,
                "outcome", PaymentEventService.OUTCOME_NOOP) >= noopBefore + 1.0);

        assertThat(countOutboxEvents(reservationId, EventTypes.INVENTORY_SEAT_RELEASED)).isEqualTo(1);
        assertThat(reservationRepository.findById(reservationId).orElseThrow().status()).isEqualTo("RELEASED");
    }

    // =========================================================================
    // 18. Hold request for SOLD seat missing from Redis rejected with 409 and healed to SOLD
    // =========================================================================

    @Test
    void holdRequestForSoldSeatMissingFromRedis_rejectedWith409AndHealedToSoldInRedis() throws Exception {
        UUID eventId = insertOnSaleEvent("Billie Eilish Tour");
        UUID seat1 = insertSeat(eventId, "M", "1", "1", 17000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        // Mark seat SOLD in PostgreSQL
        jdbcTemplate.update("UPDATE seats SET status = 'SOLD', version = version + 1 WHERE seat_id = ?", seat1);

        // Ensure key is deleted from Redis
        stringRedisTemplate.delete(RedisKeys.seatKey(eventId, seat1));

        // Submit hold request via HTTP -> must fail with 409
        CreateReservationRequest req = new CreateReservationRequest(eventId, List.of(seat1));
        mockMvc.perform(post("/api/v1/reservations")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("X-User-Roles", "USER")
                        .header(CorrelationConstants.HTTP_HEADER, UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isConflict());

        // Assert Redis key was healed to SOLD with TTL -1
        awaitCondition(TIMEOUT, () -> {
            String val = stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1));
            Long ttl = stringRedisTemplate.getExpire(RedisKeys.seatKey(eventId, seat1));
            return RedisKeys.SOLD_MARKER.equals(val) && Long.valueOf(-1L).equals(ttl);
        });
    }

    // =========================================================================
    // 19. Transient Redis failure during post-commit release of payment.failed retries and deletes key
    // =========================================================================

    @Test
    void transientRedisFailureDuringPostCommitReleaseOfPaymentFailed_retriesAndDeletesKey() throws Exception {
        UUID eventId = insertOnSaleEvent("Depeche Mode Memento Mori");
        UUID seat1 = insertSeat(eventId, "N", "1", "1", 13000L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        // Assert hold key exists in Redis
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1))).isEqualTo(reservationId.toString());

        java.util.concurrent.atomic.AtomicInteger releaseAttempts = new java.util.concurrent.atomic.AtomicInteger(0);
        Mockito.doAnswer(inv -> {
            if (releaseAttempts.getAndIncrement() == 0) {
                throw new RedisUnavailableException("Simulated transient Redis failure during release");
            }
            return inv.callRealMethod();
        }).when(redisSeatGate).release(any(), any(), any());

        paymentEventService.setRedisConfirmRetryPauseMs(10L);
        double failuresBefore = counterValue(PaymentEventService.METRIC_REDIS_RELEASE_FAILURES);

        publishPaymentFailed(UUID.randomUUID(), UUID.randomUUID(), reservationId, userId, 13000L, UUID.randomUUID().toString());

        awaitCondition(TIMEOUT, () -> Boolean.FALSE.equals(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seat1))));
        assertThat(reservationRepository.findById(reservationId).orElseThrow().status()).isEqualTo("RELEASED");
        assertThat(releaseAttempts.get()).isGreaterThanOrEqualTo(2);
        assertThat(counterValue(PaymentEventService.METRIC_REDIS_RELEASE_FAILURES)).isEqualTo(failuresBefore);
    }

    // =========================================================================
    // 20. Envelope with non-UUID correlationId ends in dead-letter topic
    // =========================================================================

    @Test
    void envelopeWithNonUuidCorrelationId_endsInDeadLetterTopic() throws Exception {
        UUID eventId = insertOnSaleEvent("The Weeknd After Hours");
        UUID seat1 = insertSeat(eventId, "O", "1", "1", 14000L, "AVAILABLE");
        ReservationResponse hold = createHoldViaHttp(eventId, UUID.randomUUID(), List.of(seat1), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        String nonUuidCorrelationId = "non-uuid-correlation-string-12345";
        String key = reservationId.toString();

        String envelopeJson = objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "eventType", EventTypes.PAYMENT_SUCCEEDED,
                "occurredAt", Instant.now().truncatedTo(ChronoUnit.MICROS).toString(),
                "correlationId", nonUuidCorrelationId,
                "schemaVersion", 1,
                "payload", Map.of(
                        "paymentId", UUID.randomUUID().toString(),
                        "reservationId", reservationId.toString(),
                        "orderId", reservationId.toString(),
                        "userId", UUID.randomUUID().toString(),
                        "amountCents", 14000L,
                        "currency", "USD",
                        "paymentIntentId", "pi_test",
                        "paidAt", Instant.now().truncatedTo(ChronoUnit.MICROS).toString()
                )
        ));

        Map<TopicPartition, Long> retry0Offsets = currentEndOffsets(PAYMENT_RETRY_0_TOPIC);
        Map<TopicPartition, Long> dltOffsets = currentEndOffsets(PAYMENT_DLT_TOPIC);

        testProducer.send(new ProducerRecord<>(Topics.PAYMENT_EVENTS, key, envelopeJson)).get(5, TimeUnit.SECONDS);

        ConsumerRecord<String, String> dltRecord = awaitSingleRecord(
                PAYMENT_DLT_TOPIC,
                dltOffsets,
                rec -> key.equals(rec.key()),
                TIMEOUT
        );
        assertThat(dltRecord.value()).isEqualTo(envelopeJson);

        assertNoNewRecordsMatching(PAYMENT_RETRY_0_TOPIC, retry0Offsets, rec -> key.equals(rec.key()));
    }

    // =========================================================================
    // 21. Stale MDC correlationId on thread does not leak into outbox row
    // =========================================================================

    @Test
    void staleMdcCorrelationIdOnThread_doesNotLeakIntoOutboxRow() throws Exception {
        UUID eventId = insertOnSaleEvent("Arctic Monkeys Tour");
        UUID seat1 = insertSeat(eventId, "P", "1", "1", 15500L, "AVAILABLE");
        UUID userId = UUID.randomUUID();

        ReservationResponse hold = createHoldViaHttp(eventId, userId, List.of(seat1), UUID.randomUUID().toString());
        UUID reservationId = hold.reservationId();

        String staleMdc = UUID.randomUUID().toString();
        String incomingCorrelationId = UUID.randomUUID().toString();

        MDC.put(CorrelationConstants.MDC_KEY, staleMdc);
        try {
            String paymentJson = objectMapper.writeValueAsString(new EventEnvelope<>(
                    UUID.randomUUID(),
                    EventTypes.PAYMENT_SUCCEEDED,
                    Instant.now().truncatedTo(ChronoUnit.MICROS),
                    incomingCorrelationId,
                    1,
                    new PaymentSucceededPayload(
                            UUID.randomUUID(),
                            reservationId,
                            reservationId,
                            userId,
                            15500L,
                            "USD",
                            "pi_mdc_test",
                            Instant.now().truncatedTo(ChronoUnit.MICROS)
                    )
            ));

            boolean processed = paymentEventService.handlePaymentEventRecord(paymentJson);
            assertThat(processed).isTrue();

            UUID outboxCorrelationId = jdbcTemplate.queryForObject(
                    "SELECT correlation_id FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                    UUID.class,
                    reservationId,
                    EventTypes.INVENTORY_SEAT_CONFIRMED
            );
            assertThat(outboxCorrelationId).isEqualTo(UUID.fromString(incomingCorrelationId));
            assertThat(MDC.get(CorrelationConstants.MDC_KEY)).isEqualTo(staleMdc);
        } finally {
            MDC.remove(CorrelationConstants.MDC_KEY);
        }
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    private void awaitListenerAssignment() throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            Collection<MessageListenerContainer> containers = listenerEndpointRegistry.getListenerContainers();
            boolean allAssigned = !containers.isEmpty() && containers.stream()
                    .filter(c -> c.getContainerProperties().getTopics() != null
                            && List.of(c.getContainerProperties().getTopics()).contains(Topics.PAYMENT_EVENTS))
                    .allMatch(c -> c.getAssignedPartitions() != null && !c.getAssignedPartitions().isEmpty());
            if (allAssigned) {
                return;
            }
            Thread.sleep(50);
        }
    }

    private UUID insertOnSaleEvent(String title) {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        transactionTemplate.executeWithoutResult(status -> eventRepository.insert(new EventRecord(
                eventId,
                title,
                "Wembley Stadium",
                now.plus(30, ChronoUnit.DAYS),
                now.minus(1, ChronoUnit.HOURS),
                "ON_SALE",
                now,
                now
        )));
        return eventId;
    }

    private UUID insertSeat(UUID eventId, String section, String row, String number, long priceCents, String seatStatus) {
        UUID seatId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        transactionTemplate.executeWithoutResult(status -> seatRepository.bulkInsert(List.of(new SeatRecord(
                seatId,
                eventId,
                section,
                row,
                number,
                priceCents,
                "USD",
                seatStatus,
                0L,
                now,
                now
        ))));
        return seatId;
    }

    private ReservationResponse createHoldViaHttp(UUID eventId, UUID userId, List<UUID> seatIds, String correlationId) throws Exception {
        CreateReservationRequest req = new CreateReservationRequest(eventId, seatIds);
        MvcResult res = mockMvc.perform(post("/api/v1/reservations")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                        .header("X-User-Roles", "USER")
                        .header(CorrelationConstants.HTTP_HEADER, correlationId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isCreated())
                .andReturn();
        return objectMapper.readValue(res.getResponse().getContentAsString(), ReservationResponse.class);
    }

    private void publishPaymentSucceeded(UUID eventId,
                                         UUID paymentId,
                                         UUID reservationId,
                                         UUID userId,
                                         long amountCents,
                                         String correlationId) throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        PaymentSucceededPayload payload = new PaymentSucceededPayload(
                paymentId,
                reservationId,
                reservationId,
                userId,
                amountCents,
                "USD",
                "pi_" + paymentId,
                now
        );
        EventEnvelope<PaymentSucceededPayload> envelope = new EventEnvelope<>(
                eventId,
                EventTypes.PAYMENT_SUCCEEDED,
                now,
                correlationId,
                1,
                payload
        );
        ProducerRecord<String, String> record = new ProducerRecord<>(
                Topics.PAYMENT_EVENTS,
                reservationId.toString(),
                objectMapper.writeValueAsString(envelope)
        );
        record.headers().add(CorrelationConstants.KAFKA_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
        testProducer.send(record).get(5, TimeUnit.SECONDS);
    }

    private void publishPaymentFailed(UUID eventId,
                                      UUID paymentId,
                                      UUID reservationId,
                                      UUID userId,
                                      long amountCents,
                                      String correlationId) throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        PaymentFailedPayload payload = new PaymentFailedPayload(
                paymentId,
                reservationId,
                reservationId,
                userId,
                amountCents,
                "USD",
                "CARD_DECLINED",
                "Insufficient funds",
                now
        );
        EventEnvelope<PaymentFailedPayload> envelope = new EventEnvelope<>(
                eventId,
                EventTypes.PAYMENT_FAILED,
                now,
                correlationId,
                1,
                payload
        );
        ProducerRecord<String, String> record = new ProducerRecord<>(
                Topics.PAYMENT_EVENTS,
                reservationId.toString(),
                objectMapper.writeValueAsString(envelope)
        );
        record.headers().add(CorrelationConstants.KAFKA_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
        testProducer.send(record).get(5, TimeUnit.SECONDS);
    }

    private Map<TopicPartition, Long> currentEndOffsets(String topic) {
        try (KafkaConsumer<String, String> consumer = newTestConsumer()) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic, Duration.ofSeconds(5));
            List<TopicPartition> topicPartitions = partitions.stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            return new HashMap<>(consumer.endOffsets(topicPartitions, Duration.ofSeconds(5)));
        }
    }

    private ConsumerRecord<String, String> awaitSingleRecord(String topic,
                                                             Map<TopicPartition, Long> startOffsets,
                                                             Predicate<ConsumerRecord<String, String>> filter,
                                                             Duration timeout) {
        try (KafkaConsumer<String, String> consumer = newTestConsumer()) {
            consumer.assign(startOffsets.keySet());
            for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (filter.test(record)) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("Timed out after " + timeout + " waiting for matching record on topic " + topic);
    }

    private void assertNoNewRecordsMatching(String topic,
                                            Map<TopicPartition, Long> startOffsets,
                                            Predicate<ConsumerRecord<String, String>> filter) {
        Map<TopicPartition, Long> currentEnds = currentEndOffsets(topic);
        boolean anyDifferent = false;
        for (Map.Entry<TopicPartition, Long> entry : currentEnds.entrySet()) {
            Long start = startOffsets.get(entry.getKey());
            if (start != null && entry.getValue() > start) {
                anyDifferent = true;
                break;
            }
        }
        if (!anyDifferent) {
            return;
        }

        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = newTestConsumer()) {
            consumer.assign(startOffsets.keySet());
            for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }

            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                boolean allCaughtUp = true;
                for (Map.Entry<TopicPartition, Long> entry : currentEnds.entrySet()) {
                    long currentPosition = consumer.position(entry.getKey());
                    if (currentPosition < entry.getValue()) {
                        allCaughtUp = false;
                        break;
                    }
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (filter.test(record)) {
                        matched.add(record);
                    }
                }
                if (allCaughtUp) {
                    break;
                }
            }
        }
        assertThat(matched).as("Expected no matching records on topic " + topic).isEmpty();
    }

    private KafkaConsumer<String, String> newTestConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"
        ));
    }

    private static String headerValue(ConsumerRecord<String, String> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private int countOutboxEvents(UUID aggregateId, String eventType) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                Integer.class,
                aggregateId,
                eventType
        );
        return count == null ? 0 : count;
    }

    private double counterValue(String name, String... tags) {
        Counter counter = meterRegistry.find(name).tags(tags).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static void awaitCondition(Duration timeout, java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        assertThat(condition.getAsBoolean()).as("Condition not met within " + timeout).isTrue();
    }
}

