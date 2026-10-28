package com.ticketing.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.events.PaymentSucceededPayload;
import com.ticketing.common.json.ObjectMapperFactory;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class OutboxIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
    }

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4-alpine"))
                    .withDatabaseName("outbox_test_db")
                    .withUsername("ticketing")
                    .withPassword("ticketing_secret");

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbcTemplate;
    private static TransactionTemplate transactionTemplate;
    private static DefaultKafkaProducerFactory<String, String> producerFactory;
    private static KafkaTemplate<String, String> kafkaTemplate;
    private static final ObjectMapper MAPPER = ObjectMapperFactory.create();

    @BeforeAll
    static void setUpInfra() throws Exception {
        dataSource = newHikariPool("outbox-test-main", 5);
        jdbcTemplate = new JdbcTemplate(dataSource);
        transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));

        ResourceDatabasePopulator populator = new ResourceDatabasePopulator(
                new ClassPathResource("db/outbox/outbox_events.sql"));
        populator.execute(dataSource);

        Map<String, Object> producerProps = OutboxProducerProperties.defaults(KAFKA.getBootstrapServers());
        producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.PAYMENT_EVENTS, 3, (short) 1),
                    new NewTopic(Topics.ORDER_EVENTS, 3, (short) 1),
                    new NewTopic(Topics.INVENTORY_EVENTS, 3, (short) 1)
            )).all().get(10, TimeUnit.SECONDS);
        }
    }

    @AfterAll
    static void tearDownInfra() {
        if (producerFactory != null) {
            producerFactory.destroy();
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @BeforeEach
    void cleanTable() {
        jdbcTemplate.execute("TRUNCATE TABLE outbox_events");
        MDC.clear();
    }

    private static HikariDataSource newHikariPool(String poolName, int maxPoolSize) {
        HikariConfig config = new HikariConfig();
        config.setPoolName(poolName);
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(maxPoolSize);
        return new HikariDataSource(config);
    }

    private PaymentSucceededPayload samplePayload(UUID orderId, long amountCents) {
        return new PaymentSucceededPayload(
                UUID.randomUUID(),
                orderId,
                orderId,
                UUID.randomUUID(),
                amountCents,
                "USD",
                "mock_ref_" + amountCents,
                Instant.parse("2026-10-27T22:01:05.100000Z")
        );
    }

    private OutboxProperties defaultProps(long lockId) {
        OutboxProperties props = new OutboxProperties();
        props.setEnabled(true);
        props.setAdvisoryLockId(lockId);
        props.setBatchSize(100);
        props.setSendTimeout(Duration.ofMillis(5000));
        return props;
    }

    @Test
    void outboxWriterThrowsWhenNoTransactionIsActive() {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID orderId = UUID.randomUUID();

        assertThatThrownBy(() -> writer.append(orderId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(orderId, 1000L)))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void outboxWriterRejectsUnrecognizedEventTypePrefix() {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID orderId = UUID.randomUUID();

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status ->
                writer.append(orderId, "unknown.event.type", samplePayload(orderId, 1000L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported eventType prefix");

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status ->
                writer.append(orderId, "CUSTOM", Topics.PAYMENT_EVENTS, "unknown.event.type", samplePayload(orderId, 1000L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported eventType prefix");

        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Long.class);
        assertThat(count).isZero();
    }

    @Test
    void pollRejectsActiveTransaction() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(jdbcTemplate, registry);
        OutboxPoller poller = new OutboxPoller(
                dataSource, jdbcTemplate, kafkaTemplate, MAPPER, defaultProps(71010L), metrics);
        try {
            assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> poller.poll()))
                    .isInstanceOf(IllegalTransactionStateException.class)
                    .hasMessageContaining("must not be called inside an active transaction");
        } finally {
            poller.destroy();
        }
    }

    @Test
    void startupFailsWhenProducerAcksIsNotAll() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(jdbcTemplate, registry);

        Map<String, Object> acksOneProps = new HashMap<>(OutboxProducerProperties.defaults(KAFKA.getBootstrapServers()));
        acksOneProps.put(ProducerConfig.ACKS_CONFIG, "1");
        acksOneProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        DefaultKafkaProducerFactory<String, String> badFactory1 = new DefaultKafkaProducerFactory<>(acksOneProps);
        KafkaTemplate<String, String> badTemplate1 = new KafkaTemplate<>(badFactory1);

        assertThatThrownBy(() -> new OutboxPoller(
                dataSource, jdbcTemplate, badTemplate1, MAPPER, defaultProps(71011L), metrics))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("acks=all");

        Map<String, Object> nonIdempotentProps = new HashMap<>(OutboxProducerProperties.defaults(KAFKA.getBootstrapServers()));
        nonIdempotentProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        DefaultKafkaProducerFactory<String, String> badFactory2 = new DefaultKafkaProducerFactory<>(nonIdempotentProps);
        KafkaTemplate<String, String> badTemplate2 = new KafkaTemplate<>(badFactory2);

        assertThatThrownBy(() -> new OutboxPoller(
                dataSource, jdbcTemplate, badTemplate2, MAPPER, defaultProps(71012L), metrics))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("enable.idempotence=true");
    }

    @Test
    void publishesRowsInExactCreationOrderPerAggregateWithHeadersAndEnvelope() throws Exception {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID aggregateId = UUID.randomUUID();
        String correlationId = "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e";
        MDC.put(CorrelationConstants.MDC_KEY, correlationId);

        List<UUID> expectedEventIds = new ArrayList<>();
        List<Instant> expectedOccurredAt = new ArrayList<>();
        transactionTemplate.executeWithoutResult(status -> {
            for (int i = 1; i <= 5; i++) {
                EventEnvelope<PaymentSucceededPayload> env = writer.append(
                        aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, i * 1000L));
                expectedEventIds.add(env.eventId());
                expectedOccurredAt.add(env.occurredAt());
            }
        });

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(jdbcTemplate, meterRegistry);
        assertThat(metrics.queryPendingRows()).isEqualTo(5.0);

        OutboxPoller poller = new OutboxPoller(
                dataSource, jdbcTemplate, kafkaTemplate, MAPPER, defaultProps(71001L), metrics);
        try {
            int published = poller.poll();
            assertThat(published).isEqualTo(5);
            assertThat(metrics.queryPendingRows()).isEqualTo(0.0);
            assertThat(meterRegistry.get("outbox_published_total").counter().count()).isEqualTo(5.0);
        } finally {
            poller.destroy();
        }

        List<ConsumerRecord<String, String>> records = consumeRecords(Topics.PAYMENT_EVENTS, 5, expectedEventIds.get(0));
        assertThat(records).hasSize(5);

        for (int i = 0; i < 5; i++) {
            ConsumerRecord<String, String> rec = records.get(i);
            assertThat(rec.key()).isEqualTo(aggregateId.toString());
            assertThat(header(rec, CorrelationConstants.KAFKA_HEADER)).isEqualTo(correlationId);
            assertThat(header(rec, OutboxPoller.HEADER_EVENT_TYPE)).isEqualTo(EventTypes.PAYMENT_SUCCEEDED);
            assertThat(header(rec, OutboxPoller.HEADER_EVENT_ID)).isEqualTo(expectedEventIds.get(i).toString());
            assertThat(header(rec, OutboxPoller.HEADER_SCHEMA_VERSION)).isEqualTo("1");

            JsonNode json = MAPPER.readTree(rec.value());
            assertThat(json.get("eventId").asText()).isEqualTo(expectedEventIds.get(i).toString());
            assertThat(json.get("eventType").asText()).isEqualTo(EventTypes.PAYMENT_SUCCEEDED);
            assertThat(json.get("correlationId").asText()).isEqualTo(correlationId);
            assertThat(json.get("schemaVersion").asInt()).isEqualTo(1);
            assertThat(Instant.parse(json.get("occurredAt").asText())).isEqualTo(expectedOccurredAt.get(i));
            assertThat(json.get("payload").get("amountCents").asLong()).isEqualTo((i + 1) * 1000L);
        }
    }

    @Test
    void outboxOrderingUsesDatabaseClockUnderApplicationClockSkew() throws Exception {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID aggregateId = UUID.randomUUID();

        // Event 1: payload timestamp is artificially skewed +10 minutes into the future
        PaymentSucceededPayload skewedFuturePayload = new PaymentSucceededPayload(
                UUID.randomUUID(),
                aggregateId,
                aggregateId,
                UUID.randomUUID(),
                1000L,
                "USD",
                "mock_ref_first",
                Instant.now().plus(Duration.ofMinutes(10))
        );
        EventEnvelope<PaymentSucceededPayload> firstEnv = transactionTemplate.execute(status ->
                writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, skewedFuturePayload));

        // Event 2: payload timestamp is artificially skewed -10 minutes into the past, committed after Event 1
        PaymentSucceededPayload skewedPastPayload = new PaymentSucceededPayload(
                UUID.randomUUID(),
                aggregateId,
                aggregateId,
                UUID.randomUUID(),
                2000L,
                "USD",
                "mock_ref_second",
                Instant.now().minus(Duration.ofMinutes(10))
        );
        EventEnvelope<PaymentSucceededPayload> secondEnv = transactionTemplate.execute(status ->
                writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, skewedPastPayload));

        assertThat(firstEnv).isNotNull();
        assertThat(secondEnv).isNotNull();

        // Verify occurredAt in the envelope is truncated to microseconds and equals the stored PostgreSQL value
        OffsetDateTime storedFirst = jdbcTemplate.queryForObject(
                "SELECT created_at FROM outbox_events WHERE event_id = ?",
                (rs, rowNum) -> rs.getObject(1, OffsetDateTime.class),
                firstEnv.eventId());
        OffsetDateTime storedSecond = jdbcTemplate.queryForObject(
                "SELECT created_at FROM outbox_events WHERE event_id = ?",
                (rs, rowNum) -> rs.getObject(1, OffsetDateTime.class),
                secondEnv.eventId());

        assertThat(firstEnv.occurredAt()).isEqualTo(storedFirst.toInstant().truncatedTo(ChronoUnit.MICROS));
        assertThat(secondEnv.occurredAt()).isEqualTo(storedSecond.toInstant().truncatedTo(ChronoUnit.MICROS));
        assertThat(firstEnv.occurredAt()).isBeforeOrEqualTo(secondEnv.occurredAt());

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(jdbcTemplate, registry);
        OutboxPoller poller = new OutboxPoller(
                dataSource, jdbcTemplate, kafkaTemplate, MAPPER, defaultProps(71020L), metrics);
        try {
            assertThat(poller.poll()).isEqualTo(2);
        } finally {
            poller.destroy();
        }

        List<ConsumerRecord<String, String>> records = consumeRecords(Topics.PAYMENT_EVENTS, 2, firstEnv.eventId());
        assertThat(records).extracting(r -> UUID.fromString(header(r, OutboxPoller.HEADER_EVENT_ID)))
                .containsExactly(firstEnv.eventId(), secondEnv.eventId());
    }

    @Test
    void failedSendStopsBatchImmediatelyAndNextCycleResumesInOrderWithNoRowSkipped() throws Exception {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID aggregateId = UUID.randomUUID();

        List<UUID> expectedIds = new ArrayList<>();
        transactionTemplate.executeWithoutResult(status -> {
            for (int i = 1; i <= 5; i++) {
                expectedIds.add(writer.append(
                        aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, i * 1000L)).eventId());
            }
        });

        AtomicInteger callCount = new AtomicInteger();
        @SuppressWarnings("unchecked")
        KafkaOperations<String, String> faultyKafka = (KafkaOperations<String, String>) Proxy.newProxyInstance(
                KafkaOperations.class.getClassLoader(),
                new Class<?>[]{KafkaOperations.class},
                (proxy, method, args) -> {
                    if ("send".equals(method.getName()) && args != null && args.length == 1 && args[0] instanceof ProducerRecord<?, ?>) {
                        int n = callCount.incrementAndGet();
                        if (n == 3) {
                            CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
                            failed.completeExceptionally(new RuntimeException("Simulated broker timeout on 3rd record"));
                            return failed;
                        }
                    }
                    return method.invoke(kafkaTemplate, args);
                }
        );

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(jdbcTemplate, meterRegistry);
        OutboxPoller poller = new OutboxPoller(
                dataSource, jdbcTemplate, faultyKafka, MAPPER, defaultProps(71002L), metrics);

        try {
            int firstCyclePublished = poller.poll();
            assertThat(firstCyclePublished).isEqualTo(2);
            assertThat(callCount.get()).isEqualTo(3);
            assertThat(metrics.queryPendingRows()).isEqualTo(3.0);
            assertThat(meterRegistry.get("outbox_publish_failures_total").counter().count()).isEqualTo(1.0);

            List<UUID> unpublishedAfterFirst = jdbcTemplate.queryForList(
                    "SELECT event_id FROM outbox_events WHERE published_at IS NULL ORDER BY created_at ASC, event_id ASC",
                    UUID.class);
            assertThat(unpublishedAfterFirst).containsExactly(expectedIds.get(2), expectedIds.get(3), expectedIds.get(4));

            int secondCyclePublished = poller.poll();
            assertThat(secondCyclePublished).isEqualTo(3);
            assertThat(metrics.queryPendingRows()).isEqualTo(0.0);
        } finally {
            poller.destroy();
        }

        List<ConsumerRecord<String, String>> records = consumeRecords(Topics.PAYMENT_EVENTS, 5, expectedIds.get(0));
        assertThat(records).extracting(r -> UUID.fromString(header(r, OutboxPoller.HEADER_EVENT_ID)))
                .containsExactlyElementsOf(expectedIds);
    }

    @Test
    void crashBeforeMarkingRowPublishedLeadsToRepublishAndNeverLosesRow() throws Exception {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID aggregateId = UUID.randomUUID();
        UUID eventId = transactionTemplate.execute(status ->
                writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, 9900L)).eventId());

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(jdbcTemplate, meterRegistry);
        AtomicInteger markCalls = new AtomicInteger();

        OutboxPoller crashingPoller = new OutboxPoller(
                dataSource, jdbcTemplate, kafkaTemplate, MAPPER, defaultProps(71003L), metrics) {
            @Override
            void markPublished(UUID id) throws SQLException {
                if (markCalls.incrementAndGet() == 1) {
                    throw new RuntimeException("Simulated crash after broker ack before UPDATE outbox_events");
                }
                super.markPublished(id);
            }
        };

        try {
            int firstCycle = crashingPoller.poll();
            assertThat(firstCycle).isZero();
            assertThat(metrics.queryPendingRows()).isEqualTo(1.0);

            int secondCycle = crashingPoller.poll();
            assertThat(secondCycle).isEqualTo(1);
            assertThat(metrics.queryPendingRows()).isEqualTo(0.0);
        } finally {
            crashingPoller.destroy();
        }

        List<ConsumerRecord<String, String>> records = consumeRecords(Topics.PAYMENT_EVENTS, 2, eventId);
        assertThat(records).hasSize(2);
        assertThat(records).allSatisfy(r ->
                assertThat(header(r, OutboxPoller.HEADER_EVENT_ID)).isEqualTo(eventId.toString()));
    }

    @Test
    void twoPollerInstancesElectSingleLeaderAndStandbyTakesOverWhenLeaderConnectionCloses() {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID aggregateId = UUID.randomUUID();

        transactionTemplate.executeWithoutResult(status -> {
            writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, 1000L));
            writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, 2000L));
        });

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(jdbcTemplate, registry);
        long sharedLockId = 71099L;

        try (HikariDataSource poolA = newHikariPool("poller-pool-a", 2);
             HikariDataSource poolB = newHikariPool("poller-pool-b", 2)) {
            OutboxPoller pollerA = new OutboxPoller(
                    poolA, new JdbcTemplate(poolA), kafkaTemplate, MAPPER, defaultProps(sharedLockId), metrics);
            OutboxPoller pollerB = new OutboxPoller(
                    poolB, new JdbcTemplate(poolB), kafkaTemplate, MAPPER, defaultProps(sharedLockId), metrics);

            try {
                int publishedByA = pollerA.poll();
                assertThat(publishedByA).isEqualTo(2);
                assertThat(pollerA.isLeader()).isTrue();

                transactionTemplate.executeWithoutResult(status -> {
                    writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, 3000L));
                    writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, 4000L));
                });

                int publishedByBWhileALeader = pollerB.poll();
                assertThat(publishedByBWhileALeader).isZero();
                assertThat(pollerB.isLeader()).isFalse();
                assertThat(metrics.queryPendingRows()).isEqualTo(2.0);

                pollerA.releaseLeadership();
                assertThat(pollerA.isLeader()).isFalse();

                int publishedByBAfterFailover = pollerB.poll();
                assertThat(publishedByBAfterFailover).isEqualTo(2);
                assertThat(pollerB.isLeader()).isTrue();
                assertThat(metrics.queryPendingRows()).isEqualTo(0.0);
            } finally {
                pollerA.destroy();
                pollerB.destroy();
            }
        }
    }

    @Test
    void leaderFailoverAcrossSeparateConnectionPoolsReleasesAdvisoryLock() {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID aggregateId = UUID.randomUUID();
        long lockId = 71088L;

        transactionTemplate.executeWithoutResult(status ->
                writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, 1000L)));

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(jdbcTemplate, registry);

        try (HikariDataSource poolA = newHikariPool("failover-pool-a", 2);
             HikariDataSource poolB = newHikariPool("failover-pool-b", 2)) {
            OutboxPoller pollerA = new OutboxPoller(
                    poolA, new JdbcTemplate(poolA), kafkaTemplate, MAPPER, defaultProps(lockId), metrics);
            OutboxPoller pollerB = new OutboxPoller(
                    poolB, new JdbcTemplate(poolB), kafkaTemplate, MAPPER, defaultProps(lockId), metrics);

            try {
                assertThat(pollerA.poll()).isEqualTo(1);
                assertThat(pollerA.isLeader()).isTrue();
                assertThat(countAdvisoryLockHolders(lockId)).isEqualTo(1);

                // Release leadership on pollerA while poolA remains open
                pollerA.releaseLeadership();
                assertThat(pollerA.isLeader()).isFalse();
                assertThat(countAdvisoryLockHolders(lockId)).isZero();

                transactionTemplate.executeWithoutResult(status ->
                        writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, 2000L)));

                assertThat(pollerB.poll()).isEqualTo(1);
                assertThat(pollerB.isLeader()).isTrue();
                assertThat(countAdvisoryLockHolders(lockId)).isEqualTo(1);
            } finally {
                pollerA.destroy();
                pollerB.destroy();
            }
        }
    }

    @Test
    void midBatchLeaderConnectionDropStopsStaleLeaderBeforeSendingNextRow() {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID aggregateId = UUID.randomUUID();
        long lockId = 71077L;

        List<UUID> expectedIds = new ArrayList<>();
        transactionTemplate.executeWithoutResult(status -> {
            for (int i = 1; i <= 5; i++) {
                expectedIds.add(writer.append(
                        aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, i * 1000L)).eventId());
            }
        });

        List<UUID> pollerASentEventIds = Collections.synchronizedList(new ArrayList<>());
        @SuppressWarnings("unchecked")
        KafkaOperations<String, String> trackingKafkaA = (KafkaOperations<String, String>) Proxy.newProxyInstance(
                KafkaOperations.class.getClassLoader(),
                new Class<?>[]{KafkaOperations.class},
                (proxy, method, args) -> {
                    if ("send".equals(method.getName()) && args != null && args.length == 1
                            && args[0] instanceof ProducerRecord<?, ?> prodRec) {
                        var h = prodRec.headers().lastHeader(OutboxPoller.HEADER_EVENT_ID);
                        if (h != null) {
                            pollerASentEventIds.add(UUID.fromString(new String(h.value(), StandardCharsets.UTF_8)));
                        }
                    }
                    return method.invoke(kafkaTemplate, args);
                }
        );

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(jdbcTemplate, registry);
        AtomicInteger publishedByStandby = new AtomicInteger();

        try (HikariDataSource poolA = newHikariPool("midbatch-pool-a", 2);
             HikariDataSource poolB = newHikariPool("midbatch-pool-b", 2)) {
            OutboxPoller pollerA = new OutboxPoller(
                    poolA, new JdbcTemplate(poolA), trackingKafkaA, MAPPER, defaultProps(lockId), metrics);
            OutboxPoller pollerB = new OutboxPoller(
                    poolB, new JdbcTemplate(poolB), kafkaTemplate, MAPPER, defaultProps(lockId), metrics);

            // Deterministic hook before row index 2 (i.e., after row 1 and row 2 have been published):
            // terminate pollerA's backend in PostgreSQL and let pollerB take over and publish rows 3..5.
            pollerA.setBeforeSendHook((rowIndex, row) -> {
                if (rowIndex == 2) {
                    jdbcTemplate.queryForList(
                            "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND granted = true AND objid = ?",
                            Boolean.class,
                            (int) lockId
                    );
                    publishedByStandby.set(pollerB.poll());
                }
            });

            try {
                int publishedByA = pollerA.poll();
                assertThat(publishedByA).isEqualTo(2);
                assertThat(pollerA.isLeader()).isFalse();
                // Stale poller sent only rows 1 and 2 and nothing after the connection drop
                assertThat(pollerASentEventIds).containsExactly(expectedIds.get(0), expectedIds.get(1));

                assertThat(publishedByStandby.get()).isEqualTo(3);
                assertThat(pollerB.isLeader()).isTrue();
                assertThat(metrics.queryPendingRows()).isEqualTo(0.0);
            } finally {
                pollerA.destroy();
                pollerB.destroy();
            }
        }

        List<ConsumerRecord<String, String>> records = consumeRecords(Topics.PAYMENT_EVENTS, 5, expectedIds.get(0));
        assertThat(records).extracting(r -> UUID.fromString(header(r, OutboxPoller.HEADER_EVENT_ID)))
                .containsExactlyElementsOf(expectedIds);
    }

    @Test
    void retentionCleanerLoopsUntilAllExpiredBatchesArePurged() {
        OutboxWriter writer = new OutboxWriter(jdbcTemplate, MAPPER);
        UUID aggregateId = UUID.randomUUID();

        List<UUID> expiredIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final long amount = (i + 1) * 1000L;
            expiredIds.add(transactionTemplate.execute(status ->
                    writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, amount)).eventId()));
        }
        UUID recentPublishedId = transactionTemplate.execute(status ->
                writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, 6000L)).eventId());
        UUID unpublishedId = transactionTemplate.execute(status ->
                writer.append(aggregateId, EventTypes.PAYMENT_SUCCEEDED, samplePayload(aggregateId, 7000L)).eventId());

        OffsetDateTime eightyHoursAgo = OffsetDateTime.ofInstant(
                Instant.now().minus(Duration.ofHours(80)).truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC);
        OffsetDateTime oneHourAgo = OffsetDateTime.ofInstant(
                Instant.now().minus(Duration.ofHours(1)).truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC);

        for (UUID expiredId : expiredIds) {
            jdbcTemplate.update(
                    "UPDATE outbox_events SET published_at = ? WHERE event_id = ?",
                    new Object[]{eightyHoursAgo, expiredId},
                    new int[]{Types.TIMESTAMP_WITH_TIMEZONE, Types.OTHER});
        }
        jdbcTemplate.update(
                "UPDATE outbox_events SET published_at = ? WHERE event_id = ?",
                new Object[]{oneHourAgo, recentPublishedId},
                new int[]{Types.TIMESTAMP_WITH_TIMEZONE, Types.OTHER});

        OutboxProperties props = defaultProps(71005L);
        props.setRetention(Duration.ofHours(72));
        props.setCleanupBatchSize(2);

        OutboxRetentionCleaner cleaner = new OutboxRetentionCleaner(jdbcTemplate, props);
        int deleted = cleaner.purgeExpired();
        assertThat(deleted).isEqualTo(5);

        List<UUID> remaining = jdbcTemplate.queryForList("SELECT event_id FROM outbox_events", UUID.class);
        assertThat(remaining).containsExactlyInAnyOrder(recentPublishedId, unpublishedId);
    }

    private static int countAdvisoryLockHolders(long lockId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND granted = true AND objid = ?",
                Integer.class,
                (int) lockId
        );
        return count == null ? 0 : count;
    }

    private static String header(ConsumerRecord<String, String> record, String key) {
        var h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static List<ConsumerRecord<String, String>> consumeRecords(String topic, int expectedCount, UUID firstEventId) {
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );

        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        boolean seenFirst = false;
        long deadline = System.currentTimeMillis() + 15_000L;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (matched.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> rec : polled) {
                    if (!seenFirst && firstEventId.toString().equals(header(rec, OutboxPoller.HEADER_EVENT_ID))) {
                        seenFirst = true;
                    }
                    if (seenFirst) {
                        matched.add(rec);
                        if (matched.size() == expectedCount) {
                            break;
                        }
                    }
                }
            }
        }
        return matched;
    }
}
