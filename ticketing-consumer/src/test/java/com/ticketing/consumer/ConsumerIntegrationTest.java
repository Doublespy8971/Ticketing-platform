package com.ticketing.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.json.ObjectMapperFactory;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.consumer.autoconfigure.ConsumerAutoConfiguration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringJUnitConfig(classes = ConsumerIntegrationTest.TestConfig.class)
class ConsumerIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
    }

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4-alpine"))
                    .withDatabaseName("consumer_test_db")
                    .withUsername("ticketing")
                    .withPassword("ticketing_secret");

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private static final ObjectMapper MAPPER = ObjectMapperFactory.create();
    private static HikariDataSource sharedDataSource;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.application.name", () -> "order-service");
        registry.add("ticketing.consumer.cleanup.batch-size", () -> "2");
        registry.add("ticketing.consumer.cleanup.retention-hours", () -> "192");
    }

    @BeforeAll
    static void provisionBrokerTopicsFromCreateTopicsScript() throws Exception {
        String scriptContent = Files.readString(Path.of("../infra/kafka/create-topics.sh"));
        List<NewTopic> topicsToCreate = new ArrayList<>();

        // Match infra/kafka/create-topics.sh: 12 partitions for main and retry topics, 3 partitions for DLT topics
        for (String primary : List.of(Topics.INVENTORY_EVENTS, Topics.ORDER_EVENTS, Topics.PAYMENT_EVENTS)) {
            assertThat(scriptContent).contains(primary);
            topicsToCreate.add(new NewTopic(primary, 12, (short) 1));
        }

        Matcher matcher = Pattern.compile("\"([a-z0-9.\\-]+-cg)\"").matcher(scriptContent);
        List<String> retryBases = new ArrayList<>();
        while (matcher.find()) {
            String base = matcher.group(1);
            retryBases.add(base);
            topicsToCreate.add(new NewTopic(base + "-retry-0", 12, (short) 1));
            topicsToCreate.add(new NewTopic(base + "-retry-1", 12, (short) 1));
            topicsToCreate.add(new NewTopic(base + "-dlt", 3, (short) 1));
        }
        assertThat(retryBases).containsExactly(
                "inventory.events-order-service-inventory-cg",
                "order.events-payment-service-order-cg",
                "payment.events-inventory-service-payment-cg",
                "payment.events-order-service-payment-cg"
        );

        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(topicsToCreate).all().get(15, TimeUnit.SECONDS);
        }
    }

    @AfterAll
    static void closeSharedDataSource() {
        if (sharedDataSource != null) {
            sharedDataSource.close();
        }
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private IdempotentEventProcessor processor;

    @Autowired
    private ConsumerMetrics metrics;

    @Autowired
    private ProcessedEventsRetentionCleaner cleaner;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TestKafkaListeners testListeners;

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("TRUNCATE TABLE processed_events, test_domain_events");
        testListeners.reset();
    }

    @Test
    void allTopicsFromCreateTopicsScriptExistOnBrokerAndMatchRetryTopicNamesProvider() throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            Set<String> brokerTopics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(brokerTopics).contains(
                    Topics.INVENTORY_EVENTS,
                    Topics.ORDER_EVENTS,
                    Topics.PAYMENT_EVENTS,
                    Topics.INVENTORY_EVENTS_ORDER_SERVICE_RETRY_0,
                    Topics.INVENTORY_EVENTS_ORDER_SERVICE_RETRY_1,
                    Topics.INVENTORY_EVENTS_ORDER_SERVICE_DLT,
                    Topics.ORDER_EVENTS_PAYMENT_SERVICE_RETRY_0,
                    Topics.ORDER_EVENTS_PAYMENT_SERVICE_RETRY_1,
                    Topics.ORDER_EVENTS_PAYMENT_SERVICE_DLT,
                    Topics.PAYMENT_EVENTS_INVENTORY_SERVICE_RETRY_0,
                    Topics.PAYMENT_EVENTS_INVENTORY_SERVICE_RETRY_1,
                    Topics.PAYMENT_EVENTS_INVENTORY_SERVICE_DLT,
                    Topics.PAYMENT_EVENTS_ORDER_SERVICE_RETRY_0,
                    Topics.PAYMENT_EVENTS_ORDER_SERVICE_RETRY_1,
                    Topics.PAYMENT_EVENTS_ORDER_SERVICE_DLT
            );
        }
    }

    @Test
    void duplicateEventDeliveredTwiceRunsBusinessLogicOnceAndIncrementsDuplicateCounter() {
        UUID eventId = UUID.randomUUID();
        String group = "order-service-inventory-cg";
        AtomicInteger runs = new AtomicInteger();

        double beforeProcessed = counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, group, EventTypes.INVENTORY_SEAT_RESERVED);
        double beforeDuplicate = counterValue(ConsumerMetrics.DUPLICATE_TOTAL_METRIC, group, EventTypes.INVENTORY_SEAT_RESERVED);

        boolean first = processor.process(eventId, group, EventTypes.INVENTORY_SEAT_RESERVED, () -> {
            runs.incrementAndGet();
            jdbcTemplate.update("INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)", eventId, "first");
        });
        boolean second = processor.process(eventId, group, EventTypes.INVENTORY_SEAT_RESERVED, () -> {
            runs.incrementAndGet();
            jdbcTemplate.update("INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)", eventId, "second");
        });

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(runs.get()).isEqualTo(1);
        assertThat(countProcessedEvents(eventId, group)).isEqualTo(1);
        assertThat(countDomainRows(eventId)).isEqualTo(1);
        assertThat(counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, group, EventTypes.INVENTORY_SEAT_RESERVED))
                .isEqualTo(beforeProcessed + 1.0);
        assertThat(counterValue(ConsumerMetrics.DUPLICATE_TOTAL_METRIC, group, EventTypes.INVENTORY_SEAT_RESERVED))
                .isEqualTo(beforeDuplicate + 1.0);
    }

    @Test
    void duplicateAcrossMainGroupAndRetry0GroupRunsLogicOnceWithMetricsUnderMainGroup() throws Exception {
        UUID eventId = UUID.randomUUID();
        String mainGroup = "order-service-payment-cg";
        String retry0Group = "order-service-payment-cg-retry-0";
        AtomicInteger runs = new AtomicInteger();

        double beforeProcessed = counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, mainGroup, EventTypes.PAYMENT_SUCCEEDED);
        double beforeDuplicate = counterValue(ConsumerMetrics.DUPLICATE_TOTAL_METRIC, mainGroup, EventTypes.PAYMENT_SUCCEEDED);

        String json = buildEnvelopeJson(eventId, EventTypes.PAYMENT_SUCCEEDED, "corr-retry-group-dedup", 1, Map.of("k", "v"));
        boolean first = processor.process(json, mainGroup, (EventEnvelope<JsonNode> env) -> {
            runs.incrementAndGet();
            jdbcTemplate.update("INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)", eventId, "main");
        });
        boolean second = processor.process(json, retry0Group, (EventEnvelope<JsonNode> env) -> {
            runs.incrementAndGet();
            jdbcTemplate.update("INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)", eventId, "retry-0");
        });

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(runs.get()).isEqualTo(1);
        assertThat(countProcessedEvents(eventId, mainGroup)).isEqualTo(1);
        assertThat(countProcessedEvents(eventId, retry0Group)).isZero();
        assertThat(countDomainRows(eventId)).isEqualTo(1);
        assertThat(counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, mainGroup, EventTypes.PAYMENT_SUCCEEDED))
                .isEqualTo(beforeProcessed + 1.0);
        assertThat(counterValue(ConsumerMetrics.DUPLICATE_TOTAL_METRIC, mainGroup, EventTypes.PAYMENT_SUCCEEDED))
                .isEqualTo(beforeDuplicate + 1.0);
        assertThat(metrics.getMeterRegistry().find(ConsumerMetrics.DUPLICATE_TOTAL_METRIC)
                .tag("consumer_group", retry0Group)
                .counter()).isNull();
    }

    @Test
    void businessLogicMarkingTransactionRollbackOnlyThrowsUnexpectedRollbackExceptionWithEmptyTableAndUnchangedCounter() {
        UUID eventId = UUID.randomUUID();
        String group = "order-service-payment-cg";
        double beforeProcessed = counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED);

        assertThatThrownBy(() -> processor.process(eventId, group, EventTypes.PAYMENT_SUCCEEDED, () -> {
            jdbcTemplate.update("INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)", eventId, "rollback-only");
            new TransactionTemplate(transactionManager).executeWithoutResult(status -> status.setRollbackOnly());
        })).isInstanceOf(UnexpectedRollbackException.class);

        assertThat(countProcessedEvents(eventId, group)).isZero();
        assertThat(countDomainRows(eventId)).isZero();
        assertThat(counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED))
                .isEqualTo(beforeProcessed);
    }

    @Test
    void hundredCharacterConsumerGroupWorksAgainstProcessedEventsTableFromSqlFile() {
        UUID eventId = UUID.randomUUID();
        String group100 = "cg-" + "a".repeat(97);
        assertThat(group100).hasSize(100);

        double unknownBefore = counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, "unknown", EventTypes.PAYMENT_SUCCEEDED);

        boolean processed = processor.process(eventId, group100, EventTypes.PAYMENT_SUCCEEDED, () ->
                jdbcTemplate.update("INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)", eventId, "len-100"));

        assertThat(processed).isTrue();
        assertThat(countProcessedEvents(eventId, group100)).isEqualTo(1);
        assertThat(countDomainRows(eventId)).isEqualTo(1);
        assertThat(counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, "unknown", EventTypes.PAYMENT_SUCCEEDED))
                .isEqualTo(unknownBefore + 1.0);
    }

    @Test
    void sameEventIdUnderTwoDifferentConsumerGroupsRunsOncePerGroup() {
        UUID eventId = UUID.randomUUID();
        AtomicInteger runs = new AtomicInteger();

        boolean groupA = processor.process(
                eventId, "inventory-service-payment-cg", EventTypes.PAYMENT_SUCCEEDED, runs::incrementAndGet);
        boolean groupB = processor.process(
                eventId, "order-service-payment-cg", EventTypes.PAYMENT_SUCCEEDED, runs::incrementAndGet);

        assertThat(groupA).isTrue();
        assertThat(groupB).isTrue();
        assertThat(runs.get()).isEqualTo(2);
        assertThat(countProcessedEvents(eventId, "inventory-service-payment-cg")).isEqualTo(1);
        assertThat(countProcessedEvents(eventId, "order-service-payment-cg")).isEqualTo(1);
    }

    @Test
    void twoThreadsRacingOnSameEventIdAndConsumerGroupRunBusinessLogicOnce() throws Exception {
        UUID eventId = UUID.randomUUID();
        String group = "order-service-payment-cg";
        AtomicInteger businessRuns = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Callable<Boolean> task = () -> {
                ready.countDown();
                assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                return processor.process(eventId, group, EventTypes.PAYMENT_SUCCEEDED, () -> {
                    businessRuns.incrementAndGet();
                    jdbcTemplate.update("INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)", eventId, "race");
                });
            };

            Future<Boolean> f1 = pool.submit(task);
            Future<Boolean> f2 = pool.submit(task);
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            boolean r1 = f1.get(10, TimeUnit.SECONDS);
            boolean r2 = f2.get(10, TimeUnit.SECONDS);

            assertThat(List.of(r1, r2)).containsExactlyInAnyOrder(true, false);
            assertThat(businessRuns.get()).isEqualTo(1);
            assertThat(countProcessedEvents(eventId, group)).isEqualTo(1);
            assertThat(countDomainRows(eventId)).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void businessLogicInsertingDomainRowThenThrowingRollsBackBothTablesAndRetrySucceeds() {
        UUID eventId = UUID.randomUUID();
        String group = "payment-service-order-cg";

        assertThatThrownBy(() -> processor.process(eventId, group, EventTypes.ORDER_PAYMENT_REQUESTED, () -> {
            jdbcTemplate.update("INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)", eventId, "will-fail");
            throw new IllegalStateException("Simulated database/provider failure after domain insert");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(countDomainRows(eventId)).isZero();
        assertThat(countProcessedEvents(eventId, group)).isZero();

        boolean retryResult = processor.process(eventId, group, EventTypes.ORDER_PAYMENT_REQUESTED, () ->
                jdbcTemplate.update("INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)", eventId, "recovered"));

        assertThat(retryResult).isTrue();
        assertThat(countDomainRows(eventId)).isEqualTo(1);
        assertThat(countProcessedEvents(eventId, group)).isEqualTo(1);
    }

    @Test
    void processCalledInsideActiveTransactionFailsFastWithIllegalTransactionStateException() {
        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
        UUID eventId = UUID.randomUUID();

        assertThatThrownBy(() -> txTemplate.executeWithoutResult(status ->
                processor.process(eventId, "order-service-inventory-cg", EventTypes.INVENTORY_SEAT_RESERVED, () -> {
                })
        )).isInstanceOf(IllegalTransactionStateException.class);

        assertThat(countProcessedEvents(eventId, "order-service-inventory-cg")).isZero();
    }

    @Test
    void handlerFailingOnceIsRetriedViaRetry0AndSucceedsWithOneProcessedRow() throws Exception {
        UUID eventId = UUID.randomUUID();
        String group = TestKafkaListeners.ORDER_PAYMENT_GROUP;
        testListeners.configureFailuresFor(eventId, 1, FailureMode.TRANSIENT);

        double retriedBefore = counterValue(ConsumerMetrics.RETRIED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED);
        double processedBefore = counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED);

        String json = buildEnvelopeJson(eventId, EventTypes.PAYMENT_SUCCEEDED, "corr-retry-once", 1, Map.of("note", "ok"));
        ProducerRecord<String, String> record = new ProducerRecord<>(Topics.PAYMENT_EVENTS, eventId.toString(), json);
        record.headers().add(CorrelationConstants.KAFKA_HEADER, "corr-retry-once".getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);

        UUID completed = testListeners.awaitProcessed(eventId, 15, TimeUnit.SECONDS);
        assertThat(completed).isEqualTo(eventId);
        assertThat(testListeners.attemptsFor(eventId)).isEqualTo(2);
        assertThat(testListeners.lastMdcCorrelationIdFor(eventId)).isEqualTo("corr-retry-once");
        assertThat(countProcessedEvents(eventId, group)).isEqualTo(1);
        assertThat(countDomainRows(eventId)).isEqualTo(1);
        assertThat(counterValue(ConsumerMetrics.RETRIED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED))
                .isEqualTo(retriedBefore + 1.0);
        assertThat(counterValue(ConsumerMetrics.PROCESSED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED))
                .isEqualTo(processedBefore + 1.0);
    }

    @Test
    void handlerAlwaysFailingOnPartition9EndsInDltPartition0To2AfterThreeAttemptsWithOriginalPartition9() throws Exception {
        UUID eventId = UUID.randomUUID();
        String group = TestKafkaListeners.ORDER_PAYMENT_GROUP;
        int sourcePartition = 9;
        testListeners.configureFailuresFor(eventId, 10, FailureMode.TRANSIENT);

        double dltBefore = dltCounterValue(Topics.PAYMENT_EVENTS, group);
        double retriedBefore = counterValue(ConsumerMetrics.RETRIED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED);

        String json = buildEnvelopeJson(eventId, EventTypes.PAYMENT_SUCCEEDED, "corr-always-fail", 1, Map.of("note", "fail"));
        RecordMetadata mainMetadata = kafkaTemplate
                .send(new ProducerRecord<>(Topics.PAYMENT_EVENTS, sourcePartition, eventId.toString(), json))
                .get(10, TimeUnit.SECONDS)
                .getRecordMetadata();
        assertThat(mainMetadata.partition()).isEqualTo(sourcePartition);

        ConsumerRecord<String, String> dltRecord = pollDltRecordByKey(
                Topics.PAYMENT_EVENTS_ORDER_SERVICE_DLT, eventId.toString(), Duration.ofSeconds(25));

        assertThat(testListeners.attemptsFor(eventId)).isEqualTo(3);
        assertThat(countProcessedEvents(eventId, group)).isZero();
        assertThat(countDomainRows(eventId)).isZero();

        assertThat(dltRecord.partition()).isBetween(0, 2);
        assertThat(headerString(dltRecord, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(Topics.PAYMENT_EVENTS);
        assertThat(headerInt(dltRecord, KafkaHeaders.DLT_ORIGINAL_PARTITION)).isEqualTo(sourcePartition);
        assertThat(headerLong(dltRecord, KafkaHeaders.DLT_ORIGINAL_OFFSET)).isEqualTo(mainMetadata.offset());
        assertThat(headerLong(dltRecord, KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isPositive();
        assertThat(headerString(dltRecord, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .isEqualTo(IllegalStateException.class.getName());
        assertThat(headerString(dltRecord, KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                .contains("Simulated transient failure for " + eventId);
        assertThat(headerString(dltRecord, KafkaHeaders.DLT_EXCEPTION_STACKTRACE))
                .contains("IllegalStateException");

        assertThat(dltCounterValue(Topics.PAYMENT_EVENTS, group)).isEqualTo(dltBefore + 1.0);
        assertThat(counterValue(ConsumerMetrics.RETRIED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED))
                .isEqualTo(retriedBefore + 2.0);
    }

    @Test
    void nonRetryableEventExceptionGoesStraightToDltWithoutHittingRetry0() throws Exception {
        UUID eventId = UUID.randomUUID();
        String group = TestKafkaListeners.ORDER_PAYMENT_GROUP;
        testListeners.configureFailuresFor(eventId, 10, FailureMode.NON_RETRYABLE);

        double dltBefore = dltCounterValue(Topics.PAYMENT_EVENTS, group);
        double retriedBefore = counterValue(ConsumerMetrics.RETRIED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED);

        String json = buildEnvelopeJson(eventId, EventTypes.PAYMENT_SUCCEEDED, "corr-non-retryable", 1, Map.of("note", "poison"));
        kafkaTemplate.send(new ProducerRecord<>(Topics.PAYMENT_EVENTS, eventId.toString(), json))
                .get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> dltRecord = pollDltRecordByKey(
                Topics.PAYMENT_EVENTS_ORDER_SERVICE_DLT, eventId.toString(), Duration.ofSeconds(10));

        assertThat(dltRecord.value()).isEqualTo(json);
        assertThat(testListeners.attemptsFor(eventId)).isEqualTo(1);
        assertThat(headerString(dltRecord, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .isEqualTo(NonRetryableEventException.class.getName());
        assertThat(dltCounterValue(Topics.PAYMENT_EVENTS, group)).isEqualTo(dltBefore + 1.0);
        assertThat(counterValue(ConsumerMetrics.RETRIED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED))
                .isEqualTo(retriedBefore);
    }

    @Test
    void nonRetryableEventExceptionWrappedInRuntimeExceptionGoesStraightToDltWithNothingOnRetryTopics() throws Exception {
        UUID eventId = UUID.randomUUID();
        String group = TestKafkaListeners.ORDER_PAYMENT_GROUP;
        testListeners.configureFailuresFor(eventId, 10, FailureMode.WRAPPED_NON_RETRYABLE);

        double dltBefore = dltCounterValue(Topics.PAYMENT_EVENTS, group);
        double retriedBefore = counterValue(ConsumerMetrics.RETRIED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED);

        String json = buildEnvelopeJson(eventId, EventTypes.PAYMENT_SUCCEEDED, "corr-wrapped-non-retryable", 1, Map.of("note", "wrapped"));
        kafkaTemplate.send(new ProducerRecord<>(Topics.PAYMENT_EVENTS, eventId.toString(), json))
                .get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> dltRecord = pollDltRecordByKey(
                Topics.PAYMENT_EVENTS_ORDER_SERVICE_DLT, eventId.toString(), Duration.ofSeconds(10));

        assertThat(dltRecord.value()).isEqualTo(json);
        assertThat(testListeners.attemptsFor(eventId)).isEqualTo(1);
        assertThat(headerString(dltRecord, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .isEqualTo(NonRetryableEventException.class.getName());
        assertThat(headerString(dltRecord, KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                .contains("Wrapped non-retryable rejection for " + eventId);
        assertThat(dltCounterValue(Topics.PAYMENT_EVENTS, group)).isEqualTo(dltBefore + 1.0);
        assertThat(counterValue(ConsumerMetrics.RETRIED_TOTAL_METRIC, group, EventTypes.PAYMENT_SUCCEEDED))
                .isEqualTo(retriedBefore);
        assertThat(hasRecordWithKeyOnTopic(Topics.PAYMENT_EVENTS_ORDER_SERVICE_RETRY_0, eventId.toString())).isFalse();
        assertThat(hasRecordWithKeyOnTopic(Topics.PAYMENT_EVENTS_ORDER_SERVICE_RETRY_1, eventId.toString())).isFalse();
    }

    @Test
    void poisonRecordWithForgedDltHeadersEndsUpWithRealOriginalTopicAndOffsetAndWhitelistedMetricTags() throws Exception {
        UUID eventId = UUID.randomUUID();
        String group = TestKafkaListeners.ORDER_PAYMENT_GROUP;
        testListeners.configureFailuresFor(eventId, 10, FailureMode.NON_RETRYABLE);

        double dltBefore = dltCounterValue(Topics.PAYMENT_EVENTS, group);
        String forgedTopic = "forged.evil.topic." + UUID.randomUUID();
        String forgedGroup = "forged-evil-cg-" + UUID.randomUUID();
        long forgedOffset = 999_999_999L;

        String json = buildEnvelopeJson(eventId, EventTypes.PAYMENT_SUCCEEDED, "corr-forged-headers", 1, Map.of("note", "forged"));
        ProducerRecord<String, String> record = new ProducerRecord<>(Topics.PAYMENT_EVENTS, 4, eventId.toString(), json);
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, forgedTopic.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(forgedOffset).array());
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(99).array());
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, forgedGroup.getBytes(StandardCharsets.UTF_8));

        RecordMetadata mainMetadata = kafkaTemplate.send(record).get(10, TimeUnit.SECONDS).getRecordMetadata();

        ConsumerRecord<String, String> dltRecord = pollDltRecordByKey(
                Topics.PAYMENT_EVENTS_ORDER_SERVICE_DLT, eventId.toString(), Duration.ofSeconds(10));

        assertThat(headerString(dltRecord, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(Topics.PAYMENT_EVENTS);
        assertThat(headerLong(dltRecord, KafkaHeaders.DLT_ORIGINAL_OFFSET)).isEqualTo(mainMetadata.offset());
        assertThat(headerInt(dltRecord, KafkaHeaders.DLT_ORIGINAL_PARTITION)).isEqualTo(mainMetadata.partition());
        assertThat(dltCounterValue(Topics.PAYMENT_EVENTS, group)).isEqualTo(dltBefore + 1.0);
        assertThat(metrics.getMeterRegistry().find(ConsumerMetrics.DLT_TOTAL_METRIC)
                .tag("topic", forgedTopic)
                .counter()).isNull();
        assertThat(metrics.getMeterRegistry().find(ConsumerMetrics.DLT_TOTAL_METRIC)
                .tag("consumer_group", forgedGroup)
                .counter()).isNull();
    }

    @Test
    void rawInvalidJsonFollowedByValidEventOnSamePartitionRoutesPoisonToDltAndProcessesValidWithinTwoSeconds() throws Exception {
        String group = TestKafkaListeners.ORDER_INVENTORY_GROUP;
        int targetPartition = 0;
        String poisonKey = "poison-" + UUID.randomUUID();
        String rawInvalidJson = "{invalid-json-payload-without-closing-brace";
        UUID validEventId = UUID.randomUUID();
        String validJson = buildEnvelopeJson(
                validEventId, EventTypes.INVENTORY_SEAT_RESERVED, "corr-after-poison", 1, Map.of("seat", "A1"));

        long unknownTimerBefore = timerCount(ConsumerMetrics.PROCESSING_SECONDS_METRIC, group, "unknown");
        double dltBefore = dltCounterValue(Topics.INVENTORY_EVENTS, group);

        kafkaTemplate.send(new ProducerRecord<>(Topics.INVENTORY_EVENTS, targetPartition, poisonKey, rawInvalidJson))
                .get(10, TimeUnit.SECONDS);
        long validSentAtNanos = System.nanoTime();
        kafkaTemplate.send(new ProducerRecord<>(Topics.INVENTORY_EVENTS, targetPartition, validEventId.toString(), validJson))
                .get(10, TimeUnit.SECONDS);

        UUID processedId = testListeners.awaitProcessed(validEventId, 2, TimeUnit.SECONDS);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - validSentAtNanos);
        assertThat(processedId).isEqualTo(validEventId);
        assertThat(elapsedMillis).isLessThanOrEqualTo(2000L);

        ConsumerRecord<String, String> dltRecord = pollDltRecordByKey(
                Topics.INVENTORY_EVENTS_ORDER_SERVICE_DLT, poisonKey, Duration.ofSeconds(10));
        assertThat(dltRecord.value()).isEqualTo(rawInvalidJson);
        assertThat(timerCount(ConsumerMetrics.PROCESSING_SECONDS_METRIC, group, "unknown"))
                .isEqualTo(unknownTimerBefore + 1L);
        assertThat(dltCounterValue(Topics.INVENTORY_EVENTS, group)).isEqualTo(dltBefore + 1.0);
    }

    @Test
    void recordsOfOneKeyAreProcessedInOrderWhenNothingFails() throws Exception {
        String aggregateKey = "order-" + UUID.randomUUID();
        int partition = 1;
        UUID e1 = UUID.randomUUID();
        UUID e2 = UUID.randomUUID();
        UUID e3 = UUID.randomUUID();

        for (UUID id : List.of(e1, e2, e3)) {
            String json = buildEnvelopeJson(id, EventTypes.INVENTORY_SEAT_RESERVED, "corr-order", 1, Map.of("seq", id.toString()));
            kafkaTemplate.send(new ProducerRecord<>(Topics.INVENTORY_EVENTS, partition, aggregateKey, json))
                    .get(10, TimeUnit.SECONDS);
        }

        assertThat(testListeners.awaitProcessed(e1, 10, TimeUnit.SECONDS)).isEqualTo(e1);
        assertThat(testListeners.awaitProcessed(e2, 10, TimeUnit.SECONDS)).isEqualTo(e2);
        assertThat(testListeners.awaitProcessed(e3, 10, TimeUnit.SECONDS)).isEqualTo(e3);

        List<UUID> observedOrder = testListeners.processedEventsInOrder().stream()
                .filter(Set.of(e1, e2, e3)::contains)
                .toList();
        assertThat(observedOrder).containsExactly(e1, e2, e3);
    }

    @Test
    void cleanupDeletesOnlyRowsOlderThan192HoursAcrossMultipleBatchesAndStopsWhenInterrupted() {
        String group = "order-service-inventory-cg";
        List<UUID> expiredIds = List.of(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        List<UUID> recentIds = List.of(UUID.randomUUID(), UUID.randomUUID());

        for (UUID id : expiredIds) {
            jdbcTemplate.update("""
                    INSERT INTO processed_events (event_id, consumer_group, event_type, processed_at)
                    VALUES (?, ?, ?, clock_timestamp() - INTERVAL '193 hours')
                    """, id, group, EventTypes.INVENTORY_SEAT_RESERVED);
        }
        for (UUID id : recentIds) {
            jdbcTemplate.update("""
                    INSERT INTO processed_events (event_id, consumer_group, event_type, processed_at)
                    VALUES (?, ?, ?, clock_timestamp() - INTERVAL '191 hours')
                    """, id, group, EventTypes.INVENTORY_SEAT_RESERVED);
        }

        // When current thread is interrupted, cleaner stops immediately before issuing batch deletes
        Thread.currentThread().interrupt();
        try {
            assertThat(cleaner.purgeExpired()).isZero();
        } finally {
            Thread.interrupted();
        }

        // Configured batch-size is 2, so deleting 5 expired rows requires 3 batch iterations (2 + 2 + 1)
        int purged = cleaner.purgeExpired();
        assertThat(purged).isEqualTo(5);

        for (UUID id : expiredIds) {
            assertThat(countProcessedEvents(id, group)).isZero();
        }
        for (UUID id : recentIds) {
            assertThat(countProcessedEvents(id, group)).isEqualTo(1);
        }
    }

    private int countProcessedEvents(UUID eventId, String consumerGroup) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = ? AND consumer_group = ?",
                Integer.class, eventId, consumerGroup);
        return count == null ? 0 : count;
    }

    private int countDomainRows(UUID eventId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM test_domain_events WHERE event_id = ?",
                Integer.class, eventId);
        return count == null ? 0 : count;
    }

    private double counterValue(String name, String consumerGroup, String eventType) {
        var counter = metrics.getMeterRegistry().find(name)
                .tags("consumer_group", consumerGroup, "event_type", eventType)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private long timerCount(String name, String consumerGroup, String eventType) {
        var timer = metrics.getMeterRegistry().find(name)
                .tags("consumer_group", consumerGroup, "event_type", eventType)
                .timer();
        return timer == null ? 0L : timer.count();
    }

    private double dltCounterValue(String topic, String consumerGroup) {
        var counter = metrics.getMeterRegistry().find(ConsumerMetrics.DLT_TOTAL_METRIC)
                .tags("topic", topic, "consumer_group", consumerGroup)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static String buildEnvelopeJson(UUID eventId,
                                            String eventType,
                                            String correlationId,
                                            int schemaVersion,
                                            Map<String, Object> payload) throws Exception {
        EventEnvelope<Map<String, Object>> envelope = new EventEnvelope<>(
                eventId,
                eventType,
                Instant.now(),
                correlationId,
                schemaVersion,
                payload
        );
        return MAPPER.writeValueAsString(envelope);
    }

    private static ConsumerRecord<String, String> pollDltRecordByKey(String dltTopic,
                                                                     String expectedKey,
                                                                     Duration timeout) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-verifier-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(dltTopic));
            while (System.nanoTime() < deadlineNanos) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> record : records) {
                    if (expectedKey.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("Timed out waiting for DLT record with key " + expectedKey + " on " + dltTopic);
    }

    private static boolean hasRecordWithKeyOnTopic(String topic, String expectedKey) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "retry-verifier-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            var partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new org.apache.kafka.common.TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<org.apache.kafka.common.TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            boolean hasAnyData = endOffsets.values().stream().anyMatch(offset -> offset > 0L);
            if (!hasAnyData) {
                return false;
            }
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
            for (ConsumerRecord<String, String> record : records) {
                if (expectedKey.equals(record.key())) {
                    return true;
                }
            }
            return false;
        }
    }

    private static String headerString(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        assertThat(header).as("Header %s must be present", key).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int headerInt(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        assertThat(header).as("Header %s must be present", key).isNotNull();
        return ByteBuffer.wrap(header.value()).getInt();
    }

    private static long headerLong(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        assertThat(header).as("Header %s must be present", key).isNotNull();
        return ByteBuffer.wrap(header.value()).getLong();
    }

    enum FailureMode {
        TRANSIENT,
        NON_RETRYABLE,
        WRAPPED_NON_RETRYABLE
    }

    @Configuration
    @Import({ConsumerAutoConfiguration.class, KafkaAutoConfiguration.class})
    static class TestConfig {

        @Bean
        public DataSource dataSource() {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(POSTGRES.getJdbcUrl());
            config.setUsername(POSTGRES.getUsername());
            config.setPassword(POSTGRES.getPassword());
            config.setMaximumPoolSize(6);
            sharedDataSource = new HikariDataSource(config);

            ResourceDatabasePopulator populator = new ResourceDatabasePopulator(
                    new ClassPathResource("db/consumer/processed_events.sql"));
            populator.execute(sharedDataSource);

            new JdbcTemplate(sharedDataSource).execute("""
                    CREATE TABLE IF NOT EXISTS test_domain_events (
                        event_id UUID PRIMARY KEY,
                        note VARCHAR(128) NOT NULL
                    )
                    """);
            return sharedDataSource;
        }

        @Bean
        public JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        @Bean
        public MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        public TestKafkaListeners testKafkaListeners(IdempotentEventProcessor processor,
                                                     JdbcTemplate jdbcTemplate) {
            return new TestKafkaListeners(processor, jdbcTemplate);
        }
    }

    static class TestKafkaListeners {

        static final String ORDER_PAYMENT_GROUP = "order-service-payment-cg";
        static final String ORDER_INVENTORY_GROUP = "order-service-inventory-cg";

        private record FailurePlan(int failuresToThrow, FailureMode mode) {
        }

        private final IdempotentEventProcessor processor;
        private final JdbcTemplate jdbcTemplate;
        private final ConcurrentMap<UUID, FailurePlan> failurePlans = new ConcurrentHashMap<>();
        private final ConcurrentMap<UUID, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();
        private final ConcurrentMap<UUID, String> mdcCorrelationByEvent = new ConcurrentHashMap<>();
        private final ConcurrentMap<UUID, BlockingQueue<UUID>> completionQueues = new ConcurrentHashMap<>();
        private final List<UUID> orderedProcessedIds = new CopyOnWriteArrayList<>();

        TestKafkaListeners(IdempotentEventProcessor processor, JdbcTemplate jdbcTemplate) {
            this.processor = processor;
            this.jdbcTemplate = jdbcTemplate;
        }

        void reset() {
            failurePlans.clear();
            attemptCounts.clear();
            mdcCorrelationByEvent.clear();
            completionQueues.clear();
            orderedProcessedIds.clear();
        }

        void configureFailuresFor(UUID eventId, int failuresToThrow, FailureMode mode) {
            failurePlans.put(eventId, new FailurePlan(failuresToThrow, mode));
        }

        int attemptsFor(UUID eventId) {
            AtomicInteger counter = attemptCounts.get(eventId);
            return counter == null ? 0 : counter.get();
        }

        String lastMdcCorrelationIdFor(UUID eventId) {
            return mdcCorrelationByEvent.get(eventId);
        }

        List<UUID> processedEventsInOrder() {
            return List.copyOf(orderedProcessedIds);
        }

        UUID awaitProcessed(UUID eventId, long timeout, TimeUnit unit) throws InterruptedException {
            BlockingQueue<UUID> queue = completionQueues.computeIfAbsent(eventId, k -> new LinkedBlockingQueue<>());
            return queue.poll(timeout, unit);
        }

        @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = ORDER_PAYMENT_GROUP)
        public void onPaymentEvent(String rawValue) {
            handleEvent(rawValue, ORDER_PAYMENT_GROUP);
        }

        @KafkaListener(topics = Topics.INVENTORY_EVENTS, groupId = ORDER_INVENTORY_GROUP)
        public void onInventoryEvent(String rawValue) {
            handleEvent(rawValue, ORDER_INVENTORY_GROUP);
        }

        private void handleEvent(String rawValue, String consumerGroup) {
            processor.process(rawValue, consumerGroup, (EventEnvelope<JsonNode> envelope) -> {
                UUID eventId = envelope.eventId();
                String mdcCorrelation = MDC.get(CorrelationConstants.MDC_KEY);
                if (mdcCorrelation != null) {
                    mdcCorrelationByEvent.put(eventId, mdcCorrelation);
                }

                int attempt = attemptCounts.computeIfAbsent(eventId, k -> new AtomicInteger()).incrementAndGet();
                FailurePlan plan = failurePlans.get(eventId);
                if (plan != null && attempt <= plan.failuresToThrow()) {
                    switch (plan.mode()) {
                        case NON_RETRYABLE ->
                                throw new NonRetryableEventException("Explicit non-retryable rejection for " + eventId);
                        case WRAPPED_NON_RETRYABLE ->
                                throw new RuntimeException(
                                        "Wrapper runtime exception",
                                        new NonRetryableEventException("Wrapped non-retryable rejection for " + eventId)
                                );
                        case TRANSIENT ->
                                throw new IllegalStateException("Simulated transient failure for " + eventId + " on attempt " + attempt);
                    }
                }

                jdbcTemplate.update(
                        "INSERT INTO test_domain_events (event_id, note) VALUES (?, ?)",
                        eventId,
                        "processed-attempt-" + attempt
                );
                orderedProcessedIds.add(eventId);
                completionQueues.computeIfAbsent(eventId, k -> new LinkedBlockingQueue<>()).offer(eventId);
            });
        }
    }
}
