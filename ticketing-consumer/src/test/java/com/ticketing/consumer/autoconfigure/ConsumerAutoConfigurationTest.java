package com.ticketing.consumer.autoconfigure;

import com.fasterxml.jackson.databind.JsonNode;
import com.ticketing.common.correlation.CorrelationRecordInterceptor;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.consumer.ConsumerCleanupProperties;
import com.ticketing.consumer.ConsumerEnvelopeParser;
import com.ticketing.consumer.ConsumerMetrics;
import com.ticketing.consumer.IdempotentEventProcessor;
import com.ticketing.consumer.NonRetryableEventException;
import com.ticketing.consumer.ProcessedEventsRetentionCleaner;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsumerAutoConfigurationTest {

    private final DataSource dataSource = new DriverManagerDataSource("jdbc:postgresql://localhost:5432/test");

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class, ConsumerAutoConfiguration.class))
            .withBean(DataSource.class, () -> dataSource)
            .withBean(JdbcTemplate.class, () -> new JdbcTemplate(dataSource))
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new);

    @Test
    void takesPrecedenceOverKafkaAutoConfigurationForConsumerWhilePreservingDefaultKafkaTemplate() {
        contextRunner
                .withPropertyValues(
                        "spring.application.name=order-service",
                        "spring.kafka.bootstrap-servers=localhost:29092",
                        "spring.kafka.producer.acks=1",
                        "spring.kafka.producer.properties.linger.ms=17"
                )
                .run(context -> {
                    assertThat(context).hasSingleBean(IdempotentEventProcessor.class);
                    assertThat(context).hasSingleBean(ConsumerEnvelopeParser.class);
                    assertThat(context).hasSingleBean(ConsumerMetrics.class);
                    assertThat(context).hasSingleBean(ProcessedEventsRetentionCleaner.class);
                    assertThat(context).hasBean("ticketingConsumerCleanupScheduler");
                    assertThat(context).hasBean("ticketingRetryBackoffScheduler");

                    @SuppressWarnings("unchecked")
                    ConsumerFactory<String, String> consumerFactory = context.getBean(ConsumerFactory.class);
                    Map<String, Object> consumerProps = consumerFactory.getConfigurationProperties();
                    assertThat(consumerProps)
                            .containsEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .containsEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                            .containsEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                            .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                            .containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100)
                            .containsEntry(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);

                    @SuppressWarnings("unchecked")
                    ConcurrentKafkaListenerContainerFactory<String, String> containerFactory =
                            context.getBean("kafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);
                    assertThat(containerFactory.getContainerProperties().getAckMode())
                            .isEqualTo(ContainerProperties.AckMode.RECORD);
                    assertThat(containerFactory.getConsumerFactory()).isSameAs(consumerFactory);
                    assertThat(context).hasSingleBean(CorrelationRecordInterceptor.class);
                    assertThat(ReflectionTestUtils.getField(containerFactory, "recordInterceptor"))
                            .isSameAs(context.getBean(CorrelationRecordInterceptor.class));

                    // Application's default KafkaTemplate and ProducerFactory from KafkaAutoConfiguration still exist
                    // and are not replaced or modified by ticketing-consumer
                    assertThat(context).hasSingleBean(KafkaTemplate.class);
                    assertThat(context).hasSingleBean(ProducerFactory.class);
                    KafkaTemplate<?, ?> defaultTemplate = context.getBean("kafkaTemplate", KafkaTemplate.class);
                    ProducerFactory<?, ?> defaultProducerFactory = context.getBean("kafkaProducerFactory", ProducerFactory.class);
                    assertThat(defaultTemplate.getProducerFactory()).isSameAs(defaultProducerFactory);
                    assertThat(defaultProducerFactory.getConfigurationProperties())
                            .containsEntry(ProducerConfig.ACKS_CONFIG, "1")
                            .containsEntry(ProducerConfig.LINGER_MS_CONFIG, "17")
                            .doesNotContainKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);

                    // Retry/DLT KafkaTemplate is dedicated, not the default template, and enforces acks=all + idempotence=true
                    ConsumerAutoConfiguration.RetryKafkaTemplateHolder retryHolder =
                            context.getBean(ConsumerAutoConfiguration.RetryKafkaTemplateHolder.class);
                    KafkaTemplate<String, String> retryTemplate = retryHolder.kafkaTemplate();
                    assertThat(retryTemplate).isNotSameAs(defaultTemplate);
                    assertThat(retryTemplate.getProducerFactory()).isNotSameAs(defaultProducerFactory);
                    assertThat(retryTemplate.getProducerFactory().getConfigurationProperties())
                            .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                            .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

                    RetryTopicConfiguration retryTopicConfig = context.getBean(RetryTopicConfiguration.class);
                    for (DestinationTopic.Properties destProps : retryTopicConfig.getDestinationTopicProperties()) {
                        assertThat(ReflectionTestUtils.getField(destProps, "kafkaOperations")).isSameAs(retryTemplate);
                    }

                    ConsumerCleanupProperties cleanupProps = context.getBean(ConsumerCleanupProperties.class);
                    assertThat(cleanupProps.isEnabled()).isTrue();
                    assertThat(cleanupProps.getRetentionHours()).isEqualTo(192);
                    assertThat(cleanupProps.getBatchSize()).isEqualTo(5000);
                    assertThat(cleanupProps.getCron()).isEqualTo("0 0 * * * *");
                });
    }

    @Test
    void closingApplicationContextShutsDownRetryBackoffScheduler() {
        AtomicReference<ScheduledThreadPoolExecutor> executorRef = new AtomicReference<>();
        contextRunner.run(context -> {
            ThreadPoolTaskScheduler retryScheduler =
                    context.getBean("ticketingRetryBackoffScheduler", ThreadPoolTaskScheduler.class);
            RetryTopicSchedulerWrapper wrapper = context.getBean(RetryTopicSchedulerWrapper.class);
            assertThat(wrapper.getScheduler()).isSameAs(retryScheduler);

            ScheduledThreadPoolExecutor executor = retryScheduler.getScheduledThreadPoolExecutor();
            assertThat(executor.isShutdown()).isFalse();
            executorRef.set(executor);
        });
        assertThat(executorRef.get()).isNotNull();
        assertThat(executorRef.get().isShutdown()).isTrue();
    }

    @Test
    void kafkaListenerWithoutExplicitGroupIdFailsContextStartupWithIllegalStateException() {
        contextRunner
                .withUserConfiguration(ListenerWithoutGroupIdConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(IllegalStateException.class)
                            .rootCause()
                            .hasMessageContaining("groupId");
                });

        contextRunner
                .withUserConfiguration(ListenerWithUnwhitelistedGroupIdConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(IllegalStateException.class)
                            .rootCause()
                            .hasMessageContaining("whitelisted");
                });
    }

    @Test
    void idempotentEventProcessorConstructorValidatesTransactionTemplateSettingsAndDataSource() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        DataSourceTransactionManager validTxManager = new DataSourceTransactionManager(dataSource);
        ConsumerMetrics metrics = new ConsumerMetrics(new SimpleMeterRegistry());

        TransactionTemplate badPropagation = new TransactionTemplate(validTxManager);
        badPropagation.setPropagationBehavior(TransactionDefinition.PROPAGATION_SUPPORTS);
        assertThatThrownBy(() -> new IdempotentEventProcessor(jdbcTemplate, badPropagation, metrics))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("propagation");

        TransactionTemplate readOnlyTemplate = new TransactionTemplate(validTxManager);
        readOnlyTemplate.setReadOnly(true);
        assertThatThrownBy(() -> new IdempotentEventProcessor(jdbcTemplate, readOnlyTemplate, metrics))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("read-only");

        DataSource otherDataSource = new DriverManagerDataSource("jdbc:postgresql://localhost:5432/other");
        TransactionTemplate mismatchedDataSourceTemplate =
                new TransactionTemplate(new DataSourceTransactionManager(otherDataSource));
        assertThatThrownBy(() -> new IdempotentEventProcessor(jdbcTemplate, mismatchedDataSourceTemplate, metrics))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DataSource");

        TransactionTemplate requiresNewTemplate = new TransactionTemplate(validTxManager);
        requiresNewTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        assertThat(new IdempotentEventProcessor(jdbcTemplate, requiresNewTemplate, metrics)).isNotNull();
    }

    @Test
    void failsStartupWhenCleanupPropertiesAreOutOfBounds() {
        contextRunner
                .withPropertyValues("ticketing.consumer.cleanup.batch-size=0")
                .run(context -> assertThat(context).hasFailed());

        contextRunner
                .withPropertyValues("ticketing.consumer.cleanup.batch-size=10001")
                .run(context -> assertThat(context).hasFailed());

        contextRunner
                .withPropertyValues("ticketing.consumer.cleanup.retention-hours=191")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void topicNamingMatchesTopicsConstantsAndCreateTopicsScript() throws Exception {
        String script = Files.readString(Path.of("../infra/kafka/create-topics.sh"));

        record Pair(String topic, String group, String retry0, String retry1, String dlt) {
        }

        List<Pair> expectedPairs = List.of(
                new Pair(
                        Topics.INVENTORY_EVENTS,
                        "order-service-inventory-cg",
                        Topics.INVENTORY_EVENTS_ORDER_SERVICE_RETRY_0,
                        Topics.INVENTORY_EVENTS_ORDER_SERVICE_RETRY_1,
                        Topics.INVENTORY_EVENTS_ORDER_SERVICE_DLT
                ),
                new Pair(
                        Topics.ORDER_EVENTS,
                        "payment-service-order-cg",
                        Topics.ORDER_EVENTS_PAYMENT_SERVICE_RETRY_0,
                        Topics.ORDER_EVENTS_PAYMENT_SERVICE_RETRY_1,
                        Topics.ORDER_EVENTS_PAYMENT_SERVICE_DLT
                ),
                new Pair(
                        Topics.PAYMENT_EVENTS,
                        "inventory-service-payment-cg",
                        Topics.PAYMENT_EVENTS_INVENTORY_SERVICE_RETRY_0,
                        Topics.PAYMENT_EVENTS_INVENTORY_SERVICE_RETRY_1,
                        Topics.PAYMENT_EVENTS_INVENTORY_SERVICE_DLT
                ),
                new Pair(
                        Topics.PAYMENT_EVENTS,
                        "order-service-payment-cg",
                        Topics.PAYMENT_EVENTS_ORDER_SERVICE_RETRY_0,
                        Topics.PAYMENT_EVENTS_ORDER_SERVICE_RETRY_1,
                        Topics.PAYMENT_EVENTS_ORDER_SERVICE_DLT
                )
        );

        TicketingRetryTopicNamesProviderFactory factory = new TicketingRetryTopicNamesProviderFactory();
        org.springframework.kafka.core.KafkaTemplate<String, String> dummyTemplate =
                new org.springframework.kafka.core.KafkaTemplate<>(
                        new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(Map.of()));
        List<DestinationTopic.Properties> destinationProps = org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder
                .newInstance()
                .maxAttempts(3)
                .exponentialBackoff(2000L, 5.0, 10000L)
                .setTopicSuffixingStrategy(org.springframework.kafka.retrytopic.TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
                .doNotAutoCreateRetryTopics()
                .dltProcessingFailureStrategy(DltStrategy.FAIL_ON_ERROR)
                .create(dummyTemplate)
                .getDestinationTopicProperties();
        DestinationTopic.Properties mainProps = destinationProps.get(0);
        DestinationTopic.Properties retry0Props = destinationProps.get(1);
        DestinationTopic.Properties retry1Props = destinationProps.get(2);
        DestinationTopic.Properties dltProps = destinationProps.get(3);
        assertThat(mainProps.isMainEndpoint()).isTrue();
        assertThat(retry0Props.delay()).isEqualTo(2000L);
        assertThat(retry1Props.delay()).isEqualTo(10000L);
        assertThat(dltProps.isDltTopic()).isTrue();

        for (Pair pair : expectedPairs) {
            TicketingRetryTopicNamesProviderFactory.RetryTopicNames names =
                    TicketingRetryTopicNamesProviderFactory.topicNamesFor(pair.topic(), pair.group());
            assertThat(names.retry0Topic()).isEqualTo(pair.retry0());
            assertThat(names.retry1Topic()).isEqualTo(pair.retry1());
            assertThat(names.dltTopic()).isEqualTo(pair.dlt());
            assertThat(names.retry0Group()).isEqualTo(pair.group() + "-retry-0");
            assertThat(names.retry1Group()).isEqualTo(pair.group() + "-retry-1");
            assertThat(names.dltGroup()).isEqualTo(pair.group() + "-dlt");

            String base = pair.topic() + "-" + pair.group();
            assertThat(script).contains("\"" + base + "\"");

            MethodKafkaListenerEndpoint<String, String> endpoint = new MethodKafkaListenerEndpoint<>();
            endpoint.setId(pair.group());
            endpoint.setGroupId(pair.group());

            TicketingRetryTopicNamesProviderFactory.withConsumerGroup(pair.group(), () -> {
                RetryTopicNamesProviderFactory.RetryTopicNamesProvider p0 =
                        factory.createRetryTopicNamesProvider(retry0Props);
                RetryTopicNamesProviderFactory.RetryTopicNamesProvider p1 =
                        factory.createRetryTopicNamesProvider(retry1Props);
                RetryTopicNamesProviderFactory.RetryTopicNamesProvider pDlt =
                        factory.createRetryTopicNamesProvider(dltProps);

                assertThat(p0.getTopicName(pair.topic())).isEqualTo(pair.retry0());
                assertThat(p1.getTopicName(pair.topic())).isEqualTo(pair.retry1());
                assertThat(pDlt.getTopicName(pair.topic())).isEqualTo(pair.dlt());
                assertThat(p0.getGroupId(endpoint)).isEqualTo(pair.group() + "-retry-0");
                assertThat(p1.getGroupId(endpoint)).isEqualTo(pair.group() + "-retry-1");
                assertThat(pDlt.getGroupId(endpoint)).isEqualTo(pair.group() + "-dlt");
                return null;
            });
        }
    }

    @Test
    void metricTagsAreStrictlyBoundedAndUnknownValuesMapToUnknown() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.config().commonTags("service", "order-service");
        ConsumerMetrics metrics = new ConsumerMetrics(registry, "order-service");

        metrics.recordDeadLetter(Topics.PAYMENT_EVENTS, "order-service-payment-cg-retry-1");
        metrics.recordDeadLetter("forged.evil.topic", "forged-evil-group");
        metrics.recordProcessed("order-service-payment-cg", EventTypes.PAYMENT_SUCCEEDED);
        metrics.recordDuplicate("order-service-payment-cg-retry-0", EventTypes.PAYMENT_SUCCEEDED);
        metrics.recordRetried("forged-evil-group", "evil.unbounded.event.type");
        metrics.recordProcessingDuration("order-service-payment-cg", null, Duration.ofMillis(12));

        Meter validDltMeter = registry.get(ConsumerMetrics.DLT_TOTAL_METRIC)
                .tags("topic", Topics.PAYMENT_EVENTS, "consumer_group", "order-service-payment-cg")
                .counter();
        assertThat(validDltMeter.getId().getTags().stream().map(Tag::getKey).toList())
                .containsExactlyInAnyOrder("service", "topic", "consumer_group");
        assertThat(validDltMeter.getId().getTag("service")).isEqualTo("order-service");

        Meter unknownDltMeter = registry.get(ConsumerMetrics.DLT_TOTAL_METRIC)
                .tags("topic", "unknown", "consumer_group", "unknown")
                .counter();
        assertThat(unknownDltMeter).isNotNull();

        for (String meterName : List.of(
                ConsumerMetrics.PROCESSED_TOTAL_METRIC,
                ConsumerMetrics.DUPLICATE_TOTAL_METRIC,
                ConsumerMetrics.RETRIED_TOTAL_METRIC,
                ConsumerMetrics.PROCESSING_SECONDS_METRIC)) {
            Meter meter = registry.get(meterName).meter();
            assertThat(meter.getId().getTags().stream().map(Tag::getKey).toList())
                    .containsExactlyInAnyOrder("service", "consumer_group", "event_type");
            assertThat(meter.getId().getTag("service")).isEqualTo("order-service");
        }

        assertThat(registry.get(ConsumerMetrics.RETRIED_TOTAL_METRIC).counter().getId().getTag("consumer_group"))
                .isEqualTo("unknown");
        assertThat(registry.get(ConsumerMetrics.RETRIED_TOTAL_METRIC).counter().getId().getTag("event_type"))
                .isEqualTo("unknown");
        assertThat(registry.get(ConsumerMetrics.PROCESSING_SECONDS_METRIC).timer().getId().getTag("event_type"))
                .isEqualTo("unknown");
    }

    @Test
    void envelopeParserRejectsMalformedUnknownEventTypeAndUnsupportedSchemaVersion() {
        ConsumerEnvelopeParser parser = new ConsumerEnvelopeParser();

        assertThatThrownBy(() -> parser.parse("{not-valid-json"))
                .isInstanceOf(NonRetryableEventException.class);

        String badSchemaJson = """
                {
                  "eventId": "%s",
                  "eventType": "payment.succeeded",
                  "occurredAt": "2026-10-31T10:00:00.000000Z",
                  "correlationId": "corr-1",
                  "schemaVersion": 2,
                  "payload": {"orderId": "%s"}
                }
                """.formatted(UUID.randomUUID(), UUID.randomUUID());
        assertThatThrownBy(() -> parser.parse(badSchemaJson))
                .isInstanceOf(NonRetryableEventException.class)
                .hasMessageContaining("schemaVersion");

        String unknownTypeJson = """
                {
                  "eventId": "%s",
                  "eventType": "unknown.bogus.event",
                  "occurredAt": "2026-10-31T10:00:00.000000Z",
                  "correlationId": "corr-1",
                  "schemaVersion": 1,
                  "payload": {"orderId": "%s"}
                }
                """.formatted(UUID.randomUUID(), UUID.randomUUID());
        assertThatThrownBy(() -> parser.parse(unknownTypeJson))
                .isInstanceOf(NonRetryableEventException.class)
                .hasMessageContaining("eventType");

        String validJson = """
                {
                  "eventId": "%s",
                  "eventType": "payment.succeeded",
                  "occurredAt": "2026-10-31T10:00:00.000000Z",
                  "correlationId": "corr-1",
                  "schemaVersion": 1,
                  "payload": {"orderId": "123"}
                }
                """.formatted(UUID.randomUUID());
        EventEnvelope<JsonNode> envelope = parser.parse(validJson);
        assertThat(envelope.eventType()).isEqualTo(EventTypes.PAYMENT_SUCCEEDED);
        assertThat(envelope.schemaVersion()).isEqualTo(1);
    }

    @Configuration(proxyBeanMethods = false)
    static class ListenerWithoutGroupIdConfig {

        @KafkaListener(topics = Topics.PAYMENT_EVENTS, autoStartup = "false")
        public void onPayment(String value) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ListenerWithUnwhitelistedGroupIdConfig {

        @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = "rogue-unwhitelisted-cg", autoStartup = "false")
        public void onPayment(String value) {
        }
    }
}
