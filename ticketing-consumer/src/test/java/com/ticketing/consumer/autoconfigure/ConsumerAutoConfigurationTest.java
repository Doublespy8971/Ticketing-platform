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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsumerAutoConfigurationTest {

    private final DataSource dataSource = new DriverManagerDataSource("jdbc:postgresql://localhost:5432/test");

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConsumerAutoConfiguration.class))
            .withBean(DataSource.class, () -> dataSource)
            .withBean(JdbcTemplate.class, () -> new JdbcTemplate(dataSource))
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new);

    @Test
    void registersConsumerBeansContainerSettingsAndDedicatedCleanupScheduler() {
        contextRunner
                .withPropertyValues("spring.application.name=order-service")
                .run(context -> {
                    assertThat(context).hasSingleBean(IdempotentEventProcessor.class);
                    assertThat(context).hasSingleBean(ConsumerEnvelopeParser.class);
                    assertThat(context).hasSingleBean(ConsumerMetrics.class);
                    assertThat(context).hasSingleBean(ProcessedEventsRetentionCleaner.class);
                    assertThat(context).hasBean("ticketingConsumerCleanupScheduler");
                    assertThat(context.getBean("ticketingConsumerCleanupScheduler"))
                            .isInstanceOf(ThreadPoolTaskScheduler.class);

                    @SuppressWarnings("unchecked")
                    ConsumerFactory<String, String> consumerFactory = context.getBean(ConsumerFactory.class);
                    Map<String, Object> props = consumerFactory.getConfigurationProperties();
                    assertThat(props)
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
                    assertThat(context).hasSingleBean(CorrelationRecordInterceptor.class);

                    ConsumerCleanupProperties cleanupProps = context.getBean(ConsumerCleanupProperties.class);
                    assertThat(cleanupProps.isEnabled()).isTrue();
                    assertThat(cleanupProps.getRetentionHours()).isEqualTo(192);
                    assertThat(cleanupProps.getBatchSize()).isEqualTo(5000);
                    assertThat(cleanupProps.getCron()).isEqualTo("0 0 * * * *");
                });
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
    void metricTagsAreStrictlyBoundedAndUnknownEventTypesMapToUnknown() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.config().commonTags("service", "order-service");
        ConsumerMetrics metrics = new ConsumerMetrics(registry, "order-service");

        metrics.recordDeadLetter(Topics.PAYMENT_EVENTS, "order-service-payment-cg");
        metrics.recordProcessed("order-service-payment-cg", EventTypes.PAYMENT_SUCCEEDED);
        metrics.recordDuplicate("order-service-payment-cg", EventTypes.PAYMENT_SUCCEEDED);
        metrics.recordRetried("order-service-payment-cg", "evil.unbounded.event.type");
        metrics.recordProcessingDuration("order-service-payment-cg", null, Duration.ofMillis(12));

        Meter dltMeter = registry.get(ConsumerMetrics.DLT_TOTAL_METRIC).counter();
        assertThat(dltMeter.getId().getTags().stream().map(Tag::getKey).toList())
                .containsExactlyInAnyOrder("service", "topic", "consumer_group");
        assertThat(dltMeter.getId().getTag("service")).isEqualTo("order-service");
        assertThat(dltMeter.getId().getTag("topic")).isEqualTo(Topics.PAYMENT_EVENTS);
        assertThat(dltMeter.getId().getTag("consumer_group")).isEqualTo("order-service-payment-cg");

        for (String meterName : List.of(
                ConsumerMetrics.PROCESSED_TOTAL_METRIC,
                ConsumerMetrics.DUPLICATE_TOTAL_METRIC,
                ConsumerMetrics.RETRIED_TOTAL_METRIC,
                ConsumerMetrics.PROCESSING_SECONDS_METRIC)) {
            Meter meter = registry.get(meterName).meter();
            assertThat(meter.getId().getTags().stream().map(Tag::getKey).toList())
                    .containsExactlyInAnyOrder("service", "consumer_group", "event_type");
            assertThat(meter.getId().getTag("service")).isEqualTo("order-service");
            assertThat(meter.getId().getTag("consumer_group")).isEqualTo("order-service-payment-cg");
        }

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
}
