package com.ticketing.outbox.autoconfigure;

import com.ticketing.outbox.OutboxMetrics;
import com.ticketing.outbox.OutboxPoller;
import com.ticketing.outbox.OutboxProducerProperties;
import com.ticketing.outbox.OutboxProperties;
import com.ticketing.outbox.OutboxRetentionCleaner;
import com.ticketing.outbox.OutboxWriter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxAutoConfigurationTest {

    private final DataSource dataSource = new DriverManagerDataSource("jdbc:postgresql://localhost:5432/test");

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OutboxAutoConfiguration.class))
            .withBean(DataSource.class, () -> dataSource)
            .withBean(JdbcTemplate.class, () -> new JdbcTemplate(dataSource))
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withBean(KafkaOperations.class, () -> new KafkaTemplate<>(
                    new DefaultKafkaProducerFactory<>(OutboxProducerProperties.defaults("localhost:9092"))));

    @Test
    void registersWriterMetricsServiceTagAndProducerCustomizerByDefaultWithoutPoller() {
        contextRunner
                .withPropertyValues("spring.application.name=order-service")
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxWriter.class);
                    assertThat(context).hasSingleBean(OutboxMetrics.class);
                    assertThat(context).hasSingleBean(DefaultKafkaProducerFactoryCustomizer.class);
                    assertThat(context).doesNotHaveBean(OutboxPoller.class);
                    assertThat(context).doesNotHaveBean(OutboxRetentionCleaner.class);

                    MeterRegistry registry = context.getBean(MeterRegistry.class);
                    assertThat(registry.get("outbox_published_total").counter().getId().getTag("service"))
                            .isEqualTo("order-service");

                    DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(new HashMap<>(Map.of(
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                    )));
                    context.getBean(DefaultKafkaProducerFactoryCustomizer.class).customize(factory);

                    Map<String, Object> config = factory.getConfigurationProperties();
                    assertThat(config)
                            .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                            .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
                            .containsEntry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5)
                            .containsEntry(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE)
                            .containsEntry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
                            .containsEntry(ProducerConfig.LINGER_MS_CONFIG, 5)
                            .containsEntry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1500)
                            .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 4000);
                });
    }

    @Test
    void registersPollerCleanerAndDedicatedSchedulerWhenEnabledAndBindsEnvAliases() {
        contextRunner
                .withPropertyValues(
                        "ticketing.outbox.enabled=true",
                        "OUTBOX_BATCH_SIZE=75",
                        "OUTBOX_POLL_INTERVAL_MS=350"
                )
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxWriter.class);
                    assertThat(context).hasSingleBean(OutboxMetrics.class);
                    assertThat(context).hasSingleBean(OutboxPoller.class);
                    assertThat(context).hasSingleBean(OutboxRetentionCleaner.class);
                    assertThat(context).hasBean("ticketingOutboxScheduler");
                    assertThat(context.getBean("ticketingOutboxScheduler")).isInstanceOf(ThreadPoolTaskScheduler.class);

                    OutboxProperties props = context.getBean(OutboxProperties.class);
                    assertThat(props.getBatchSize()).isEqualTo(75);
                    assertThat(props.getPollIntervalMs()).isEqualTo(350L);
                });
    }

    @Test
    void failsStartupWhenBatchSizeOrCleanupBatchSizeIsOutOfBounds() {
        contextRunner
                .withPropertyValues("ticketing.outbox.batch-size=0")
                .run(context -> assertThat(context).hasFailed());

        contextRunner
                .withPropertyValues("ticketing.outbox.batch-size=1001")
                .run(context -> assertThat(context).hasFailed());

        contextRunner
                .withPropertyValues("ticketing.outbox.cleanup-batch-size=0")
                .run(context -> assertThat(context).hasFailed());

        contextRunner
                .withPropertyValues("ticketing.outbox.cleanup-batch-size=1001")
                .run(context -> assertThat(context).hasFailed());
    }
}
