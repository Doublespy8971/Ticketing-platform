package com.ticketing.consumer.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerConcurrencyAutoConfigurationTest {

    private final DataSource dataSource = new DriverManagerDataSource("jdbc:postgresql://localhost:5432/test");

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class, ConsumerAutoConfiguration.class))
            .withBean(DataSource.class, () -> dataSource)
            .withBean(JdbcTemplate.class, () -> new JdbcTemplate(dataSource))
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new);

    @Test
    void defaultsListenerContainerFactoryConcurrencyToOne() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            ConcurrentKafkaListenerContainerFactory<?, ?> factory = context.getBean(
                    "kafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);
            assertThat(ReflectionTestUtils.getField(factory, "concurrency")).isEqualTo(1);
        });
    }

    @Test
    void appliesConfiguredConcurrencyToListenerContainerFactory() {
        contextRunner
                .withPropertyValues("ticketing.consumer.concurrency=6")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ConcurrentKafkaListenerContainerFactory<?, ?> factory = context.getBean(
                            "kafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);
                    assertThat(ReflectionTestUtils.getField(factory, "concurrency")).isEqualTo(6);
                });

        contextRunner
                .withPropertyValues("ticketing.consumer.concurrency=12")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ConcurrentKafkaListenerContainerFactory<?, ?> factory = context.getBean(
                            "kafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);
                    assertThat(ReflectionTestUtils.getField(factory, "concurrency")).isEqualTo(12);
                });
    }

    @Test
    void failsStartupWhenConcurrencyIsOutOfBounds() {
        for (String invalid : new String[]{"0", "-1", "13"}) {
            contextRunner
                    .withPropertyValues("ticketing.consumer.concurrency=" + invalid)
                    .run(context -> {
                        assertThat(context).hasFailed();
                        assertThat(context.getStartupFailure())
                                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                                .rootCause()
                                .hasMessageContaining("ticketing.consumer.concurrency");
                    });
        }
    }
}
