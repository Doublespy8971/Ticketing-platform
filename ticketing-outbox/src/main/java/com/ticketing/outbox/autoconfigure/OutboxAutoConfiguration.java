package com.ticketing.outbox.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.json.ObjectMapperFactory;
import com.ticketing.outbox.OutboxMetrics;
import com.ticketing.outbox.OutboxPoller;
import com.ticketing.outbox.OutboxProducerProperties;
import com.ticketing.outbox.OutboxProperties;
import com.ticketing.outbox.OutboxRetentionCleaner;
import com.ticketing.outbox.OutboxWriter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Spring Boot auto-configuration for the transactional outbox writer, metrics, idempotent Kafka producer
 * settings, and (when {@code ticketing.outbox.enabled=true}) the scheduled single-leader poller and
 * retention cleaner.
 */
@AutoConfiguration(after = {JdbcTemplateAutoConfiguration.class, KafkaAutoConfiguration.class})
@ConditionalOnClass({JdbcTemplate.class, KafkaOperations.class})
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OutboxWriter outboxWriter(JdbcTemplate jdbcTemplate) {
        return new OutboxWriter(jdbcTemplate, ObjectMapperFactory.create());
    }

    @Bean
    @ConditionalOnMissingBean(name = "outboxServiceTagMeterRegistryCustomizer")
    public MeterRegistryCustomizer<MeterRegistry> outboxServiceTagMeterRegistryCustomizer(
            @Value("${spring.application.name:unknown-service}") String serviceName) {
        return registry -> registry.config().commonTags("service", serviceName);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxMetrics outboxMetrics(JdbcTemplate jdbcTemplate,
                                       ObjectProvider<MeterRegistry> meterRegistryProvider,
                                       MeterRegistryCustomizer<MeterRegistry> outboxServiceTagMeterRegistryCustomizer) {
        MeterRegistry registry = meterRegistryProvider.getIfAvailable(SimpleMeterRegistry::new);
        outboxServiceTagMeterRegistryCustomizer.customize(registry);
        return new OutboxMetrics(jdbcTemplate, registry);
    }

    @Bean
    public DefaultKafkaProducerFactoryCustomizer outboxIdempotentProducerCustomizer() {
        return producerFactory -> {
            Map<String, Object> overrides = new HashMap<>();
            OutboxProducerProperties.applyIdempotentSettings(overrides);
            producerFactory.updateConfigs(overrides);
        };
    }

    @Bean
    @ConditionalOnBean(KafkaOperations.class)
    public SmartInitializingSingleton outboxKafkaProducerStartupValidator(
            KafkaOperations<?, ?> kafkaOperations) {
        return () -> OutboxProducerProperties.validateConfiguration(kafkaOperations);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "ticketing.outbox", name = "enabled", havingValue = "true")
    @ConditionalOnBean(KafkaOperations.class)
    @EnableScheduling
    static class SchedulingConfiguration {

        @Bean(name = "ticketingOutboxScheduler")
        @ConditionalOnMissingBean(name = "ticketingOutboxScheduler")
        public ThreadPoolTaskScheduler ticketingOutboxScheduler(OutboxProperties properties) {
            ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
            scheduler.setPoolSize(1);
            scheduler.setThreadNamePrefix("outbox-poller-");
            scheduler.setWaitForTasksToCompleteOnShutdown(true);
            scheduler.setAwaitTerminationMillis(Math.max(1L, properties.getSendTimeout().toMillis()));
            return scheduler;
        }

        @Bean
        @ConditionalOnMissingBean
        public OutboxPoller outboxPoller(DataSource dataSource,
                                         JdbcTemplate jdbcTemplate,
                                         KafkaOperations<String, String> kafkaTemplate,
                                         OutboxProperties properties,
                                         OutboxMetrics metrics) {
            ObjectMapper mapper = ObjectMapperFactory.create();
            return new OutboxPoller(dataSource, jdbcTemplate, kafkaTemplate, mapper, properties, metrics);
        }

        @Bean
        @ConditionalOnMissingBean
        public OutboxRetentionCleaner outboxRetentionCleaner(JdbcTemplate jdbcTemplate,
                                                             OutboxProperties properties) {
            return new OutboxRetentionCleaner(jdbcTemplate, properties);
        }
    }
}
