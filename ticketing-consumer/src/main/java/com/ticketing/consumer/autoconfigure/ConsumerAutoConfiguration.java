package com.ticketing.consumer.autoconfigure;

import com.ticketing.common.correlation.CorrelationRecordInterceptor;
import com.ticketing.common.json.ObjectMapperFactory;
import com.ticketing.consumer.ConsumerCleanupProperties;
import com.ticketing.consumer.ConsumerEnvelopeParser;
import com.ticketing.consumer.ConsumerMetrics;
import com.ticketing.consumer.IdempotentEventProcessor;
import com.ticketing.consumer.NonRetryableEventException;
import com.ticketing.consumer.ProcessedEventsRetentionCleaner;
import com.ticketing.consumer.TicketingConsumerProperties;
import com.ticketing.consumer.TicketingDeadLetterPublishingRecoverer;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.DestinationTopicProcessor;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.EndpointCustomizer;
import org.springframework.kafka.retrytopic.ListenerContainerFactoryConfigurer;
import org.springframework.kafka.retrytopic.ListenerContainerFactoryResolver;
import org.springframework.kafka.retrytopic.RetryTopicComponentFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.kafka.retrytopic.RetryTopicConfigurer;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Spring Boot auto-configuration for {@code ticketing-consumer}:
 * <ul>
 *     <li>{@link IdempotentEventProcessor}, {@link ConsumerEnvelopeParser}, and {@link ConsumerMetrics}</li>
 *     <li>{@link ConsumerFactory} and {@link ConcurrentKafkaListenerContainerFactory} with plain
 *         {@code StringDeserializer}, {@code enable.auto.commit=false}, {@code AckMode.RECORD},
 *         {@code isolation.level=read_committed}, {@code auto.offset.reset=earliest},
 *         {@code max.poll.records=100}, {@code max.poll.interval.ms=300000}, and
 *         {@link CorrelationRecordInterceptor}</li>
 *     <li>Non-blocking retry topics ({@code -retry-0} at 2,000 ms, {@code -retry-1} at 10,000 ms) and
 *         dead-letter topic ({@code -dlt}) via {@link TicketingRetryTopicNamesProviderFactory} and
 *         {@link TicketingDeadLetterPublishingRecoverer}</li>
 *     <li>Scheduled {@code processed_events} retention cleaner on {@code ticketingConsumerCleanupScheduler}</li>
 * </ul>
 */
@AutoConfiguration(after = {
        JdbcTemplateAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        KafkaAutoConfiguration.class
})
@ConditionalOnClass({JdbcTemplate.class, KafkaTemplate.class})
@EnableConfigurationProperties(ConsumerCleanupProperties.class)
public class ConsumerAutoConfiguration {

    public static final int MAX_ATTEMPTS = 3;
    public static final long INITIAL_BACKOFF_DELAY_MS = 2_000L;
    public static final double BACKOFF_MULTIPLIER = 5.0;
    public static final long MAX_BACKOFF_DELAY_MS = 10_000L;

    @Bean
    @ConditionalOnMissingBean(name = "consumerServiceTagMeterRegistryCustomizer")
    public MeterRegistryCustomizer<MeterRegistry> consumerServiceTagMeterRegistryCustomizer(
            @Value("${spring.application.name:unknown-service}") String serviceName) {
        return registry -> registry.config().commonTags("service", serviceName);
    }

    @Bean
    @ConditionalOnMissingBean
    public ConsumerEnvelopeParser consumerEnvelopeParser() {
        return new ConsumerEnvelopeParser(ObjectMapperFactory.create());
    }

    @Bean
    @ConditionalOnMissingBean
    public ConsumerMetrics consumerMetrics(
            ObjectProvider<MeterRegistry> meterRegistryProvider,
            MeterRegistryCustomizer<MeterRegistry> consumerServiceTagMeterRegistryCustomizer,
            @Value("${spring.application.name:unknown-service}") String serviceName) {
        MeterRegistry registry = meterRegistryProvider.getIfAvailable(SimpleMeterRegistry::new);
        consumerServiceTagMeterRegistryCustomizer.customize(registry);
        return new ConsumerMetrics(registry, serviceName);
    }

    @Bean
    @ConditionalOnMissingBean(PlatformTransactionManager.class)
    public PlatformTransactionManager consumerTransactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public IdempotentEventProcessor idempotentEventProcessor(JdbcTemplate jdbcTemplate,
                                                             PlatformTransactionManager transactionManager,
                                                             ConsumerMetrics consumerMetrics,
                                                             ConsumerEnvelopeParser consumerEnvelopeParser) {
        return new IdempotentEventProcessor(
                jdbcTemplate,
                new org.springframework.transaction.support.TransactionTemplate(transactionManager),
                consumerMetrics,
                consumerEnvelopeParser
        );
    }

    @Bean
    public DefaultKafkaConsumerFactoryCustomizer ticketingConsumerFactoryCustomizer() {
        return consumerFactory -> {
            Map<String, Object> overrides = new HashMap<>();
            TicketingConsumerProperties.applyConsumerSettings(overrides);
            consumerFactory.updateConfigs(overrides);
        };
    }

    @Bean
    @ConditionalOnMissingBean(ConsumerFactory.class)
    public ConsumerFactory<String, String> kafkaConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            ObjectProvider<DefaultKafkaConsumerFactoryCustomizer> customizers) {
        DefaultKafkaConsumerFactory<String, String> factory =
                new DefaultKafkaConsumerFactory<>(TicketingConsumerProperties.defaults(bootstrapServers));
        customizers.orderedStream().forEach(customizer -> customizer.customize(factory));
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean(ProducerFactory.class)
    public ProducerFactory<String, String> kafkaProducerFactory(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    @ConditionalOnMissingBean(KafkaOperations.class)
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @ConditionalOnMissingBean(RecordInterceptor.class)
    public CorrelationRecordInterceptor<Object, Object> correlationRecordInterceptor() {
        return new CorrelationRecordInterceptor<>();
    }

    @Bean(name = "kafkaListenerContainerFactory")
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<?, ?> consumerFactory,
            ObjectProvider<RecordInterceptor<?, ?>> recordInterceptorProvider) {
        if (consumerFactory instanceof DefaultKafkaConsumerFactory<?, ?> defaultFactory) {
            Map<String, Object> overrides = new HashMap<>();
            TicketingConsumerProperties.applyConsumerSettings(overrides);
            defaultFactory.updateConfigs(overrides);
        }
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory((ConsumerFactory) consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        RecordInterceptor recordInterceptor = recordInterceptorProvider.getIfAvailable(CorrelationRecordInterceptor::new);
        factory.setRecordInterceptor(recordInterceptor);
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean(RetryTopicNamesProviderFactory.class)
    public TicketingRetryTopicNamesProviderFactory ticketingRetryTopicNamesProviderFactory() {
        return new TicketingRetryTopicNamesProviderFactory();
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryTopicSchedulerWrapper ticketingRetryTopicSchedulerWrapper() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("consumer-retry-backoff-");
        scheduler.setDaemon(true);
        scheduler.initialize();
        return new RetryTopicSchedulerWrapper(scheduler);
    }

    @Bean
    @ConditionalOnMissingBean(RetryTopicConfiguration.class)
    public RetryTopicConfiguration ticketingRetryTopicConfiguration(
            KafkaOperations<?, ?> kafkaTemplate) {
        return RetryTopicConfigurationBuilder.newInstance()
                .maxAttempts(MAX_ATTEMPTS)
                .exponentialBackoff(INITIAL_BACKOFF_DELAY_MS, BACKOFF_MULTIPLIER, MAX_BACKOFF_DELAY_MS)
                .setTopicSuffixingStrategy(TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
                .doNotAutoCreateRetryTopics()
                .dltProcessingFailureStrategy(DltStrategy.FAIL_ON_ERROR)
                .notRetryOn(NonRetryableEventException.class)
                .listenerFactory("kafkaListenerContainerFactory")
                .create(kafkaTemplate);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableKafka
    static class RetryTopicInfrastructureConfiguration extends RetryTopicConfigurationSupport {

        private final ConsumerMetrics consumerMetrics;
        private final ConsumerEnvelopeParser envelopeParser;
        private final RetryTopicNamesProviderFactory namesProviderFactory;

        RetryTopicInfrastructureConfiguration(ConsumerMetrics consumerMetrics,
                                              ConsumerEnvelopeParser envelopeParser,
                                              RetryTopicNamesProviderFactory namesProviderFactory) {
            this.consumerMetrics = consumerMetrics;
            this.envelopeParser = envelopeParser;
            this.namesProviderFactory = namesProviderFactory;
        }

        @Override
        protected void manageNonBlockingFatalExceptions(List<Class<? extends Throwable>> failingExceptions) {
            failingExceptions.clear();
            failingExceptions.add(NonRetryableEventException.class);
        }

        @Override
        protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
            return factory -> {
                factory.setRetainAllRetryHeaderValues(false);
                factory.setDeadLetterPublisherCreator((templateResolver, destinationResolver) ->
                        new TicketingDeadLetterPublishingRecoverer(
                                templateResolver,
                                destinationResolver,
                                consumerMetrics,
                                envelopeParser
                        ));
            };
        }

        @Override
        protected RetryTopicComponentFactory createComponentFactory() {
            return new RetryTopicComponentFactory() {
                @Override
                public RetryTopicNamesProviderFactory retryTopicNamesProviderFactory() {
                    return namesProviderFactory;
                }

                @Override
                public RetryTopicConfigurer retryTopicConfigurer(
                        DestinationTopicProcessor destinationTopicProcessor,
                        ListenerContainerFactoryConfigurer containerFactoryConfigurer,
                        ListenerContainerFactoryResolver factoryResolver,
                        RetryTopicNamesProviderFactory retryTopicNamesProviderFactory) {
                    return new RetryTopicConfigurer(
                            destinationTopicProcessor,
                            factoryResolver,
                            containerFactoryConfigurer,
                            retryTopicNamesProviderFactory
                    ) {
                        @Override
                        protected EndpointCustomizer<MethodKafkaListenerEndpoint<?, ?>> createEndpointCustomizer(
                                EndpointHandlerMethod endpointHandlerMethod,
                                DestinationTopic.Properties properties) {
                            EndpointCustomizer<MethodKafkaListenerEndpoint<?, ?>> delegate =
                                    super.createEndpointCustomizer(endpointHandlerMethod, properties);
                            return endpoint -> {
                                String groupId = endpoint.getGroupId();
                                if (groupId == null || groupId.isBlank()) {
                                    groupId = endpoint.getGroup();
                                }
                                if (groupId == null || groupId.isBlank()) {
                                    groupId = endpoint.getId();
                                }
                                return TicketingRetryTopicNamesProviderFactory.withConsumerGroup(
                                        groupId,
                                        () -> delegate.customizeEndpointAndCollectTopics(endpoint)
                                );
                            };
                        }
                    };
                }
            };
        }

        @Override
        @Bean(name = "org.springframework.kafka.config.internalKafkaConsumerBackOffManager")
        public KafkaConsumerBackoffManager kafkaConsumerBackoffManager(
                ApplicationContext applicationContext,
                @Qualifier(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
                ListenerContainerRegistry registry,
                ObjectProvider<RetryTopicComponentFactory> componentFactoryProvider,
                RetryTopicSchedulerWrapper wrapper,
                @Qualifier("ticketingConsumerCleanupScheduler")
                @Autowired(required = false) TaskScheduler taskScheduler) {
            return super.kafkaConsumerBackoffManager(
                    applicationContext,
                    registry,
                    componentFactoryProvider,
                    wrapper,
                    taskScheduler
            );
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(
            prefix = "ticketing.consumer.cleanup",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    @EnableScheduling
    static class CleanupSchedulingConfiguration {

        @Bean(name = "ticketingConsumerCleanupScheduler")
        @ConditionalOnMissingBean(name = "ticketingConsumerCleanupScheduler")
        public ThreadPoolTaskScheduler ticketingConsumerCleanupScheduler() {
            ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
            scheduler.setPoolSize(1);
            scheduler.setThreadNamePrefix("consumer-cleanup-");
            scheduler.setDaemon(true);
            scheduler.setWaitForTasksToCompleteOnShutdown(true);
            scheduler.setAwaitTerminationSeconds(5);
            return scheduler;
        }

        @Bean
        @ConditionalOnMissingBean
        public ProcessedEventsRetentionCleaner processedEventsRetentionCleaner(
                JdbcTemplate jdbcTemplate,
                ConsumerCleanupProperties properties) {
            return new ProcessedEventsRetentionCleaner(jdbcTemplate, properties);
        }
    }
}
