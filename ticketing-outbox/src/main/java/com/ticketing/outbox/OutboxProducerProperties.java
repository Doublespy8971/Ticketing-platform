package com.ticketing.outbox;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Provides and validates the Kafka producer settings for the transactional outbox poller:
 * {@code acks=all}, {@code enable.idempotence=true}, {@code max.in.flight.requests.per.connection=5},
 * {@code retries=Integer.MAX_VALUE}, {@code compression.type=lz4}, {@code linger.ms=5},
 * {@code request.timeout.ms=1500}, and {@code delivery.timeout.ms=4000}.
 */
public final class OutboxProducerProperties {

    public static final int DEFAULT_LINGER_MS = 5;
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 1500;
    public static final int DEFAULT_DELIVERY_TIMEOUT_MS = 4000;

    private OutboxProducerProperties() {
    }

    public static Map<String, Object> defaults(String bootstrapServers) {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers must not be null");
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        applyIdempotentSettings(props);
        return props;
    }

    public static void applyIdempotentSettings(Map<String, Object> props) {
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.putIfAbsent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG, DEFAULT_LINGER_MS);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, DEFAULT_REQUEST_TIMEOUT_MS);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DEFAULT_DELIVERY_TIMEOUT_MS);
    }

    public static void validateConfiguration(KafkaOperations<?, ?> kafkaOperations) {
        Objects.requireNonNull(kafkaOperations, "kafkaOperations must not be null");
        validateConfiguration(kafkaOperations.getProducerFactory());
    }

    public static void validateConfiguration(ProducerFactory<?, ?> producerFactory) {
        if (producerFactory == null) {
            throw new IllegalStateException("Outbox KafkaOperations must expose a non-null ProducerFactory");
        }
        Map<String, Object> configs = producerFactory.getConfigurationProperties();
        if (configs == null) {
            throw new IllegalStateException("Outbox ProducerFactory must expose non-null configurationProperties");
        }
        Object acks = configs.get(ProducerConfig.ACKS_CONFIG);
        if (!"all".equals(acks == null ? null : acks.toString())) {
            throw new IllegalStateException(
                    "Outbox producer configuration requires acks=all, but got acks=" + acks);
        }
        Object idempotence = configs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        boolean isIdempotent = (idempotence instanceof Boolean b && b)
                || (idempotence instanceof String s && "true".equalsIgnoreCase(s.trim()));
        if (!isIdempotent) {
            throw new IllegalStateException(
                    "Outbox producer configuration requires enable.idempotence=true, but got enable.idempotence="
                            + idempotence);
        }
    }
}
