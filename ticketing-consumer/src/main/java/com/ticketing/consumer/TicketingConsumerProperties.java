package com.ticketing.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Mandatory Kafka consumer configuration settings (docs/arch/03-kafka.md):
 * <ul>
 *     <li>{@code key.deserializer = StringDeserializer}</li>
 *     <li>{@code value.deserializer = StringDeserializer} (plain StringDeserializer, no JSON or ErrorHandlingDeserializer)</li>
 *     <li>{@code enable.auto.commit = false}</li>
 *     <li>{@code isolation.level = "read_committed"}</li>
 *     <li>{@code auto.offset.reset = "earliest"}</li>
 *     <li>{@code max.poll.records = 100}</li>
 *     <li>{@code max.poll.interval.ms = 300000}</li>
 * </ul>
 */
public final class TicketingConsumerProperties {

    public static final String ISOLATION_LEVEL = "read_committed";
    public static final String AUTO_OFFSET_RESET = "earliest";
    public static final int MAX_POLL_RECORDS = 100;
    public static final int MAX_POLL_INTERVAL_MS = 300_000;

    private TicketingConsumerProperties() {
    }

    public static Map<String, Object> defaults(String bootstrapServers) {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers must not be null");
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        applyConsumerSettings(props);
        return props;
    }

    public static void applyConsumerSettings(Map<String, Object> props) {
        Objects.requireNonNull(props, "props must not be null");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ISOLATION_LEVEL);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, MAX_POLL_INTERVAL_MS);
    }
}

