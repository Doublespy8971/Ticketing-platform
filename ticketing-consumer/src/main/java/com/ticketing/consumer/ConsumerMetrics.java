package com.ticketing.consumer;

import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.MeterFilter;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Consumer Micrometer metrics conforming to {@code docs/arch/09-conventions.md} and {@code docs/arch/03-kafka.md}:
 * <ul>
 *     <li>{@code ticketing_kafka_consumer_dlt_total} (Counter, tags: {@code service} from common tags, {@code topic}, {@code consumer_group})</li>
 *     <li>{@code consumer_events_processed_total} (Counter, tags: {@code service} from common tags, {@code consumer_group}, {@code event_type})</li>
 *     <li>{@code consumer_events_duplicate_total} (Counter, tags: {@code service} from common tags, {@code consumer_group}, {@code event_type})</li>
 *     <li>{@code consumer_events_retried_total} (Counter, tags: {@code service} from common tags, {@code consumer_group}, {@code event_type})</li>
 *     <li>{@code consumer_event_processing_seconds} (Timer, tags: {@code service} from common tags, {@code consumer_group}, {@code event_type})</li>
 * </ul>
 * <p>
 * The {@code service} tag is never added manually on individual meters; it comes from the registry's common tags.
 * All {@code event_type}, {@code topic}, and {@code consumer_group} tag values are whitelisted against known
 * architecture constants, with any unrecognized value mapped to {@code "unknown"} to prevent tag cardinality explosion.
 */
public class ConsumerMetrics {

    public static final String DLT_TOTAL_METRIC = "ticketing_kafka_consumer_dlt_total";
    public static final String PROCESSED_TOTAL_METRIC = "consumer_events_processed_total";
    public static final String DUPLICATE_TOTAL_METRIC = "consumer_events_duplicate_total";
    public static final String RETRIED_TOTAL_METRIC = "consumer_events_retried_total";
    public static final String PROCESSING_SECONDS_METRIC = "consumer_event_processing_seconds";

    public static final String UNKNOWN_TAG = "unknown";
    public static final String UNKNOWN_EVENT_TYPE = UNKNOWN_TAG;
    public static final String DEFAULT_SERVICE_NAME = "unknown-service";

    public static final Set<String> WHITELISTED_EVENT_TYPES = Set.of(
            EventTypes.INVENTORY_SEAT_RESERVED,
            EventTypes.INVENTORY_SEAT_RELEASED,
            EventTypes.INVENTORY_RESERVATION_EXPIRED,
            EventTypes.INVENTORY_SEAT_CONFIRMED,
            EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED,
            EventTypes.ORDER_PAYMENT_REQUESTED,
            EventTypes.ORDER_REFUND_REQUESTED,
            EventTypes.ORDER_CONFIRMED,
            EventTypes.ORDER_CANCELLED,
            EventTypes.PAYMENT_SUCCEEDED,
            EventTypes.PAYMENT_FAILED,
            EventTypes.PAYMENT_REFUNDED
    );

    public static final Set<String> WHITELISTED_TOPICS = Set.of(
            Topics.INVENTORY_EVENTS,
            Topics.ORDER_EVENTS,
            Topics.PAYMENT_EVENTS
    );

    public static final Set<String> WHITELISTED_CONSUMER_GROUPS =
            TicketingRetryTopicNamesProviderFactory.WHITELISTED_CONSUMER_GROUPS;

    private record GroupEventKey(String consumerGroup, String eventType) {
    }

    private record TopicGroupKey(String topic, String consumerGroup) {
    }

    private final MeterRegistry meterRegistry;
    private final ConcurrentMap<GroupEventKey, Counter> processedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<GroupEventKey, Counter> duplicateCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<GroupEventKey, Counter> retriedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<GroupEventKey, Timer> processingTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<TopicGroupKey, Counter> dltCounters = new ConcurrentHashMap<>();

    public ConsumerMetrics(MeterRegistry meterRegistry) {
        this(meterRegistry, DEFAULT_SERVICE_NAME);
    }

    public ConsumerMetrics(MeterRegistry meterRegistry, String fallbackServiceName) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        String resolvedFallback = (fallbackServiceName == null || fallbackServiceName.isBlank())
                ? DEFAULT_SERVICE_NAME
                : fallbackServiceName;
        this.meterRegistry.config().meterFilter(new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                if (id.getTag("service") == null) {
                    return id.withTag(Tag.of("service", resolvedFallback));
                }
                return id;
            }
        });
    }

    /**
     * Whitelists {@code eventType} against {@link EventTypes} constants, mapping {@code null}, blank,
     * or unrecognized values to {@code "unknown"}.
     */
    public static String sanitizeEventType(String eventType) {
        if (eventType != null && WHITELISTED_EVENT_TYPES.contains(eventType)) {
            return eventType;
        }
        return UNKNOWN_TAG;
    }

    /**
     * Whitelists {@code topic} against the three domain {@link Topics} constants, mapping {@code null},
     * blank, or unrecognized values to {@code "unknown"}.
     */
    public static String sanitizeTopic(String topic) {
        if (topic != null && WHITELISTED_TOPICS.contains(topic)) {
            return topic;
        }
        return UNKNOWN_TAG;
    }

    /**
     * Strips any retry/DLT suffix and whitelists {@code consumerGroup} against the consumer groups in
     * {@code docs/arch/03-kafka.md}, mapping {@code null}, blank, or unrecognized values to {@code "unknown"}.
     */
    public static String sanitizeConsumerGroup(String consumerGroup) {
        if (consumerGroup == null || consumerGroup.isBlank()) {
            return UNKNOWN_TAG;
        }
        String stripped = TicketingRetryTopicNamesProviderFactory.stripRetryOrDltSuffix(consumerGroup.trim());
        if (stripped != null && WHITELISTED_CONSUMER_GROUPS.contains(stripped)) {
            return stripped;
        }
        return UNKNOWN_TAG;
    }

    public void recordProcessed(String consumerGroup, String eventType) {
        Objects.requireNonNull(consumerGroup, "consumerGroup must not be null");
        String safeGroup = sanitizeConsumerGroup(consumerGroup);
        String safeEventType = sanitizeEventType(eventType);
        processedCounters.computeIfAbsent(new GroupEventKey(safeGroup, safeEventType), key ->
                Counter.builder(PROCESSED_TOTAL_METRIC)
                        .tag("consumer_group", key.consumerGroup())
                        .tag("event_type", key.eventType())
                        .description("Total Kafka events processed and committed for the first time")
                        .register(meterRegistry)
        ).increment();
    }

    public void recordDuplicate(String consumerGroup, String eventType) {
        Objects.requireNonNull(consumerGroup, "consumerGroup must not be null");
        String safeGroup = sanitizeConsumerGroup(consumerGroup);
        String safeEventType = sanitizeEventType(eventType);
        duplicateCounters.computeIfAbsent(new GroupEventKey(safeGroup, safeEventType), key ->
                Counter.builder(DUPLICATE_TOTAL_METRIC)
                        .tag("consumer_group", key.consumerGroup())
                        .tag("event_type", key.eventType())
                        .description("Total duplicate Kafka events skipped via processed_events ON CONFLICT DO NOTHING")
                        .register(meterRegistry)
        ).increment();
    }

    public void recordRetried(String consumerGroup, String eventType) {
        Objects.requireNonNull(consumerGroup, "consumerGroup must not be null");
        String safeGroup = sanitizeConsumerGroup(consumerGroup);
        String safeEventType = sanitizeEventType(eventType);
        retriedCounters.computeIfAbsent(new GroupEventKey(safeGroup, safeEventType), key ->
                Counter.builder(RETRIED_TOTAL_METRIC)
                        .tag("consumer_group", key.consumerGroup())
                        .tag("event_type", key.eventType())
                        .description("Total failed Kafka event deliveries forwarded to a retry topic")
                        .register(meterRegistry)
        ).increment();
    }

    public void recordProcessingDuration(String consumerGroup, String eventType, long durationNanos) {
        Objects.requireNonNull(consumerGroup, "consumerGroup must not be null");
        String safeGroup = sanitizeConsumerGroup(consumerGroup);
        String safeEventType = sanitizeEventType(eventType);
        if (durationNanos >= 0L) {
            processingTimers.computeIfAbsent(new GroupEventKey(safeGroup, safeEventType), key ->
                    Timer.builder(PROCESSING_SECONDS_METRIC)
                            .tag("consumer_group", key.consumerGroup())
                            .tag("event_type", key.eventType())
                            .description("End-to-end duration of IdempotentEventProcessor.process")
                            .register(meterRegistry)
            ).record(durationNanos, TimeUnit.NANOSECONDS);
        }
    }

    public void recordProcessingDuration(String consumerGroup, String eventType, Duration duration) {
        Objects.requireNonNull(duration, "duration must not be null");
        recordProcessingDuration(consumerGroup, eventType, duration.toNanos());
    }

    public void recordDeadLetter(String originalTopic, String consumerGroup) {
        Objects.requireNonNull(originalTopic, "originalTopic must not be null");
        Objects.requireNonNull(consumerGroup, "consumerGroup must not be null");
        String safeTopic = sanitizeTopic(originalTopic);
        String safeGroup = sanitizeConsumerGroup(consumerGroup);
        dltCounters.computeIfAbsent(new TopicGroupKey(safeTopic, safeGroup), key ->
                Counter.builder(DLT_TOTAL_METRIC)
                        .tag("topic", key.topic())
                        .tag("consumer_group", key.consumerGroup())
                        .description("Total messages routed to <topic>-<consumer-group>-dlt after exhausting retries")
                        .register(meterRegistry)
        ).increment();
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }
}
