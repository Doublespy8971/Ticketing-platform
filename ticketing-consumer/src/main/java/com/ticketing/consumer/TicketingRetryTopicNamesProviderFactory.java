package com.ticketing.consumer;

import com.ticketing.common.messaging.Topics;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.kafka.support.Suffixer;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Naming provider factory that qualifies retry and dead-letter topic names with the consumer group
 * and suffixes consumer group IDs to match {@code docs/arch/03-kafka.md} and {@code infra/kafka/create-topics.sh}:
 * <ul>
 *     <li>Retry 0 topic: {@code <topic>-<consumer-group>-retry-0} (group: {@code <consumer-group>-retry-0})</li>
 *     <li>Retry 1 topic: {@code <topic>-<consumer-group>-retry-1} (group: {@code <consumer-group>-retry-1})</li>
 *     <li>Dead-letter topic: {@code <topic>-<consumer-group>-dlt} (group: {@code <consumer-group>-dlt})</li>
 * </ul>
 */
public class TicketingRetryTopicNamesProviderFactory implements RetryTopicNamesProviderFactory {

    public static final String RETRY_0_SUFFIX = "-retry-0";
    public static final String RETRY_1_SUFFIX = "-retry-1";
    public static final String DLT_SUFFIX = "-dlt";

    public static final String ORDER_SERVICE_INVENTORY_CG = "order-service-inventory-cg";
    public static final String PAYMENT_SERVICE_ORDER_CG = "payment-service-order-cg";
    public static final String INVENTORY_SERVICE_PAYMENT_CG = "inventory-service-payment-cg";
    public static final String ORDER_SERVICE_PAYMENT_CG = "order-service-payment-cg";
    public static final String NOTIFICATION_SERVICE_ORDER_CG = "notification-service-order-cg";

    public static final Set<String> WHITELISTED_CONSUMER_GROUPS = Set.of(
            ORDER_SERVICE_INVENTORY_CG,
            PAYMENT_SERVICE_ORDER_CG,
            INVENTORY_SERVICE_PAYMENT_CG,
            ORDER_SERVICE_PAYMENT_CG,
            NOTIFICATION_SERVICE_ORDER_CG
    );

    private static final Set<String> WHITELISTED_TOPICS = Set.of(
            Topics.INVENTORY_EVENTS,
            Topics.ORDER_EVENTS,
            Topics.PAYMENT_EVENTS
    );

    private static final ThreadLocal<String> ENDPOINT_CONSUMER_GROUP = new ThreadLocal<>();

    private final String defaultConsumerGroup;

    public TicketingRetryTopicNamesProviderFactory() {
        this(null);
    }

    public TicketingRetryTopicNamesProviderFactory(String defaultConsumerGroup) {
        this.defaultConsumerGroup = defaultConsumerGroup;
    }

    /**
     * Binds {@code consumerGroup} to the current thread while {@link org.springframework.kafka.retrytopic.RetryTopicConfigurer}
     * customizes a {@code @KafkaListener} endpoint and collects its retry and DLT topic names.
     */
    public static <T> T withConsumerGroup(String consumerGroup, Supplier<T> action) {
        String validated = validateWhitelistedConsumerGroup(consumerGroup);
        String previous = ENDPOINT_CONSUMER_GROUP.get();
        try {
            ENDPOINT_CONSUMER_GROUP.set(validated);
            return action.get();
        } finally {
            if (previous == null) {
                ENDPOINT_CONSUMER_GROUP.remove();
            } else {
                ENDPOINT_CONSUMER_GROUP.set(previous);
            }
        }
    }

    /**
     * Validates that {@code groupId} is non-blank and, after stripping any retry or DLT suffix,
     * belongs to {@link #WHITELISTED_CONSUMER_GROUPS} defined in {@code docs/arch/03-kafka.md}.
     *
     * @throws IllegalStateException if {@code groupId} is null, blank, or not in the whitelist
     */
    public static String validateWhitelistedConsumerGroup(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalStateException(
                    "A @KafkaListener endpoint must declare an explicit non-blank groupId");
        }
        String baseGroup = stripRetryOrDltSuffix(groupId.trim());
        if (baseGroup == null || baseGroup.isBlank() || !WHITELISTED_CONSUMER_GROUPS.contains(baseGroup)) {
            throw new IllegalStateException(
                    "Consumer group '" + groupId + "' (base group '" + baseGroup
                            + "') is not in the whitelisted consumer groups from docs/arch/03-kafka.md: "
                            + WHITELISTED_CONSUMER_GROUPS);
        }
        return baseGroup;
    }

    public static String retryTopicName(String mainTopic, String consumerGroup, int attemptIndex) {
        requireNonBlank(mainTopic, "mainTopic");
        requireNonBlank(consumerGroup, "consumerGroup");
        if (attemptIndex < 0) {
            throw new IllegalArgumentException("attemptIndex must be >= 0, but was: " + attemptIndex);
        }
        return mainTopic + "-" + stripRetryOrDltSuffix(consumerGroup.trim()) + "-retry-" + attemptIndex;
    }

    public static String dltTopicName(String mainTopic, String consumerGroup) {
        requireNonBlank(mainTopic, "mainTopic");
        requireNonBlank(consumerGroup, "consumerGroup");
        return mainTopic + "-" + stripRetryOrDltSuffix(consumerGroup.trim()) + DLT_SUFFIX;
    }

    public static String retryGroupId(String consumerGroup, int attemptIndex) {
        requireNonBlank(consumerGroup, "consumerGroup");
        if (attemptIndex < 0) {
            throw new IllegalArgumentException("attemptIndex must be >= 0, but was: " + attemptIndex);
        }
        return stripRetryOrDltSuffix(consumerGroup.trim()) + "-retry-" + attemptIndex;
    }

    public static String dltGroupId(String consumerGroup) {
        requireNonBlank(consumerGroup, "consumerGroup");
        return stripRetryOrDltSuffix(consumerGroup.trim()) + DLT_SUFFIX;
    }

    public static RetryTopicNames topicNamesFor(String mainTopic, String consumerGroup) {
        String baseGroup = stripRetryOrDltSuffix(consumerGroup.trim());
        return new RetryTopicNames(
                mainTopic,
                baseGroup,
                retryTopicName(mainTopic, baseGroup, 0),
                retryTopicName(mainTopic, baseGroup, 1),
                dltTopicName(mainTopic, baseGroup),
                retryGroupId(baseGroup, 0),
                retryGroupId(baseGroup, 1),
                dltGroupId(baseGroup)
        );
    }

    /**
     * Strips any {@code -retry-0}, {@code -retry-1}, or {@code -dlt} suffix from a consumer group ID or topic name.
     */
    public static String stripRetryOrDltSuffix(String value) {
        if (value == null) {
            return null;
        }
        if (value.endsWith(RETRY_0_SUFFIX)) {
            return value.substring(0, value.length() - RETRY_0_SUFFIX.length());
        }
        if (value.endsWith(RETRY_1_SUFFIX)) {
            return value.substring(0, value.length() - RETRY_1_SUFFIX.length());
        }
        if (value.endsWith(DLT_SUFFIX)) {
            return value.substring(0, value.length() - DLT_SUFFIX.length());
        }
        return value;
    }

    /**
     * Returns {@code true} when {@code topic} is a primary domain topic (not a {@code -retry-N} or {@code -dlt} topic).
     */
    public static boolean isMainTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            return false;
        }
        return !topic.endsWith(RETRY_0_SUFFIX)
                && !topic.endsWith(RETRY_1_SUFFIX)
                && !topic.endsWith(DLT_SUFFIX)
                && !topic.contains("-retry-");
    }

    /**
     * Derives the original main topic from trusted listener/destination topic names using suffix stripping,
     * never consulting untrusted Kafka record headers.
     */
    public static String deriveTrustedOriginalTopic(String destinationTopic, String sourceTopic) {
        if (isMainTopic(sourceTopic)) {
            return sourceTopic;
        }
        String withoutSuffix = stripRetryOrDltSuffix(destinationTopic != null ? destinationTopic : sourceTopic);
        if (withoutSuffix != null) {
            for (String group : WHITELISTED_CONSUMER_GROUPS) {
                String suffix = "-" + group;
                if (withoutSuffix.endsWith(suffix) && withoutSuffix.length() > suffix.length()) {
                    return withoutSuffix.substring(0, withoutSuffix.length() - suffix.length());
                }
            }
            for (String knownTopic : WHITELISTED_TOPICS) {
                if (withoutSuffix.startsWith(knownTopic + "-")) {
                    return knownTopic;
                }
            }
        }
        return sourceTopic != null ? sourceTopic : ConsumerMetrics.UNKNOWN_TAG;
    }

    /**
     * Derives the base consumer group from the trusted listener exception group ID or destination topic name
     * using suffix stripping, never consulting untrusted Kafka record headers.
     */
    public static String deriveTrustedConsumerGroup(String listenerGroupId, String destinationTopic, String originalTopic) {
        if (listenerGroupId != null && !listenerGroupId.isBlank()) {
            return stripRetryOrDltSuffix(listenerGroupId.trim());
        }
        String withoutSuffix = stripRetryOrDltSuffix(destinationTopic);
        if (withoutSuffix != null) {
            for (String group : WHITELISTED_CONSUMER_GROUPS) {
                if (withoutSuffix.endsWith("-" + group)) {
                    return group;
                }
            }
            if (originalTopic != null && withoutSuffix.startsWith(originalTopic + "-")
                    && withoutSuffix.length() > originalTopic.length() + 1) {
                return withoutSuffix.substring(originalTopic.length() + 1);
            }
        }
        return ConsumerMetrics.UNKNOWN_TAG;
    }

    @Override
    public RetryTopicNamesProvider createRetryTopicNamesProvider(DestinationTopic.Properties properties) {
        Objects.requireNonNull(properties, "properties must not be null");
        return new TicketingRetryTopicNamesProvider(properties, resolveConsumerGroup(null));
    }

    private String resolveConsumerGroup(String candidate) {
        if (candidate != null && !candidate.isBlank()) {
            return validateWhitelistedConsumerGroup(candidate);
        }
        String fromThreadLocal = ENDPOINT_CONSUMER_GROUP.get();
        if (fromThreadLocal != null && !fromThreadLocal.isBlank()) {
            return validateWhitelistedConsumerGroup(fromThreadLocal);
        }
        if (defaultConsumerGroup != null && !defaultConsumerGroup.isBlank()) {
            return validateWhitelistedConsumerGroup(defaultConsumerGroup);
        }
        return null;
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be null or blank");
        }
    }

    public record RetryTopicNames(
            String mainTopic,
            String consumerGroup,
            String retry0Topic,
            String retry1Topic,
            String dltTopic,
            String retry0Group,
            String retry1Group,
            String dltGroup) {

        public List<String> allDerivedTopics() {
            return List.of(retry0Topic, retry1Topic, dltTopic);
        }
    }

    public static final class TicketingRetryTopicNamesProvider implements RetryTopicNamesProvider {

        private final DestinationTopic.Properties properties;
        private final Suffixer suffixer;
        private volatile String consumerGroup;

        public TicketingRetryTopicNamesProvider(DestinationTopic.Properties properties, String consumerGroup) {
            this.properties = Objects.requireNonNull(properties, "properties must not be null");
            this.suffixer = new Suffixer(properties.suffix());
            this.consumerGroup = consumerGroup != null ? validateWhitelistedConsumerGroup(consumerGroup) : null;
        }

        @Override
        public String getEndpointId(KafkaListenerEndpoint endpoint) {
            captureConsumerGroup(endpoint);
            return suffixer.maybeAddTo(endpoint.getId());
        }

        @Override
        public String getGroupId(KafkaListenerEndpoint endpoint) {
            String baseGroup = captureConsumerGroup(endpoint);
            return suffixer.maybeAddTo(baseGroup);
        }

        @Override
        public String getClientIdPrefix(KafkaListenerEndpoint endpoint) {
            captureConsumerGroup(endpoint);
            return suffixer.maybeAddTo(endpoint.getClientIdPrefix());
        }

        @Override
        public String getGroup(KafkaListenerEndpoint endpoint) {
            captureConsumerGroup(endpoint);
            return suffixer.maybeAddTo(endpoint.getGroup());
        }

        @Override
        public String getTopicName(String topic) {
            String resolvedGroup = this.consumerGroup;
            if (resolvedGroup == null || resolvedGroup.isBlank()) {
                resolvedGroup = ENDPOINT_CONSUMER_GROUP.get();
            }
            String baseGroup = validateWhitelistedConsumerGroup(resolvedGroup);
            if (properties.isMainEndpoint()) {
                return topic;
            }
            return suffixer.maybeAddTo(topic + "-" + baseGroup);
        }

        private String captureConsumerGroup(KafkaListenerEndpoint endpoint) {
            if (endpoint == null) {
                return validateWhitelistedConsumerGroup(this.consumerGroup);
            }
            String baseGroup = validateWhitelistedConsumerGroup(endpoint.getGroupId());
            this.consumerGroup = baseGroup;
            return baseGroup;
        }
    }
}
