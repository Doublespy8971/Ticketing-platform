package com.ticketing.consumer;

import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.kafka.support.Suffixer;

import java.util.List;
import java.util.Objects;
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
        String previous = ENDPOINT_CONSUMER_GROUP.get();
        try {
            ENDPOINT_CONSUMER_GROUP.set(consumerGroup);
            return action.get();
        } finally {
            if (previous == null) {
                ENDPOINT_CONSUMER_GROUP.remove();
            } else {
                ENDPOINT_CONSUMER_GROUP.set(previous);
            }
        }
    }

    public static String retryTopicName(String mainTopic, String consumerGroup, int attemptIndex) {
        requireNonBlank(mainTopic, "mainTopic");
        requireNonBlank(consumerGroup, "consumerGroup");
        if (attemptIndex < 0) {
            throw new IllegalArgumentException("attemptIndex must be >= 0, but was: " + attemptIndex);
        }
        return mainTopic + "-" + consumerGroup + "-retry-" + attemptIndex;
    }

    public static String dltTopicName(String mainTopic, String consumerGroup) {
        requireNonBlank(mainTopic, "mainTopic");
        requireNonBlank(consumerGroup, "consumerGroup");
        return mainTopic + "-" + consumerGroup + DLT_SUFFIX;
    }

    public static String retryGroupId(String consumerGroup, int attemptIndex) {
        requireNonBlank(consumerGroup, "consumerGroup");
        if (attemptIndex < 0) {
            throw new IllegalArgumentException("attemptIndex must be >= 0, but was: " + attemptIndex);
        }
        return consumerGroup + "-retry-" + attemptIndex;
    }

    public static String dltGroupId(String consumerGroup) {
        requireNonBlank(consumerGroup, "consumerGroup");
        return consumerGroup + DLT_SUFFIX;
    }

    public static RetryTopicNames topicNamesFor(String mainTopic, String consumerGroup) {
        return new RetryTopicNames(
                mainTopic,
                consumerGroup,
                retryTopicName(mainTopic, consumerGroup, 0),
                retryTopicName(mainTopic, consumerGroup, 1),
                dltTopicName(mainTopic, consumerGroup),
                retryGroupId(consumerGroup, 0),
                retryGroupId(consumerGroup, 1),
                dltGroupId(consumerGroup)
        );
    }

    /**
     * Strips any {@code -retry-0}, {@code -retry-1}, or {@code -dlt} suffix from a consumer group ID.
     */
    public static String stripRetryOrDltSuffix(String groupId) {
        if (groupId == null) {
            return null;
        }
        if (groupId.endsWith(RETRY_0_SUFFIX)) {
            return groupId.substring(0, groupId.length() - RETRY_0_SUFFIX.length());
        }
        if (groupId.endsWith(RETRY_1_SUFFIX)) {
            return groupId.substring(0, groupId.length() - RETRY_1_SUFFIX.length());
        }
        if (groupId.endsWith(DLT_SUFFIX)) {
            return groupId.substring(0, groupId.length() - DLT_SUFFIX.length());
        }
        return groupId;
    }

    @Override
    public RetryTopicNamesProvider createRetryTopicNamesProvider(DestinationTopic.Properties properties) {
        Objects.requireNonNull(properties, "properties must not be null");
        return new TicketingRetryTopicNamesProvider(properties, resolveConsumerGroup(null));
    }

    private String resolveConsumerGroup(String candidate) {
        if (candidate != null && !candidate.isBlank()) {
            return stripRetryOrDltSuffix(candidate);
        }
        String fromThreadLocal = ENDPOINT_CONSUMER_GROUP.get();
        if (fromThreadLocal != null && !fromThreadLocal.isBlank()) {
            return stripRetryOrDltSuffix(fromThreadLocal);
        }
        if (defaultConsumerGroup != null && !defaultConsumerGroup.isBlank()) {
            return stripRetryOrDltSuffix(defaultConsumerGroup);
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
            this.consumerGroup = consumerGroup;
        }

        @Override
        public String getEndpointId(KafkaListenerEndpoint endpoint) {
            captureConsumerGroup(endpoint);
            return suffixer.maybeAddTo(endpoint.getId());
        }

        @Override
        public String getGroupId(KafkaListenerEndpoint endpoint) {
            captureConsumerGroup(endpoint);
            return suffixer.maybeAddTo(endpoint.getGroupId());
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
            if (properties.isMainEndpoint()) {
                return topic;
            }
            String resolvedGroup = this.consumerGroup;
            if (resolvedGroup == null || resolvedGroup.isBlank()) {
                resolvedGroup = ENDPOINT_CONSUMER_GROUP.get();
            }
            if (resolvedGroup == null || resolvedGroup.isBlank()) {
                throw new IllegalStateException(
                        "Cannot build retry/DLT topic name for topic '" + topic
                                + "' without a non-blank consumer group ID on the @KafkaListener endpoint");
            }
            String baseGroup = stripRetryOrDltSuffix(resolvedGroup);
            return suffixer.maybeAddTo(topic + "-" + baseGroup);
        }

        private void captureConsumerGroup(KafkaListenerEndpoint endpoint) {
            if (endpoint == null) {
                return;
            }
            String groupId = endpoint.getGroupId();
            if (groupId == null || groupId.isBlank()) {
                groupId = endpoint.getGroup();
            }
            if (groupId == null || groupId.isBlank()) {
                groupId = endpoint.getId();
            }
            if (groupId != null && !groupId.isBlank()) {
                this.consumerGroup = stripRetryOrDltSuffix(groupId);
            }
        }
    }
}

