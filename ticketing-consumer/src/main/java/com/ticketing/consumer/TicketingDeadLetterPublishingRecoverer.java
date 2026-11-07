package com.ticketing.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandlingUtils;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.kafka.support.KafkaHeaders;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Custom {@link DeadLetterPublishingRecoverer} used across the non-blocking retry topic chain
 * ({@code main -> -retry-0 -> -retry-1 -> -dlt}) that:
 * <ol>
 *     <li>Enforces the standard {@link KafkaHeaders} {@code kafka_dlt-*} header names on all forwarded records.</li>
 *     <li>Preserves the first-failure {@code kafka_dlt-original-topic}, {@code kafka_dlt-original-partition},
 *         {@code kafka_dlt-original-offset}, and {@code kafka_dlt-original-timestamp} from the main topic across
 *         all retry hops ({@code appendOriginalHeaders = false}).</li>
 *     <li>Updates {@code kafka_dlt-exception-fqcn}, {@code kafka_dlt-exception-message}, and
 *         {@code kafka_dlt-exception-stacktrace} on each hop to reflect the unwrapped exception from the latest
 *         failure.</li>
 *     <li>Increments {@code consumer_events_retried_total} when forwarding to {@code -retry-0} or {@code -retry-1},
 *         and {@code ticketing_kafka_consumer_dlt_total} when forwarding to {@code -dlt}.</li>
 * </ol>
 */
public class TicketingDeadLetterPublishingRecoverer extends DeadLetterPublishingRecoverer {

    private static final HeaderNames STANDARD_DLT_HEADER_NAMES = HeaderNames.Builder
            .original()
            .offsetHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)
            .timestampHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)
            .timestampTypeHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)
            .topicHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)
            .partitionHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)
            .consumerGroupHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)
            .exception()
            .keyExceptionFqcn(KafkaHeaders.DLT_KEY_EXCEPTION_FQCN)
            .exceptionFqcn(KafkaHeaders.DLT_EXCEPTION_FQCN)
            .exceptionCauseFqcn(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)
            .keyExceptionMessage(KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE)
            .exceptionMessage(KafkaHeaders.DLT_EXCEPTION_MESSAGE)
            .keyExceptionStacktrace(KafkaHeaders.DLT_KEY_EXCEPTION_STACKTRACE)
            .exceptionStacktrace(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)
            .build();

    private final ConsumerMetrics metrics;
    private final ConsumerEnvelopeParser envelopeParser;

    public TicketingDeadLetterPublishingRecoverer(
            Function<ProducerRecord<?, ?>, KafkaOperations<?, ?>> templateResolver,
            BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver,
            ConsumerMetrics metrics,
            ConsumerEnvelopeParser envelopeParser) {
        super(templateResolver, destinationResolver);
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.envelopeParser = Objects.requireNonNull(envelopeParser, "envelopeParser must not be null");
        super.setHeaderNamesSupplier(() -> STANDARD_DLT_HEADER_NAMES);
        super.setAppendOriginalHeaders(false);
        super.setStripPreviousExceptionHeaders(true);
        super.setFailIfSendResultIsError(true);
        super.setExceptionHeadersCreator(TicketingDeadLetterPublishingRecoverer::writeLatestExceptionHeaders);
    }

    @Override
    public void setHeaderNamesSupplier(Supplier<HeaderNames> headerNamesSupplier) {
        // Ignore overrides from DeadLetterPublishingRecovererFactory so standard kafka_dlt-* header names are preserved
        super.setHeaderNamesSupplier(() -> STANDARD_DLT_HEADER_NAMES);
    }

    @Override
    protected void publish(ProducerRecord<Object, Object> outRecord,
                           KafkaOperations<Object, Object> kafkaTemplate,
                           ConsumerRecord<?, ?> inRecord) {
        super.publish(outRecord, kafkaTemplate, inRecord);

        String destinationTopic = outRecord.topic();
        String originalTopic = resolveOriginalTopic(outRecord, inRecord);
        String consumerGroup = resolveOriginalConsumerGroup(outRecord, originalTopic, destinationTopic);

        if (destinationTopic.endsWith(TicketingRetryTopicNamesProviderFactory.DLT_SUFFIX)) {
            metrics.recordDeadLetter(originalTopic, consumerGroup);
        } else if (destinationTopic.contains("-retry-")) {
            String eventType = envelopeParser.extractWhitelistedEventTypeOrUnknown(inRecord);
            metrics.recordRetried(consumerGroup, eventType);
        }
    }

    static void writeLatestExceptionHeaders(Headers headers,
                                            Exception exception,
                                            boolean isKey,
                                            HeaderNames headerNames) {
        Throwable unwrapped = unwrapFrameworkException(exception);
        String fqcnHeader = isKey
                ? KafkaHeaders.DLT_KEY_EXCEPTION_FQCN
                : KafkaHeaders.DLT_EXCEPTION_FQCN;
        String messageHeader = isKey
                ? KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE
                : KafkaHeaders.DLT_EXCEPTION_MESSAGE;
        String stacktraceHeader = isKey
                ? KafkaHeaders.DLT_KEY_EXCEPTION_STACKTRACE
                : KafkaHeaders.DLT_EXCEPTION_STACKTRACE;
        String causeFqcnHeader = KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN;

        replaceHeader(headers, fqcnHeader, unwrapped.getClass().getName().getBytes(StandardCharsets.UTF_8));

        Exception rootCauseSource = unwrapped instanceof Exception ex ? ex : exception;
        Throwable rootCause = ErrorHandlingUtils.findRootCause(rootCauseSource);
        if (rootCause != null) {
            replaceHeader(headers, causeFqcnHeader, rootCause.getClass().getName().getBytes(StandardCharsets.UTF_8));
        }

        String message = unwrapped.getMessage();
        if (message == null && rootCause != null) {
            message = rootCause.getMessage();
        }
        if (message == null) {
            message = "";
        }
        replaceHeader(headers, messageHeader, message.getBytes(StandardCharsets.UTF_8));

        StringWriter sw = new StringWriter();
        unwrapped.printStackTrace(new PrintWriter(sw, true));
        replaceHeader(headers, stacktraceHeader, sw.toString().getBytes(StandardCharsets.UTF_8));
    }

    static Throwable unwrapFrameworkException(Throwable throwable) {
        Throwable current = throwable;
        while (current instanceof TimestampedException || current instanceof ListenerExecutionFailedException) {
            if (current.getCause() == null || current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return current != null ? current : throwable;
    }

    private static void replaceHeader(Headers headers, String key, byte[] value) {
        headers.remove(key);
        headers.add(key, value);
    }

    private static String resolveOriginalTopic(ProducerRecord<Object, Object> outRecord, ConsumerRecord<?, ?> inRecord) {
        Header header = outRecord.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC);
        if (header != null && header.value() != null && header.value().length > 0) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return inRecord.topic();
    }

    private static String resolveOriginalConsumerGroup(ProducerRecord<Object, Object> outRecord,
                                                       String originalTopic,
                                                       String destinationTopic) {
        Header header = outRecord.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP);
        if (header != null && header.value() != null && header.value().length > 0) {
            String rawGroup = new String(header.value(), StandardCharsets.UTF_8);
            return TicketingRetryTopicNamesProviderFactory.stripRetryOrDltSuffix(rawGroup);
        }
        String withoutSuffix = TicketingRetryTopicNamesProviderFactory.stripRetryOrDltSuffix(destinationTopic);
        String prefix = originalTopic + "-";
        if (withoutSuffix != null && withoutSuffix.startsWith(prefix) && withoutSuffix.length() > prefix.length()) {
            return withoutSuffix.substring(prefix.length());
        }
        return "unknown-group";
    }
}
