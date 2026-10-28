package com.ticketing.common.correlation;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

import java.nio.charset.StandardCharsets;

/** Binds the {@code X-Correlation-Id} Kafka header to the MDC for the duration of record processing. */
public class CorrelationRecordInterceptor<K, V> implements RecordInterceptor<K, V> {

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        Header header = record.headers().lastHeader(CorrelationConstants.KAFKA_HEADER);
        if (header != null && header.value() != null) {
            MDC.put(CorrelationConstants.MDC_KEY, new String(header.value(), StandardCharsets.UTF_8));
        }
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        MDC.remove(CorrelationConstants.MDC_KEY);
    }
}
