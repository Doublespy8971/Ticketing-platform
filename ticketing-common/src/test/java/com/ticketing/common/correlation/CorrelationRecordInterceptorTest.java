package com.ticketing.common.correlation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationRecordInterceptorTest {

    private final CorrelationRecordInterceptor<String, String> interceptor = new CorrelationRecordInterceptor<>();

    @AfterEach
    void cleanup() {
        MDC.clear();
    }

    @Test
    void putsHeaderInMdcAndClearsAfterRecord() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("order.events", 0, 0L, "k", "v");
        record.headers().add(CorrelationConstants.KAFKA_HEADER, "abc-123".getBytes(StandardCharsets.UTF_8));

        interceptor.intercept(record, null);
        assertThat(MDC.get(CorrelationConstants.MDC_KEY)).isEqualTo("abc-123");

        interceptor.afterRecord(record, null);
        assertThat(MDC.get(CorrelationConstants.MDC_KEY)).isNull();
    }

    @Test
    void leavesMdcUntouchedWithoutHeader() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("order.events", 0, 0L, "k", "v");
        interceptor.intercept(record, null);
        assertThat(MDC.get(CorrelationConstants.MDC_KEY)).isNull();
    }
}
