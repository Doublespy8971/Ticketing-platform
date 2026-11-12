package com.ticketing.payment.messaging;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.messaging.Topics;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import com.ticketing.payment.service.PaymentService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Component
public class PaymentOrderEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentOrderEventsConsumer.class);

    private final PaymentService paymentService;

    public PaymentOrderEventsConsumer(PaymentService paymentService) {
        this.paymentService = Objects.requireNonNull(paymentService, "paymentService must not be null");
    }

    @KafkaListener(
            topics = Topics.ORDER_EVENTS,
            groupId = TicketingRetryTopicNamesProviderFactory.PAYMENT_SERVICE_ORDER_CG,
            autoStartup = "${spring.kafka.listener.auto-startup:true}"
    )
    public void onOrderEvent(ConsumerRecord<String, String> record) {
        log.info("Received order event on topic={}, partition={}, offset={}, key={}",
                record != null ? record.topic() : null,
                record != null ? record.partition() : null,
                record != null ? record.offset() : null,
                record != null ? record.key() : null);
        String correlationId = extractCorrelationId(record);
        String previousCorrelationId = MDC.get(CorrelationConstants.MDC_KEY);
        if (correlationId != null) {
            MDC.put(CorrelationConstants.MDC_KEY, correlationId);
        }
        try {
            paymentService.processOrderEvent(record.value());
        } finally {
            if (previousCorrelationId != null) {
                MDC.put(CorrelationConstants.MDC_KEY, previousCorrelationId);
            } else {
                MDC.remove(CorrelationConstants.MDC_KEY);
            }
        }
    }

    private static String extractCorrelationId(ConsumerRecord<String, String> record) {
        if (record == null || record.headers() == null) {
            return null;
        }
        Header header = record.headers().lastHeader(CorrelationConstants.KAFKA_HEADER);
        if (header == null) {
            header = record.headers().lastHeader(CorrelationConstants.HTTP_HEADER);
        }
        if (header == null || header.value() == null || header.value().length == 0) {
            return null;
        }
        String value = new String(header.value(), StandardCharsets.UTF_8).trim();
        return value.isEmpty() ? null : value;
    }
}

