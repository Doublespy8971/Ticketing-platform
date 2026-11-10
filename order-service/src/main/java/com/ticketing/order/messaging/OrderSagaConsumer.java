package com.ticketing.order.messaging;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.messaging.Topics;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import com.ticketing.order.service.OrderSagaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Kafka listener entry points for {@code inventory.events} ({@code order-service-inventory-cg})
 * and {@code payment.events} ({@code order-service-payment-cg}).
 */
@Component
public class OrderSagaConsumer {

    private final OrderSagaService orderSagaService;

    public OrderSagaConsumer(OrderSagaService orderSagaService) {
        this.orderSagaService = Objects.requireNonNull(orderSagaService, "orderSagaService must not be null");
    }

    @KafkaListener(
            topics = Topics.INVENTORY_EVENTS,
            groupId = TicketingRetryTopicNamesProviderFactory.ORDER_SERVICE_INVENTORY_CG,
            autoStartup = "${spring.kafka.listener.auto-startup:true}"
    )
    public void onInventoryEvent(ConsumerRecord<String, String> record) {
        String correlationId = extractCorrelationId(record);
        String previousCorrelationId = MDC.get(CorrelationConstants.MDC_KEY);
        if (correlationId != null) {
            MDC.put(CorrelationConstants.MDC_KEY, correlationId);
        }
        try {
            orderSagaService.processInventoryRecord(record.value(), correlationId);
        } finally {
            if (previousCorrelationId != null) {
                MDC.put(CorrelationConstants.MDC_KEY, previousCorrelationId);
            } else {
                MDC.remove(CorrelationConstants.MDC_KEY);
            }
        }
    }

    @KafkaListener(
            topics = Topics.PAYMENT_EVENTS,
            groupId = TicketingRetryTopicNamesProviderFactory.ORDER_SERVICE_PAYMENT_CG,
            autoStartup = "${spring.kafka.listener.auto-startup:true}"
    )
    public void onPaymentEvent(ConsumerRecord<String, String> record) {
        String correlationId = extractCorrelationId(record);
        String previousCorrelationId = MDC.get(CorrelationConstants.MDC_KEY);
        if (correlationId != null) {
            MDC.put(CorrelationConstants.MDC_KEY, correlationId);
        }
        try {
            orderSagaService.processPaymentRecord(record.value(), correlationId);
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
