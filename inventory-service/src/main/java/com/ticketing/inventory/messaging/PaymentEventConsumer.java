package com.ticketing.inventory.messaging;

import com.ticketing.common.messaging.Topics;
import com.ticketing.inventory.service.PaymentEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Kafka listener for {@code payment.events} with consumer group {@code inventory-service-payment-cg}
 * (docs/arch/03-kafka.md, Section 3.1).
 */
@Component
public class PaymentEventConsumer {

    private final PaymentEventService paymentEventService;

    public PaymentEventConsumer(PaymentEventService paymentEventService) {
        this.paymentEventService = Objects.requireNonNull(paymentEventService, "paymentEventService must not be null");
    }

    @KafkaListener(
            topics = Topics.PAYMENT_EVENTS,
            groupId = PaymentEventService.CONSUMER_GROUP,
            autoStartup = "${spring.kafka.listener.auto-startup:true}"
    )
    public void onPaymentEvent(ConsumerRecord<String, String> record) {
        paymentEventService.handlePaymentEventRecord(record.value());
    }
}

