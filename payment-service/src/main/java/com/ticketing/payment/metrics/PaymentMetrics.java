package com.ticketing.payment.metrics;

import com.ticketing.common.messaging.EventTypes;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Component
public class PaymentMetrics {

    public static final String METRIC_PROVIDER_CALLS = "payment_provider_calls_total";
    public static final String METRIC_PROVIDER_DURATION = "payment_provider_duration_seconds";
    public static final String METRIC_EVENTS_HANDLED = "payment_events_handled_total";
    public static final String METRIC_PAYMENTS_PROCESSED = "ticketing_payments_processed_total";

    private static final Set<String> WHITELISTED_EVENT_TYPES = Set.of(
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

    private final MeterRegistry meterRegistry;
    private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();

    public PaymentMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
    }

    public void recordProviderCall(String operation, String outcome) {
        String key = "calls:" + operation + ":" + outcome;
        counters.computeIfAbsent(key, k ->
                Counter.builder(METRIC_PROVIDER_CALLS)
                        .description("Total calls to payment provider")
                        .tag("operation", operation)
                        .tag("outcome", outcome)
                        .register(meterRegistry)
        ).increment();
    }

    public void recordProviderDuration(String operation, long durationNanos) {
        String key = "duration:" + operation;
        timers.computeIfAbsent(key, k ->
                Timer.builder(METRIC_PROVIDER_DURATION)
                        .description("Duration of payment provider calls")
                        .tag("operation", operation)
                        .register(meterRegistry)
        ).record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordEventHandled(String eventType, String outcome) {
        String safeEventType = (eventType != null && WHITELISTED_EVENT_TYPES.contains(eventType))
                ? eventType
                : "unknown";
        String key = "events:" + safeEventType + ":" + outcome;
        counters.computeIfAbsent(key, k ->
                Counter.builder(METRIC_EVENTS_HANDLED)
                        .description("Total order events handled by payment-service")
                        .tag("event_type", safeEventType)
                        .tag("outcome", outcome)
                        .register(meterRegistry)
        ).increment();
    }

    public void recordPaymentProcessed(String status, String failureCode) {
        String code = failureCode != null ? failureCode : "";
        String key = "processed:" + status + ":" + code;
        counters.computeIfAbsent(key, k ->
                Counter.builder(METRIC_PAYMENTS_PROCESSED)
                        .description("Total payment charges and compensating refunds processed")
                        .tag("status", status)
                        .tag("failure_code", code)
                        .register(meterRegistry)
        ).increment();
    }
}

