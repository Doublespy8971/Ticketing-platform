package com.ticketing.order.service;

import java.util.Set;

public final class CheckoutMetrics {

    public static final String COUNTER_NAME = "ticketing_orders_checkout_total";
    public static final String TIMER_NAME = "order_checkout_duration_seconds";
    public static final String TAG_OUTCOME = "outcome";

    public static final String OUTCOME_ACCEPTED = "ACCEPTED";
    public static final String OUTCOME_ORDER_NOT_READY = "ORDER_NOT_READY";
    public static final String OUTCOME_RESERVATION_EXPIRED = "RESERVATION_EXPIRED";
    public static final String OUTCOME_INVALID_ORDER_STATE = "INVALID_ORDER_STATE";
    public static final String OUTCOME_REPLAY = "REPLAY";

    public static final Set<String> ALLOWED_OUTCOMES = Set.of(
            OUTCOME_ACCEPTED,
            OUTCOME_ORDER_NOT_READY,
            OUTCOME_RESERVATION_EXPIRED,
            OUTCOME_INVALID_ORDER_STATE,
            OUTCOME_REPLAY
    );

    private CheckoutMetrics() {
    }
}

