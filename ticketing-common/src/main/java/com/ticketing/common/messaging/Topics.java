package com.ticketing.common.messaging;

/** Kafka topic names (docs/arch/03-kafka.md). notification-service has no retry or DLT topics. */
public final class Topics {

    public static final String INVENTORY_EVENTS = "inventory.events";
    public static final String ORDER_EVENTS = "order.events";
    public static final String PAYMENT_EVENTS = "payment.events";

    public static final String INVENTORY_EVENTS_ORDER_SERVICE_RETRY_0 = "inventory.events-order-service-inventory-cg-retry-0";
    public static final String INVENTORY_EVENTS_ORDER_SERVICE_RETRY_1 = "inventory.events-order-service-inventory-cg-retry-1";
    public static final String INVENTORY_EVENTS_ORDER_SERVICE_DLT = "inventory.events-order-service-inventory-cg-dlt";
    public static final String ORDER_EVENTS_PAYMENT_SERVICE_RETRY_0 = "order.events-payment-service-order-cg-retry-0";
    public static final String ORDER_EVENTS_PAYMENT_SERVICE_RETRY_1 = "order.events-payment-service-order-cg-retry-1";
    public static final String ORDER_EVENTS_PAYMENT_SERVICE_DLT = "order.events-payment-service-order-cg-dlt";
    public static final String PAYMENT_EVENTS_INVENTORY_SERVICE_RETRY_0 = "payment.events-inventory-service-payment-cg-retry-0";
    public static final String PAYMENT_EVENTS_INVENTORY_SERVICE_RETRY_1 = "payment.events-inventory-service-payment-cg-retry-1";
    public static final String PAYMENT_EVENTS_INVENTORY_SERVICE_DLT = "payment.events-inventory-service-payment-cg-dlt";
    public static final String PAYMENT_EVENTS_ORDER_SERVICE_RETRY_0 = "payment.events-order-service-payment-cg-retry-0";
    public static final String PAYMENT_EVENTS_ORDER_SERVICE_RETRY_1 = "payment.events-order-service-payment-cg-retry-1";
    public static final String PAYMENT_EVENTS_ORDER_SERVICE_DLT = "payment.events-order-service-payment-cg-dlt";

    private Topics() {
    }
}
