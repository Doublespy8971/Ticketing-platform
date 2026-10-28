package com.ticketing.common.messaging;

/** Event type discriminators (docs/arch/04-events.md, section 4.2). */
public final class EventTypes {

    public static final String INVENTORY_SEAT_RESERVED = "inventory.seat.reserved";
    public static final String INVENTORY_SEAT_RELEASED = "inventory.seat.released";
    public static final String INVENTORY_RESERVATION_EXPIRED = "inventory.reservation.expired";
    public static final String INVENTORY_SEAT_CONFIRMED = "inventory.seat.confirmed";
    public static final String INVENTORY_SEAT_CONFIRMATION_REJECTED = "inventory.seat.confirmation_rejected";
    public static final String ORDER_PAYMENT_REQUESTED = "order.payment.requested";
    public static final String ORDER_REFUND_REQUESTED = "order.refund.requested";
    public static final String ORDER_CONFIRMED = "order.confirmed";
    public static final String ORDER_CANCELLED = "order.cancelled";
    public static final String PAYMENT_SUCCEEDED = "payment.succeeded";
    public static final String PAYMENT_FAILED = "payment.failed";
    public static final String PAYMENT_REFUNDED = "payment.refunded";

    private EventTypes() {
    }
}
