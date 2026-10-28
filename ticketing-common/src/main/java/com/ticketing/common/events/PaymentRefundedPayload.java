package com.ticketing.common.events;

import java.time.Instant;
import java.util.UUID;

/** Payload of `payment.refunded`. */
public record PaymentRefundedPayload(
        UUID refundId,
        UUID paymentId,
        UUID orderId,
        UUID reservationId,
        UUID userId,
        long amountCents,
        String currency,
        String reason,
        Instant refundedAt) {
}
