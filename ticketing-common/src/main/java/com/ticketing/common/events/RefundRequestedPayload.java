package com.ticketing.common.events;

import java.time.Instant;
import java.util.UUID;

/** Payload of `order.refund.requested`. */
public record RefundRequestedPayload(
        UUID orderId,
        UUID reservationId,
        UUID paymentId,
        UUID userId,
        long amountCents,
        String currency,
        String reason,
        Instant requestedAt) {
}
