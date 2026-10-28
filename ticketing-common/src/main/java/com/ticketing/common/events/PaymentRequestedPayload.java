package com.ticketing.common.events;

import java.time.Instant;
import java.util.UUID;

/** Payload of `order.payment.requested`. */
public record PaymentRequestedPayload(
        UUID orderId,
        UUID reservationId,
        UUID userId,
        UUID eventId,
        long amountCents,
        String currency,
        String paymentMethodToken,
        Instant requestedAt,
        Instant reservationExpiresAt) {
}
