package com.ticketing.common.events;

import java.time.Instant;
import java.util.UUID;

/** Payload of `payment.succeeded`. */
public record PaymentSucceededPayload(
        UUID paymentId,
        UUID orderId,
        UUID reservationId,
        UUID userId,
        long amountCents,
        String currency,
        String providerReference,
        Instant paidAt) {
}
