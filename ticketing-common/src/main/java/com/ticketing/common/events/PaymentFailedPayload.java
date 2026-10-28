package com.ticketing.common.events;

import java.time.Instant;
import java.util.UUID;

/** Payload of `payment.failed`. */
public record PaymentFailedPayload(
        UUID paymentId,
        UUID orderId,
        UUID reservationId,
        UUID userId,
        long amountCents,
        String currency,
        String failureCode,
        String failureMessage,
        Instant failedAt) {
}
