package com.ticketing.payment.domain;

import java.time.Instant;
import java.util.UUID;

public record PaymentRecord(
        UUID paymentId,
        UUID orderId,
        UUID reservationId,
        UUID userId,
        long amountCents,
        String currency,
        String paymentMethodToken,
        PaymentStatus status,
        String providerReference,
        String failureCode,
        String failureMessage,
        UUID refundId,
        String refundReason,
        Instant createdAt,
        Instant paidAt,
        Instant failedAt,
        Instant refundedAt,
        Instant updatedAt
) {
}

