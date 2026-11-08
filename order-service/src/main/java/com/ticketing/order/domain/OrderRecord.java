package com.ticketing.order.domain;

import java.time.Instant;
import java.util.UUID;

public record OrderRecord(
        UUID orderId,
        UUID reservationId,
        UUID userId,
        UUID eventId,
        String status,
        long totalAmountCents,
        String currency,
        String paymentMethodToken,
        UUID paymentId,
        String cancellationReason,
        long version,
        Instant reservedAt,
        Instant expiresAt,
        Instant checkoutAt,
        Instant paidAt,
        Instant confirmedAt,
        Instant cancelledAt,
        Instant refundedAt,
        Instant createdAt,
        Instant updatedAt
) {}

