package com.ticketing.order.api.dto;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderDetailsResponse(
        UUID orderId,
        UUID reservationId,
        UUID userId,
        UUID eventId,
        String status,
        UUID paymentId,
        List<OrderItemResponse> seats,
        long totalAmountCents,
        String currency,
        Instant reservedAt,
        Instant expiresAt,
        Instant checkoutAt,
        Instant confirmedAt,
        Instant cancelledAt,
        String cancellationReason
) {
}

