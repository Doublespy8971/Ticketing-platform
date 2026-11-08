package com.ticketing.order.api.dto;

import java.time.Instant;
import java.util.UUID;

public record CheckoutResponse(
        UUID orderId,
        UUID reservationId,
        UUID userId,
        UUID eventId,
        String status,
        long totalAmountCents,
        String currency,
        Instant checkoutAt,
        Instant expiresAt
) {
}

