package com.ticketing.inventory.domain;

import java.time.Instant;
import java.util.UUID;

public record ReservationRecord(
        UUID reservationId,
        UUID eventId,
        UUID userId,
        String status,
        long totalAmountCents,
        String currency,
        Instant createdAt,
        Instant expiresAt,
        Instant confirmedAt,
        Instant releasedAt,
        Instant expiredAt,
        Instant updatedAt
) {}

