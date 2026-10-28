package com.ticketing.inventory.domain;

import java.time.Instant;
import java.util.UUID;

public record ReservationItemRecord(
        UUID reservationItemId,
        UUID reservationId,
        UUID eventId,
        UUID seatId,
        String sectionCode,
        String rowLabel,
        String seatNumber,
        long priceCents,
        String status,
        Instant createdAt,
        Instant updatedAt
) {}

