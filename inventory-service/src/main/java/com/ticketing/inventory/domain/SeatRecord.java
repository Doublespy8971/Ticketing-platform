package com.ticketing.inventory.domain;

import java.time.Instant;
import java.util.UUID;

public record SeatRecord(
        UUID seatId,
        UUID eventId,
        String sectionCode,
        String rowLabel,
        String seatNumber,
        long priceCents,
        String currency,
        String status,
        long version,
        Instant createdAt,
        Instant updatedAt
) {}

