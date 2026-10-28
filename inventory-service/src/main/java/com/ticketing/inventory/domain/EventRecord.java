package com.ticketing.inventory.domain;

import java.time.Instant;
import java.util.UUID;

public record EventRecord(
        UUID eventId,
        String name,
        String venueName,
        Instant eventTime,
        Instant dropTime,
        String status,
        Instant createdAt,
        Instant updatedAt
) {}

