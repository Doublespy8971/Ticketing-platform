package com.ticketing.inventory.api.dto;

import java.time.Instant;
import java.util.UUID;

public record EventResponse(
        UUID eventId,
        String name,
        String venueName,
        Instant eventTime,
        Instant dropTime,
        String status,
        Instant createdAt
) {}

