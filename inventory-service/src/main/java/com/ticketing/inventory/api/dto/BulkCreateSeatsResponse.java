package com.ticketing.inventory.api.dto;

import java.util.List;
import java.util.UUID;

public record BulkCreateSeatsResponse(
        UUID eventId,
        int createdCount,
        int skippedDuplicateCount,
        List<SeatResponseItem> seats
) {}

