package com.ticketing.inventory.api.dto;

import java.util.UUID;

public record ReservedSeatItemResponse(
        UUID seatId,
        String sectionCode,
        String rowLabel,
        String seatNumber,
        long priceCents
) {
}

