package com.ticketing.order.api.dto;

import java.util.UUID;

public record OrderItemResponse(
        UUID seatId,
        String sectionCode,
        String rowLabel,
        String seatNumber,
        long priceCents
) {
}

