package com.ticketing.inventory.api.dto;

import java.util.UUID;

public record SeatResponseItem(
        UUID seatId,
        String sectionCode,
        String rowLabel,
        String seatNumber,
        long priceCents,
        String status
) {}

