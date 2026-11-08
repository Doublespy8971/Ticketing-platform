package com.ticketing.order.domain;

import java.util.UUID;

public record OrderItemRecord(
        UUID orderItemId,
        UUID orderId,
        UUID seatId,
        String sectionCode,
        String rowLabel,
        String seatNumber,
        long priceCents
) {}

