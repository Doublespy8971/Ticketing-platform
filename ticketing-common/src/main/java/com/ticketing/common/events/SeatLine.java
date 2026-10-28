package com.ticketing.common.events;

import java.util.UUID;

/** Payload of a seat line item (seats[] element in inventory.seat.reserved and order.confirmed). */
public record SeatLine(
        UUID seatId,
        String sectionCode,
        String rowLabel,
        String seatNumber,
        long priceCents) {
}
