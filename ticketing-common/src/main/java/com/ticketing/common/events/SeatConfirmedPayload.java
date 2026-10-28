package com.ticketing.common.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Payload of `inventory.seat.confirmed`. */
public record SeatConfirmedPayload(
        UUID reservationId,
        UUID orderId,
        UUID paymentId,
        UUID eventId,
        List<UUID> seatIds,
        Instant confirmedAt) {
}
