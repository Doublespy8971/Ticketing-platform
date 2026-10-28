package com.ticketing.common.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Payload of `inventory.seat.released`. */
public record SeatReleasedPayload(
        UUID reservationId,
        UUID orderId,
        UUID eventId,
        List<UUID> seatIds,
        String reason,
        Instant releasedAt) {
}
