package com.ticketing.common.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Payload of `inventory.reservation.expired`. */
public record ReservationExpiredPayload(
        UUID reservationId,
        UUID orderId,
        UUID userId,
        UUID eventId,
        List<UUID> seatIds,
        Instant expiredAt) {
}
