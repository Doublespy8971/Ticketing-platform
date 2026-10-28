package com.ticketing.common.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Payload of `inventory.seat.reserved`. */
public record SeatReservedPayload(
        UUID reservationId,
        UUID orderId,
        UUID userId,
        UUID eventId,
        List<SeatLine> seats,
        long totalAmountCents,
        String currency,
        Instant reservedAt,
        Instant expiresAt) {
}
