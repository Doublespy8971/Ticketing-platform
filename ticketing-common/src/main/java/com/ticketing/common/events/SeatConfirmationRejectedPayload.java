package com.ticketing.common.events;

import java.time.Instant;
import java.util.UUID;

/** Payload of `inventory.seat.confirmation_rejected`. */
public record SeatConfirmationRejectedPayload(
        UUID reservationId,
        UUID orderId,
        UUID paymentId,
        UUID userId,
        UUID eventId,
        String reason,
        Instant rejectedAt) {
}
