package com.ticketing.common.events;

import java.time.Instant;
import java.util.UUID;

/** Payload of `order.cancelled`. */
public record OrderCancelledPayload(
        UUID orderId,
        UUID reservationId,
        UUID userId,
        UUID eventId,
        String finalStatus,
        String cancellationReason,
        Instant cancelledAt) {
}
