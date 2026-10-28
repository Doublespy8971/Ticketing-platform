package com.ticketing.common.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Payload of `order.confirmed`. */
public record OrderConfirmedPayload(
        UUID orderId,
        UUID reservationId,
        UUID userId,
        UUID eventId,
        UUID paymentId,
        List<SeatLine> seats,
        long totalAmountCents,
        String currency,
        Instant confirmedAt) {
}
