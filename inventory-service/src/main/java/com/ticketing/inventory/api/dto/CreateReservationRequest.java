package com.ticketing.inventory.api.dto;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;

public record CreateReservationRequest(
        @NotNull(message = "eventId must not be null")
        UUID eventId,

        @NotEmpty(message = "seatIds must not be empty")
        @Size(min = 1, max = 6, message = "seatIds must contain between 1 and 6 seat IDs")
        List<@NotNull(message = "seatId must not be null") UUID> seatIds
) {
    @AssertTrue(message = "seatIds must contain distinct UUIDs")
    public boolean isDistinctSeatIds() {
        if (seatIds == null || seatIds.isEmpty()) {
            return true;
        }
        return new HashSet<>(seatIds).size() == seatIds.size();
    }
}

