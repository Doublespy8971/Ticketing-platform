package com.ticketing.inventory.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record SeatAvailabilityResponse(
        UUID eventId,
        Instant dropTime,
        List<SeatAvailabilityItem> items,
        String nextCursor
) {
    @JsonProperty("seats")
    public List<SeatAvailabilityItem> seats() {
        return items;
    }
}
