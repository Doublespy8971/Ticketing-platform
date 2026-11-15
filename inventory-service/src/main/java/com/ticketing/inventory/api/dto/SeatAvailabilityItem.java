package com.ticketing.inventory.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public record SeatAvailabilityItem(
        UUID seatId,
        String sectionCode,
        String rowLabel,
        String seatNumber,
        long priceCents,
        String currency,
        String status
) {
    @JsonProperty("id")
    public UUID id() {
        return seatId;
    }

    @JsonProperty("section")
    public String section() {
        return sectionCode;
    }
}
