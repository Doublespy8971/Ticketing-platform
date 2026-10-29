package com.ticketing.inventory.api.dto;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;

public record CreateEventRequest(
        @NotBlank(message = "name must not be blank")
        @Size(max = 255, message = "name must not exceed 255 characters")
        String name,

        @NotBlank(message = "venueName must not be blank")
        @Size(max = 255, message = "venueName must not exceed 255 characters")
        String venueName,

        @NotNull(message = "eventTime must not be null")
        Instant eventTime,

        @NotNull(message = "dropTime must not be null")
        Instant dropTime
) {
    @AssertTrue(message = "dropTime must be before eventTime")
    public boolean isDropTimeBeforeEventTime() {
        if (dropTime == null || eventTime == null) {
            return true;
        }
        return dropTime.isBefore(eventTime);
    }
}

