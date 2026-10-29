package com.ticketing.inventory.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record CreateSeatItemRequest(
        @NotBlank(message = "sectionCode must not be blank")
        @Size(max = 32, message = "sectionCode must not exceed 32 characters")
        String sectionCode,

        @NotBlank(message = "rowLabel must not be blank")
        @Size(max = 16, message = "rowLabel must not exceed 16 characters")
        String rowLabel,

        @NotBlank(message = "seatNumber must not be blank")
        @Size(max = 16, message = "seatNumber must not exceed 16 characters")
        String seatNumber,

        @Positive(message = "priceCents must be positive")
        long priceCents
) {}

