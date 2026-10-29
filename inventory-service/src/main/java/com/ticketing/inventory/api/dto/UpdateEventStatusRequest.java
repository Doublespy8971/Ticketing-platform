package com.ticketing.inventory.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record UpdateEventStatusRequest(
        @NotBlank(message = "status must not be blank")
        @Pattern(
                regexp = "^(SCHEDULED|ON_SALE|SOLD_OUT|COMPLETED|CANCELLED)$",
                message = "status must be one of: SCHEDULED, ON_SALE, SOLD_OUT, COMPLETED, CANCELLED"
        )
        String status
) {
}

