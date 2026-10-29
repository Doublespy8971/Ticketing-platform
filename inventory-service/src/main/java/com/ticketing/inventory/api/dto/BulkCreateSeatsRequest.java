package com.ticketing.inventory.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

public record BulkCreateSeatsRequest(
        @NotNull(message = "seats must not be null")
        @NotEmpty(message = "seats list must not be empty")
        @Size(min = 1, max = 5000, message = "seats count must be between 1 and 5000")
        @Valid
        List<CreateSeatItemRequest> seats
) {}

