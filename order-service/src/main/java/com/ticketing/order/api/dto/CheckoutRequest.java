package com.ticketing.order.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CheckoutRequest(
        @NotBlank(message = "paymentMethodToken must not be blank")
        @Size(max = 128, message = "paymentMethodToken must not exceed 128 characters")
        String paymentMethodToken
) {
    @Override
    public String toString() {
        return "CheckoutRequest[paymentMethodToken=***]";
    }
}

