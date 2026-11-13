package com.ticketing.gateway.api;

public record DevTokenResponse(
        String accessToken,
        String tokenType,
        long expiresIn,
        String issuedAt
) {
}

