package com.ticketing.gateway.auth;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Validated JWT claims.
 */
public record ParsedJwt(
        UUID userId,
        List<String> roles,
        Instant issuedAt,
        Instant expiresAt
) {
}

