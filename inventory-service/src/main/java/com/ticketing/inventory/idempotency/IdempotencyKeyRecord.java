package com.ticketing.inventory.idempotency;

import java.time.Instant;
import java.util.UUID;

public record IdempotencyKeyRecord(
        UUID userId,
        UUID idempotencyKey,
        String endpoint,
        String requestHash,
        String status,
        Integer responseStatus,
        String responseBody,
        Instant createdAt,
        Instant completedAt,
        Instant expiresAt
) {
}

