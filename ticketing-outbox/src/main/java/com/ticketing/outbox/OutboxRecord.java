package com.ticketing.outbox;

import java.time.Instant;
import java.util.UUID;

/** Represents a row from the {@code outbox_events} table. */
public record OutboxRecord(
        UUID eventId,
        String aggregateType,
        UUID aggregateId,
        String topic,
        String eventType,
        UUID correlationId,
        int schemaVersion,
        String payloadJson,
        Instant createdAt,
        Instant publishedAt) {
}

