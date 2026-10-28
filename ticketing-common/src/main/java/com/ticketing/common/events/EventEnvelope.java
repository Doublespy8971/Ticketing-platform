package com.ticketing.common.events;

import java.time.Instant;
import java.util.UUID;

/** Common event envelope (docs/arch/04-events.md, section 4.1). */
public record EventEnvelope<T>(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        String correlationId,
        int schemaVersion,
        T payload) {
}
