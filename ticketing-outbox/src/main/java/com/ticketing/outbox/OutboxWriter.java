package com.ticketing.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.messaging.Topics;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;

/**
 * Persists domain events into {@code outbox_events} inside the caller's active transaction
 * ({@link Propagation#MANDATORY}).
 */
public class OutboxWriter {

    private static final String INSERT_SQL = """
            INSERT INTO outbox_events (
                event_id,
                aggregate_type,
                aggregate_id,
                topic,
                event_type,
                correlation_id,
                schema_version,
                payload,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, clock_timestamp())
            RETURNING created_at
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public OutboxWriter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public <T> EventEnvelope<T> append(UUID aggregateId, String eventType, T payload) {
        return append(aggregateId, resolveAggregateType(eventType), resolveTopic(eventType), eventType, payload);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public <T> EventEnvelope<T> append(UUID aggregateId, String aggregateType, String topic, String eventType, T payload) {
        Objects.requireNonNull(aggregateId, "aggregateId must not be null");
        Objects.requireNonNull(aggregateType, "aggregateType must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(payload, "payload must not be null");

        resolveAggregateType(eventType);
        resolveTopic(eventType);

        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalTransactionStateException(
                    "OutboxWriter.append requires an active transaction (Propagation.MANDATORY)");
        }

        UUID eventId = UuidV7.generate();
        UUID correlationUuid = resolveCorrelationId();
        int schemaVersion = 1;

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize outbox payload for eventType=" + eventType, e);
        }

        OffsetDateTime storedCreatedAt = jdbcTemplate.queryForObject(
                INSERT_SQL,
                new Object[]{
                        eventId,
                        aggregateType,
                        aggregateId,
                        topic,
                        eventType,
                        correlationUuid,
                        schemaVersion,
                        payloadJson
                },
                new int[]{
                        Types.OTHER,
                        Types.VARCHAR,
                        Types.OTHER,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.OTHER,
                        Types.INTEGER,
                        Types.VARCHAR
                },
                (rs, rowNum) -> rs.getObject("created_at", OffsetDateTime.class)
        );

        Instant occurredAt = Objects.requireNonNull(storedCreatedAt, "storedCreatedAt must not be null")
                .toInstant()
                .truncatedTo(ChronoUnit.MICROS);

        return new EventEnvelope<>(eventId, eventType, occurredAt, correlationUuid.toString(), schemaVersion, payload);
    }

    static String resolveTopic(String eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        if (eventType.startsWith("inventory.")) {
            return Topics.INVENTORY_EVENTS;
        }
        if (eventType.startsWith("order.")) {
            return Topics.ORDER_EVENTS;
        }
        if (eventType.startsWith("payment.")) {
            return Topics.PAYMENT_EVENTS;
        }
        throw new IllegalArgumentException(
                "Unsupported eventType prefix (expected inventory., order., or payment.): " + eventType);
    }

    static String resolveAggregateType(String eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        if (eventType.startsWith("inventory.")) {
            return "RESERVATION";
        }
        if (eventType.startsWith("order.")) {
            return "ORDER";
        }
        if (eventType.startsWith("payment.")) {
            return "PAYMENT";
        }
        throw new IllegalArgumentException(
                "Unsupported eventType prefix (expected inventory., order., or payment.): " + eventType);
    }

    private static UUID resolveCorrelationId() {
        String mdcVal = MDC.get(CorrelationConstants.MDC_KEY);
        if (mdcVal != null && !mdcVal.isBlank()) {
            try {
                return UUID.fromString(mdcVal.trim());
            } catch (IllegalArgumentException ignored) {
                // generate a fresh UUID if MDC value is not a valid UUID
            }
        }
        return UUID.randomUUID();
    }
}
