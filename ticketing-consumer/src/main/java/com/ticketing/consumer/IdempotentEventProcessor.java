package com.ticketing.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.ticketing.common.events.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Executes Kafka consumer business logic idempotently inside a single local PostgreSQL transaction
 * along with an {@code INSERT INTO processed_events ... ON CONFLICT (event_id, consumer_group) DO NOTHING}
 * deduplication guard (docs/arch/05-outbox-idempotency.md, section 5.5).
 * <p>
 * Fails fast with {@link IllegalTransactionStateException} if invoked when a transaction is already active
 * so that the deduplication row and business mutations always commit or roll back as a single atomic unit
 * before the Kafka listener container acknowledges the record offset.
 */
public class IdempotentEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(IdempotentEventProcessor.class);

    private static final String INSERT_PROCESSED_EVENT_SQL = """
            INSERT INTO processed_events (event_id, consumer_group, event_type, processed_at)
            VALUES (?, ?, ?, clock_timestamp())
            ON CONFLICT (event_id, consumer_group) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final ConsumerMetrics metrics;
    private final ConsumerEnvelopeParser envelopeParser;

    public IdempotentEventProcessor(JdbcTemplate jdbcTemplate,
                                    PlatformTransactionManager transactionManager,
                                    ConsumerMetrics metrics) {
        this(jdbcTemplate, createTransactionTemplate(transactionManager), metrics, new ConsumerEnvelopeParser());
    }

    public IdempotentEventProcessor(JdbcTemplate jdbcTemplate,
                                    TransactionTemplate transactionTemplate,
                                    ConsumerMetrics metrics) {
        this(jdbcTemplate, transactionTemplate, metrics, new ConsumerEnvelopeParser());
    }

    public IdempotentEventProcessor(JdbcTemplate jdbcTemplate,
                                    TransactionTemplate transactionTemplate,
                                    ConsumerMetrics metrics,
                                    ConsumerEnvelopeParser envelopeParser) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
        this.transactionTemplate = Objects.requireNonNull(transactionTemplate, "transactionTemplate must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.envelopeParser = Objects.requireNonNull(envelopeParser, "envelopeParser must not be null");
    }

    private static TransactionTemplate createTransactionTemplate(PlatformTransactionManager transactionManager) {
        Objects.requireNonNull(transactionManager, "transactionManager must not be null");
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        return template;
    }

    /**
     * Runs the {@code processed_events} deduplication insert and {@code businessLogic} inside ONE database
     * transaction.
     *
     * @param eventId       unique event identifier from the {@link EventEnvelope}
     * @param consumerGroup logical Kafka consumer group name
     * @param eventType     event type discriminator
     * @param businessLogic domain state mutations and outbox writes to execute if not a duplicate
     * @return {@code true} if the event was processed and committed, {@code false} if skipped as a duplicate
     * @throws IllegalTransactionStateException if called inside an already-active transaction
     */
    public boolean process(UUID eventId, String consumerGroup, String eventType, Runnable businessLogic) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        requireNonBlank(consumerGroup, "consumerGroup");
        requireNonBlank(eventType, "eventType");
        Objects.requireNonNull(businessLogic, "businessLogic must not be null");

        ensureNoActiveTransaction();

        String safeEventType = ConsumerMetrics.sanitizeEventType(eventType);
        long startNanos = System.nanoTime();
        try {
            Boolean processed = transactionTemplate.execute(status -> {
                int inserted = jdbcTemplate.update(
                        INSERT_PROCESSED_EVENT_SQL,
                        eventId,
                        consumerGroup,
                        eventType
                );
                if (inserted == 0) {
                    log.debug("Skipping duplicate event eventId={} consumerGroup={} eventType={}",
                            eventId, consumerGroup, eventType);
                    return Boolean.FALSE;
                }
                businessLogic.run();
                return Boolean.TRUE;
            });

            if (Boolean.TRUE.equals(processed)) {
                metrics.recordProcessed(consumerGroup, safeEventType);
                return true;
            } else {
                metrics.recordDuplicate(consumerGroup, safeEventType);
                return false;
            }
        } finally {
            metrics.recordProcessingDuration(consumerGroup, safeEventType, System.nanoTime() - startNanos);
        }
    }

    /**
     * Parses and validates a raw JSON {@code EventEnvelope<JsonNode>} payload and delegates to
     * {@link #process(UUID, String, String, Runnable)}.
     * <p>
     * If parsing or validation fails, throws {@link NonRetryableEventException} and records the failed attempt
     * on {@code consumer_event_processing_seconds} with {@code event_type="unknown"} (or the whitelisted
     * {@code eventType} if present).
     */
    public boolean process(String rawJson, String consumerGroup, Consumer<EventEnvelope<JsonNode>> handler) {
        requireNonBlank(consumerGroup, "consumerGroup");
        Objects.requireNonNull(handler, "handler must not be null");
        ensureNoActiveTransaction();

        final EventEnvelope<JsonNode> envelope;
        long parseStartNanos = System.nanoTime();
        try {
            envelope = envelopeParser.parse(rawJson);
        } catch (RuntimeException ex) {
            String fallbackEventType = envelopeParser.extractWhitelistedEventTypeOrUnknown(rawJson);
            metrics.recordProcessingDuration(consumerGroup, fallbackEventType, System.nanoTime() - parseStartNanos);
            throw ex;
        }
        return process(envelope.eventId(), consumerGroup, envelope.eventType(), () -> handler.accept(envelope));
    }

    /**
     * Parses and validates a raw JSON {@code EventEnvelope<T>} payload into {@code payloadType} and delegates to
     * {@link #process(UUID, String, String, Runnable)}.
     */
    public <T> boolean process(String rawJson,
                               String consumerGroup,
                               Class<T> payloadType,
                               Consumer<EventEnvelope<T>> handler) {
        requireNonBlank(consumerGroup, "consumerGroup");
        Objects.requireNonNull(payloadType, "payloadType must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        ensureNoActiveTransaction();

        final EventEnvelope<T> envelope;
        long parseStartNanos = System.nanoTime();
        try {
            envelope = envelopeParser.parse(rawJson, payloadType);
        } catch (RuntimeException ex) {
            String fallbackEventType = envelopeParser.extractWhitelistedEventTypeOrUnknown(rawJson);
            metrics.recordProcessingDuration(consumerGroup, fallbackEventType, System.nanoTime() - parseStartNanos);
            throw ex;
        }
        return process(envelope.eventId(), consumerGroup, envelope.eventType(), () -> handler.accept(envelope));
    }

    private static void ensureNoActiveTransaction() {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalTransactionStateException(
                    "IdempotentEventProcessor.process must not be called within an already-active transaction; "
                            + "it manages its own single database transaction so the processed_events insert and "
                            + "business logic commit or roll back together before the Kafka offset is acknowledged.");
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be null or blank");
        }
    }
}

