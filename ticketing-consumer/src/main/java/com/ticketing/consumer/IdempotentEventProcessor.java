package com.ticketing.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.ticketing.common.events.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
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
        validateTransactionTemplate(this.jdbcTemplate, this.transactionTemplate);
    }

    private static TransactionTemplate createTransactionTemplate(PlatformTransactionManager transactionManager) {
        Objects.requireNonNull(transactionManager, "transactionManager must not be null");
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        return template;
    }

    private static void validateTransactionTemplate(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        int propagation = transactionTemplate.getPropagationBehavior();
        if (propagation != TransactionDefinition.PROPAGATION_REQUIRED
                && propagation != TransactionDefinition.PROPAGATION_REQUIRES_NEW) {
            throw new IllegalArgumentException(
                    "TransactionTemplate propagation must be PROPAGATION_REQUIRED or PROPAGATION_REQUIRES_NEW, but was: "
                            + propagation);
        }
        if (transactionTemplate.isReadOnly()) {
            throw new IllegalArgumentException("TransactionTemplate must not be read-only");
        }
        PlatformTransactionManager txManager = transactionTemplate.getTransactionManager();
        if (txManager == null) {
            throw new IllegalArgumentException("TransactionTemplate transactionManager must not be null");
        }
        if (txManager instanceof DataSourceTransactionManager dstm) {
            DataSource txDataSource = dstm.getDataSource();
            DataSource jdbcDataSource = jdbcTemplate.getDataSource();
            if (txDataSource == null || txDataSource != jdbcDataSource) {
                throw new IllegalArgumentException(
                        "DataSourceTransactionManager DataSource must be the same instance as JdbcTemplate DataSource");
            }
        }
    }

    /**
     * Runs the {@code processed_events} deduplication insert and {@code businessLogic} inside ONE database
     * transaction.
     *
     * @param eventId       unique event identifier from the {@link EventEnvelope}
     * @param consumerGroup logical Kafka consumer group name (any {@code -retry-0}, {@code -retry-1}, or
     *                      {@code -dlt} suffix is stripped before deduplication and metric recording)
     * @param eventType     event type discriminator
     * @param businessLogic domain state mutations and outbox writes to execute if not a duplicate
     * @return {@code true} if the event was processed and committed, {@code false} if skipped as a duplicate
     * @throws IllegalTransactionStateException if called inside an already-active transaction
     * @throws UnexpectedRollbackException      if {@code businessLogic} marks the transaction rollback-only
     */
    public boolean process(UUID eventId, String consumerGroup, String eventType, Runnable businessLogic) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        String normalizedGroup = normalizeConsumerGroup(consumerGroup);
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
                        normalizedGroup,
                        eventType
                );
                if (inserted == 0) {
                    log.debug("Skipping duplicate event eventId={} consumerGroup={} eventType={}",
                            eventId, normalizedGroup, eventType);
                    return Boolean.FALSE;
                }
                businessLogic.run();
                if (status.isRollbackOnly()) {
                    throw new UnexpectedRollbackException(
                            "Transaction was marked rollback-only during businessLogic execution for eventId="
                                    + eventId + ", consumerGroup=" + normalizedGroup);
                }
                return Boolean.TRUE;
            });

            if (Boolean.TRUE.equals(processed)) {
                metrics.recordProcessed(normalizedGroup, safeEventType);
                return true;
            } else {
                metrics.recordDuplicate(normalizedGroup, safeEventType);
                return false;
            }
        } finally {
            metrics.recordProcessingDuration(normalizedGroup, safeEventType, System.nanoTime() - startNanos);
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
        String normalizedGroup = normalizeConsumerGroup(consumerGroup);
        Objects.requireNonNull(handler, "handler must not be null");
        ensureNoActiveTransaction();

        final EventEnvelope<JsonNode> envelope;
        long parseStartNanos = System.nanoTime();
        try {
            envelope = envelopeParser.parse(rawJson);
        } catch (RuntimeException ex) {
            String fallbackEventType = envelopeParser.extractWhitelistedEventTypeOrUnknown(rawJson);
            metrics.recordProcessingDuration(normalizedGroup, fallbackEventType, System.nanoTime() - parseStartNanos);
            throw ex;
        }
        return process(envelope.eventId(), normalizedGroup, envelope.eventType(), () -> handler.accept(envelope));
    }

    /**
     * Parses and validates a raw JSON {@code EventEnvelope<T>} payload into {@code payloadType} and delegates to
     * {@link #process(UUID, String, String, Runnable)}.
     */
    public <T> boolean process(String rawJson,
                               String consumerGroup,
                               Class<T> payloadType,
                               Consumer<EventEnvelope<T>> handler) {
        String normalizedGroup = normalizeConsumerGroup(consumerGroup);
        Objects.requireNonNull(payloadType, "payloadType must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        ensureNoActiveTransaction();

        final EventEnvelope<T> envelope;
        long parseStartNanos = System.nanoTime();
        try {
            envelope = envelopeParser.parse(rawJson, payloadType);
        } catch (RuntimeException ex) {
            String fallbackEventType = envelopeParser.extractWhitelistedEventTypeOrUnknown(rawJson);
            metrics.recordProcessingDuration(normalizedGroup, fallbackEventType, System.nanoTime() - parseStartNanos);
            throw ex;
        }
        return process(envelope.eventId(), normalizedGroup, envelope.eventType(), () -> handler.accept(envelope));
    }

    private static String normalizeConsumerGroup(String consumerGroup) {
        requireNonBlank(consumerGroup, "consumerGroup");
        String stripped = TicketingRetryTopicNamesProviderFactory.stripRetryOrDltSuffix(consumerGroup.trim());
        requireNonBlank(stripped, "consumerGroup");
        return stripped;
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
