package com.ticketing.inventory.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.events.PaymentFailedPayload;
import com.ticketing.common.events.PaymentSucceededPayload;
import com.ticketing.common.events.SeatConfirmationRejectedPayload;
import com.ticketing.common.events.SeatConfirmedPayload;
import com.ticketing.common.events.SeatReleasedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.consumer.ConsumerMetrics;
import com.ticketing.consumer.IdempotentEventProcessor;
import com.ticketing.consumer.NonRetryableEventException;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.ReservationRepository.ConfirmedReservation;
import com.ticketing.inventory.domain.ReservationRepository.LockedReservation;
import com.ticketing.inventory.domain.ReservationRepository.ReleasedReservation;
import com.ticketing.inventory.redis.ConfirmOutcome;
import com.ticketing.inventory.redis.RedisSeatGate;
import com.ticketing.outbox.OutboxWriter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Handles events consumed from {@code payment.events} inside {@link IdempotentEventProcessor}'s
 * single database transaction (docs/arch/02-saga.md, 03-kafka.md, 04-events.md, 05-outbox-idempotency.md).
 */
@Service
public class PaymentEventService {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventService.class);

    public static final String CONSUMER_GROUP = "inventory-service-payment-cg";

    public static final String METRIC_PAYMENT_EVENTS_HANDLED = "inventory_payment_events_handled_total";
    public static final String METRIC_REDIS_CONFIRM_FAILURES = "inventory_redis_confirm_failures_total";

    public static final String OUTCOME_CONFIRMED = "confirmed";
    public static final String OUTCOME_REJECTED = "rejected";
    public static final String OUTCOME_RELEASED = "released";
    public static final String OUTCOME_NOOP = "noop";
    public static final String OUTCOME_IGNORED = "ignored";

    public static final String REASON_RESERVATION_ALREADY_EXPIRED = "RESERVATION_ALREADY_EXPIRED";
    public static final String REASON_RESERVATION_ALREADY_RELEASED = "RESERVATION_ALREADY_RELEASED";
    public static final String REASON_PAYMENT_FAILED = "PAYMENT_FAILED";

    private static final int REDIS_CONFIRM_MAX_RETRIES = 3;
    private static final long DEFAULT_REDIS_CONFIRM_RETRY_PAUSE_MS = 50L;

    private final IdempotentEventProcessor idempotentEventProcessor;
    private final ReservationRepository reservationRepository;
    private final RedisSeatGate redisSeatGate;
    private final OutboxWriter outboxWriter;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final Counter redisConfirmFailuresCounter;

    private volatile long redisConfirmRetryPauseMs = DEFAULT_REDIS_CONFIRM_RETRY_PAUSE_MS;

    public PaymentEventService(IdempotentEventProcessor idempotentEventProcessor,
                               ReservationRepository reservationRepository,
                               RedisSeatGate redisSeatGate,
                               OutboxWriter outboxWriter,
                               ObjectMapper objectMapper,
                               MeterRegistry meterRegistry) {
        this.idempotentEventProcessor = Objects.requireNonNull(idempotentEventProcessor, "idempotentEventProcessor must not be null");
        this.reservationRepository = Objects.requireNonNull(reservationRepository, "reservationRepository must not be null");
        this.redisSeatGate = Objects.requireNonNull(redisSeatGate, "redisSeatGate must not be null");
        this.outboxWriter = Objects.requireNonNull(outboxWriter, "outboxWriter must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.redisConfirmFailuresCounter = Counter.builder(METRIC_REDIS_CONFIRM_FAILURES)
                .description("Total Redis confirm failures after retries in inventory-service")
                .register(meterRegistry);

        preRegisterMeters();
    }

    private void preRegisterMeters() {
        for (String outcome : List.of(OUTCOME_CONFIRMED, OUTCOME_REJECTED, OUTCOME_NOOP)) {
            outcomeCounter(EventTypes.PAYMENT_SUCCEEDED, outcome);
        }
        for (String outcome : List.of(OUTCOME_RELEASED, OUTCOME_NOOP)) {
            outcomeCounter(EventTypes.PAYMENT_FAILED, outcome);
        }
    }

    /**
     * Parses a raw JSON {@link EventEnvelope} from {@code payment.events} and executes the appropriate
     * handler inside {@link IdempotentEventProcessor#process(UUID, String, String, Runnable)}.
     *
     * @param rawJson serialized {@code EventEnvelope} JSON
     * @return {@code true} if processed and committed, {@code false} if skipped as a duplicate
     */
    public boolean handlePaymentEventRecord(String rawJson) {
        EventEnvelope<JsonNode> envelope = parseEnvelope(rawJson);

        String existingMdcCorrelationId = MDC.get(CorrelationConstants.MDC_KEY);
        boolean setMdc = false;
        if (existingMdcCorrelationId == null || existingMdcCorrelationId.isBlank()) {
            MDC.put(CorrelationConstants.MDC_KEY, envelope.correlationId());
            setMdc = true;
        }

        try {
            return idempotentEventProcessor.process(
                    envelope.eventId(),
                    CONSUMER_GROUP,
                    envelope.eventType(),
                    () -> dispatchInsideTransaction(envelope)
            );
        } finally {
            if (setMdc) {
                MDC.remove(CorrelationConstants.MDC_KEY);
            }
        }
    }

    private void dispatchInsideTransaction(EventEnvelope<JsonNode> envelope) {
        String eventType = envelope.eventType();
        switch (eventType) {
            case EventTypes.PAYMENT_SUCCEEDED -> {
                PaymentSucceededPayload payload = parsePaymentSucceededPayload(envelope.payload());
                handlePaymentSucceeded(envelope, payload);
            }
            case EventTypes.PAYMENT_FAILED -> {
                PaymentFailedPayload payload = parsePaymentFailedPayload(envelope.payload());
                handlePaymentFailed(envelope, payload);
            }
            default -> handleIgnoredEvent(envelope);
        }
    }

    private void handlePaymentSucceeded(EventEnvelope<JsonNode> envelope, PaymentSucceededPayload payload) {
        UUID reservationId = payload.reservationId();
        UUID paymentId = payload.paymentId();
        UUID orderId = payload.orderId() != null ? payload.orderId() : reservationId;
        String correlationId = envelope.correlationId();

        Optional<LockedReservation> lockedOpt = reservationRepository.lockForUpdate(reservationId);
        if (lockedOpt.isEmpty()) {
            throw new NonRetryableEventException(
                    "Reservation does not exist for payment.succeeded: reservationId=" + reservationId
                            + ", paymentId=" + paymentId);
        }

        LockedReservation locked = lockedOpt.get();
        String status = locked.status();

        if ("HELD".equals(status)) {
            ConfirmedReservation confirmed = reservationRepository.confirmIfHeld(reservationId)
                    .orElseThrow(() -> new IllegalStateException(
                            "Failed to confirm HELD reservation under lock: " + reservationId));

            SeatConfirmedPayload confirmedPayload = new SeatConfirmedPayload(
                    reservationId,
                    orderId,
                    paymentId,
                    confirmed.eventId(),
                    confirmed.seatIds(),
                    confirmed.confirmedAt()
            );
            outboxWriter.append(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMED, confirmedPayload);

            registerAfterCommit(() -> {
                recordOutcome(EventTypes.PAYMENT_SUCCEEDED, OUTCOME_CONFIRMED);
                confirmSeatsInRedisAfterCommit(
                        confirmed.eventId(),
                        confirmed.seatIds(),
                        reservationId,
                        correlationId
                );
            });
            return;
        }

        if ("EXPIRED".equals(status) || "RELEASED".equals(status)) {
            String reason = "EXPIRED".equals(status)
                    ? REASON_RESERVATION_ALREADY_EXPIRED
                    : REASON_RESERVATION_ALREADY_RELEASED;
            Instant rejectedAt = reservationRepository.currentDatabaseTimestamp();

            SeatConfirmationRejectedPayload rejectedPayload = new SeatConfirmationRejectedPayload(
                    reservationId,
                    orderId,
                    paymentId,
                    locked.userId(),
                    locked.eventId(),
                    reason,
                    rejectedAt
            );
            outboxWriter.append(reservationId, EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED, rejectedPayload);

            registerAfterCommit(() -> {
                recordOutcome(EventTypes.PAYMENT_SUCCEEDED, OUTCOME_REJECTED);
                log.info("Rejected late payment confirmation for {} reservation: reservationId={}, paymentId={}, reason={}, correlationId={}",
                        status, reservationId, paymentId, reason, correlationId);
            });
            return;
        }

        if ("CONFIRMED".equals(status)) {
            registerAfterCommit(() -> {
                recordOutcome(EventTypes.PAYMENT_SUCCEEDED, OUTCOME_NOOP);
                log.debug("Reservation already CONFIRMED on payment.succeeded; no-op: reservationId={}, paymentId={}, correlationId={}",
                        reservationId, paymentId, correlationId);
            });
            return;
        }

        throw new NonRetryableEventException(
                "Unexpected reservation status '" + status + "' for reservationId=" + reservationId);
    }

    private void handlePaymentFailed(EventEnvelope<JsonNode> envelope, PaymentFailedPayload payload) {
        UUID reservationId = payload.reservationId();
        UUID paymentId = payload.paymentId();
        UUID orderId = payload.orderId() != null ? payload.orderId() : reservationId;
        String correlationId = envelope.correlationId();

        Optional<LockedReservation> lockedOpt = reservationRepository.lockForUpdate(reservationId);
        if (lockedOpt.isEmpty()) {
            throw new NonRetryableEventException(
                    "Reservation does not exist for payment.failed: reservationId=" + reservationId
                            + ", paymentId=" + paymentId);
        }

        LockedReservation locked = lockedOpt.get();
        String status = locked.status();

        if ("HELD".equals(status)) {
            ReleasedReservation released = reservationRepository.releaseIfHeld(reservationId)
                    .orElseThrow(() -> new IllegalStateException(
                            "Failed to release HELD reservation under lock: " + reservationId));

            SeatReleasedPayload releasedPayload = new SeatReleasedPayload(
                    reservationId,
                    orderId,
                    released.eventId(),
                    released.seatIds(),
                    REASON_PAYMENT_FAILED,
                    released.releasedAt()
            );
            outboxWriter.append(reservationId, EventTypes.INVENTORY_SEAT_RELEASED, releasedPayload);

            registerAfterCommit(() -> {
                recordOutcome(EventTypes.PAYMENT_FAILED, OUTCOME_RELEASED);
                releaseSeatsInRedisAfterCommit(
                        released.eventId(),
                        released.seatIds(),
                        reservationId,
                        correlationId
                );
            });
            return;
        }

        if ("CONFIRMED".equals(status) || "EXPIRED".equals(status) || "RELEASED".equals(status)) {
            registerAfterCommit(() -> {
                recordOutcome(EventTypes.PAYMENT_FAILED, OUTCOME_NOOP);
                log.debug("Reservation in status {} on payment.failed; no-op: reservationId={}, paymentId={}, correlationId={}",
                        status, reservationId, paymentId, correlationId);
            });
            return;
        }

        throw new NonRetryableEventException(
                "Unexpected reservation status '" + status + "' for reservationId=" + reservationId);
    }

    private void handleIgnoredEvent(EventEnvelope<JsonNode> envelope) {
        String rawEventType = envelope.eventType();
        String safeEventType = ConsumerMetrics.sanitizeEventType(rawEventType);
        log.debug("Ignoring unhandled event type on payment.events: eventType={}, eventId={}, correlationId={}",
                rawEventType, envelope.eventId(), envelope.correlationId());
        registerAfterCommit(() -> recordOutcome(safeEventType, OUTCOME_IGNORED));
    }

    private void registerAfterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                action.run();
            }
        });
    }

    private void confirmSeatsInRedisAfterCommit(UUID eventId,
                                                List<UUID> seatIds,
                                                UUID reservationId,
                                                String correlationId) {
        Throwable lastError = null;
        boolean interrupted = Thread.interrupted();
        try {
            for (int attempt = 0; attempt <= REDIS_CONFIRM_MAX_RETRIES; attempt++) {
                if (attempt > 0 && redisConfirmRetryPauseMs > 0) {
                    try {
                        Thread.sleep(redisConfirmRetryPauseMs);
                    } catch (InterruptedException ie) {
                        interrupted = true;
                    }
                }
                try {
                    ConfirmOutcome outcome = redisSeatGate.confirm(eventId, seatIds, reservationId);
                    log.debug("Confirmed seats in Redis after commit: reservationId={}, eventId={}, outcome={}, correlationId={}",
                            reservationId, eventId, outcome, correlationId);
                    return;
                } catch (Throwable ex) {
                    lastError = ex;
                    log.warn("Redis confirm failed (attempt {}/{}) for reservationId={}, eventId={}, correlationId={}",
                            attempt + 1, REDIS_CONFIRM_MAX_RETRIES + 1, reservationId, eventId, correlationId, ex);
                }
            }
            redisConfirmFailuresCounter.increment();
            log.error("Failed to confirm seats in Redis after {} retries for reservationId={}, eventId={}, seatIds={}, correlationId={}; relying on Redis rewarm / PostgreSQL source of truth",
                    REDIS_CONFIRM_MAX_RETRIES, reservationId, eventId, seatIds, correlationId, lastError);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void releaseSeatsInRedisAfterCommit(UUID eventId,
                                                List<UUID> seatIds,
                                                UUID reservationId,
                                                String correlationId) {
        boolean interrupted = Thread.interrupted();
        try {
            long releasedCount = redisSeatGate.release(eventId, seatIds, reservationId);
            log.debug("Released seats in Redis after payment.failed commit: reservationId={}, eventId={}, releasedCount={}, correlationId={}",
                    reservationId, eventId, releasedCount, correlationId);
        } catch (Throwable ex) {
            log.warn("Failed to release Redis seat hold after payment.failed for reservationId={}, eventId={}, correlationId={}; relying on 660s safety-net TTL",
                    reservationId, eventId, correlationId, ex);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void recordOutcome(String eventType, String outcome) {
        outcomeCounter(eventType, outcome).increment();
    }

    private Counter outcomeCounter(String eventType, String outcome) {
        return Counter.builder(METRIC_PAYMENT_EVENTS_HANDLED)
                .description("Total payment events handled by inventory-service")
                .tag("event_type", eventType)
                .tag("outcome", outcome)
                .register(meterRegistry);
    }

    private EventEnvelope<JsonNode> parseEnvelope(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            throw new NonRetryableEventException("Kafka message payload is null or blank");
        }
        final JsonNode root;
        try {
            root = objectMapper.readTree(rawJson);
        } catch (Exception e) {
            throw new NonRetryableEventException("Malformed JSON in EventEnvelope", e);
        }
        if (root == null || !root.isObject()) {
            throw new NonRetryableEventException("EventEnvelope root must be a JSON object");
        }

        JsonNode schemaVersionNode = root.get("schemaVersion");
        if (schemaVersionNode == null || !schemaVersionNode.isIntegralNumber()) {
            throw new NonRetryableEventException("Missing or non-integer schemaVersion in EventEnvelope");
        }
        int schemaVersion = schemaVersionNode.intValue();
        if (schemaVersion != 1) {
            throw new NonRetryableEventException("Unsupported schemaVersion: " + schemaVersion);
        }

        String eventType = requireText(root, "eventType");
        String eventIdStr = requireText(root, "eventId");
        final UUID eventId;
        try {
            eventId = UUID.fromString(eventIdStr);
        } catch (IllegalArgumentException e) {
            throw new NonRetryableEventException("Invalid UUID for eventId: " + eventIdStr, e);
        }

        String occurredAtStr = requireText(root, "occurredAt");
        final Instant occurredAt;
        try {
            occurredAt = Instant.parse(occurredAtStr);
        } catch (Exception e) {
            throw new NonRetryableEventException("Invalid ISO-8601 timestamp for occurredAt: " + occurredAtStr, e);
        }

        String correlationId = requireText(root, "correlationId");

        JsonNode payloadNode = root.get("payload");
        if (payloadNode == null || payloadNode.isNull() || payloadNode.isMissingNode()) {
            throw new NonRetryableEventException("Missing or null payload in EventEnvelope");
        }

        return new EventEnvelope<>(
                eventId,
                eventType,
                occurredAt,
                correlationId,
                schemaVersion,
                payloadNode
        );
    }

    private PaymentSucceededPayload parsePaymentSucceededPayload(JsonNode payloadNode) {
        final PaymentSucceededPayload payload;
        try {
            payload = objectMapper.treeToValue(payloadNode, PaymentSucceededPayload.class);
        } catch (Exception e) {
            throw new NonRetryableEventException("Failed to deserialize PaymentSucceededPayload", e);
        }
        if (payload == null || payload.reservationId() == null || payload.paymentId() == null) {
            throw new NonRetryableEventException(
                    "PaymentSucceededPayload requires non-null reservationId and paymentId");
        }
        return payload;
    }

    private PaymentFailedPayload parsePaymentFailedPayload(JsonNode payloadNode) {
        final PaymentFailedPayload payload;
        try {
            payload = objectMapper.treeToValue(payloadNode, PaymentFailedPayload.class);
        } catch (Exception e) {
            throw new NonRetryableEventException("Failed to deserialize PaymentFailedPayload", e);
        }
        if (payload == null || payload.reservationId() == null) {
            throw new NonRetryableEventException("PaymentFailedPayload requires non-null reservationId");
        }
        return payload;
    }

    private static String requireText(JsonNode root, String fieldName) {
        JsonNode node = root.get(fieldName);
        if (node == null || !node.isTextual()) {
            throw new NonRetryableEventException("Missing or non-textual required field: " + fieldName);
        }
        String text = node.asText().trim();
        if (text.isEmpty()) {
            throw new NonRetryableEventException("Required field is blank: " + fieldName);
        }
        return text;
    }

    public void setRedisConfirmRetryPauseMs(long redisConfirmRetryPauseMs) {
        this.redisConfirmRetryPauseMs = Math.max(0L, redisConfirmRetryPauseMs);
    }
}

