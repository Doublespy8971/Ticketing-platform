package com.ticketing.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.OrderCancelledPayload;
import com.ticketing.common.events.OrderConfirmedPayload;
import com.ticketing.common.events.PaymentFailedPayload;
import com.ticketing.common.events.PaymentRefundedPayload;
import com.ticketing.common.events.PaymentSucceededPayload;
import com.ticketing.common.events.RefundRequestedPayload;
import com.ticketing.common.events.ReservationExpiredPayload;
import com.ticketing.common.events.SeatConfirmationRejectedPayload;
import com.ticketing.common.events.SeatConfirmedPayload;
import com.ticketing.common.events.SeatLine;
import com.ticketing.common.events.SeatReservedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.consumer.ConsumerMetrics;
import com.ticketing.consumer.IdempotentEventProcessor;
import com.ticketing.consumer.NonRetryableEventException;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import com.ticketing.order.domain.OrderItemRecord;
import com.ticketing.order.domain.OrderRecord;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.outbox.OutboxWriter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Handles the order saga state machine driven by {@code inventory.events} and {@code payment.events}.
 */
@Service
public class OrderSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);

    public static final String STATUS_RESERVED = "RESERVED";
    public static final String STATUS_PAYMENT_PENDING = "PAYMENT_PENDING";
    public static final String STATUS_CONFIRMED = "CONFIRMED";
    public static final String STATUS_CANCELLED_PAYMENT_FAILED = "CANCELLED_PAYMENT_FAILED";
    public static final String STATUS_CANCELLED_EXPIRED = "CANCELLED_EXPIRED";
    public static final String STATUS_REFUND_PENDING = "REFUND_PENDING";
    public static final String STATUS_CANCELLED_REFUNDED = "CANCELLED_REFUNDED";

    public static final String REASON_RESERVATION_EXPIRED = "RESERVATION_EXPIRED";
    public static final String REASON_PAYMENT_FAILED = "PAYMENT_FAILED";
    public static final String REASON_LATE_PAYMENT_EXPIRED_HOLD = "LATE_PAYMENT_EXPIRED_HOLD";
    public static final String REASON_LATE_PAYMENT_AFTER_CANCELLATION = "LATE_PAYMENT_AFTER_CANCELLATION";

    public static final String METRIC_SAGA_EVENTS_HANDLED = "order_saga_events_handled_total";
    public static final String METRIC_MATERIALIZATION_LAG = "ticketing_orders_materialization_lag_seconds";
    public static final String METRIC_TERMINAL_ORDERS = "ticketing_orders_terminal_total";

    public static final String OUTCOME_APPLIED = "applied";
    public static final String OUTCOME_NOOP = "noop";
    public static final String OUTCOME_IGNORED = "ignored";
    public static final String OUTCOME_REFUND_REQUESTED = "refund_requested";

    private static final String AGGREGATE_TYPE_ORDER = "ORDER";
    private static final int SUPPORTED_SCHEMA_VERSION = 1;

    private final IdempotentEventProcessor idempotentEventProcessor;
    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final Timer materializationLagTimer;
    private final ConcurrentMap<String, Counter> handledCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> terminalCounters = new ConcurrentHashMap<>();

    private volatile Runnable afterOrderLockHook;

    public OrderSagaService(
            IdempotentEventProcessor idempotentEventProcessor,
            OrderRepository orderRepository,
            OutboxWriter outboxWriter,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry
    ) {
        this.idempotentEventProcessor = Objects.requireNonNull(idempotentEventProcessor, "idempotentEventProcessor must not be null");
        this.orderRepository = Objects.requireNonNull(orderRepository, "orderRepository must not be null");
        this.outboxWriter = Objects.requireNonNull(outboxWriter, "outboxWriter must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.materializationLagTimer = Timer.builder(METRIC_MATERIALIZATION_LAG)
                .description("Lag between inventory.seat.reserved and order row insert")
                .register(meterRegistry);
    }

    /**
     * Test hook invoked immediately after acquiring the {@code SELECT ... FOR UPDATE} lock on the order row.
     */
    public void setAfterOrderLockHook(Runnable afterOrderLockHook) {
        this.afterOrderLockHook = afterOrderLockHook;
    }

    public void processInventoryRecord(String rawJson, String headerCorrelationId) {
        processRecord(rawJson, headerCorrelationId, TicketingRetryTopicNamesProviderFactory.ORDER_SERVICE_INVENTORY_CG);
    }

    public void processPaymentRecord(String rawJson, String headerCorrelationId) {
        processRecord(rawJson, headerCorrelationId, TicketingRetryTopicNamesProviderFactory.ORDER_SERVICE_PAYMENT_CG);
    }

    public void processRecord(String rawJson, String headerCorrelationId, String consumerGroup) {
        ParsedEnvelope envelope = parseEnvelope(rawJson);

        String previousCorrelationId = MDC.get(CorrelationConstants.MDC_KEY);
        String effectiveCorrelationId = envelope.correlationId() != null
                ? envelope.correlationId().toString()
                : headerCorrelationId;
        if (effectiveCorrelationId != null && !effectiveCorrelationId.isBlank()) {
            MDC.put(CorrelationConstants.MDC_KEY, effectiveCorrelationId);
        }

        try {
            idempotentEventProcessor.process(
                    envelope.eventId(),
                    consumerGroup,
                    envelope.eventType(),
                    () -> dispatchInsideTransaction(envelope)
            );
        } finally {
            if (previousCorrelationId != null) {
                MDC.put(CorrelationConstants.MDC_KEY, previousCorrelationId);
            } else if (effectiveCorrelationId != null && !effectiveCorrelationId.isBlank()) {
                MDC.remove(CorrelationConstants.MDC_KEY);
            }
        }
    }

    private void dispatchInsideTransaction(ParsedEnvelope envelope) {
        switch (envelope.eventType()) {
            case EventTypes.INVENTORY_SEAT_RESERVED -> handleSeatReserved(envelope);
            case EventTypes.INVENTORY_RESERVATION_EXPIRED -> handleReservationExpired(envelope);
            case EventTypes.INVENTORY_SEAT_CONFIRMED -> handleSeatConfirmed(envelope);
            case EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED -> handleSeatConfirmationRejected(envelope);
            case EventTypes.PAYMENT_SUCCEEDED -> handlePaymentSucceeded(envelope);
            case EventTypes.PAYMENT_FAILED -> handlePaymentFailed(envelope);
            case EventTypes.PAYMENT_REFUNDED -> handlePaymentRefunded(envelope);
            default -> handleIgnoredEvent(envelope);
        }
    }

    private void handleSeatReserved(ParsedEnvelope envelope) {
        SeatReservedPayload payload = parseSeatReservedPayload(envelope.payload());
        UUID orderId = payload.reservationId();

        Optional<OrderRecord> existing = orderRepository.findByIdForUpdate(orderId);
        invokeAfterOrderLockHook();

        if (existing.isPresent()) {
            log.info("Order {} already exists in status {}; ignoring inventory.seat.reserved (eventId={})",
                    orderId, existing.get().status(), envelope.eventId());
            registerAfterCommitOutcome(EventTypes.INVENTORY_SEAT_RESERVED, OUTCOME_NOOP);
            return;
        }

        OrderRecord newOrder = new OrderRecord(
                orderId,
                payload.reservationId(),
                payload.userId(),
                payload.eventId(),
                STATUS_RESERVED,
                payload.totalAmountCents(),
                payload.currency(),
                null,
                null,
                null,
                0L,
                payload.reservedAt(),
                payload.expiresAt(),
                null,
                null,
                null,
                null,
                null,
                payload.reservedAt(),
                payload.reservedAt()
        );

        List<OrderItemRecord> items = new ArrayList<>(payload.seats().size());
        for (SeatLine seat : payload.seats()) {
            items.add(new OrderItemRecord(
                    UUID.randomUUID(),
                    orderId,
                    seat.seatId(),
                    seat.sectionCode(),
                    seat.rowLabel(),
                    seat.seatNumber(),
                    seat.priceCents()
            ));
        }

        Optional<Instant> createdAtOpt = orderRepository.insertIfAbsent(newOrder, items);
        if (createdAtOpt.isEmpty()) {
            log.info("Order {} was concurrently inserted; ignoring inventory.seat.reserved (eventId={})",
                    orderId, envelope.eventId());
            registerAfterCommitOutcome(EventTypes.INVENTORY_SEAT_RESERVED, OUTCOME_NOOP);
            return;
        }

        Instant createdAt = createdAtOpt.get();
        log.info("Materialized order {} in RESERVED for user={} event={} seats={} totalAmountCents={} {} (eventId={})",
                orderId, payload.userId(), payload.eventId(), items.size(), payload.totalAmountCents(),
                payload.currency(), envelope.eventId());

        registerAfterCommit(() -> {
            recordOutcome(EventTypes.INVENTORY_SEAT_RESERVED, OUTCOME_APPLIED);
            long lagNanos = Math.max(0L, Duration.between(payload.reservedAt(), createdAt).toNanos());
            materializationLagTimer.record(lagNanos, TimeUnit.NANOSECONDS);
        });
    }

    private void handleReservationExpired(ParsedEnvelope envelope) {
        ReservationExpiredPayload payload = parseReservationExpiredPayload(envelope.payload());
        UUID orderId = payload.reservationId();

        Optional<OrderRecord> orderOpt = orderRepository.findByIdForUpdate(orderId);
        invokeAfterOrderLockHook();

        if (orderOpt.isEmpty()) {
            throw new IllegalStateException(
                    "Order not yet materialized for reservationId=" + orderId + " on inventory.reservation.expired"
            );
        }

        OrderRecord order = orderOpt.get();
        switch (order.status()) {
            case STATUS_RESERVED -> {
                Instant cancelledAt = orderRepository.cancelExpiredFromReserved(orderId);
                writeOrderCancelledOutbox(order, envelope.correlationId(), STATUS_CANCELLED_EXPIRED, REASON_RESERVATION_EXPIRED, cancelledAt);
                log.info("Order {} transitioned from RESERVED to CANCELLED_EXPIRED on inventory.reservation.expired (eventId={})",
                        orderId, envelope.eventId());
                registerAfterCommitTerminalOutcome(EventTypes.INVENTORY_RESERVATION_EXPIRED, OUTCOME_APPLIED, STATUS_CANCELLED_EXPIRED);
            }
            case STATUS_PAYMENT_PENDING -> {
                if (order.paymentId() == null) {
                    Instant cancelledAt = orderRepository.cancelExpiredFromPaymentPendingWithoutPayment(orderId);
                    writeOrderCancelledOutbox(order, envelope.correlationId(), STATUS_CANCELLED_EXPIRED, REASON_RESERVATION_EXPIRED, cancelledAt);
                    log.info("Order {} transitioned from PAYMENT_PENDING (no payment) to CANCELLED_EXPIRED on inventory.reservation.expired (eventId={})",
                            orderId, envelope.eventId());
                    registerAfterCommitTerminalOutcome(EventTypes.INVENTORY_RESERVATION_EXPIRED, OUTCOME_APPLIED, STATUS_CANCELLED_EXPIRED);
                } else {
                    log.info("Order {} in PAYMENT_PENDING already has paymentId={}; ignoring inventory.reservation.expired as no-op (eventId={})",
                            orderId, order.paymentId(), envelope.eventId());
                    registerAfterCommitOutcome(EventTypes.INVENTORY_RESERVATION_EXPIRED, OUTCOME_NOOP);
                }
            }
            default -> {
                log.info("Order {} is in status {}; ignoring inventory.reservation.expired as no-op (eventId={})",
                        orderId, order.status(), envelope.eventId());
                registerAfterCommitOutcome(EventTypes.INVENTORY_RESERVATION_EXPIRED, OUTCOME_NOOP);
            }
        }
    }

    private void handleSeatConfirmed(ParsedEnvelope envelope) {
        SeatConfirmedPayload payload = parseSeatConfirmedPayload(envelope.payload());
        UUID orderId = payload.orderId();

        Optional<OrderRecord> orderOpt = orderRepository.findByIdForUpdate(orderId);
        invokeAfterOrderLockHook();

        if (orderOpt.isEmpty()) {
            throw new IllegalStateException(
                    "Order not yet materialized for orderId=" + orderId + " on inventory.seat.confirmed"
            );
        }

        OrderRecord order = orderOpt.get();
        if (STATUS_PAYMENT_PENDING.equals(order.status())) {
            OrderRepository.OrderConfirmedUpdateResult updated =
                    orderRepository.confirmFromPaymentPending(orderId, payload.paymentId());

            List<OrderItemRecord> itemRecords = orderRepository.findItemsByOrderId(orderId);
            List<SeatLine> confirmedSeats = new ArrayList<>(itemRecords.size());
            for (OrderItemRecord item : itemRecords) {
                confirmedSeats.add(new SeatLine(
                        item.seatId(),
                        item.sectionCode(),
                        item.rowLabel(),
                        item.seatNumber(),
                        item.priceCents()
                ));
            }

            OrderConfirmedPayload confirmedPayload = new OrderConfirmedPayload(
                    order.orderId(),
                    order.reservationId(),
                    order.userId(),
                    order.eventId(),
                    updated.paymentId(),
                    confirmedSeats,
                    order.totalAmountCents(),
                    order.currency(),
                    updated.confirmedAt()
            );

            outboxWriter.append(
                    order.orderId(),
                    EventTypes.ORDER_CONFIRMED,
                    confirmedPayload
            );

            log.info("Order {} transitioned from PAYMENT_PENDING to CONFIRMED (paymentId={}, eventId={})",
                    orderId, updated.paymentId(), envelope.eventId());
            registerAfterCommitTerminalOutcome(EventTypes.INVENTORY_SEAT_CONFIRMED, OUTCOME_APPLIED, STATUS_CONFIRMED);
        } else {
            log.info("Order {} is in status {}; ignoring inventory.seat.confirmed as no-op (eventId={})",
                    orderId, order.status(), envelope.eventId());
            registerAfterCommitOutcome(EventTypes.INVENTORY_SEAT_CONFIRMED, OUTCOME_NOOP);
        }
    }

    private void handleSeatConfirmationRejected(ParsedEnvelope envelope) {
        SeatConfirmationRejectedPayload payload = parseSeatConfirmationRejectedPayload(envelope.payload());
        UUID orderId = payload.orderId();

        Optional<OrderRecord> orderOpt = orderRepository.findByIdForUpdate(orderId);
        invokeAfterOrderLockHook();

        if (orderOpt.isEmpty()) {
            throw new IllegalStateException(
                    "Order not yet materialized for orderId=" + orderId + " on inventory.seat.confirmation_rejected"
            );
        }

        OrderRecord order = orderOpt.get();
        switch (order.status()) {
            case STATUS_PAYMENT_PENDING, STATUS_CANCELLED_EXPIRED, STATUS_CANCELLED_PAYMENT_FAILED -> {
                String dbCancellationReason = STATUS_PAYMENT_PENDING.equals(order.status())
                        ? REASON_LATE_PAYMENT_EXPIRED_HOLD
                        : (payload.reason() != null && !payload.reason().isBlank()
                                ? payload.reason()
                                : REASON_LATE_PAYMENT_AFTER_CANCELLATION);

                Instant requestedAt = orderRepository.transitionToRefundPending(
                        orderId,
                        order.status(),
                        payload.paymentId(),
                        null,
                        dbCancellationReason
                );

                writeRefundRequestedOutbox(order, payload.paymentId(), envelope.correlationId(), requestedAt);
                log.info("Order {} transitioned from {} to REFUND_PENDING on inventory.seat.confirmation_rejected (paymentId={}, eventId={})",
                        orderId, order.status(), payload.paymentId(), envelope.eventId());
                registerAfterCommitOutcome(EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED, OUTCOME_REFUND_REQUESTED);
            }
            default -> {
                log.info("Order {} is in status {}; ignoring inventory.seat.confirmation_rejected as no-op (eventId={})",
                        orderId, order.status(), envelope.eventId());
                registerAfterCommitOutcome(EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED, OUTCOME_NOOP);
            }
        }
    }

    private void handlePaymentSucceeded(ParsedEnvelope envelope) {
        PaymentSucceededPayload payload = parsePaymentSucceededPayload(envelope.payload());
        UUID orderId = payload.orderId();

        Optional<OrderRecord> orderOpt = orderRepository.findByIdForUpdate(orderId);
        invokeAfterOrderLockHook();

        if (orderOpt.isEmpty()) {
            throw new NonRetryableEventException(
                    "Order not found for orderId=" + orderId + " on payment.succeeded"
            );
        }

        OrderRecord order = orderOpt.get();
        switch (order.status()) {
            case STATUS_PAYMENT_PENDING -> {
                orderRepository.recordPaymentInPaymentPending(orderId, payload.paymentId(), payload.paidAt());
                log.info("Recorded payment.succeeded on order {} in PAYMENT_PENDING (paymentId={}, eventId={}); awaiting inventory confirmation",
                        orderId, payload.paymentId(), envelope.eventId());
                registerAfterCommitOutcome(EventTypes.PAYMENT_SUCCEEDED, OUTCOME_APPLIED);
            }
            case STATUS_CONFIRMED -> {
                orderRepository.backfillPaymentInConfirmed(orderId, payload.paymentId(), payload.paidAt());
                log.info("Backfilled payment details on already CONFIRMED order {} (paymentId={}, eventId={})",
                        orderId, payload.paymentId(), envelope.eventId());
                registerAfterCommitOutcome(EventTypes.PAYMENT_SUCCEEDED, OUTCOME_APPLIED);
            }
            case STATUS_CANCELLED_EXPIRED, STATUS_CANCELLED_PAYMENT_FAILED -> {
                Instant requestedAt = orderRepository.transitionToRefundPending(
                        orderId,
                        order.status(),
                        payload.paymentId(),
                        payload.paidAt(),
                        REASON_LATE_PAYMENT_AFTER_CANCELLATION
                );
                writeRefundRequestedOutbox(order, payload.paymentId(), envelope.correlationId(), requestedAt);
                log.info("Order {} transitioned from {} to REFUND_PENDING on late payment.succeeded (paymentId={}, eventId={})",
                        orderId, order.status(), payload.paymentId(), envelope.eventId());
                registerAfterCommitOutcome(EventTypes.PAYMENT_SUCCEEDED, OUTCOME_REFUND_REQUESTED);
            }
            default -> {
                log.info("Order {} is in status {}; ignoring payment.succeeded as no-op (eventId={})",
                        orderId, order.status(), envelope.eventId());
                registerAfterCommitOutcome(EventTypes.PAYMENT_SUCCEEDED, OUTCOME_NOOP);
            }
        }
    }

    private void handlePaymentFailed(ParsedEnvelope envelope) {
        PaymentFailedPayload payload = parsePaymentFailedPayload(envelope.payload());
        UUID orderId = payload.orderId();

        Optional<OrderRecord> orderOpt = orderRepository.findByIdForUpdate(orderId);
        invokeAfterOrderLockHook();

        if (orderOpt.isEmpty()) {
            throw new NonRetryableEventException(
                    "Order not found for orderId=" + orderId + " on payment.failed"
            );
        }

        OrderRecord order = orderOpt.get();
        if (STATUS_PAYMENT_PENDING.equals(order.status())) {
            Instant cancelledAt = orderRepository.cancelPaymentFailedFromPaymentPending(orderId, payload.paymentId());
            writeOrderCancelledOutbox(order, envelope.correlationId(), STATUS_CANCELLED_PAYMENT_FAILED, REASON_PAYMENT_FAILED, cancelledAt);
            log.info("Order {} transitioned from PAYMENT_PENDING to CANCELLED_PAYMENT_FAILED on payment.failed (paymentId={}, eventId={})",
                    orderId, payload.paymentId(), envelope.eventId());
            registerAfterCommitTerminalOutcome(EventTypes.PAYMENT_FAILED, OUTCOME_APPLIED, STATUS_CANCELLED_PAYMENT_FAILED);
        } else {
            log.info("Order {} is in status {}; ignoring payment.failed as no-op (eventId={})",
                    orderId, order.status(), envelope.eventId());
            registerAfterCommitOutcome(EventTypes.PAYMENT_FAILED, OUTCOME_NOOP);
        }
    }

    private void handlePaymentRefunded(ParsedEnvelope envelope) {
        PaymentRefundedPayload payload = parsePaymentRefundedPayload(envelope.payload());
        UUID orderId = payload.orderId();

        Optional<OrderRecord> orderOpt = orderRepository.findByIdForUpdate(orderId);
        invokeAfterOrderLockHook();

        if (orderOpt.isEmpty()) {
            throw new NonRetryableEventException(
                    "Order not found for orderId=" + orderId + " on payment.refunded"
            );
        }

        OrderRecord order = orderOpt.get();
        if (STATUS_REFUND_PENDING.equals(order.status())) {
            Instant refundedAt = orderRepository.cancelRefundedFromRefundPending(orderId, payload.paymentId());
            writeOrderCancelledOutbox(order, envelope.correlationId(), STATUS_CANCELLED_REFUNDED, REASON_LATE_PAYMENT_EXPIRED_HOLD, refundedAt);
            log.info("Order {} transitioned from REFUND_PENDING to CANCELLED_REFUNDED on payment.refunded (paymentId={}, refundId={}, eventId={})",
                    orderId, payload.paymentId(), payload.refundId(), envelope.eventId());
            registerAfterCommitTerminalOutcome(EventTypes.PAYMENT_REFUNDED, OUTCOME_APPLIED, STATUS_CANCELLED_REFUNDED);
        } else {
            log.info("Order {} is in status {}; ignoring payment.refunded as no-op (eventId={})",
                    orderId, order.status(), envelope.eventId());
            registerAfterCommitOutcome(EventTypes.PAYMENT_REFUNDED, OUTCOME_NOOP);
        }
    }

    private void handleIgnoredEvent(ParsedEnvelope envelope) {
        log.debug("Ignoring unhandled event type={} eventId={}", envelope.eventType(), envelope.eventId());
        registerAfterCommitOutcome(envelope.eventType(), OUTCOME_IGNORED);
    }

    private void writeOrderCancelledOutbox(
            OrderRecord order,
            UUID correlationId,
            String finalStatus,
            String cancellationReason,
            Instant cancelledAt
    ) {
        OrderCancelledPayload cancelledPayload = new OrderCancelledPayload(
                order.orderId(),
                order.reservationId(),
                order.userId(),
                order.eventId(),
                finalStatus,
                cancellationReason,
                cancelledAt
        );
        outboxWriter.append(
                order.orderId(),
                EventTypes.ORDER_CANCELLED,
                cancelledPayload
        );
    }

    private void writeRefundRequestedOutbox(
            OrderRecord order,
            UUID paymentId,
            UUID correlationId,
            Instant requestedAt
    ) {
        RefundRequestedPayload refundPayload = new RefundRequestedPayload(
                order.orderId(),
                order.reservationId(),
                paymentId,
                order.userId(),
                order.totalAmountCents(),
                order.currency(),
                REASON_LATE_PAYMENT_EXPIRED_HOLD,
                requestedAt
        );
        outboxWriter.append(
                order.orderId(),
                EventTypes.ORDER_REFUND_REQUESTED,
                refundPayload
        );
    }

    private void invokeAfterOrderLockHook() {
        Runnable hook = this.afterOrderLockHook;
        if (hook != null) {
            hook.run();
        }
    }

    private void registerAfterCommitOutcome(String eventType, String outcome) {
        registerAfterCommit(() -> recordOutcome(eventType, outcome));
    }

    private void registerAfterCommitTerminalOutcome(String eventType, String outcome, String finalStatus) {
        registerAfterCommit(() -> {
            recordOutcome(eventType, outcome);
            recordTerminalOrder(finalStatus);
        });
    }

    private void registerAfterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            });
        } else {
            action.run();
        }
    }

    private void recordOutcome(String eventType, String outcome) {
        String safeEventType = ConsumerMetrics.sanitizeEventType(eventType);
        String key = safeEventType + ":" + outcome;
        Counter counter = handledCounters.computeIfAbsent(key, k ->
                Counter.builder(METRIC_SAGA_EVENTS_HANDLED)
                        .description("Total Kafka events handled by order-service saga")
                        .tag("event_type", safeEventType)
                        .tag("outcome", outcome)
                        .register(meterRegistry)
        );
        counter.increment();
    }

    private void recordTerminalOrder(String finalStatus) {
        Counter counter = terminalCounters.computeIfAbsent(finalStatus, status ->
                Counter.builder(METRIC_TERMINAL_ORDERS)
                        .description("Orders reaching a terminal state")
                        .tag("final_status", status)
                        .register(meterRegistry)
        );
        counter.increment();
    }

    private ParsedEnvelope parseEnvelope(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            throw new NonRetryableEventException("Event payload must not be null or blank");
        }

        final JsonNode root;
        try {
            root = objectMapper.readTree(rawJson);
        } catch (JsonProcessingException ex) {
            throw new NonRetryableEventException("Malformed JSON in event envelope", ex);
        }

        if (root == null || !root.isObject()) {
            throw new NonRetryableEventException("Event envelope must be a JSON object");
        }

        JsonNode schemaNode = root.get("schemaVersion");
        if (schemaNode == null || !schemaNode.isIntegralNumber()) {
            throw new NonRetryableEventException("Missing or non-integer 'schemaVersion' in event envelope");
        }
        int schemaVersion = schemaNode.asInt();
        if (schemaVersion != SUPPORTED_SCHEMA_VERSION) {
            throw new NonRetryableEventException(
                    "Unsupported schemaVersion=" + schemaVersion + " (expected " + SUPPORTED_SCHEMA_VERSION + ")"
            );
        }

        UUID eventId = parseRequiredUuid(root, "eventId");
        String eventType = parseRequiredString(root, "eventType");
        Instant occurredAt = parseRequiredInstant(root, "occurredAt");
        UUID correlationId = parseRequiredUuid(root, "correlationId");

        JsonNode payloadNode = root.get("payload");
        if (payloadNode == null || payloadNode.isNull() || !payloadNode.isObject()) {
            throw new NonRetryableEventException("Missing or non-object 'payload' in event envelope");
        }

        return new ParsedEnvelope(eventId, eventType, occurredAt, correlationId, schemaVersion, payloadNode);
    }

    private SeatReservedPayload parseSeatReservedPayload(JsonNode payloadNode) {
        final SeatReservedPayload payload;
        try {
            payload = objectMapper.treeToValue(payloadNode, SeatReservedPayload.class);
        } catch (JsonProcessingException ex) {
            throw new NonRetryableEventException("Invalid inventory.seat.reserved payload", ex);
        }
        if (payload == null
                || payload.reservationId() == null
                || payload.userId() == null
                || payload.eventId() == null
                || payload.seats() == null
                || payload.seats().isEmpty()
                || payload.totalAmountCents() <= 0
                || payload.currency() == null || payload.currency().isBlank()
                || payload.reservedAt() == null
                || payload.expiresAt() == null) {
            throw new NonRetryableEventException("Missing required fields in inventory.seat.reserved payload");
        }
        for (SeatLine seat : payload.seats()) {
            if (seat == null
                    || seat.seatId() == null
                    || seat.sectionCode() == null || seat.sectionCode().isBlank()
                    || seat.rowLabel() == null || seat.rowLabel().isBlank()
                    || seat.seatNumber() == null || seat.seatNumber().isBlank()
                    || seat.priceCents() <= 0) {
                throw new NonRetryableEventException("Invalid seat item in inventory.seat.reserved payload");
            }
        }
        return payload;
    }

    private ReservationExpiredPayload parseReservationExpiredPayload(JsonNode payloadNode) {
        final ReservationExpiredPayload payload;
        try {
            payload = objectMapper.treeToValue(payloadNode, ReservationExpiredPayload.class);
        } catch (JsonProcessingException ex) {
            throw new NonRetryableEventException("Invalid inventory.reservation.expired payload", ex);
        }
        if (payload == null
                || payload.reservationId() == null
                || payload.eventId() == null
                || payload.seatIds() == null
                || payload.expiredAt() == null) {
            throw new NonRetryableEventException("Missing required fields in inventory.reservation.expired payload");
        }
        return payload;
    }

    private SeatConfirmedPayload parseSeatConfirmedPayload(JsonNode payloadNode) {
        final SeatConfirmedPayload payload;
        try {
            payload = objectMapper.treeToValue(payloadNode, SeatConfirmedPayload.class);
        } catch (JsonProcessingException ex) {
            throw new NonRetryableEventException("Invalid inventory.seat.confirmed payload", ex);
        }
        if (payload == null
                || payload.reservationId() == null
                || payload.orderId() == null
                || payload.paymentId() == null
                || payload.eventId() == null
                || payload.seatIds() == null || payload.seatIds().isEmpty()
                || payload.confirmedAt() == null) {
            throw new NonRetryableEventException("Missing required fields in inventory.seat.confirmed payload");
        }
        return payload;
    }

    private SeatConfirmationRejectedPayload parseSeatConfirmationRejectedPayload(JsonNode payloadNode) {
        final SeatConfirmationRejectedPayload payload;
        try {
            payload = objectMapper.treeToValue(payloadNode, SeatConfirmationRejectedPayload.class);
        } catch (JsonProcessingException ex) {
            throw new NonRetryableEventException("Invalid inventory.seat.confirmation_rejected payload", ex);
        }
        if (payload == null
                || payload.reservationId() == null
                || payload.orderId() == null
                || payload.paymentId() == null
                || payload.reason() == null || payload.reason().isBlank()
                || payload.rejectedAt() == null) {
            throw new NonRetryableEventException("Missing required fields in inventory.seat.confirmation_rejected payload");
        }
        return payload;
    }

    private PaymentSucceededPayload parsePaymentSucceededPayload(JsonNode payloadNode) {
        final PaymentSucceededPayload payload;
        try {
            payload = objectMapper.treeToValue(payloadNode, PaymentSucceededPayload.class);
        } catch (JsonProcessingException ex) {
            throw new NonRetryableEventException("Invalid payment.succeeded payload", ex);
        }
        if (payload == null
                || payload.paymentId() == null
                || payload.orderId() == null
                || payload.reservationId() == null
                || payload.userId() == null
                || payload.amountCents() <= 0
                || payload.currency() == null || payload.currency().isBlank()
                || payload.providerReference() == null || payload.providerReference().isBlank()
                || payload.paidAt() == null) {
            throw new NonRetryableEventException("Missing required fields in payment.succeeded payload");
        }
        return payload;
    }

    private PaymentFailedPayload parsePaymentFailedPayload(JsonNode payloadNode) {
        final PaymentFailedPayload payload;
        try {
            payload = objectMapper.treeToValue(payloadNode, PaymentFailedPayload.class);
        } catch (JsonProcessingException ex) {
            throw new NonRetryableEventException("Invalid payment.failed payload", ex);
        }
        if (payload == null
                || payload.paymentId() == null
                || payload.orderId() == null
                || payload.reservationId() == null
                || payload.userId() == null
                || payload.amountCents() <= 0
                || payload.currency() == null || payload.currency().isBlank()
                || payload.failureCode() == null || payload.failureCode().isBlank()
                || payload.failureMessage() == null || payload.failureMessage().isBlank()
                || payload.failedAt() == null) {
            throw new NonRetryableEventException("Missing required fields in payment.failed payload");
        }
        return payload;
    }

    private PaymentRefundedPayload parsePaymentRefundedPayload(JsonNode payloadNode) {
        final PaymentRefundedPayload payload;
        try {
            payload = objectMapper.treeToValue(payloadNode, PaymentRefundedPayload.class);
        } catch (JsonProcessingException ex) {
            throw new NonRetryableEventException("Invalid payment.refunded payload", ex);
        }
        if (payload == null
                || payload.refundId() == null
                || payload.paymentId() == null
                || payload.orderId() == null
                || payload.reservationId() == null
                || payload.userId() == null
                || payload.amountCents() <= 0
                || payload.currency() == null || payload.currency().isBlank()
                || payload.reason() == null || payload.reason().isBlank()
                || payload.refundedAt() == null) {
            throw new NonRetryableEventException("Missing required fields in payment.refunded payload");
        }
        return payload;
    }

    private static String parseRequiredString(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || !node.isTextual() || node.asText().isBlank()) {
            throw new NonRetryableEventException("Missing or blank '" + field + "' in event envelope");
        }
        return node.asText();
    }

    private static UUID parseRequiredUuid(JsonNode root, String field) {
        String raw = parseRequiredString(root, field);
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException ex) {
            throw new NonRetryableEventException("Invalid UUID for '" + field + "': " + raw, ex);
        }
    }

    private static Instant parseRequiredInstant(JsonNode root, String field) {
        String raw = parseRequiredString(root, field);
        try {
            return Instant.parse(raw);
        } catch (DateTimeParseException ex) {
            throw new NonRetryableEventException("Invalid ISO-8601 timestamp for '" + field + "': " + raw, ex);
        }
    }

    private record ParsedEnvelope(
            UUID eventId,
            String eventType,
            Instant occurredAt,
            UUID correlationId,
            int schemaVersion,
            JsonNode payload
    ) {}
}
