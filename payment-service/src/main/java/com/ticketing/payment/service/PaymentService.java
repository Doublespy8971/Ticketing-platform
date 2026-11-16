package com.ticketing.payment.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.events.PaymentFailedPayload;
import com.ticketing.common.events.PaymentRefundedPayload;
import com.ticketing.common.events.PaymentRequestedPayload;
import com.ticketing.common.events.PaymentSucceededPayload;
import com.ticketing.common.events.RefundRequestedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.consumer.ConsumerEnvelopeParser;
import com.ticketing.consumer.IdempotentEventProcessor;
import com.ticketing.consumer.NonRetryableEventException;
import com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory;
import com.ticketing.outbox.OutboxWriter;
import com.ticketing.payment.domain.PaymentRecord;
import com.ticketing.payment.domain.PaymentRepository;
import com.ticketing.payment.domain.PaymentStatus;
import com.ticketing.payment.metrics.PaymentMetrics;
import com.ticketing.payment.service.provider.PaymentProvider;
import com.ticketing.payment.service.provider.PaymentResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    public static final String CONSUMER_GROUP = TicketingRetryTopicNamesProviderFactory.PAYMENT_SERVICE_ORDER_CG;

    private final PaymentRepository paymentRepository;
    private final PaymentProvider paymentProvider;
    private final OutboxWriter outboxWriter;
    private final IdempotentEventProcessor idempotentEventProcessor;
    private final ConsumerEnvelopeParser envelopeParser;
    private final ObjectMapper objectMapper;
    private final PaymentMetrics metrics;

    private volatile Runnable insideTransactionHook;

    public PaymentService(
            PaymentRepository paymentRepository,
            PaymentProvider paymentProvider,
            OutboxWriter outboxWriter,
            IdempotentEventProcessor idempotentEventProcessor,
            ConsumerEnvelopeParser envelopeParser,
            ObjectMapper objectMapper,
            PaymentMetrics metrics
    ) {
        this.paymentRepository = Objects.requireNonNull(paymentRepository, "paymentRepository must not be null");
        this.paymentProvider = Objects.requireNonNull(paymentProvider, "paymentProvider must not be null");
        this.outboxWriter = Objects.requireNonNull(outboxWriter, "outboxWriter must not be null");
        this.idempotentEventProcessor = Objects.requireNonNull(idempotentEventProcessor, "idempotentEventProcessor must not be null");
        this.envelopeParser = Objects.requireNonNull(envelopeParser, "envelopeParser must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    }

    public void setInsideTransactionHook(Runnable hook) {
        this.insideTransactionHook = hook;
    }

    private void invokeInsideTransactionHook() {
        Runnable hook = this.insideTransactionHook;
        if (hook != null) {
            hook.run();
        }
    }

    public void processOrderEvent(String rawJson) {
        EventEnvelope<JsonNode> envelope = envelopeParser.parse(rawJson);

        String correlationId = envelope.correlationId();
        if (correlationId == null || correlationId.isBlank()) {
            throw new NonRetryableEventException("Missing correlationId in event envelope");
        }
        try {
            UUID.fromString(correlationId);
        } catch (IllegalArgumentException ex) {
            throw new NonRetryableEventException("Invalid correlationId in event envelope: " + correlationId, ex);
        }

        String previousCorrelationId = MDC.get(CorrelationConstants.MDC_KEY);
        MDC.put(CorrelationConstants.MDC_KEY, correlationId);
        try {
            String eventType = envelope.eventType();
            UUID eventId = envelope.eventId();

            switch (eventType) {
                case EventTypes.ORDER_PAYMENT_REQUESTED -> handleOrderPaymentRequested(envelope);
                case EventTypes.ORDER_REFUND_REQUESTED -> handleOrderRefundRequested(envelope);
                default -> {
                    log.debug("Acknowledging and ignoring unhandled eventType={} for eventId={}", eventType, eventId);
                    metrics.recordEventHandled(eventType, "ignored");
                }
            }
        } finally {
            if (previousCorrelationId != null) {
                MDC.put(CorrelationConstants.MDC_KEY, previousCorrelationId);
            } else {
                MDC.remove(CorrelationConstants.MDC_KEY);
            }
        }
    }

    private void handleOrderPaymentRequested(EventEnvelope<JsonNode> envelope) {
        PaymentRequestedPayload payload;
        try {
            payload = objectMapper.treeToValue(envelope.payload(), PaymentRequestedPayload.class);
        } catch (Exception ex) {
            throw new NonRetryableEventException("Malformed order.payment.requested payload: " + ex.getMessage(), ex);
        }

        // 1. Pre-charge validation before touching the provider or database
        PaymentPayloadValidator.validatePaymentRequested(payload);

        UUID orderId = payload.orderId();
        UUID eventId = envelope.eventId();
        String eventType = envelope.eventType();

        log.info("Processing order.payment.requested for orderId={}, eventId={}", orderId, eventId);

        // 2. Check whether a payment row for the order already exists (read without lock)
        Optional<PaymentRecord> existing = paymentRepository.findByOrderId(orderId);
        if (existing.isPresent()) {
            log.info("Payment for orderId={} already exists (status={}); skipping provider charge",
                    orderId, existing.get().status());
            idempotentEventProcessor.process(eventId, CONSUMER_GROUP, eventType, () -> {
                // Record event as handled; no-op
            });
            metrics.recordEventHandled(eventType, "noop");
            return;
        }

        // 3. Call provider before the transaction with idempotencyKey equal to orderId
        long startNanos = System.nanoTime();
        PaymentResult result;
        try {
            result = paymentProvider.charge(orderId, payload.amountCents(), payload.currency(), payload.paymentMethodToken());
            metrics.recordProviderDuration("charge", System.nanoTime() - startNanos);
            metrics.recordProviderCall("charge", result.outcome() == PaymentResult.Outcome.SUCCEEDED ? "succeeded" : "declined");
        } catch (Exception ex) {
            metrics.recordProviderDuration("charge", System.nanoTime() - startNanos);
            metrics.recordProviderCall("charge", "error");
            throw ex; // Technical error propagates to retry topics
        }

        // 4. Processor transaction
        boolean processed = idempotentEventProcessor.process(eventId, CONSUMER_GROUP, eventType, () -> {
            invokeInsideTransactionHook();

            UUID paymentId = UUID.randomUUID();
            PaymentStatus paymentStatus = (result.outcome() == PaymentResult.Outcome.SUCCEEDED)
                    ? PaymentStatus.SUCCEEDED
                    : PaymentStatus.FAILED;

            PaymentRecord recordToInsert = new PaymentRecord(
                    paymentId,
                    orderId,
                    payload.reservationId(),
                    payload.userId(),
                    payload.amountCents(),
                    payload.currency(),
                    payload.paymentMethodToken(),
                    paymentStatus,
                    result.providerReference(),
                    result.failureCode(),
                    result.failureMessage(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
            );

            Optional<PaymentRecord> insertedOpt = paymentRepository.insertPayment(recordToInsert);
            if (insertedOpt.isEmpty()) {
                log.info("Payment for orderId={} was already inserted concurrently; skipping outbox write", orderId);
                return;
            }

            PaymentRecord persisted = insertedOpt.get();
            if (persisted.status() == PaymentStatus.SUCCEEDED) {
                PaymentSucceededPayload outboxPayload = new PaymentSucceededPayload(
                        persisted.paymentId(),
                        persisted.orderId(),
                        persisted.reservationId(),
                        persisted.userId(),
                        persisted.amountCents(),
                        persisted.currency(),
                        persisted.providerReference(),
                        persisted.paidAt()
                );
                outboxWriter.append(orderId, EventTypes.PAYMENT_SUCCEEDED, outboxPayload);
                metrics.recordPaymentProcessed(PaymentStatus.SUCCEEDED.name(), null);
            } else {
                PaymentFailedPayload outboxPayload = new PaymentFailedPayload(
                        persisted.paymentId(),
                        persisted.orderId(),
                        persisted.reservationId(),
                        persisted.userId(),
                        persisted.amountCents(),
                        persisted.currency(),
                        persisted.failureCode(),
                        persisted.failureMessage(),
                        persisted.failedAt()
                );
                outboxWriter.append(orderId, EventTypes.PAYMENT_FAILED, outboxPayload);
                metrics.recordPaymentProcessed(PaymentStatus.FAILED.name(), persisted.failureCode());
            }
        });

        if (processed) {
            metrics.recordEventHandled(eventType, "applied");
        } else {
            metrics.recordEventHandled(eventType, "noop");
        }
    }

    private void handleOrderRefundRequested(EventEnvelope<JsonNode> envelope) {
        RefundRequestedPayload payload;
        try {
            payload = objectMapper.treeToValue(envelope.payload(), RefundRequestedPayload.class);
        } catch (Exception ex) {
            throw new NonRetryableEventException("Malformed order.refund.requested payload: " + ex.getMessage(), ex);
        }

        // 1. Pre-refund validation before touching provider or database
        PaymentPayloadValidator.validateRefundRequested(payload);
        String truncatedReason = PaymentPayloadValidator.sanitizeRefundReason(payload.reason());

        UUID orderId = payload.orderId();
        UUID eventId = envelope.eventId();
        String eventType = envelope.eventType();

        // 2. Look up payment by order id
        Optional<PaymentRecord> paymentOpt = paymentRepository.findByOrderId(orderId);
        if (paymentOpt.isEmpty()) {
            throw new NonRetryableEventException("Payment not found for orderId: " + orderId);
        }
        PaymentRecord payment = paymentOpt.get();

        // 3. A payment that is FAILED or already REFUNDED is a no-op
        if (payment.status() == PaymentStatus.FAILED || payment.status() == PaymentStatus.REFUNDED) {
            log.info("Payment for orderId={} is already in terminal non-refundable status={}; skipping refund",
                    orderId, payment.status());
            idempotentEventProcessor.process(eventId, CONSUMER_GROUP, eventType, () -> {});
            metrics.recordEventHandled(eventType, "noop");
            return;
        }

        // 4. A refund amount or currency that differs from payment's throws NonRetryableEventException
        if (payload.amountCents() != payment.amountCents() || !payload.currency().equalsIgnoreCase(payment.currency())) {
            throw new NonRetryableEventException("Refund amount or currency does not match original payment for orderId: " + orderId);
        }

        // 5. For SUCCEEDED payment: call provider refund with idempotencyKey equal to paymentId before transaction
        long startNanos = System.nanoTime();
        PaymentResult result;
        try {
            result = paymentProvider.refund(
                    payment.paymentId(),
                    payment.providerReference(),
                    payload.amountCents(),
                    payload.currency(),
                    payment.paymentMethodToken()
            );
            metrics.recordProviderDuration("refund", System.nanoTime() - startNanos);
            metrics.recordProviderCall("refund", result.outcome() == PaymentResult.Outcome.SUCCEEDED ? "succeeded" : "declined");
        } catch (Exception ex) {
            metrics.recordProviderDuration("refund", System.nanoTime() - startNanos);
            metrics.recordProviderCall("refund", "error");
            throw ex; // Technical error propagates to retry topics
        }

        // If refund is DECLINED: do not mark refunded, do not write outbox, log ERROR, increment metric, route to DLT
        if (result.outcome() == PaymentResult.Outcome.DECLINED) {
            log.error("Payment refund declined by provider for paymentId={}, failureCode={}, failureMessage={}",
                    payment.paymentId(), result.failureCode(), result.failureMessage());
            metrics.recordRefundDeclined(result.failureCode());
            throw new NonRetryableEventException(
                    "Payment refund declined for paymentId=" + payment.paymentId() + ": " + result.failureCode());
        }

        // 6. In processor transaction: lock payment row, move SUCCEEDED to REFUNDED with refunded_at from DB clock
        boolean processed = idempotentEventProcessor.process(eventId, CONSUMER_GROUP, eventType, () -> {
            invokeInsideTransactionHook();

            Optional<PaymentRecord> lockedOpt = paymentRepository.findByIdForUpdate(payment.paymentId());
            if (lockedOpt.isEmpty() || lockedOpt.get().status() != PaymentStatus.SUCCEEDED) {
                log.info("Payment {} is no longer in SUCCEEDED status; skipping refund transition", payment.paymentId());
                return;
            }

            UUID refundId = UUID.randomUUID();

            Optional<Instant> refundedAtOpt = paymentRepository.updateRefunded(payment.paymentId(), refundId, truncatedReason);
            if (refundedAtOpt.isEmpty()) {
                log.info("Payment {} was already refunded; skipping outbox", payment.paymentId());
                return;
            }
            Instant refundedAt = refundedAtOpt.get();

            PaymentRefundedPayload outboxPayload = new PaymentRefundedPayload(
                    refundId,
                    payment.paymentId(),
                    orderId,
                    payment.reservationId(),
                    payment.userId(),
                    payment.amountCents(),
                    payment.currency(),
                    truncatedReason,
                    refundedAt
            );
            outboxWriter.append(orderId, EventTypes.PAYMENT_REFUNDED, outboxPayload);
            metrics.recordPaymentProcessed(PaymentStatus.REFUNDED.name(), null);
        });

        if (processed) {
            metrics.recordEventHandled(eventType, "applied");
        } else {
            metrics.recordEventHandled(eventType, "noop");
        }
    }
}
