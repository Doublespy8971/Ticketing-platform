package com.ticketing.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.ConflictException;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ResourceNotFoundException;
import com.ticketing.common.events.PaymentRequestedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.order.api.ForbiddenException;
import com.ticketing.order.api.IdempotencyPayloadMismatchException;
import com.ticketing.order.api.IdempotencyRequestInProgressException;
import com.ticketing.order.api.dto.CheckoutRequest;
import com.ticketing.order.api.dto.CheckoutResponse;
import com.ticketing.order.api.dto.OrderDetailsResponse;
import com.ticketing.order.api.dto.OrderItemResponse;
import com.ticketing.order.domain.OrderItemRecord;
import com.ticketing.order.domain.OrderRecord;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.idempotency.IdempotencyKeyRecord;
import com.ticketing.order.idempotency.IdempotencyKeyRepository;
import com.ticketing.outbox.OutboxWriter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class CheckoutService {

    private static final Logger log = LoggerFactory.getLogger(CheckoutService.class);

    public record CheckoutExecutionResult(
            int httpStatus,
            CheckoutResponse response,
            boolean idempotentReplay
    ) {}

    private record IdempotencyClaimOutcome(
            Instant claimedAt,
            IdempotencyKeyRecord completedRecord
    ) {}

    private final OrderRepository orderRepository;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final OutboxWriter outboxWriter;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    // Test hooks for concurrency verification
    private volatile Runnable afterIdempotencyClaimHook;
    private volatile Runnable inTransactionHook;

    public CheckoutService(
            OrderRepository orderRepository,
            IdempotencyKeyRepository idempotencyKeyRepository,
            OutboxWriter outboxWriter,
            PlatformTransactionManager transactionManager,
            ObjectMapper objectMapper,
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        this.orderRepository = Objects.requireNonNull(orderRepository, "orderRepository must not be null");
        this.idempotencyKeyRepository = Objects.requireNonNull(idempotencyKeyRepository, "idempotencyKeyRepository must not be null");
        this.outboxWriter = Objects.requireNonNull(outboxWriter, "outboxWriter must not be null");
        Objects.requireNonNull(transactionManager, "transactionManager must not be null");
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.meterRegistry = meterRegistryProvider != null ? meterRegistryProvider.getIfAvailable() : null;
    }

    public void setAfterIdempotencyClaimHook(Runnable hook) {
        this.afterIdempotencyClaimHook = hook;
    }

    public void setInTransactionHook(Runnable hook) {
        this.inTransactionHook = hook;
    }

    public CheckoutExecutionResult checkout(
            UUID orderId,
            UUID userId,
            UUID idempotencyKey,
            CheckoutRequest request) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(request, "request must not be null");

        long startNanos = System.nanoTime();
        String correlationId = MDC.get(CorrelationConstants.MDC_KEY);
        String endpoint = "/api/v1/orders/" + orderId + "/checkout";
        String requestHash = computeRequestHash(orderId, request);

        try {
            IdempotencyClaimOutcome claimOutcome = claimOrCheckIdempotency(userId, idempotencyKey, endpoint, requestHash);
            if (claimOutcome.completedRecord() != null) {
                IdempotencyKeyRecord existing = claimOutcome.completedRecord();
                if (existing.responseStatus() == null || existing.responseStatus() != HttpStatus.ACCEPTED.value()) {
                    throw new IllegalStateException(
                            "Expected response_status 202 for COMPLETED idempotency key " + idempotencyKey
                                    + ", but found " + existing.responseStatus());
                }
                CheckoutResponse storedResponse = deserializeResponse(existing.responseBody());
                recordOutcome("replay", System.nanoTime() - startNanos);
                log.info("Idempotent replay for checkout: orderId={}, userId={}, idempotencyKey={}, correlationId={}",
                        orderId, userId, idempotencyKey, correlationId);
                return new CheckoutExecutionResult(HttpStatus.ACCEPTED.value(), storedResponse, true);
            }

            Instant claimedAt = Objects.requireNonNull(claimOutcome.claimedAt(), "claimedAt must not be null");
            boolean idempotencyCommitted = false;

            try {
                Runnable claimHook = this.afterIdempotencyClaimHook;
                if (claimHook != null) {
                    claimHook.run();
                }

                CheckoutResponse createdResponse = transactionTemplate.execute(status -> {
                    Runnable txHook = this.inTransactionHook;
                    if (txHook != null) {
                        txHook.run();
                    }

                    // 1. Lock the order and verify DB clock
                    Optional<OrderRepository.OrderCheckoutLockResult> lockResultOpt =
                            orderRepository.findByIdForCheckout(orderId);

                    // Early-checkout race / missing order / mismatched user:
                    // Return 409 ORDER_NOT_READY without leaking whether order exists for another user
                    if (lockResultOpt.isEmpty() || !lockResultOpt.get().order().userId().equals(userId)) {
                        log.info("Order not ready or user mismatch: orderId={}, userId={}, correlationId={}",
                                orderId, userId, correlationId);
                        throw new ConflictException(ErrorCode.ORDER_NOT_READY,
                                "Order " + orderId + " is not ready for checkout. Retry after 1 second.");
                    }

                    OrderRepository.OrderCheckoutLockResult lockResult = lockResultOpt.get();
                    OrderRecord order = lockResult.order();

                    // 2. Validate order status
                    if (!"RESERVED".equals(order.status())) {
                        if ("CANCELLED_EXPIRED".equals(order.status())) {
                            log.info("Order reservation already expired: orderId={}, status={}, correlationId={}",
                                    orderId, order.status(), correlationId);
                            throw new ConflictException(ErrorCode.RESERVATION_EXPIRED,
                                    "Reservation for order " + orderId + " has expired.");
                        }
                        if ("PAYMENT_PENDING".equals(order.status())) {
                            log.info("Order is already in PAYMENT_PENDING: orderId={}, correlationId={}",
                                    orderId, correlationId);
                            throw new ConflictException(ErrorCode.INVALID_ORDER_STATE,
                                    "Order " + orderId + " is already in PAYMENT_PENDING.");
                        }
                        log.info("Order in invalid state for checkout: orderId={}, status={}, correlationId={}",
                                orderId, order.status(), correlationId);
                        throw new ConflictException(ErrorCode.INVALID_ORDER_STATE,
                                "Order " + orderId + " cannot be checked out in status: " + order.status());
                    }

                    // 3. Verify grace buffer against database clock (expires_at - clock_timestamp() >= INTERVAL '15 seconds')
                    if (!lockResult.graceBufferValid()) {
                        log.info("Order reservation grace period expired: orderId={}, expiresAt={}, dbClock={}, correlationId={}",
                                orderId, order.expiresAt(), lockResult.dbClock(), correlationId);
                        throw new ConflictException(ErrorCode.RESERVATION_EXPIRED,
                                "Reservation for order " + orderId + " has expired.");
                    }

                    // 4. Transition RESERVED to PAYMENT_PENDING
                    OrderRepository.OrderCheckoutUpdateResult updateResult =
                            orderRepository.updateStatusToPaymentPending(orderId, request.paymentMethodToken());

                    // 5. Emit order.payment.requested outbox event
                    PaymentRequestedPayload payload = new PaymentRequestedPayload(
                            orderId,
                            order.reservationId(),
                            order.userId(),
                            order.eventId(),
                            order.totalAmountCents(),
                            order.currency(),
                            request.paymentMethodToken(),
                            updateResult.checkoutAt(),
                            order.expiresAt()
                    );
                    outboxWriter.append(orderId, EventTypes.ORDER_PAYMENT_REQUESTED, payload);

                    // 6. Build response and mark idempotency COMPLETED within the same transaction
                    CheckoutResponse response = new CheckoutResponse(
                            orderId,
                            order.reservationId(),
                            order.userId(),
                            order.eventId(),
                            "PAYMENT_PENDING",
                            order.totalAmountCents(),
                            order.currency(),
                            updateResult.checkoutAt(),
                            order.expiresAt()
                    );

                    String responseJson = serializeResponse(response);
                    idempotencyKeyRepository.markCompleted(
                            userId,
                            idempotencyKey,
                            claimedAt,
                            HttpStatus.ACCEPTED.value(),
                            responseJson
                    );

                    return response;
                });

                idempotencyCommitted = true;
                recordOutcome("accepted", System.nanoTime() - startNanos);
                log.info("Order checkout accepted: orderId={}, userId={}, totalAmountCents={}, correlationId={}",
                        orderId, userId, createdResponse.totalAmountCents(), correlationId);
                return new CheckoutExecutionResult(HttpStatus.ACCEPTED.value(), createdResponse, false);
            } finally {
                if (!idempotencyCommitted) {
                    safeDeleteInProgress(userId, idempotencyKey, claimedAt);
                }
            }
        } catch (ConflictException ex) {
            long duration = System.nanoTime() - startNanos;
            if (ex.errorCode() == ErrorCode.ORDER_NOT_READY) {
                recordOutcome("not_ready", duration);
            } else if (ex.errorCode() == ErrorCode.RESERVATION_EXPIRED) {
                recordOutcome("expired", duration);
            } else {
                recordOutcome("conflict", duration);
            }
            throw ex;
        } catch (IdempotencyRequestInProgressException | IdempotencyPayloadMismatchException ex) {
            recordOutcome("conflict", System.nanoTime() - startNanos);
            throw ex;
        } catch (Exception ex) {
            recordOutcome("conflict", System.nanoTime() - startNanos);
            throw ex;
        }
    }

    public OrderDetailsResponse getOrder(UUID orderId, UUID callerUserId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(callerUserId, "callerUserId must not be null");

        OrderRecord order = orderRepository.findById(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found with ID: " + orderId));

        if (!order.userId().equals(callerUserId)) {
            throw new ForbiddenException("Caller does not have access to order " + orderId);
        }

        List<OrderItemRecord> items = orderRepository.findItemsByOrderId(orderId);
        List<OrderItemResponse> seatResponses = items.stream()
                .map(item -> new OrderItemResponse(
                        item.seatId(),
                        item.sectionCode(),
                        item.rowLabel(),
                        item.seatNumber(),
                        item.priceCents()
                ))
                .toList();

        return new OrderDetailsResponse(
                order.orderId(),
                order.reservationId(),
                order.userId(),
                order.eventId(),
                order.status(),
                order.paymentId(),
                seatResponses,
                order.totalAmountCents(),
                order.currency(),
                order.reservedAt(),
                order.expiresAt(),
                order.checkoutAt(),
                order.confirmedAt(),
                order.cancelledAt(),
                order.cancellationReason()
        );
    }

    private IdempotencyClaimOutcome claimOrCheckIdempotency(
            UUID userId,
            UUID idempotencyKey,
            String endpoint,
            String requestHash) {
        Optional<Instant> claimedAt = idempotencyKeyRepository.tryClaim(userId, idempotencyKey, endpoint, requestHash);
        if (claimedAt.isPresent()) {
            return new IdempotencyClaimOutcome(claimedAt.get(), null);
        }

        Optional<IdempotencyKeyRecord> existingOpt = idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey);
        if (existingOpt.isEmpty()) {
            Optional<Instant> retriedClaim = idempotencyKeyRepository.tryClaim(userId, idempotencyKey, endpoint, requestHash);
            if (retriedClaim.isPresent()) {
                return new IdempotencyClaimOutcome(retriedClaim.get(), null);
            }
            existingOpt = idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey);
        }

        IdempotencyKeyRecord existing = existingOpt.orElseThrow(() -> new IdempotencyRequestInProgressException(
                "Concurrent request with Idempotency-Key " + idempotencyKey + " is currently in progress."));

        if (!endpoint.equals(existing.endpoint()) || !requestHash.equals(existing.requestHash())) {
            throw new IdempotencyPayloadMismatchException(
                    "Idempotency-Key " + idempotencyKey + " was already used with a different request payload.");
        }

        if (IdempotencyKeyRepository.STATUS_IN_PROGRESS.equals(existing.status())) {
            Optional<Instant> reclaimedAt = idempotencyKeyRepository.reclaimStaleInProgress(userId, idempotencyKey);
            if (reclaimedAt.isPresent()) {
                return new IdempotencyClaimOutcome(reclaimedAt.get(), null);
            }
            throw new IdempotencyRequestInProgressException(
                    "Request with Idempotency-Key " + idempotencyKey + " is currently in progress. Retry after 1 second.");
        }

        return new IdempotencyClaimOutcome(null, existing);
    }

    private void safeDeleteInProgress(UUID userId, UUID idempotencyKey, Instant claimedAt) {
        try {
            idempotencyKeyRepository.deleteInProgress(userId, idempotencyKey, claimedAt);
        } catch (Exception e) {
            log.warn("Failed to delete in-progress idempotency key: userId={}, key={}", userId, idempotencyKey, e);
        }
    }

    private String computeRequestHash(UUID orderId, CheckoutRequest request) {
        try {
            String canonicalJson = objectMapper.writeValueAsString(request);
            String input = "POST:/api/v1/orders/" + orderId + "/checkout:" + canonicalJson;
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hashBytes);
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to compute request SHA-256 hash", e);
        }
    }

    private String serializeResponse(CheckoutResponse response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize CheckoutResponse", e);
        }
    }

    private CheckoutResponse deserializeResponse(String json) {
        try {
            return objectMapper.readValue(json, CheckoutResponse.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize stored CheckoutResponse", e);
        }
    }

    private void recordOutcome(String outcome, long durationNanos) {
        if (meterRegistry != null) {
            Counter.builder("order_checkout_requests_total")
                    .tag("outcome", outcome)
                    .register(meterRegistry)
                    .increment();
            Timer.builder("order_checkout_duration_seconds")
                    .register(meterRegistry)
                    .record(durationNanos, TimeUnit.NANOSECONDS);
        }
    }
}

