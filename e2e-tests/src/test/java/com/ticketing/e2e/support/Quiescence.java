package com.ticketing.e2e.support;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Waits for distributed cross-service quiescence across PostgreSQL outboxes,
 * Kafka consumer groups (including retry topics), and saga state machines.
 */
public final class Quiescence {

    private final Db db;
    private final KafkaProbe kafkaProbe;
    private volatile Instant scenarioStart = Instant.EPOCH;
    private final Set<UUID> allowedRefundPendingOrders = ConcurrentHashMap.newKeySet();

    public Quiescence(Db db, KafkaProbe kafkaProbe) {
        this.db = db;
        this.kafkaProbe = kafkaProbe;
    }

    public void markScenarioStart(Instant start) {
        this.scenarioStart = start != null ? start : Instant.EPOCH;
    }

    public Instant scenarioStart() {
        return scenarioStart;
    }

    public void registerAllowedRefundPendingOrder(UUID orderId) {
        if (orderId != null) {
            allowedRefundPendingOrders.add(orderId);
        }
    }

    public void registerAllowedRefundPendingOrders(Collection<UUID> orderIds) {
        if (orderIds != null) {
            allowedRefundPendingOrders.addAll(orderIds);
        }
    }

    public Set<UUID> allowedRefundPendingOrders() {
        return Collections.unmodifiableSet(allowedRefundPendingOrders);
    }

    public void await(UUID eventId, Duration timeout) {
        await(eventId, timeout, false);
    }

    public void awaitForEvents(Collection<UUID> eventIds, Duration timeout) {
        awaitForEvents(eventIds, timeout, false);
    }

    public void awaitForEvents(Collection<UUID> eventIds, Duration timeout, boolean allowRefundPendingResting) {
        AtomicReference<String> lastDiagnostic = new AtomicReference<>("No check executed yet");
        AtomicInteger consecutiveGreen = new AtomicInteger(0);

        try {
            Awaitility.await("Quiescence for events=" + (eventIds != null ? eventIds.size() : "ALL"))
                    .atMost(timeout)
                    .pollInterval(Duration.ofMillis(250))
                    .until(() -> {
                        String diag = evaluateForEventsOnce(eventIds, allowRefundPendingResting);
                        if (diag == null) {
                            return consecutiveGreen.incrementAndGet() >= 2;
                        } else {
                            consecutiveGreen.set(0);
                            lastDiagnostic.set(diag);
                            return false;
                        }
                    });
        } catch (ConditionTimeoutException e) {
            throw new AssertionError("Quiescence timed out after " + timeout.toSeconds()
                    + "s for events=" + eventIds
                    + ".\nDiagnostic:\n" + lastDiagnostic.get(), e);
        }
    }

    private String evaluateForEventsOnce(Collection<UUID> eventIds, boolean allowRefundPendingResting) {
        List<String> issues = new ArrayList<>();
        // Require zero unpublished outbox rows across all three outbox tables
        List<Db.OutboxRow> unpublished = db.findUnpublishedOutboxSince(null);
        for (Db.OutboxRow row : unpublished) {
            issues.add(String.format("Unpublished outbox row: db=%s eventId=%s type=%s aggregateId=%s createdAt=%s",
                    row.dbName(), row.eventId(), row.eventType(), row.aggregateId(), row.createdAt()));
        }
        Map<String, KafkaProbe.GroupLag> lags = kafkaProbe.allConsumerGroupsLag();
        for (KafkaProbe.GroupLag gl : lags.values()) {
            if (gl.totalLag() > 0) {
                Map<Integer, Long> nonZeroParts = gl.partitionLag().entrySet().stream()
                        .filter(e -> e.getValue() > 0)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                issues.add(String.format("Consumer group lag > 0: group=%s topic=%s totalLag=%d partitions=%s",
                        gl.groupId(), gl.topic(), gl.totalLag(), nonZeroParts));
            }
        }
        if (eventIds != null && !eventIds.isEmpty()) {
            for (UUID eventId : eventIds) {
                String evDiag = evaluateOnce(eventId, allowRefundPendingResting);
                if (evDiag != null) {
                    issues.add(evDiag);
                }
            }
        } else {
            String allDiag = evaluateOnce(null, allowRefundPendingResting);
            if (allDiag != null) {
                issues.add(allDiag);
            }
        }
        return issues.isEmpty() ? null : String.join("\n  - ", issues);
    }

    public void await(UUID eventId, Duration timeout, boolean allowRefundPendingResting) {
        AtomicReference<String> lastDiagnostic = new AtomicReference<>("No check executed yet");
        AtomicInteger consecutiveGreen = new AtomicInteger(0);

        try {
            Awaitility.await("Quiescence for eventId=" + (eventId != null ? eventId : "ALL"))
                    .atMost(timeout)
                    .pollInterval(Duration.ofMillis(250))
                    .until(() -> {
                        String diag = evaluateOnce(eventId, allowRefundPendingResting);
                        if (diag == null) {
                            return consecutiveGreen.incrementAndGet() >= 2;
                        } else {
                            consecutiveGreen.set(0);
                            lastDiagnostic.set(diag);
                            return false;
                        }
                    });
        } catch (ConditionTimeoutException e) {
            throw new AssertionError("Quiescence timed out after " + timeout.toSeconds()
                    + "s for eventId=" + (eventId != null ? eventId : "ALL")
                    + ".\nDiagnostic:\n" + lastDiagnostic.get(), e);
        }
    }

    /**
     * Evaluates whether the system is quiescent.
     *
     * @return {@code null} if quiescent, or a detailed human-readable diagnostic string if not quiescent.
     */
    public String evaluateOnce(UUID eventId, boolean allowRefundPendingResting) {
        List<String> issues = new ArrayList<>();

        // (a) Require zero unpublished outbox rows across all 3 databases
        List<Db.OutboxRow> unpublished = db.findUnpublishedOutboxSince(null);
        if (!unpublished.isEmpty()) {
            for (Db.OutboxRow row : unpublished) {
                issues.add(String.format("Unpublished outbox row: db=%s eventId=%s type=%s aggregateId=%s createdAt=%s",
                        row.dbName(), row.eventId(), row.eventType(), row.aggregateId(), row.createdAt()));
            }
        }

        // (b) Check consumer group lag on all main and retry topics
        Map<String, KafkaProbe.GroupLag> lags = kafkaProbe.allConsumerGroupsLag();
        for (KafkaProbe.GroupLag gl : lags.values()) {
            if (gl.totalLag() > 0) {
                Map<Integer, Long> nonZeroParts = gl.partitionLag().entrySet().stream()
                        .filter(e -> e.getValue() > 0)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                issues.add(String.format("Consumer group lag > 0: group=%s topic=%s totalLag=%d partitions=%s",
                        gl.groupId(), gl.topic(), gl.totalLag(), nonZeroParts));
            }
        }

        // (c) Check every reservation and order of the event (or all events if eventId == null)
        List<Db.ReservationRow> reservations = db.findReservations(eventId);
        List<UUID> resIds = reservations.stream().map(Db.ReservationRow::reservationId).toList();
        List<Db.OrderRow> orders = db.findOrders(eventId);
        Map<UUID, Db.OrderRow> ordersById = orders.stream()
                .collect(Collectors.toMap(Db.OrderRow::orderId, Function.identity(), (a, b) -> a));
        Map<UUID, Db.PaymentRow> paymentsByOrderId = (eventId == null
                ? db.findPaymentsByOrderIds(null)
                : db.findPaymentsByOrderIds(resIds)).stream()
                .collect(Collectors.toMap(Db.PaymentRow::orderId, Function.identity(), (a, b) -> a));

        Instant dbNow = db.currentTimestampInInventoryDb();

        for (Db.ReservationRow res : reservations) {
            Db.OrderRow order = ordersById.get(res.reservationId());
            if (order == null) {
                issues.add(String.format("Reservation %s (status=%s) has not yet materialized in order_db.orders",
                        res.reservationId(), res.status()));
                continue;
            }
            if ("HELD".equals(res.status()) && res.expiresAt() != null && !res.expiresAt().isAfter(dbNow)) {
                issues.add(String.format("Reservation %s is still HELD but expires_at (%s) <= dbNow (%s); awaiting expiry sweeper",
                        res.reservationId(), res.expiresAt(), dbNow));
            }
        }

        for (Db.OrderRow order : orders) {
            UUID orderId = order.orderId();
            String status = order.status();
            Db.PaymentRow payment = paymentsByOrderId.get(orderId);
            String paymentStatus = payment != null ? payment.status() : "NONE";

            Db.ReservationRow res = reservations.stream()
                    .filter(r -> r.reservationId().equals(orderId))
                    .findFirst()
                    .orElse(null);
            String resStatus = res != null ? res.status() : "NONE";

            switch (status) {
                case "PAYMENT_PENDING" -> issues.add(String.format(
                        "Order %s is still in non-final status PAYMENT_PENDING (reservation=%s, payment=%s)",
                        orderId, resStatus, paymentStatus));
                case "REFUND_PENDING" -> {
                    boolean allowed = allowRefundPendingResting || allowedRefundPendingOrders.contains(orderId);
                    if (!allowed) {
                        issues.add(String.format(
                                "Order %s is in REFUND_PENDING awaiting refund completion (reservation=%s, payment=%s)",
                                orderId, resStatus, paymentStatus));
                    } else if (!"SUCCEEDED".equals(paymentStatus)) {
                        issues.add(String.format(
                                "Order %s in allowed REFUND_PENDING has payment=%s (expected SUCCEEDED)",
                                orderId, paymentStatus));
                    }
                }
                case "RESERVED" -> {
                    if (!"HELD".equals(resStatus)) {
                        issues.add(String.format(
                                "Order %s is RESERVED while reservation is %s; awaiting cancellation transition",
                                orderId, resStatus));
                    }
                }
                case "CONFIRMED" -> {
                    if (!"CONFIRMED".equals(resStatus) || !"SUCCEEDED".equals(paymentStatus)) {
                        issues.add(String.format(
                                "Order %s is CONFIRMED but reservation=%s, payment=%s",
                                orderId, resStatus, paymentStatus));
                    }
                }
                case "CANCELLED_PAYMENT_FAILED" -> {
                    if ("HELD".equals(resStatus)) {
                        issues.add(String.format(
                                "Order %s is CANCELLED_PAYMENT_FAILED but reservation is still HELD",
                                orderId));
                    }
                    if ("SUCCEEDED".equals(paymentStatus)) {
                        issues.add(String.format(
                                "Order %s is CANCELLED_PAYMENT_FAILED with SUCCEEDED payment; awaiting REFUND_PENDING transition",
                                orderId));
                    }
                }
                case "CANCELLED_EXPIRED" -> {
                    if ("HELD".equals(resStatus)) {
                        issues.add(String.format(
                                "Order %s is CANCELLED_EXPIRED but reservation is still HELD",
                                orderId));
                    }
                    if ("SUCCEEDED".equals(paymentStatus)) {
                        issues.add(String.format(
                                "Order %s is CANCELLED_EXPIRED with SUCCEEDED payment; awaiting late-payment REFUND_PENDING compensation",
                                orderId));
                    }
                }
                case "CANCELLED_REFUNDED" -> {
                    if (!"REFUNDED".equals(paymentStatus)) {
                        issues.add(String.format(
                                "Order %s is CANCELLED_REFUNDED but payment status is %s",
                                orderId, paymentStatus));
                    }
                }
                default -> {
                    if (!Set.of("CONFIRMED", "CANCELLED_PAYMENT_FAILED", "CANCELLED_EXPIRED", "CANCELLED_REFUNDED").contains(status)) {
                        issues.add(String.format("Order %s is in unexpected status %s", orderId, status));
                    }
                }
            }
        }

        if (issues.isEmpty()) {
            return null;
        }
        return String.join("\n  - ", issues);
    }
}
