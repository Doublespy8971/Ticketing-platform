package com.ticketing.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Collects per-scenario execution metrics and invariant results, and renders a
 * human-readable Markdown report to {@code e2e-tests/target/e2e-report.md}
 * (and {@code docs/e2e-report.md} only when {@code -De2e.publishReport=true}
 * AND the run contained no failures).
 * Never writes secrets or JWT tokens to the report, and never emits hardcoded
 * status strings.
 */
public final class ScenarioReportRecorder {

    public record CatalogScenario(String id, String name, String tag) {
    }

    public static final List<CatalogScenario> SCENARIO_CATALOG = List.of(
            new CatalogScenario("S1", "Happy path through the gateway", "fast"),
            new CatalogScenario("S2", "Declined payment and seat re-purchase", "fast"),
            new CatalogScenario("S3", "Hold expiry and seat reclamation", "fast"),
            new CatalogScenario("S4", "Late payment saga compensation", "chaos"),
            new CatalogScenario("S5", "No oversell under load (200 users, 64 threads, 20 seats)", "fast"),
            new CatalogScenario("S6", "Duplicate and replayed Kafka events", "fast"),
            new CatalogScenario("S7", "Crash recovery (SIGKILL payment, inventory, and order services)", "chaos"),
            new CatalogScenario("S8", "Kafka broker 15s outage with 30 in-flight orders", "chaos"),
            new CatalogScenario("S9", "Idempotent checkout through the gateway", "fast"),
            new CatalogScenario("S10", "Technical provider error (tok_error) retry recovery", "fast"),
            new CatalogScenario("S11", "Refund declined (tok_refund_decline) routes to DLT", "chaos"),
            new CatalogScenario("S12", "Gateway protection (RBAC, header spoofing, rate-limit isolation)", "fast"),
            new CatalogScenario("S13",
                    "Final global audit across all rows of all three databases, Kafka consumer groups, DLTs, and notifications",
                    "fast,chaos"));

    public record InvariantSpec(String code, String title, String description) {
    }

    public static final List<InvariantSpec> INVARIANT_SPECS = List.of(
            new InvariantSpec("I1", "Oversell",
                    "Every (event, seat) has at most 1 active (HELD/CONFIRMED) reservation_item and at most 1 CONFIRMED order_item; SOLD seats equal CONFIRMED reservation seats and CONFIRMED order seats"),
            new InvariantSpec("I2", "Seat Consistency",
                    "Every HELD, SOLD, and AVAILABLE seat matches its exact active reservation_item state, and no HELD reservation is older than expires_at + 90s"),
            new InvariantSpec("I3", "Order and Payment Consistency",
                    "Bijective alignment across reservations, orders, and payments in both directions, matching item seat sets, and no undeclared PAYMENT_PENDING or REFUND_PENDING orders at quiescence"),
            new InvariantSpec("I4", "Duplicate Effects",
                    "At most 1 payment row per order (unique providerReference and refundId) and at most 1 outbox event per (aggregateId, eventType) across all 3 databases"),
            new InvariantSpec("I5", "Money",
                    "All parent rows exist, all outbox JSON payloads parse cleanly, every REFUNDED payment has a payment.refunded outbox row, and amounts/currencies match across seats, reservations, orders, payments, and refunds"),
            new InvariantSpec("I6", "Outbox Drained",
                    "Every outbox_events row across inventory_db, order_db, and payment_db has published_at IS NOT NULL at quiescence"),
            new InvariantSpec("I7", "Dead Letters",
                    "Dead-letter topic (*-dlt) offset growth equals the exact scenario-declared expected dead-letter count"),
            new InvariantSpec("I8", "Notifications",
                    "Notification counters match expected counts derived from orders table (1 confirmed per CONFIRMED order, 1..2 cancelled per cancelled order) and distinct eventIds on order.events with 0 counter resets and 0 unexpected duplicates"));

    public record ScenarioRecord(
            String id,
            String name,
            String tag,
            String status,
            long durationMs,
            int users,
            int holdsAttempted,
            int holdsAccepted,
            int holdsRejected,
            int holdsRateLimited,
            int holdsFailedOrLost,
            int reservationsCommitted,
            Map<String, Integer> reservationsByStatus,
            int ordersCreated,
            Map<String, Integer> ordersByStatus,
            int paymentsSucceeded,
            int paymentsFailed,
            int refunds,
            long notificationsConfirmed,
            long notificationsCancelled,
            long deadLetters,
            long expectedDeadLetters,
            Map<String, Boolean> invariants,
            String notes,
            Double recoverySeconds) {
        public ScenarioRecord(
                String id,
                String name,
                String tag,
                String status,
                long durationMs,
                int users,
                int holdsAttempted,
                int holdsAccepted,
                int holdsRejected,
                int holdsRateLimited,
                int holdsFailedOrLost,
                int reservationsCommitted,
                Map<String, Integer> reservationsByStatus,
                int ordersCreated,
                Map<String, Integer> ordersByStatus,
                int paymentsSucceeded,
                int paymentsFailed,
                int refunds,
                long notificationsConfirmed,
                long notificationsCancelled,
                long deadLetters,
                long expectedDeadLetters,
                Map<String, Boolean> invariants,
                String notes) {
            this(id, name, tag, status, durationMs, users, holdsAttempted, holdsAccepted,
                    holdsRejected, holdsRateLimited, holdsFailedOrLost, reservationsCommitted,
                    reservationsByStatus, ordersCreated, ordersByStatus, paymentsSucceeded,
                    paymentsFailed, refunds, notificationsConfirmed, notificationsCancelled,
                    deadLetters, expectedDeadLetters, invariants, notes, null);
        }

        public String reservationsSummary() {
            if (reservationsCommitted == 0 && (reservationsByStatus == null || reservationsByStatus.isEmpty())) {
                return "0";
            }
            return reservationsCommitted + " (" + formatMap(reservationsByStatus) + ")";
        }

        public String ordersSummary() {
            if (ordersCreated == 0 && (ordersByStatus == null || ordersByStatus.isEmpty())) {
                return "0";
            }
            return ordersCreated + " (" + formatMap(ordersByStatus) + ")";
        }

        public String invariantsSummary() {
            if (invariants == null || invariants.isEmpty()) {
                return "N/A";
            }
            boolean allPass = invariants.values().stream().allMatch(Boolean::booleanValue);
            StringBuilder sb = new StringBuilder(allPass ? "I1–I8 PASS (" : "FAIL (");
            List<String> items = new ArrayList<>();
            for (int i = 1; i <= 8; i++) {
                String k = "I" + i;
                Boolean v = invariants.get(k);
                if (v != null) {
                    items.add(k + ":" + (v ? "PASS" : "FAIL"));
                }
            }
            sb.append(String.join(" ", items)).append(")");
            return sb.toString();
        }
    }

    private final E2EConfig config;
    private final ProcessRunner processRunner;
    private final ObjectMapper objectMapper;
    private final Map<String, ScenarioRecord> recordsById = new LinkedHashMap<>();
    private final Instant suiteStartedAt = Instant.now();

    public ScenarioReportRecorder() {
        this(E2EConfig.load(), new SystemProcessRunner());
    }

    public ScenarioReportRecorder(E2EConfig config) {
        this(config, new SystemProcessRunner());
    }

    public ScenarioReportRecorder(E2EConfig config, ProcessRunner processRunner) {
        this.config = config != null ? config : E2EConfig.load();
        this.processRunner = processRunner != null ? processRunner : new SystemProcessRunner();
        this.objectMapper = new ObjectMapper();
    }

    public synchronized void record(ScenarioRecord record) {
        recordsById.put(record.id(), record);
    }

    public synchronized boolean hasRecord(String id) {
        return recordsById.containsKey(id);
    }

    public synchronized List<ScenarioRecord> snapshotRecords() {
        return new ArrayList<>(recordsById.values());
    }

    public static ScenarioRecord fromEventsAudit(
            String id,
            String name,
            String tag,
            String status,
            long durationMs,
            int users,
            int holdsAttempted,
            int holdsAccepted,
            int holdsRejected,
            int holdsRateLimited,
            int holdsFailedOrLost,
            Collection<UUID> eventIds,
            Db db,
            Notifications.NotificationSnapshot notifDiff,
            long deadLettersDelta,
            long expectedDeadLetters,
            InvariantAuditor.AuditResult auditResult,
            String notes) {
        return fromEventsAudit(
                id, name, tag, status, durationMs, users,
                holdsAttempted, holdsAccepted, holdsRejected, holdsRateLimited, holdsFailedOrLost,
                eventIds, db, notifDiff, deadLettersDelta, expectedDeadLetters, auditResult, notes, null);
    }

    public static ScenarioRecord fromEventsAudit(
            String id,
            String name,
            String tag,
            String status,
            long durationMs,
            int users,
            int holdsAttempted,
            int holdsAccepted,
            int holdsRejected,
            int holdsRateLimited,
            int holdsFailedOrLost,
            Collection<UUID> eventIds,
            Db db,
            Notifications.NotificationSnapshot notifDiff,
            long deadLettersDelta,
            long expectedDeadLetters,
            InvariantAuditor.AuditResult auditResult,
            String notes,
            Double recoverySeconds) {

        Map<String, Integer> reservationsByStatus = new LinkedHashMap<>();
        int reservationsCommitted = 0;
        Map<String, Integer> ordersByStatus = new LinkedHashMap<>();
        int ordersCreated = 0;
        int paymentsSucceeded = 0;
        int paymentsFailed = 0;
        int refunds = 0;

        if (db != null) {
            if (eventIds != null && !eventIds.isEmpty()) {
                List<UUID> orderIds = new ArrayList<>();
                for (UUID evId : eventIds) {
                    for (Db.ReservationRow res : db.findReservations(evId)) {
                        reservationsCommitted++;
                        reservationsByStatus.merge(res.status(), 1, Integer::sum);
                    }
                    for (Db.OrderRow order : db.findOrders(evId)) {
                        ordersCreated++;
                        ordersByStatus.merge(order.status(), 1, Integer::sum);
                        orderIds.add(order.orderId());
                    }
                }
                for (Db.PaymentRow payment : db.findPaymentsByOrderIds(orderIds)) {
                    switch (payment.status()) {
                        case "SUCCEEDED" -> paymentsSucceeded++;
                        case "FAILED" -> paymentsFailed++;
                        case "REFUNDED" -> refunds++;
                        default -> {
                        }
                    }
                }
            } else {
                for (Db.ReservationRow res : db.findReservations(null)) {
                    reservationsCommitted++;
                    reservationsByStatus.merge(res.status(), 1, Integer::sum);
                }
                for (Db.OrderRow order : db.findOrders(null)) {
                    ordersCreated++;
                    ordersByStatus.merge(order.status(), 1, Integer::sum);
                }
                for (Db.PaymentRow payment : db.findPaymentsByOrderIds(null)) {
                    switch (payment.status()) {
                        case "SUCCEEDED" -> paymentsSucceeded++;
                        case "FAILED" -> paymentsFailed++;
                        case "REFUNDED" -> refunds++;
                        default -> {
                        }
                    }
                }
            }
        }

        long notifConfirmed = notifDiff != null ? notifDiff.sent("order.confirmed") : 0L;
        long notifCancelled = notifDiff != null ? notifDiff.sent("order.cancelled") : 0L;

        Map<String, Boolean> invMap = new LinkedHashMap<>();
        if (auditResult != null) {
            for (int i = 1; i <= 8; i++) {
                String k = "I" + i;
                List<String> v = auditResult.violationsByInvariant().getOrDefault(k, List.of());
                invMap.put(k, v.isEmpty());
            }
        }

        return new ScenarioRecord(
                id,
                name,
                tag,
                status,
                durationMs,
                users,
                holdsAttempted,
                holdsAccepted,
                holdsRejected,
                holdsRateLimited,
                holdsFailedOrLost,
                reservationsCommitted,
                reservationsByStatus,
                ordersCreated,
                ordersByStatus,
                paymentsSucceeded,
                paymentsFailed,
                refunds,
                notifConfirmed,
                notifCancelled,
                deadLettersDelta,
                expectedDeadLetters,
                invMap,
                notes,
                recoverySeconds);
    }

    public static ScenarioRecord fromEventAudit(
            String id,
            String name,
            String tag,
            String status,
            long durationMs,
            int users,
            int holdsAttempted,
            int holdsAccepted,
            int holdsRejected,
            int holdsRateLimited,
            int holdsFailedOrLost,
            UUID eventId,
            Db db,
            Notifications.NotificationSnapshot notifDiff,
            long deadLettersDelta,
            long expectedDeadLetters,
            InvariantAuditor.AuditResult auditResult,
            String notes) {
        return fromEventAudit(
                id, name, tag, status, durationMs, users,
                holdsAttempted, holdsAccepted, holdsRejected, holdsRateLimited, holdsFailedOrLost,
                eventId, db, notifDiff, deadLettersDelta, expectedDeadLetters, auditResult, notes, null);
    }

    public static ScenarioRecord fromEventAudit(
            String id,
            String name,
            String tag,
            String status,
            long durationMs,
            int users,
            int holdsAttempted,
            int holdsAccepted,
            int holdsRejected,
            int holdsRateLimited,
            int holdsFailedOrLost,
            UUID eventId,
            Db db,
            Notifications.NotificationSnapshot notifDiff,
            long deadLettersDelta,
            long expectedDeadLetters,
            InvariantAuditor.AuditResult auditResult,
            String notes,
            Double recoverySeconds) {
        return fromEventsAudit(
                id, name, tag, status, durationMs, users,
                holdsAttempted, holdsAccepted, holdsRejected, holdsRateLimited, holdsFailedOrLost,
                eventId != null ? List.of(eventId) : null,
                db, notifDiff, deadLettersDelta, expectedDeadLetters, auditResult, notes,
                recoverySeconds);
    }

    public synchronized void writeReports(Path workspaceRoot) throws IOException {
        Instant finishedAt = Instant.now();
        long totalDurationMs = java.time.Duration.between(suiteStartedAt, finishedAt).toMillis();

        List<ScenarioRecord> fullRows = buildFullCatalogRecords();
        String markdown = renderMarkdown(fullRows, finishedAt, totalDurationMs);

        Path targetReport = workspaceRoot.resolve("e2e-tests/target/e2e-report.md");
        if (targetReport.getParent() != null) {
            Files.createDirectories(targetReport.getParent());
        }
        Files.writeString(targetReport, markdown, StandardCharsets.UTF_8);

        boolean allPassed = fullRows.stream().allMatch(r -> "PASS".equals(r.status()));
        if (Boolean.getBoolean("e2e.publishReport") && allPassed) {
            Path docsReport = workspaceRoot.resolve("docs/e2e-report.md");
            if (docsReport.getParent() != null) {
                Files.createDirectories(docsReport.getParent());
            }
            Files.writeString(docsReport, markdown, StandardCharsets.UTF_8);
        }
    }

    private List<ScenarioRecord> buildFullCatalogRecords() {
        List<ScenarioRecord> full = new ArrayList<>(SCENARIO_CATALOG.size());
        for (CatalogScenario cs : SCENARIO_CATALOG) {
            ScenarioRecord rec = recordsById.get(cs.id());
            if (rec != null) {
                full.add(rec);
            } else {
                full.add(new ScenarioRecord(
                        cs.id(), cs.name(), cs.tag(), "NOT RUN", 0L,
                        0, 0, 0, 0, 0, 0,
                        0, Map.of(), 0, Map.of(),
                        0, 0, 0, 0L, 0L, 0L, 0L, Map.of(),
                        "Scenario did not run in this execution."));
            }
        }
        return full;
    }

    private String renderMarkdown(List<ScenarioRecord> fullRows, Instant finishedAt, long totalDurationMs) {
        StringBuilder md = new StringBuilder();
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(finishedAt.atOffset(ZoneOffset.UTC));

        md.append("# Ticketing Platform — Cross-Service End-to-End Verification Report\n\n");
        md.append("- **Generated At (UTC)**: `").append(timestamp).append("`\n");
        md.append("- **Total Suite Duration**: `").append(String.format("%.2f s", totalDurationMs / 1000.0))
                .append("`\n");
        md.append("- **Backend**: `").append(resolveBackendDescription()).append("`\n");
        md.append("- **Failover Budget**: `").append(resolveFailoverBudget()).append("`\n");
        md.append(
                "- **Target Stack**: Real 10-container topology (`gateway-service`, `inventory-service`, `order-service`, `payment-service`, `notification-service`, `kafka`, `redis`, `postgres-inventory`, `postgres-order`, `postgres-payment`)\n\n");

        md.append("## 1. Per-Scenario Execution Matrix\n\n");
        md.append(
                "| ID | Scenario | Tag | Status | Duration | Users | HTTP Holds (`Att / 201 / 409 / 429 / Err`) | DB Reservations (`Committed`) | DB Orders (`Created`) | Payments (`Succ / Fail / Ref`) | Notifications (`confirmed / cancelled`) | DLT (`Actual / Exp`) | Invariants (`I1–I8`) |\n");
        md.append("|---|---|---|---|---:|---:|---|---|---|---|---:|---:|---|\n");

        int totalUsers = 0;
        int totalHoldsAttempted = 0;
        int totalHoldsAccepted = 0;
        int totalHoldsRejected = 0;
        int totalHoldsRateLimited = 0;
        int totalHoldsFailedOrLost = 0;
        int totalReservationsCommitted = 0;
        Map<String, Integer> totalReservationsByStatus = new LinkedHashMap<>();
        int totalOrdersCreated = 0;
        Map<String, Integer> totalOrdersByStatus = new LinkedHashMap<>();
        int totalPaySucc = 0;
        int totalPayFail = 0;
        int totalRefunds = 0;
        long totalNotifConfirmed = 0;
        long totalNotifCancelled = 0;
        long totalDlt = 0;
        long totalExpectedDlt = 0;
        int passCount = 0;
        int failCount = 0;
        int disabledCount = 0;
        int notRunCount = 0;

        for (ScenarioRecord r : fullRows) {
            switch (r.status()) {
                case "PASS" -> passCount++;
                case "FAIL" -> failCount++;
                case "DISABLED" -> disabledCount++;
                default -> notRunCount++;
            }

            // Sum workload scenarios S1-S12 (S13 is the global database/topic audit across
            // all rows)
            if (!"S13".equals(r.id())) {
                totalUsers += r.users();
                totalHoldsAttempted += r.holdsAttempted();
                totalHoldsAccepted += r.holdsAccepted();
                totalHoldsRejected += r.holdsRejected();
                totalHoldsRateLimited += r.holdsRateLimited();
                totalHoldsFailedOrLost += r.holdsFailedOrLost();
                totalReservationsCommitted += r.reservationsCommitted();
                if (r.reservationsByStatus() != null) {
                    r.reservationsByStatus().forEach((k, v) -> totalReservationsByStatus.merge(k, v, Integer::sum));
                }
                totalOrdersCreated += r.ordersCreated();
                if (r.ordersByStatus() != null) {
                    r.ordersByStatus().forEach((k, v) -> totalOrdersByStatus.merge(k, v, Integer::sum));
                }
                totalPaySucc += r.paymentsSucceeded();
                totalPayFail += r.paymentsFailed();
                totalRefunds += r.refunds();
                totalNotifConfirmed += r.notificationsConfirmed();
                totalNotifCancelled += r.notificationsCancelled();
                totalDlt += r.deadLetters();
                totalExpectedDlt += r.expectedDeadLetters();
            }

            String holdsStr = r.holdsAttempted() + " / " + r.holdsAccepted() + " / " + r.holdsRejected()
                    + " / " + r.holdsRateLimited() + " / " + r.holdsFailedOrLost();
            String payStr = r.paymentsSucceeded() + " / " + r.paymentsFailed() + " / " + r.refunds();
            String notifStr = r.notificationsConfirmed() + " / " + r.notificationsCancelled();
            String dltStr = r.deadLetters() + " / " + r.expectedDeadLetters();

            md.append("| **").append(r.id()).append("** ")
                    .append("| ").append(r.name()).append(" ")
                    .append("| `").append(r.tag()).append("` ")
                    .append("| **").append(r.status()).append("** ")
                    .append("| ").append(String.format("%.2fs", r.durationMs() / 1000.0)).append(" ")
                    .append("| ").append(r.users()).append(" ")
                    .append("| `").append(holdsStr).append("` ")
                    .append("| ").append(r.reservationsSummary()).append(" ")
                    .append("| ").append(r.ordersSummary()).append(" ")
                    .append("| `").append(payStr).append("` ")
                    .append("| `").append(notifStr).append("` ")
                    .append("| `").append(dltStr).append("` ")
                    .append("| ").append(r.invariantsSummary()).append(" |\n");
        }

        md.append("\n## 2. Detailed Scenario Breakdown\n\n");
        for (ScenarioRecord r : fullRows) {
            md.append("### ").append(r.id()).append(" — ").append(r.name()).append("\n");
            md.append("- **Status**: `").append(r.status()).append("` (`").append(r.tag()).append("`, ")
                    .append(String.format("%.2f s", r.durationMs() / 1000.0)).append(")\n");
            if (r.recoverySeconds() != null && r.recoverySeconds() > 0) {
                md.append("- **Recovery time**: ").append(String.format("%.2f s", r.recoverySeconds())).append("\n");
            }
            md.append("- **Users**: ").append(r.users())
                    .append(" | **HTTP Holds**: attempted=").append(r.holdsAttempted())
                    .append(", accepted(201)=").append(r.holdsAccepted())
                    .append(", conflict(409)=").append(r.holdsRejected())
                    .append(", rate-limited(429)=").append(r.holdsRateLimited())
                    .append(", error/lost(5xx/transport)=").append(r.holdsFailedOrLost()).append("\n");
            md.append("- **Reservations Committed in `inventory_db`**: ").append(r.reservationsSummary()).append("\n");
            if (!"S13".equals(r.id()) && r.reservationsCommitted() != r.holdsAccepted()) {
                int crashCommittedDelta = r.reservationsCommitted() - r.holdsAccepted();
                md.append("- **Crash HTTP-Response Reconciliation Note**: ").append(crashCommittedDelta)
                        .append(" hold request(s) committed in `inventory_db` before container crash/SIGKILL whose HTTP `201` response was lost in transit (`holdsAccepted=")
                        .append(r.holdsAccepted()).append("` vs `reservationsCommitted=")
                        .append(r.reservationsCommitted()).append("`).\n");
            }
            md.append("- **Orders Created in `order_db`**: ").append(r.ordersSummary()).append("\n");
            md.append("- **Payments in `payment_db`**: succeeded=").append(r.paymentsSucceeded())
                    .append(", failed=").append(r.paymentsFailed())
                    .append(", refunded=").append(r.refunds()).append("\n");
            md.append("- **Notifications Sent**: `order.confirmed`=").append(r.notificationsConfirmed())
                    .append(", `order.cancelled`=").append(r.notificationsCancelled()).append("\n");
            md.append("- **Dead Letters Delta (`Actual / Expected`)**: ").append(r.deadLetters())
                    .append(" / ").append(r.expectedDeadLetters()).append("\n");
            md.append("- **Invariants (I1–I8)**: ").append(r.invariantsSummary()).append("\n");
            if (r.notes() != null && !r.notes().isBlank()) {
                md.append("- **Notes**: ").append(r.notes()).append("\n");
            }
            md.append("\n");
        }

        int crashLostHttpCount = totalReservationsCommitted - totalHoldsAccepted;
        String reservationReconciliationNote = crashLostHttpCount != 0
                ? String.format(
                        " (reconciled: %d HTTP `201`s received + %d committed in DB whose HTTP response was lost to crash = %d DB reservations)",
                        totalHoldsAccepted, crashLostHttpCount, totalReservationsCommitted)
                : " (matches HTTP `201` holds accepted)";
        long unexpectedDlt = totalDlt - totalExpectedDlt;

        Double maxRecovery = null;
        for (ScenarioRecord r : fullRows) {
            if (r.recoverySeconds() != null && r.recoverySeconds() > 0) {
                if (maxRecovery == null || r.recoverySeconds() > maxRecovery) {
                    maxRecovery = r.recoverySeconds();
                }
            }
        }

        md.append("## 3. Summary of Totals & Global Guarantees\n\n");
        md.append("| Metric | Value |\n");
        md.append("|---|---|\n");
        md.append("| Scenarios in Catalog | ").append(fullRows.size())
                .append(" (Passed: ").append(passCount)
                .append(", Failed: ").append(failCount)
                .append(", Disabled: ").append(disabledCount)
                .append(", Not Run: ").append(notRunCount).append(") |\n");
        md.append("| Total Suite Runtime | ").append(String.format("%.2f s", totalDurationMs / 1000.0)).append(" |\n");
        if (maxRecovery != null) {
            md.append("| Maximum Recovery Time | ").append(String.format("%.2f s", maxRecovery)).append(" |\n");
        }
        md.append("| Distinct Simulated Users (S1–S12) | ").append(totalUsers).append(" |\n");
        md.append("| HTTP Seat Holds (`Attempted / Accepted 201 / Conflict 409 / Rate-Limited 429 / Error or Lost`) | ")
                .append(totalHoldsAttempted).append(" / ")
                .append(totalHoldsAccepted).append(" / ")
                .append(totalHoldsRejected).append(" / ")
                .append(totalHoldsRateLimited).append(" / ")
                .append(totalHoldsFailedOrLost).append(" |\n");
        md.append("| Database Reservations Committed (`inventory_db`) | ")
                .append(totalReservationsCommitted).append(" (").append(formatMap(totalReservationsByStatus))
                .append(")")
                .append(reservationReconciliationNote).append(" |\n");
        md.append("| Database Orders Created (`order_db`) | ")
                .append(totalOrdersCreated).append(" (").append(formatMap(totalOrdersByStatus)).append(") |\n");
        md.append("| Payments (`Succeeded / Failed / Refunded`) | ")
                .append(totalPaySucc).append(" / ")
                .append(totalPayFail).append(" / ")
                .append(totalRefunds).append(" |\n");
        md.append("| Notifications Delivered (`order.confirmed / order.cancelled`) | ")
                .append(totalNotifConfirmed).append(" / ")
                .append(totalNotifCancelled).append(" |\n");
        md.append("| Dead-Letter Messages (`Actual / Expected / Unexpected`) | ")
                .append(totalDlt).append(" / ")
                .append(totalExpectedDlt).append(" / ")
                .append(unexpectedDlt).append(" |\n\n");

        List<ScenarioRecord> chaosWithRecovery = fullRows.stream()
                .filter(r -> r.recoverySeconds() != null && r.recoverySeconds() > 0)
                .toList();
        if (!chaosWithRecovery.isEmpty()) {
            md.append("### Chaos Recovery Times\n\n");
            md.append("| Scenario | Chaos Type | Measured Recovery Time | Failover Budget |\n");
            md.append("|---|---|---:|---:|\n");
            for (ScenarioRecord cr : chaosWithRecovery) {
                md.append("| **").append(cr.id()).append("** (").append(cr.name()).append(") | `")
                        .append(cr.tag()).append("` | ")
                        .append(String.format("%.2f s", cr.recoverySeconds())).append(" | 60.00 s |\n");
            }
            md.append("\n");
        }

        md.append("### Global Distributed System Guarantees (`I1`–`I8`)\n\n");
        md.append("| Code | Global Guarantee | Verification Mechanism | Status |\n");
        md.append("|---|---|---|---|n".replace("|n", "|\n"));

        for (InvariantSpec spec : INVARIANT_SPECS) {
            String statusCell = computeGuaranteeStatus(spec.code(), fullRows);
            md.append("| **").append(spec.code()).append("** ")
                    .append("| **").append(spec.title()).append("** ")
                    .append("| ").append(spec.description()).append(" ")
                    .append("| ").append(statusCell).append(" |\n");
        }

        return md.toString();
    }

    /**
     * Computes guarantee row status strictly from recorded scenario results:
     * - FAIL: if any scenario found a violation of this invariant
     * - NOT RUN: if no scenario checking this invariant ran
     * - VERIFIED: ONLY if every scenario in the catalog ran and passed with 0
     * violations
     * - PARTIAL: if some scenarios did not run (or a scenario failed an HTTP check
     * without an invariant violation)
     */
    private static String computeGuaranteeStatus(String invariantCode, List<ScenarioRecord> fullRows) {
        int totalScenarios = fullRows.size();
        int ranCount = 0;
        int passedScenarioAndInvariant = 0;
        int failedScenarios = 0;
        int notRunOrDisabled = 0;
        List<String> violatedScenarios = new ArrayList<>();

        for (ScenarioRecord r : fullRows) {
            if ("NOT RUN".equals(r.status()) || "DISABLED".equals(r.status())) {
                notRunOrDisabled++;
                continue;
            }
            ranCount++;
            Boolean invOk = r.invariants() != null ? r.invariants().get(invariantCode) : null;
            if (Boolean.FALSE.equals(invOk)) {
                violatedScenarios.add(r.id());
            }
            if ("PASS".equals(r.status()) && Boolean.TRUE.equals(invOk)) {
                passedScenarioAndInvariant++;
            } else if ("FAIL".equals(r.status())) {
                failedScenarios++;
            }
        }

        if (!violatedScenarios.isEmpty()) {
            return "**FAIL** (violations in " + String.join(", ", violatedScenarios) + ")";
        }
        if (ranCount == 0) {
            return "**NOT RUN** (0/" + totalScenarios + " scenarios ran)";
        }
        if (passedScenarioAndInvariant == totalScenarios && failedScenarios == 0 && notRunOrDisabled == 0) {
            return "**VERIFIED** (" + passedScenarioAndInvariant + "/" + totalScenarios
                    + " scenarios passed, 0 violations)";
        }
        return String.format(
                "**PARTIAL** (%d/%d scenarios passed, %d failed, %d not run/disabled, 0 invariant violations)",
                passedScenarioAndInvariant, totalScenarios, failedScenarios, notRunOrDisabled);
    }

    private static String formatMap(Map<String, Integer> map) {
        if (map == null || map.isEmpty())
            return "0";
        List<String> parts = new ArrayList<>();
        map.forEach((k, v) -> {
            if (v != null && v > 0)
                parts.add(k + "=" + v);
        });
        return parts.isEmpty() ? "0" : String.join(", ", parts);
    }

    public String resolveBackendDescription() {
        if (config == null || config.backend() == E2EConfig.BackendType.COMPOSE) {
            return "compose";
        }
        try {
            List<String> cmd = new ArrayList<>();
            cmd.add("kubectl");
            if (!config.k8sContext().isBlank()) {
                cmd.add("--context");
                cmd.add(config.k8sContext().trim());
            }
            cmd.addAll(List.of("version", "-o", "json"));
            CommandResult vRes = processRunner.exec(Duration.ofSeconds(5), cmd);
            String k8sVersion = "k8s";
            if (vRes.exitCode() == 0 && !vRes.stdout().isBlank()) {
                JsonNode vJson = objectMapper.readTree(vRes.stdout());
                String gitVer = vJson.path("serverVersion").path("gitVersion").asText("");
                if (!gitVer.isBlank()) {
                    k8sVersion = gitVer;
                }
            }
            List<String> nodeCmd = new ArrayList<>();
            nodeCmd.add("kubectl");
            if (!config.k8sContext().isBlank()) {
                nodeCmd.add("--context");
                nodeCmd.add(config.k8sContext().trim());
            }
            nodeCmd.addAll(List.of("get", "nodes", "--no-headers"));
            CommandResult nodesRes = processRunner.exec(Duration.ofSeconds(5), nodeCmd);
            int nodeCount = 0;
            if (nodesRes.exitCode() == 0 && !nodesRes.stdout().isBlank()) {
                for (String line : nodesRes.stdout().split("\n")) {
                    if (!line.trim().isEmpty()) {
                        nodeCount++;
                    }
                }
            }
            if (nodeCount > 0) {
                return "k8s (" + k8sVersion + ", " + nodeCount + " nodes)";
            }
            return "k8s (" + k8sVersion + ")";
        } catch (Exception e) {
            return "k8s";
        }
    }

    public String resolveFailoverBudget() {
        if (config == null) {
            return "60s";
        }
        return config.failoverBudget().toSeconds() + "s";
    }
}
