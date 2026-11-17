package com.ticketing.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Cross-service Invariant Auditor verifying I1 through I8 across
 * {@code inventory_db},
 * {@code order_db}, {@code payment_db}, Kafka topics/DLTs, and
 * {@code notification-service} metrics.
 */
public final class InvariantAuditor {

    /**
     * Small interface abstracting Kafka topic and DLT inspection so I7 and I8 can
     * be unit-tested.
     */
    public interface KafkaAccess {
        KafkaProbe.DltDiff diffDltOffsets(KafkaProbe.DltSnapshot before);

        List<KafkaProbe.ConsumedEvent> readOrderEventsSince(Map<TopicPartition, Long> startOffsets);
    }

    /**
     * Small interface abstracting {@code notification-service} counter scraping so
     * I8 can be unit-tested.
     */
    public interface NotificationAccess {
        Notifications.NotificationSnapshot snapshot();
    }

    private final Db db;
    private final KafkaAccess kafkaAccess;
    private final NotificationAccess notificationAccess;
    private final ObjectMapper objectMapper;

    public InvariantAuditor(Db db) {
        this(
                db,
                db != null && db.memoryStore() != null ? db.memoryStore().toKafkaAccess() : null,
                db != null && db.memoryStore() != null ? db.memoryStore().toNotificationAccess() : null);
    }

    public InvariantAuditor(Db db, KafkaAccess kafkaAccess, NotificationAccess notificationAccess) {
        this.db = Objects.requireNonNull(db, "db must not be null");
        this.kafkaAccess = kafkaAccess;
        this.notificationAccess = notificationAccess;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public record ScenarioContext(
            UUID eventId,
            Instant scenarioStart,
            KafkaAccess kafkaAccess,
            Map<TopicPartition, Long> orderEventsStartOffsets,
            KafkaProbe.DltSnapshot dltSnapshotBefore,
            long expectedDeadLetters,
            Notifications.NotificationSnapshot notificationsBefore,
            Notifications.NotificationSnapshot notificationsAfter,
            long expectedDuplicateNotifications,
            Set<UUID> allowedRefundPendingOrderIds) {
        public ScenarioContext(
                UUID eventId,
                Instant scenarioStart,
                KafkaAccess kafkaAccess,
                Map<TopicPartition, Long> orderEventsStartOffsets,
                KafkaProbe.DltSnapshot dltSnapshotBefore,
                long expectedDeadLetters,
                Notifications.NotificationSnapshot notificationsBefore,
                Notifications.NotificationSnapshot notificationsAfter) {
            this(
                    eventId,
                    scenarioStart,
                    kafkaAccess,
                    orderEventsStartOffsets,
                    dltSnapshotBefore,
                    expectedDeadLetters,
                    notificationsBefore,
                    notificationsAfter,
                    0L,
                    Set.of());
        }
    }

    public record AuditResult(Map<String, List<String>> violationsByInvariant) {
        public boolean allPassed() {
            return violationsByInvariant.values().stream().allMatch(List::isEmpty);
        }

        public List<String> allViolations() {
            List<String> list = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : violationsByInvariant.entrySet()) {
                for (String v : entry.getValue()) {
                    list.add("[" + entry.getKey() + "] " + v);
                }
            }
            return list;
        }

        public void assertAllPassed() {
            if (!allPassed()) {
                throw new AssertionError("Invariant Auditor detected violations:\n  "
                        + String.join("\n  ", allViolations()));
            }
        }

        public String summaryStatus() {
            if (allPassed()) {
                return "PASS (I1-I" + violationsByInvariant.size() + ")";
            }
            List<String> failed = violationsByInvariant.entrySet().stream()
                    .filter(e -> !e.getValue().isEmpty())
                    .map(Map.Entry::getKey)
                    .toList();
            return "FAIL (" + String.join(",", failed) + ")";
        }
    }

    /**
     * Audits invariants I1 through I8 for a single scenario's {@code eventId} and
     * context.
     */
    public AuditResult auditScenario(ScenarioContext ctx) {
        Objects.requireNonNull(ctx, "ctx must not be null");
        UUID eventId = Objects.requireNonNull(ctx.eventId(), "eventId must not be null");

        List<Db.SeatRow> seats = db.findSeats(eventId);
        List<Db.ReservationRow> reservations = db.findReservations(eventId);
        List<Db.ReservationItemRow> resItems = db.findReservationItems(eventId);
        List<Db.OrderRow> orders = db.findOrders(eventId);

        Set<UUID> aggregateIds = new HashSet<>();
        reservations.forEach(r -> aggregateIds.add(r.reservationId()));
        orders.forEach(o -> aggregateIds.add(o.orderId()));

        List<Db.OrderItemRow> orderItems = db.findOrderItemsByOrderIds(aggregateIds);
        List<Db.PaymentRow> payments = db.findPaymentsByOrderIds(aggregateIds);
        List<Db.OutboxRow> invOutbox = db.findInventoryOutboxByAggregateIds(aggregateIds);
        List<Db.OutboxRow> ordOutbox = db.findOrderOutboxByAggregateIds(aggregateIds);
        List<Db.OutboxRow> payOutbox = db.findPaymentOutboxByAggregateIds(aggregateIds);

        Instant dbNow = db.currentTimestampInInventoryDb();
        KafkaAccess effectiveKafka = ctx.kafkaAccess() != null ? ctx.kafkaAccess() : this.kafkaAccess;
        Notifications.NotificationSnapshot effectiveAfter = ctx.notificationsAfter() != null
                ? ctx.notificationsAfter()
                : (this.notificationAccess != null ? this.notificationAccess.snapshot() : null);

        Map<String, List<String>> results = new LinkedHashMap<>();
        results.put("I1", checkI1NoOversell(seats, resItems, orders, orderItems));
        results.put("I2", checkI2SeatAndReservationConsistency(seats, reservations, resItems, dbNow));
        results.put("I3", checkI3OrderAndPaymentConsistency(
                reservations, resItems, orders, orderItems, payments, ctx.allowedRefundPendingOrderIds()));
        results.put("I4", checkI4NoDuplicateEffects(payments, invOutbox, ordOutbox, payOutbox));
        results.put("I5",
                checkI5Money(seats, reservations, resItems, orders, orderItems, payments, ordOutbox, payOutbox));
        results.put("I6", checkI6OutboxDrained(invOutbox, ordOutbox, payOutbox));
        results.put("I7", checkI7DeadLetters(effectiveKafka, ctx.dltSnapshotBefore(), ctx.expectedDeadLetters()));
        results.put("I8", checkI8Notifications(
                effectiveKafka,
                ctx.orderEventsStartOffsets(),
                ctx.notificationsBefore(),
                effectiveAfter,
                ctx.expectedDuplicateNotifications(),
                orders,
                aggregateIds,
                ordOutbox,
                Set.of()));

        return new AuditResult(Collections.unmodifiableMap(results));
    }

    /**
     * Audits invariants I1 through I8 (for unit tests or single-event checks).
     */
    public AuditResult auditDatabaseOnly(UUID optionalEventId) {
        return auditDatabaseForEvents(optionalEventId != null ? Set.of(optionalEventId) : null);
    }

    /**
     * Audits invariants I1 through I8 across all specified events (or all data if
     * {@code eventIds == null}).
     */
    public AuditResult auditDatabaseForEvents(Set<UUID> eventIds) {
        return auditDatabaseForEvents(eventIds, null);
    }

    /**
     * Audits invariants I1 through I8 across all specified events (or all rows in
     * all databases when {@code eventIds == null})
     * with an explicit set of orders allowed to rest in {@code REFUND_PENDING}.
     */
    public AuditResult auditDatabaseForEvents(Set<UUID> eventIds, Set<UUID> allowedRefundPendingOrders) {
        List<Db.SeatRow> seats;
        List<Db.ReservationRow> reservations;
        List<Db.ReservationItemRow> resItems;
        List<Db.OrderRow> orders;

        if (eventIds == null) {
            seats = db.findSeats(null);
            reservations = db.findReservations(null);
            resItems = db.findReservationItems(null);
            orders = db.findOrders(null);
        } else {
            seats = new ArrayList<>();
            reservations = new ArrayList<>();
            resItems = new ArrayList<>();
            orders = new ArrayList<>();
            for (UUID eventId : eventIds) {
                seats.addAll(db.findSeats(eventId));
                reservations.addAll(db.findReservations(eventId));
                resItems.addAll(db.findReservationItems(eventId));
                orders.addAll(db.findOrders(eventId));
            }
        }

        Collection<UUID> orderIdsFilter;
        Set<UUID> allKnownOrderIds = new HashSet<>();
        reservations.forEach(r -> allKnownOrderIds.add(r.reservationId()));
        orders.forEach(o -> allKnownOrderIds.add(o.orderId()));
        if (db.memoryStore() != null) {
            db.memoryStore().orderItems.forEach(oi -> allKnownOrderIds.add(oi.orderId()));
            db.memoryStore().payments.forEach(p -> allKnownOrderIds.add(p.orderId()));
        }

        if (eventIds == null) {
            orderIdsFilter = null;
        } else {
            orderIdsFilter = allKnownOrderIds;
        }

        List<Db.OrderItemRow> orderItems = db.findOrderItemsByOrderIds(orderIdsFilter);
        List<Db.PaymentRow> payments = db.findPaymentsByOrderIds(orderIdsFilter);
        List<Db.OutboxRow> invOutbox = db.findInventoryOutboxByAggregateIds(orderIdsFilter);
        List<Db.OutboxRow> ordOutbox = db.findOrderOutboxByAggregateIds(orderIdsFilter);
        List<Db.OutboxRow> payOutbox = db.findPaymentOutboxByAggregateIds(orderIdsFilter);

        Instant dbNow = db.currentTimestampInInventoryDb();
        Set<UUID> effectiveAllowedRefundPending = allowedRefundPendingOrders != null
                ? allowedRefundPendingOrders
                : (db.memoryStore() != null ? db.memoryStore().allowedRefundPendingOrderIds : Set.of());

        Map<String, List<String>> results = new LinkedHashMap<>();
        results.put("I1", checkI1NoOversell(seats, resItems, orders, orderItems));
        results.put("I2", checkI2SeatAndReservationConsistency(seats, reservations, resItems, dbNow));
        results.put("I3", checkI3OrderAndPaymentConsistency(
                reservations, resItems, orders, orderItems, payments, effectiveAllowedRefundPending));
        results.put("I4", checkI4NoDuplicateEffects(payments, invOutbox, ordOutbox, payOutbox));
        results.put("I5",
                checkI5Money(seats, reservations, resItems, orders, orderItems, payments, ordOutbox, payOutbox));
        results.put("I6", checkI6OutboxDrained(invOutbox, ordOutbox, payOutbox));

        long expectedDlt = db.memoryStore() != null ? db.memoryStore().expectedDeadLetters : 0L;
        long expectedDupNotifs = db.memoryStore() != null ? db.memoryStore().expectedDuplicateNotifications : 0L;
        Notifications.NotificationSnapshot notifBefore = db.memoryStore() != null
                ? db.memoryStore().notificationsBefore
                : Notifications.NotificationSnapshot.empty();
        Notifications.NotificationSnapshot notifAfter = notificationAccess != null
                ? notificationAccess.snapshot()
                : null;

        results.put("I7", checkI7DeadLetters(
                kafkaAccess, new KafkaProbe.DltSnapshot(Map.of()), expectedDlt));
        results.put("I8", checkI8Notifications(
                kafkaAccess,
                Map.of(),
                notifBefore,
                notifAfter,
                expectedDupNotifs,
                orders,
                allKnownOrderIds,
                ordOutbox,
                Set.of()));

        return new AuditResult(Collections.unmodifiableMap(results));
    }

    /**
     * Global audit across ALL rows of {@code inventory_db}, {@code order_db}, and
     * {@code payment_db},
     * plus global DLT and notification counter checks since suite start.
     */
    public AuditResult auditGlobalAllRows(
            KafkaAccess effectiveKafka,
            Map<TopicPartition, Long> suiteStartOrderOffsets,
            KafkaProbe.DltSnapshot suiteStartDltSnapshot,
            long expectedDeadLettersInRun,
            Notifications.NotificationSnapshot suiteStartNotifications,
            Notifications.NotificationSnapshot suiteEndNotifications,
            long expectedDuplicateNotificationsInRun,
            Set<UUID> allowedRefundPendingOrderIds,
            Set<UUID> baselineOrderOutboxEventIds) {

        List<Db.SeatRow> seats = db.findSeats(null);
        List<Db.ReservationRow> reservations = db.findReservations(null);
        List<Db.ReservationItemRow> resItems = db.findReservationItems(null);
        List<Db.OrderRow> orders = db.findOrders(null);
        List<Db.OrderItemRow> orderItems = db.findOrderItemsByOrderIds(null);
        List<Db.PaymentRow> payments = db.findPaymentsByOrderIds(null);
        List<Db.OutboxRow> invOutbox = db.findInventoryOutboxByAggregateIds(null);
        List<Db.OutboxRow> ordOutbox = db.findOrderOutboxByAggregateIds(null);
        List<Db.OutboxRow> payOutbox = db.findPaymentOutboxByAggregateIds(null);

        Set<UUID> allOrderIds = new HashSet<>();
        reservations.forEach(r -> allOrderIds.add(r.reservationId()));
        orders.forEach(o -> allOrderIds.add(o.orderId()));

        Instant dbNow = db.currentTimestampInInventoryDb();

        Map<String, List<String>> results = new LinkedHashMap<>();
        results.put("I1", checkI1NoOversell(seats, resItems, orders, orderItems));
        results.put("I2", checkI2SeatAndReservationConsistency(seats, reservations, resItems, dbNow));
        results.put("I3", checkI3OrderAndPaymentConsistency(
                reservations, resItems, orders, orderItems, payments, allowedRefundPendingOrderIds));
        results.put("I4", checkI4NoDuplicateEffects(payments, invOutbox, ordOutbox, payOutbox));
        results.put("I5",
                checkI5Money(seats, reservations, resItems, orders, orderItems, payments, ordOutbox, payOutbox));
        results.put("I6", checkI6OutboxDrained(invOutbox, ordOutbox, payOutbox));
        results.put("I7", checkI7DeadLetters(effectiveKafka, suiteStartDltSnapshot, expectedDeadLettersInRun));
        results.put("I8", checkI8Notifications(
                effectiveKafka,
                suiteStartOrderOffsets,
                suiteStartNotifications,
                suiteEndNotifications,
                expectedDuplicateNotificationsInRun,
                orders,
                allOrderIds,
                ordOutbox,
                baselineOrderOutboxEventIds != null ? baselineOrderOutboxEventIds : Set.of()));

        return new AuditResult(Collections.unmodifiableMap(results));
    }

    /**
     * I1 no oversell:
     * - For every (event, seat) at most one reservation item in HELD or CONFIRMED
     * - In order_db: at most one CONFIRMED order_item per seat
     * - Seats in SOLD equals seats with a CONFIRMED reservation_item AND equals
     * seats of CONFIRMED orders,
     * and never exceeds the seat count of the event
     */
    private List<String> checkI1NoOversell(
            List<Db.SeatRow> seats,
            List<Db.ReservationItemRow> resItems,
            List<Db.OrderRow> orders,
            List<Db.OrderItemRow> orderItems) {
        List<String> violations = new ArrayList<>();

        Map<String, List<Db.ReservationItemRow>> activeByEventSeat = new LinkedHashMap<>();
        for (Db.ReservationItemRow item : resItems) {
            if ("HELD".equals(item.status()) || "CONFIRMED".equals(item.status())) {
                String key = item.eventId() + ":" + item.seatId();
                activeByEventSeat.computeIfAbsent(key, k -> new ArrayList<>()).add(item);
            }
        }
        for (Map.Entry<String, List<Db.ReservationItemRow>> entry : activeByEventSeat.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("Multiple active (HELD/CONFIRMED) reservation_items for (event:seat)="
                        + entry.getKey() + ": " + entry.getValue());
            }
        }

        // Check order_db order_items: at most one CONFIRMED order per seat
        Map<UUID, Db.OrderRow> ordersById = orders.stream()
                .collect(Collectors.toMap(Db.OrderRow::orderId, Function.identity(), (a, b) -> a));

        Map<UUID, List<Db.OrderItemRow>> confirmedOrderItemsBySeat = new LinkedHashMap<>();
        Map<UUID, Set<UUID>> confirmedOrderSeatIdsByEvent = new HashMap<>();

        for (Db.OrderItemRow oi : orderItems) {
            Db.OrderRow parentOrder = ordersById.get(oi.orderId());
            if (parentOrder != null && "CONFIRMED".equals(parentOrder.status())) {
                confirmedOrderItemsBySeat.computeIfAbsent(oi.seatId(), k -> new ArrayList<>()).add(oi);
                confirmedOrderSeatIdsByEvent
                        .computeIfAbsent(parentOrder.eventId(), k -> new HashSet<>())
                        .add(oi.seatId());
            }
        }

        for (Map.Entry<UUID, List<Db.OrderItemRow>> entry : confirmedOrderItemsBySeat.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("Multiple CONFIRMED order_items for seatId="
                        + entry.getKey() + ": " + entry.getValue());
            }
        }

        Map<UUID, List<Db.SeatRow>> seatsByEvent = seats.stream()
                .collect(Collectors.groupingBy(Db.SeatRow::eventId));
        Map<UUID, List<Db.ReservationItemRow>> itemsByEvent = resItems.stream()
                .collect(Collectors.groupingBy(Db.ReservationItemRow::eventId));

        Set<UUID> allEventIds = new HashSet<>();
        allEventIds.addAll(seatsByEvent.keySet());
        allEventIds.addAll(itemsByEvent.keySet());
        orders.forEach(o -> allEventIds.add(o.eventId()));

        for (UUID eventId : allEventIds) {
            List<Db.SeatRow> eventSeats = seatsByEvent.getOrDefault(eventId, List.of());
            List<Db.ReservationItemRow> eventItems = itemsByEvent.getOrDefault(eventId, List.of());

            Set<UUID> soldSeatIds = eventSeats.stream()
                    .filter(s -> "SOLD".equals(s.status()))
                    .map(Db.SeatRow::seatId)
                    .collect(Collectors.toSet());

            Set<UUID> confirmedItemSeatIds = eventItems.stream()
                    .filter(i -> "CONFIRMED".equals(i.status()))
                    .map(Db.ReservationItemRow::seatId)
                    .collect(Collectors.toSet());

            Set<UUID> confirmedOrderSeatIds = confirmedOrderSeatIdsByEvent.getOrDefault(eventId, Set.of());

            if (!soldSeatIds.equals(confirmedItemSeatIds)) {
                violations.add(String.format(
                        "Event %s SOLD seats mismatch CONFIRMED reservation_items: soldSeats=%s vs confirmedItems=%s",
                        eventId, soldSeatIds, confirmedItemSeatIds));
            }

            if (!soldSeatIds.equals(confirmedOrderSeatIds)) {
                violations.add(String.format(
                        "Event %s SOLD seats mismatch CONFIRMED order_items: soldSeats=%s vs confirmedOrderSeats=%s",
                        eventId, soldSeatIds, confirmedOrderSeatIds));
            }

            if (soldSeatIds.size() > eventSeats.size()) {
                violations.add(String.format(
                        "Event %s SOLD seats (%d) exceeds total event seats (%d)",
                        eventId, soldSeatIds.size(), eventSeats.size()));
            }
        }

        return violations;
    }

    /**
     * I2 seat and reservation consistency:
     * - A HELD seat has exactly one HELD reservation item whose reservation is HELD
     * - A SOLD seat has a CONFIRMED item (whose reservation is CONFIRMED)
     * - An AVAILABLE seat has no active item (HELD or CONFIRMED)
     * - No HELD reservation is older than its expires_at plus 90 seconds once
     * quiescent
     */
    private List<String> checkI2SeatAndReservationConsistency(
            List<Db.SeatRow> seats,
            List<Db.ReservationRow> reservations,
            List<Db.ReservationItemRow> resItems,
            Instant dbNow) {
        List<String> violations = new ArrayList<>();

        Map<UUID, Db.ReservationRow> resById = reservations.stream()
                .collect(Collectors.toMap(Db.ReservationRow::reservationId, Function.identity(), (a, b) -> a));

        Map<UUID, List<Db.ReservationItemRow>> itemsBySeatId = resItems.stream()
                .collect(Collectors.groupingBy(Db.ReservationItemRow::seatId));

        for (Db.SeatRow seat : seats) {
            List<Db.ReservationItemRow> seatItems = itemsBySeatId.getOrDefault(seat.seatId(), List.of());
            List<Db.ReservationItemRow> heldItems = seatItems.stream()
                    .filter(i -> "HELD".equals(i.status()))
                    .toList();
            List<Db.ReservationItemRow> confirmedItems = seatItems.stream()
                    .filter(i -> "CONFIRMED".equals(i.status()))
                    .toList();

            switch (seat.status()) {
                case "HELD" -> {
                    if (heldItems.size() != 1 || !confirmedItems.isEmpty()) {
                        violations.add(String.format(
                                "HELD seat %s must have exactly 1 HELD item and 0 CONFIRMED items (found held=%s, confirmed=%s)",
                                seat, heldItems, confirmedItems));
                    } else {
                        Db.ReservationItemRow item = heldItems.get(0);
                        Db.ReservationRow parent = resById.get(item.reservationId());
                        if (parent == null || !"HELD".equals(parent.status())) {
                            violations.add(String.format(
                                    "HELD seat %s has HELD item %s whose parent reservation is %s",
                                    seat, item, parent));
                        }
                    }
                }
                case "SOLD" -> {
                    if (confirmedItems.size() != 1 || !heldItems.isEmpty()) {
                        violations.add(String.format(
                                "SOLD seat %s must have exactly 1 CONFIRMED item and 0 HELD items (found confirmed=%s, held=%s)",
                                seat, confirmedItems, heldItems));
                    } else {
                        Db.ReservationItemRow item = confirmedItems.get(0);
                        Db.ReservationRow parent = resById.get(item.reservationId());
                        if (parent == null || !"CONFIRMED".equals(parent.status())) {
                            violations.add(String.format(
                                    "SOLD seat %s has CONFIRMED item %s whose parent reservation is %s",
                                    seat, item, parent));
                        }
                    }
                }
                case "AVAILABLE" -> {
                    if (!heldItems.isEmpty() || !confirmedItems.isEmpty()) {
                        violations.add(String.format(
                                "AVAILABLE seat %s has active reservation items: held=%s, confirmed=%s",
                                seat, heldItems, confirmedItems));
                    }
                }
                default -> violations.add("Unexpected seat status on seat: " + seat);
            }
        }

        // Also check orphan active reservation_items whose seat is missing or not
        // matching
        Map<UUID, Db.SeatRow> seatById = seats.stream()
                .collect(Collectors.toMap(Db.SeatRow::seatId, Function.identity(), (a, b) -> a));
        for (Db.ReservationItemRow item : resItems) {
            Db.ReservationRow parent = resById.get(item.reservationId());
            if (parent == null) {
                violations.add("Reservation item has missing parent reservation: " + item);
            } else if (!Objects.equals(item.status(), parent.status())) {
                violations.add(String.format(
                        "Reservation item status (%s) mismatches parent reservation status (%s): item=%s, reservation=%s",
                        item.status(), parent.status(), item, parent));
            }
            Db.SeatRow seat = seatById.get(item.seatId());
            if ("HELD".equals(item.status()) && (seat == null || !"HELD".equals(seat.status()))) {
                violations.add(String.format("HELD reservation item %s points to seat %s", item, seat));
            }
            if ("CONFIRMED".equals(item.status()) && (seat == null || !"SOLD".equals(seat.status()))) {
                violations.add(String.format("CONFIRMED reservation item %s points to seat %s", item, seat));
            }
        }

        // Check no HELD reservation is older than expires_at + 90 seconds
        for (Db.ReservationRow res : reservations) {
            if ("HELD".equals(res.status()) && res.expiresAt() != null && dbNow != null) {
                Instant deadline = res.expiresAt().plus(Duration.ofSeconds(90));
                if (dbNow.isAfter(deadline)) {
                    violations.add(String.format(
                            "HELD reservation %s is older than expires_at + 90s (expiresAt=%s, dbNow=%s)",
                            res, res.expiresAt(), dbNow));
                }
            }
        }

        return violations;
    }

    /**
     * I3 order and payment consistency (both directions + seat sets + non-terminal
     * statuses):
     * - At most one payment per order
     * - An order CONFIRMED has a SUCCEEDED payment and a CONFIRMED reservation
     * - Reverse: Every CONFIRMED reservation has a CONFIRMED order (and SUCCEEDED
     * payment)
     * - Reverse: Every SUCCEEDED payment belongs to a CONFIRMED order (or a
     * declared REFUND_PENDING order)
     * - A payment REFUNDED if and only if the order is CANCELLED_REFUNDED
     * - No SUCCEEDED payment belongs to an order in CANCELLED_EXPIRED or
     * CANCELLED_PAYMENT_FAILED once quiescent
     * - No order is left in PAYMENT_PENDING or REFUND_PENDING after quiescence
     * unless the scenario declares it
     * - Order item seat sets equal reservation item seat sets for every
     * order/reservation pair
     */
    private List<String> checkI3OrderAndPaymentConsistency(
            List<Db.ReservationRow> reservations,
            List<Db.ReservationItemRow> resItems,
            List<Db.OrderRow> orders,
            List<Db.OrderItemRow> orderItems,
            List<Db.PaymentRow> payments,
            Set<UUID> allowedRefundPendingOrderIds) {
        List<String> violations = new ArrayList<>();
        Set<UUID> allowedRefundPending = allowedRefundPendingOrderIds != null ? allowedRefundPendingOrderIds : Set.of();

        Map<UUID, Db.ReservationRow> resById = reservations.stream()
                .collect(Collectors.toMap(Db.ReservationRow::reservationId, Function.identity(), (a, b) -> a));
        Map<UUID, Db.OrderRow> ordersById = orders.stream()
                .collect(Collectors.toMap(Db.OrderRow::orderId, Function.identity(), (a, b) -> a));
        Map<UUID, List<Db.ReservationItemRow>> resItemsByResId = resItems.stream()
                .collect(Collectors.groupingBy(Db.ReservationItemRow::reservationId));
        Map<UUID, List<Db.OrderItemRow>> orderItemsByOrderId = orderItems.stream()
                .collect(Collectors.groupingBy(Db.OrderItemRow::orderId));

        Map<UUID, List<Db.PaymentRow>> paymentsByOrder = new LinkedHashMap<>();
        for (Db.PaymentRow p : payments) {
            paymentsByOrder.computeIfAbsent(p.orderId(), k -> new ArrayList<>()).add(p);
        }

        for (Map.Entry<UUID, List<Db.PaymentRow>> entry : paymentsByOrder.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("Multiple payment rows for orderId=" + entry.getKey() + ": " + entry.getValue());
            }
        }

        // Reverse check on reservations: every reservation has an order, and every
        // CONFIRMED reservation has a CONFIRMED order
        for (Db.ReservationRow res : reservations) {
            Db.OrderRow order = ordersById.get(res.reservationId());
            if (order == null) {
                violations.add(String.format(
                        "Reservation %s (status=%s) has no corresponding order in order_db",
                        res.reservationId(), res.status()));
                continue;
            }
            if ("CONFIRMED".equals(res.status()) && !"CONFIRMED".equals(order.status())) {
                violations.add(String.format(
                        "CONFIRMED reservation %s must have CONFIRMED order, found: %s",
                        res, order));
            }
        }

        for (Db.OrderRow order : orders) {
            UUID orderId = order.orderId();
            List<Db.PaymentRow> orderPayments = paymentsByOrder.getOrDefault(orderId, List.of());
            Db.PaymentRow payment = orderPayments.isEmpty() ? null : orderPayments.get(0);
            Db.ReservationRow res = resById.get(order.reservationId());

            if (res == null) {
                violations.add(String.format(
                        "Order %s references missing reservation %s",
                        orderId, order.reservationId()));
            } else {
                // Compare order item seat sets with reservation item seat sets
                Set<UUID> orderSeatSet = orderItemsByOrderId.getOrDefault(orderId, List.of()).stream()
                        .map(Db.OrderItemRow::seatId)
                        .collect(Collectors.toSet());
                Set<UUID> resSeatSet = resItemsByResId.getOrDefault(res.reservationId(), List.of()).stream()
                        .map(Db.ReservationItemRow::seatId)
                        .collect(Collectors.toSet());
                if (orderSeatSet.isEmpty() || !orderSeatSet.equals(resSeatSet)) {
                    violations.add(String.format(
                            "Order %s order_items seat set (%s) != reservation %s reservation_items seat set (%s)",
                            orderId, orderSeatSet, res.reservationId(), resSeatSet));
                }
            }

            if ("PAYMENT_PENDING".equals(order.status())) {
                violations.add(String.format(
                        "Order %s is left in non-terminal status PAYMENT_PENDING after quiescence",
                        order));
            }

            if ("REFUND_PENDING".equals(order.status())) {
                if (!allowedRefundPending.contains(orderId)) {
                    violations.add(String.format(
                            "Order %s is left in REFUND_PENDING after quiescence without being declared by the scenario",
                            order));
                } else {
                    if (payment == null || !"SUCCEEDED".equals(payment.status())) {
                        violations.add(String.format(
                                "Declared REFUND_PENDING order %s must have SUCCEEDED payment, found: %s",
                                order, payment));
                    }
                    if (res == null || (!"EXPIRED".equals(res.status()) && !"RELEASED".equals(res.status()))) {
                        violations.add(String.format(
                                "Declared REFUND_PENDING order %s must have EXPIRED or RELEASED reservation, found: %s",
                                order, res));
                    }
                }
            }

            if ("CONFIRMED".equals(order.status())) {
                if (payment == null || !"SUCCEEDED".equals(payment.status())) {
                    violations.add(String.format(
                            "CONFIRMED order %s must have SUCCEEDED payment, found: %s",
                            order, payment));
                }
                if (res == null || !"CONFIRMED".equals(res.status())) {
                    violations.add(String.format(
                            "CONFIRMED order %s must have CONFIRMED reservation, found: %s",
                            order, res));
                }
            }

            if ("CANCELLED_REFUNDED".equals(order.status())) {
                if (payment == null || !"REFUNDED".equals(payment.status())) {
                    violations.add(String.format(
                            "CANCELLED_REFUNDED order %s must have REFUNDED payment, found: %s",
                            order, payment));
                }
            }

            if ("CANCELLED_EXPIRED".equals(order.status()) || "CANCELLED_PAYMENT_FAILED".equals(order.status())) {
                if (payment != null && "SUCCEEDED".equals(payment.status())) {
                    violations.add(String.format(
                            "Order %s in %s has an uncompensated SUCCEEDED payment %s",
                            order, order.status(), payment));
                }
            }
        }

        for (Db.PaymentRow payment : payments) {
            Db.OrderRow order = ordersById.get(payment.orderId());
            if ("REFUNDED".equals(payment.status())) {
                if (order == null || !"CANCELLED_REFUNDED".equals(order.status())) {
                    violations.add(String.format(
                            "REFUNDED payment %s requires order in CANCELLED_REFUNDED, found: %s",
                            payment, order));
                }
            } else if ("SUCCEEDED".equals(payment.status())) {
                boolean allowedRestingRefund = order != null
                        && "REFUND_PENDING".equals(order.status())
                        && allowedRefundPending.contains(order.orderId());
                if (order == null || (!"CONFIRMED".equals(order.status()) && !allowedRestingRefund)) {
                    violations.add(String.format(
                            "SUCCEEDED payment %s requires CONFIRMED order (or declared REFUND_PENDING order), found: %s",
                            payment, order));
                }
            } else if ("FAILED".equals(payment.status())) {
                if (order == null || (!"CANCELLED_PAYMENT_FAILED".equals(order.status())
                        && !"CANCELLED_EXPIRED".equals(order.status()))) {
                    violations.add(String.format(
                            "FAILED payment %s requires cancelled order, found: %s",
                            payment, order));
                }
            }
        }

        return violations;
    }

    /**
     * I4 no duplicate effects:
     * - Payments table: at most 1 row per orderId, unique providerReference, unique
     * refundId
     * - Payment outbox counts match payments table rows (no duplicated provider
     * charge or refund effects)
     * - Per order at most one outbox row each of:
     * order.payment.requested, order.confirmed, order.refund.requested,
     * payment.succeeded, payment.failed, payment.refunded
     * - Per reservation at most one outbox row each of:
     * inventory.seat.reserved, inventory.reservation.expired,
     * inventory.seat.confirmed, inventory.seat.released,
     * inventory.seat.confirmation_rejected
     */
    private List<String> checkI4NoDuplicateEffects(
            List<Db.PaymentRow> payments,
            List<Db.OutboxRow> invOutbox,
            List<Db.OutboxRow> ordOutbox,
            List<Db.OutboxRow> payOutbox) {
        List<String> violations = new ArrayList<>();

        // 1. Verify payments table: one row per order, unique providerReference, unique
        // refundId
        Map<UUID, List<Db.PaymentRow>> paymentsByOrder = new LinkedHashMap<>();
        Map<String, List<Db.PaymentRow>> paymentsByProviderRef = new LinkedHashMap<>();
        Map<UUID, List<Db.PaymentRow>> paymentsByRefundId = new LinkedHashMap<>();

        for (Db.PaymentRow p : payments) {
            paymentsByOrder.computeIfAbsent(p.orderId(), k -> new ArrayList<>()).add(p);
            if (p.providerReference() != null && !p.providerReference().isBlank()) {
                paymentsByProviderRef.computeIfAbsent(p.providerReference(), k -> new ArrayList<>()).add(p);
            }
            if (p.refundId() != null) {
                paymentsByRefundId.computeIfAbsent(p.refundId(), k -> new ArrayList<>()).add(p);
            }
        }

        for (Map.Entry<UUID, List<Db.PaymentRow>> entry : paymentsByOrder.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("Duplicate payment rows in payment_db for orderId="
                        + entry.getKey() + ": count=" + entry.getValue().size() + " rows=" + entry.getValue());
            }
        }
        for (Map.Entry<String, List<Db.PaymentRow>> entry : paymentsByProviderRef.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("Duplicate providerReference in payment_db: ref="
                        + entry.getKey() + " rows=" + entry.getValue());
            }
        }
        for (Map.Entry<UUID, List<Db.PaymentRow>> entry : paymentsByRefundId.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("Duplicate refundId in payment_db: refundId="
                        + entry.getKey() + " rows=" + entry.getValue());
            }
        }

        // 2. Verify outbox counts per order and per reservation
        Set<String> singlePerOrderTypes = Set.of(
                "order.payment.requested",
                "order.confirmed",
                "order.refund.requested",
                "payment.succeeded",
                "payment.failed",
                "payment.refunded");
        Set<String> singlePerReservationTypes = Set.of(
                "inventory.seat.reserved",
                "inventory.reservation.expired",
                "inventory.seat.confirmed",
                "inventory.seat.released",
                "inventory.seat.confirmation_rejected");

        List<Db.OutboxRow> orderAndPaymentOutbox = new ArrayList<>();
        orderAndPaymentOutbox.addAll(ordOutbox);
        orderAndPaymentOutbox.addAll(payOutbox);

        Map<String, List<Db.OutboxRow>> byOrderAndType = new LinkedHashMap<>();
        for (Db.OutboxRow row : orderAndPaymentOutbox) {
            if (singlePerOrderTypes.contains(row.eventType())) {
                String key = row.aggregateId() + "|" + row.eventType();
                byOrderAndType.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }
        for (Map.Entry<String, List<Db.OutboxRow>> entry : byOrderAndType.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("Duplicate outbox rows for (orderId|eventType)="
                        + entry.getKey() + ": count=" + entry.getValue().size() + " rows=" + entry.getValue());
            }
        }

        Map<String, List<Db.OutboxRow>> byResAndType = new LinkedHashMap<>();
        for (Db.OutboxRow row : invOutbox) {
            if (singlePerReservationTypes.contains(row.eventType())) {
                String key = row.aggregateId() + "|" + row.eventType();
                byResAndType.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }
        for (Map.Entry<String, List<Db.OutboxRow>> entry : byResAndType.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("Duplicate outbox rows for (reservationId|eventType)="
                        + entry.getKey() + ": count=" + entry.getValue().size() + " rows=" + entry.getValue());
            }
        }

        // 3. Cross-check payment_db outbox rows with payments table
        Map<UUID, List<Db.OutboxRow>> payOutboxByOrder = payOutbox.stream()
                .collect(Collectors.groupingBy(Db.OutboxRow::aggregateId));
        for (Db.OutboxRow row : payOutbox) {
            if (!paymentsByOrder.containsKey(row.aggregateId())) {
                violations.add("Orphan payment outbox row without payment record in payment_db: " + row);
            }
        }
        for (Db.PaymentRow payment : payments) {
            List<Db.OutboxRow> rows = payOutboxByOrder.getOrDefault(payment.orderId(), List.of());
            long succeededEvents = rows.stream().filter(r -> "payment.succeeded".equals(r.eventType())).count();
            long failedEvents = rows.stream().filter(r -> "payment.failed".equals(r.eventType())).count();
            if ("SUCCEEDED".equals(payment.status()) || "REFUNDED".equals(payment.status())) {
                if (succeededEvents > 1) {
                    violations.add("Payment " + payment.paymentId() + " for order " + payment.orderId()
                            + " has " + succeededEvents + " payment.succeeded outbox rows");
                }
                if (failedEvents > 0) {
                    violations.add("Payment " + payment.paymentId() + " (" + payment.status() + ") for order "
                            + payment.orderId() + " has unexpected payment.failed outbox rows: " + failedEvents);
                }
            } else if ("FAILED".equals(payment.status())) {
                if (failedEvents > 1) {
                    violations.add("Payment " + payment.paymentId() + " for order " + payment.orderId()
                            + " has " + failedEvents + " payment.failed outbox rows");
                }
                if (succeededEvents > 0) {
                    violations.add("Payment " + payment.paymentId() + " (FAILED) for order "
                            + payment.orderId() + " has unexpected payment.succeeded outbox rows: " + succeededEvents);
                }
            }
        }

        return violations;
    }

    /**
     * I5 money:
     * - Fails (never skips) when a parent row (seat, reservation, order, payment)
     * is missing
     * - Payment amount equals order total equals the sum of its items equals the
     * reservation total
     * - Fails when a REFUNDED payment has no payment.refunded outbox row
     * - Fails when any outbox JSON payload cannot be parsed or is missing monetary
     * fields
     * - Refund amount equals the charge
     * - Currency equal everywhere
     */
    private List<String> checkI5Money(
            List<Db.SeatRow> seats,
            List<Db.ReservationRow> reservations,
            List<Db.ReservationItemRow> resItems,
            List<Db.OrderRow> orders,
            List<Db.OrderItemRow> orderItems,
            List<Db.PaymentRow> payments,
            List<Db.OutboxRow> ordOutbox,
            List<Db.OutboxRow> payOutbox) {
        List<String> violations = new ArrayList<>();

        Map<UUID, Db.SeatRow> seatById = seats.stream()
                .collect(Collectors.toMap(Db.SeatRow::seatId, Function.identity(), (a, b) -> a));
        Map<UUID, Db.ReservationRow> resById = reservations.stream()
                .collect(Collectors.toMap(Db.ReservationRow::reservationId, Function.identity(), (a, b) -> a));
        Map<UUID, List<Db.ReservationItemRow>> resItemsByResId = resItems.stream()
                .collect(Collectors.groupingBy(Db.ReservationItemRow::reservationId));
        Map<UUID, Db.OrderRow> ordersById = orders.stream()
                .collect(Collectors.toMap(Db.OrderRow::orderId, Function.identity(), (a, b) -> a));
        Map<UUID, List<Db.OrderItemRow>> orderItemsByOrderId = orderItems.stream()
                .collect(Collectors.groupingBy(Db.OrderItemRow::orderId));

        // Check orphan reservation_items (missing parent reservation or missing seat)
        for (Db.ReservationItemRow item : resItems) {
            if (!resById.containsKey(item.reservationId())) {
                violations.add(String.format(
                        "Reservation item %s references missing parent reservation %s",
                        item.reservationItemId(), item.reservationId()));
            }
            if (!seatById.containsKey(item.seatId())) {
                violations.add(String.format(
                        "Reservation item %s references missing seat %s",
                        item.reservationItemId(), item.seatId()));
            }
        }

        // Check orphan order_items (missing parent order or missing seat)
        for (Db.OrderItemRow item : orderItems) {
            if (!ordersById.containsKey(item.orderId())) {
                violations.add(String.format(
                        "Order item %s references missing parent order %s",
                        item.orderItemId(), item.orderId()));
            }
            Db.SeatRow seat = seatById.get(item.seatId());
            if (seat == null) {
                violations.add(String.format(
                        "Order item %s references missing seat %s",
                        item.orderItemId(), item.seatId()));
            } else if (item.priceCents() != seat.priceCents()) {
                violations.add(String.format(
                        "Order item %s priceCents (%d) != seat priceCents (%d)",
                        item, item.priceCents(), seat.priceCents()));
            }
        }

        for (Db.ReservationRow res : reservations) {
            List<Db.ReservationItemRow> items = resItemsByResId.getOrDefault(res.reservationId(), List.of());
            if (items.isEmpty()) {
                violations.add(String.format(
                        "Reservation %s has 0 reservation_items",
                        res.reservationId()));
            }
            long sumItems = items.stream().mapToLong(Db.ReservationItemRow::priceCents).sum();
            if (res.totalAmountCents() != sumItems) {
                violations.add(String.format(
                        "Reservation %s totalAmountCents (%d) != sum of reservation_items (%d): items=%s",
                        res.reservationId(), res.totalAmountCents(), sumItems, items));
            }
            for (Db.ReservationItemRow item : items) {
                Db.SeatRow seat = seatById.get(item.seatId());
                if (seat != null) {
                    if (item.priceCents() != seat.priceCents()) {
                        violations.add(String.format(
                                "Reservation item %s priceCents (%d) != seat priceCents (%d)",
                                item, item.priceCents(), seat.priceCents()));
                    }
                    if (!Objects.equals(res.currency(), seat.currency())) {
                        violations.add(String.format(
                                "Reservation %s currency (%s) != seat %s currency (%s)",
                                res.reservationId(), res.currency(), seat.seatId(), seat.currency()));
                    }
                }
            }
        }

        for (Db.OrderRow order : orders) {
            List<Db.OrderItemRow> items = orderItemsByOrderId.getOrDefault(order.orderId(), List.of());
            if (items.isEmpty()) {
                violations.add(String.format(
                        "Order %s has 0 order_items",
                        order.orderId()));
            }
            long sumItems = items.stream().mapToLong(Db.OrderItemRow::priceCents).sum();
            if (order.totalAmountCents() != sumItems) {
                violations.add(String.format(
                        "Order %s totalAmountCents (%d) != sum of order_items (%d): items=%s",
                        order.orderId(), order.totalAmountCents(), sumItems, items));
            }
            Db.ReservationRow res = resById.get(order.reservationId());
            if (res == null) {
                violations.add(String.format(
                        "Order %s references missing parent reservation %s",
                        order.orderId(), order.reservationId()));
            } else {
                if (order.totalAmountCents() != res.totalAmountCents()) {
                    violations.add(String.format(
                            "Order %s totalAmountCents (%d) != reservation totalAmountCents (%d)",
                            order.orderId(), order.totalAmountCents(), res.totalAmountCents()));
                }
                if (!Objects.equals(order.currency(), res.currency())) {
                    violations.add(String.format(
                            "Order %s currency (%s) != reservation currency (%s)",
                            order.orderId(), order.currency(), res.currency()));
                }
            }
        }

        Map<UUID, Db.PaymentRow> paymentByOrderId = new HashMap<>();
        for (Db.PaymentRow payment : payments) {
            paymentByOrderId.put(payment.orderId(), payment);
            Db.OrderRow order = ordersById.get(payment.orderId());
            if (order == null) {
                violations.add(String.format(
                        "Payment %s references missing parent order %s",
                        payment.paymentId(), payment.orderId()));
            } else {
                if (payment.amountCents() != order.totalAmountCents()) {
                    violations.add(String.format(
                            "Payment %s amountCents (%d) != order %s totalAmountCents (%d)",
                            payment.paymentId(), payment.amountCents(), order.orderId(), order.totalAmountCents()));
                }
                if (!Objects.equals(payment.currency(), order.currency())) {
                    violations.add(String.format(
                            "Payment %s currency (%s) != order %s currency (%s)",
                            payment.paymentId(), payment.currency(), order.orderId(), order.currency()));
                }
            }
            Db.ReservationRow res = resById.get(payment.reservationId());
            if (res == null) {
                violations.add(String.format(
                        "Payment %s references missing parent reservation %s",
                        payment.paymentId(), payment.reservationId()));
            } else {
                if (payment.amountCents() != res.totalAmountCents()) {
                    violations.add(String.format(
                            "Payment %s amountCents (%d) != reservation %s totalAmountCents (%d)",
                            payment.paymentId(), payment.amountCents(), res.reservationId(), res.totalAmountCents()));
                }
                if (!Objects.equals(payment.currency(), res.currency())) {
                    violations.add(String.format(
                            "Payment %s currency (%s) != reservation %s currency (%s)",
                            payment.paymentId(), payment.currency(), res.reservationId(), res.currency()));
                }
            }
        }

        // Verify every REFUNDED payment has a payment.refunded outbox row in payment_db
        Map<UUID, List<Db.OutboxRow>> refundedOutboxByOrder = payOutbox.stream()
                .filter(r -> "payment.refunded".equals(r.eventType()))
                .collect(Collectors.groupingBy(Db.OutboxRow::aggregateId));

        for (Db.PaymentRow payment : payments) {
            if ("REFUNDED".equals(payment.status())) {
                List<Db.OutboxRow> refRows = refundedOutboxByOrder.getOrDefault(payment.orderId(), List.of());
                if (refRows.isEmpty()) {
                    violations.add(String.format(
                            "REFUNDED payment %s (orderId=%s) has no payment.refunded outbox row in payment_db",
                            payment.paymentId(), payment.orderId()));
                }
            }
        }

        // Verify JSON payloads on all order and payment outbox rows (fail on malformed
        // JSON or missing parent)
        List<Db.OutboxRow> allMonetaryOutboxes = new ArrayList<>();
        allMonetaryOutboxes.addAll(ordOutbox);
        allMonetaryOutboxes.addAll(payOutbox);

        for (Db.OutboxRow outbox : allMonetaryOutboxes) {
            if (outbox.payloadJson() == null || outbox.payloadJson().isBlank()) {
                violations.add(String.format(
                        "Outbox event %s (%s) in %s has null or blank JSON payload",
                        outbox.eventId(), outbox.eventType(), outbox.dbName()));
                continue;
            }
            final JsonNode root;
            try {
                root = objectMapper.readTree(outbox.payloadJson());
                if (root == null || !root.isObject()) {
                    violations.add(String.format(
                            "Outbox event %s (%s) in %s JSON payload is not a JSON object: %s",
                            outbox.eventId(), outbox.eventType(), outbox.dbName(), outbox.payloadJson()));
                    continue;
                }
            } catch (Exception ex) {
                violations.add(String.format(
                        "Failed to parse JSON payload for outbox event %s (%s) in %s: %s (raw=%s)",
                        outbox.eventId(), outbox.eventType(), outbox.dbName(), ex.getMessage(), outbox.payloadJson()));
                continue;
            }

            JsonNode payloadNode = root.has("payload") && root.get("payload").isObject() ? root.get("payload") : root;

            if ("order.refund.requested".equals(outbox.eventType()) || "payment.refunded".equals(outbox.eventType())) {
                Db.PaymentRow payment = paymentByOrderId.get(outbox.aggregateId());
                if (payment == null) {
                    violations.add(String.format(
                            "Refund event %s (%s) references missing payment for order %s",
                            outbox.eventId(), outbox.eventType(), outbox.aggregateId()));
                    continue;
                }
                if (!payloadNode.hasNonNull("amountCents") || !payloadNode.get("amountCents").isNumber()) {
                    violations.add(String.format(
                            "Refund event %s (%s) is missing numeric amountCents for order %s: %s",
                            outbox.eventId(), outbox.eventType(), outbox.aggregateId(), outbox.payloadJson()));
                } else {
                    long refundAmount = payloadNode.get("amountCents").asLong();
                    if (refundAmount != payment.amountCents()) {
                        violations.add(String.format(
                                "Refund event %s (%s) amountCents (%d) != payment charge amountCents (%d) for order %s",
                                outbox.eventId(), outbox.eventType(), refundAmount, payment.amountCents(),
                                outbox.aggregateId()));
                    }
                }
                if (!payloadNode.hasNonNull("currency") || !payloadNode.get("currency").isTextual()) {
                    violations.add(String.format(
                            "Refund event %s (%s) is missing textual currency for order %s: %s",
                            outbox.eventId(), outbox.eventType(), outbox.aggregateId(), outbox.payloadJson()));
                } else {
                    String refundCurrency = payloadNode.get("currency").asText();
                    if (!Objects.equals(refundCurrency, payment.currency())) {
                        violations.add(String.format(
                                "Refund event %s (%s) currency (%s) != payment currency (%s) for order %s",
                                outbox.eventId(), outbox.eventType(), refundCurrency, payment.currency(),
                                outbox.aggregateId()));
                    }
                }
            }
        }

        return violations;
    }

    /**
     * I6 outbox drained: every outbox row published.
     */
    private List<String> checkI6OutboxDrained(
            List<Db.OutboxRow> invOutbox,
            List<Db.OutboxRow> ordOutbox,
            List<Db.OutboxRow> payOutbox) {
        List<String> violations = new ArrayList<>();
        List<Db.OutboxRow> all = new ArrayList<>();
        all.addAll(invOutbox);
        all.addAll(ordOutbox);
        all.addAll(payOutbox);

        for (Db.OutboxRow row : all) {
            if (row.publishedAt() == null) {
                violations.add(String.format(
                        "Unpublished outbox row in %s: eventId=%s, eventType=%s, aggregateId=%s, createdAt=%s",
                        row.dbName(), row.eventId(), row.eventType(), row.aggregateId(), row.createdAt()));
            }
        }
        return violations;
    }

    /**
     * I7 dead letters: the dead-letter end offsets did not change during the
     * scenario
     * unless the scenario declares an expected number of dead-lettered records.
     */
    private List<String> checkI7DeadLetters(
            KafkaAccess effectiveKafka,
            KafkaProbe.DltSnapshot before,
            long expectedDeadLetters) {
        if (effectiveKafka == null) {
            return List.of();
        }
        KafkaProbe.DltDiff diff = effectiveKafka.diffDltOffsets(before);
        if (diff.totalDelta() != expectedDeadLetters) {
            return List.of(String.format(
                    "Dead-letter offset growth (%d) != expectedDeadLetters (%d). Per-topic delta: %s",
                    diff.totalDelta(), expectedDeadLetters, diff.deltaByTopic()));
        }
        return List.of();
    }

    /**
     * I8 notifications:
     * - Derive expected notifications from the orders table:
     * - Every CONFIRMED order has exactly 1 order.confirmed outbox row (and 0
     * order.cancelled rows)
     * - Every cancelled order (CANCELLED_EXPIRED, CANCELLED_PAYMENT_FAILED,
     * CANCELLED_REFUNDED) has
     * at least 1 and at most 2 order.cancelled outbox rows (and 0 order.confirmed
     * rows)
     * - Derive expected notifications from distinct eventIds on order.events (must
     * match outbox)
     * - Detect counter resets of notification-service (a decreasing counter fails
     * with a clear message)
     * - Require notifications sent (diff of notification-service counters) to equal
     * expected count
     * with no unexpected duplicates and no missing notifications.
     */
    private List<String> checkI8Notifications(
            KafkaAccess effectiveKafka,
            Map<TopicPartition, Long> orderEventsStartOffsets,
            Notifications.NotificationSnapshot notificationsBefore,
            Notifications.NotificationSnapshot notificationsAfter,
            long expectedDuplicateNotifications,
            List<Db.OrderRow> orders,
            Set<UUID> scenarioOrderIds,
            List<Db.OutboxRow> ordOutbox,
            Set<UUID> baselineOrderOutboxEventIds) {
        List<String> violations = new ArrayList<>();

        Map<UUID, List<Db.OutboxRow>> confirmedOutboxByOrder = new HashMap<>();
        Map<UUID, List<Db.OutboxRow>> cancelledOutboxByOrder = new HashMap<>();

        for (Db.OutboxRow row : ordOutbox) {
            if ("order.confirmed".equals(row.eventType())) {
                confirmedOutboxByOrder.computeIfAbsent(row.aggregateId(), k -> new ArrayList<>()).add(row);
            } else if ("order.cancelled".equals(row.eventType())) {
                cancelledOutboxByOrder.computeIfAbsent(row.aggregateId(), k -> new ArrayList<>()).add(row);
            }
        }

        // 1. Derive expected notifications from orders table
        for (Db.OrderRow order : orders) {
            UUID orderId = order.orderId();
            int confirmedCount = confirmedOutboxByOrder.getOrDefault(orderId, List.of()).size();
            int cancelledCount = cancelledOutboxByOrder.getOrDefault(orderId, List.of()).size();

            if ("CONFIRMED".equals(order.status())) {
                if (confirmedCount != 1) {
                    violations.add(String.format(
                            "CONFIRMED order %s must have exactly 1 order.confirmed outbox row, found %d",
                            orderId, confirmedCount));
                }
                if (cancelledCount != 0) {
                    violations.add(String.format(
                            "CONFIRMED order %s must have 0 order.cancelled outbox rows, found %d",
                            orderId, cancelledCount));
                }
            } else if ("CANCELLED_EXPIRED".equals(order.status())
                    || "CANCELLED_PAYMENT_FAILED".equals(order.status())
                    || "CANCELLED_REFUNDED".equals(order.status())) {
                if (cancelledCount < 1 || cancelledCount > 2) {
                    violations.add(String.format(
                            "Cancelled order %s (status=%s) must have between 1 and 2 order.cancelled outbox rows, found %d",
                            orderId, order.status(), cancelledCount));
                }
                if (confirmedCount != 0) {
                    violations.add(String.format(
                            "Cancelled order %s (status=%s) must have 0 order.confirmed outbox rows, found %d",
                            orderId, order.status(), confirmedCount));
                }
            }
        }

        if (effectiveKafka == null || notificationsBefore == null || notificationsAfter == null) {
            return violations;
        }

        // 2. Detect counter resets on notification-service
        List<String> counterResets = Notifications.findCounterResets(notificationsBefore, notificationsAfter);
        violations.addAll(counterResets);

        // 3. Compare distinct eventIds in outbox (excluding pre-run baseline if any)
        // with distinct eventIds on order.events
        Set<UUID> excludedBaseline = baselineOrderOutboxEventIds != null ? baselineOrderOutboxEventIds : Set.of();

        Set<UUID> distinctConfirmedInOutbox = ordOutbox.stream()
                .filter(r -> "order.confirmed".equals(r.eventType()))
                .map(Db.OutboxRow::eventId)
                .filter(id -> !excludedBaseline.contains(id))
                .collect(Collectors.toSet());
        Set<UUID> distinctCancelledInOutbox = ordOutbox.stream()
                .filter(r -> "order.cancelled".equals(r.eventType()))
                .map(Db.OutboxRow::eventId)
                .filter(id -> !excludedBaseline.contains(id))
                .collect(Collectors.toSet());

        List<KafkaProbe.ConsumedEvent> orderTopicEvents = effectiveKafka.readOrderEventsSince(orderEventsStartOffsets);
        Set<String> orderIdStrings = scenarioOrderIds.stream().map(UUID::toString).collect(Collectors.toSet());

        Set<UUID> distinctConfirmedOnKafka = new HashSet<>();
        Set<UUID> distinctCancelledOnKafka = new HashSet<>();
        long rawConfirmedOnKafka = 0L;
        long rawCancelledOnKafka = 0L;

        for (KafkaProbe.ConsumedEvent ev : orderTopicEvents) {
            if (ev.key() != null && orderIdStrings.contains(ev.key()) && ev.eventId() != null) {
                if ("order.confirmed".equals(ev.eventType())) {
                    rawConfirmedOnKafka++;
                    distinctConfirmedOnKafka.add(ev.eventId());
                } else if ("order.cancelled".equals(ev.eventType())) {
                    rawCancelledOnKafka++;
                    distinctCancelledOnKafka.add(ev.eventId());
                }
            }
        }

        long kafkaDuplicateRecords = (rawConfirmedOnKafka - distinctConfirmedOnKafka.size())
                + (rawCancelledOnKafka - distinctCancelledOnKafka.size());
        if (kafkaDuplicateRecords != expectedDuplicateNotifications) {
            violations.add("Kafka order.events duplicate record count mismatch: rawConfirmed="
                    + rawConfirmedOnKafka + " distinctConfirmed=" + distinctConfirmedOnKafka.size()
                    + ", rawCancelled=" + rawCancelledOnKafka + " distinctCancelled=" + distinctCancelledOnKafka.size()
                    + ", expectedDuplicates=" + expectedDuplicateNotifications);
        }

        if (!distinctConfirmedOnKafka.equals(distinctConfirmedInOutbox)) {
            violations.add(String.format(
                    "order.confirmed events on Kafka order.events (%s) != order_db.outbox_events (%s)",
                    distinctConfirmedOnKafka, distinctConfirmedInOutbox));
        }
        if (!distinctCancelledOnKafka.equals(distinctCancelledInOutbox)) {
            violations.add(String.format(
                    "order.cancelled events on Kafka order.events (%s) != order_db.outbox_events (%s)",
                    distinctCancelledOnKafka, distinctCancelledInOutbox));
        }

        // 4. Require notifications sent to equal expected counts with no duplicates and
        // no missing ones
        Notifications.NotificationSnapshot diff = Notifications.diff(notificationsBefore, notificationsAfter);

        long sentConfirmed = diff.sent("order.confirmed");
        long sentCancelled = diff.sent("order.cancelled");
        long duplicateTotal = diff.totalDuplicate();
        long failedNotifications = diff.totalFailed();

        if (failedNotifications > 0) {
            violations.add("notification-service recorded failed notifications: count=" + failedNotifications);
        }
        if (sentConfirmed != distinctConfirmedInOutbox.size()) {
            String kind = sentConfirmed < distinctConfirmedInOutbox.size() ? "missing" : "duplicate";
            violations.add(String.format(
                    "notification-service sent count for order.confirmed (%d) != expected (%d) [%s notification(s); events=%s]",
                    sentConfirmed, distinctConfirmedInOutbox.size(), kind, distinctConfirmedInOutbox));
        }
        if (sentCancelled != distinctCancelledInOutbox.size()) {
            String kind = sentCancelled < distinctCancelledInOutbox.size() ? "missing" : "duplicate";
            violations.add(String.format(
                    "notification-service sent count for order.cancelled (%d) != expected (%d) [%s notification(s); events=%s]",
                    sentCancelled, distinctCancelledInOutbox.size(), kind, distinctCancelledInOutbox));
        }
        if (duplicateTotal != expectedDuplicateNotifications) {
            violations.add(String.format(
                    "notification-service duplicate counter delta (%d) != expectedDuplicateNotifications (%d)",
                    duplicateTotal, expectedDuplicateNotifications));
        }

        return violations;
    }
}
