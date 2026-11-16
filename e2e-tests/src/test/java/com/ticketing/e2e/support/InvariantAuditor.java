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
 * Cross-service Invariant Auditor verifying I1 through I8 across {@code inventory_db},
 * {@code order_db}, {@code payment_db}, Kafka topics/DLTs, and {@code notification-service} metrics.
 */
public final class InvariantAuditor {

    private final Db db;
    private final ObjectMapper objectMapper;

    public InvariantAuditor(Db db) {
        this.db = Objects.requireNonNull(db, "db must not be null");
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public record ScenarioContext(
            UUID eventId,
            Instant scenarioStart,
            KafkaProbe kafkaProbe,
            Map<TopicPartition, Long> orderEventsStartOffsets,
            KafkaProbe.DltSnapshot dltSnapshotBefore,
            long expectedDeadLetters,
            Notifications.NotificationSnapshot notificationsBefore,
            Notifications.NotificationSnapshot notificationsAfter
    ) {}

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
     * Audits invariants I1 through I8 for a single scenario's {@code eventId} and context.
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

        Map<String, List<String>> results = new LinkedHashMap<>();
        results.put("I1", checkI1NoOversell(seats, resItems));
        results.put("I2", checkI2SeatAndReservationConsistency(seats, reservations, resItems, dbNow));
        results.put("I3", checkI3OrderAndPaymentConsistency(reservations, orders, payments));
        results.put("I4", checkI4NoDuplicateEffects(invOutbox, ordOutbox, payOutbox));
        results.put("I5", checkI5Money(seats, reservations, resItems, orders, orderItems, payments, ordOutbox, payOutbox));
        results.put("I6", checkI6OutboxDrained(invOutbox, ordOutbox, payOutbox));
        results.put("I7", checkI7DeadLetters(ctx.kafkaProbe(), ctx.dltSnapshotBefore(), ctx.expectedDeadLetters()));
        results.put("I8", checkI8Notifications(ctx, aggregateIds, ordOutbox));

        return new AuditResult(Collections.unmodifiableMap(results));
    }

    /**
     * Audits database invariants I1 through I6 (for unit tests or global database checks).
     */
    public AuditResult auditDatabaseOnly(UUID optionalEventId) {
        return auditDatabaseForEvents(optionalEventId != null ? Set.of(optionalEventId) : null);
    }

    /**
     * Audits database invariants I1 through I6 across all specified events (or all data if {@code eventIds == null}).
     */
    public AuditResult auditDatabaseForEvents(Set<UUID> eventIds) {
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
        if (eventIds == null) {
            orderIdsFilter = null;
        } else {
            Set<UUID> ids = new HashSet<>();
            reservations.forEach(r -> ids.add(r.reservationId()));
            orders.forEach(o -> ids.add(o.orderId()));
            orderIdsFilter = ids;
        }

        List<Db.OrderItemRow> orderItems = db.findOrderItemsByOrderIds(orderIdsFilter);
        List<Db.PaymentRow> payments = db.findPaymentsByOrderIds(orderIdsFilter);
        List<Db.OutboxRow> invOutbox = db.findInventoryOutboxByAggregateIds(orderIdsFilter);
        List<Db.OutboxRow> ordOutbox = db.findOrderOutboxByAggregateIds(orderIdsFilter);
        List<Db.OutboxRow> payOutbox = db.findPaymentOutboxByAggregateIds(orderIdsFilter);

        Instant dbNow = db.currentTimestampInInventoryDb();

        Map<String, List<String>> results = new LinkedHashMap<>();
        results.put("I1", checkI1NoOversell(seats, resItems));
        results.put("I2", checkI2SeatAndReservationConsistency(seats, reservations, resItems, dbNow));
        results.put("I3", checkI3OrderAndPaymentConsistency(reservations, orders, payments));
        results.put("I4", checkI4NoDuplicateEffects(invOutbox, ordOutbox, payOutbox));
        results.put("I5", checkI5Money(seats, reservations, resItems, orders, orderItems, payments, ordOutbox, payOutbox));
        results.put("I6", checkI6OutboxDrained(invOutbox, ordOutbox, payOutbox));

        return new AuditResult(Collections.unmodifiableMap(results));
    }

    /**
     * I1 no oversell:
     * - For every (event, seat) at most one reservation item in HELD or CONFIRMED
     * - Seats in SOLD equals seats with a CONFIRMED item and never exceeds the seat count of the event
     */
    private List<String> checkI1NoOversell(List<Db.SeatRow> seats, List<Db.ReservationItemRow> resItems) {
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

        Map<UUID, List<Db.SeatRow>> seatsByEvent = seats.stream()
                .collect(Collectors.groupingBy(Db.SeatRow::eventId));
        Map<UUID, List<Db.ReservationItemRow>> itemsByEvent = resItems.stream()
                .collect(Collectors.groupingBy(Db.ReservationItemRow::eventId));

        Set<UUID> allEventIds = new HashSet<>();
        allEventIds.addAll(seatsByEvent.keySet());
        allEventIds.addAll(itemsByEvent.keySet());

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

            if (!soldSeatIds.equals(confirmedItemSeatIds)) {
                violations.add(String.format(
                        "Event %s SOLD seats mismatch CONFIRMED reservation_items: soldSeats=%s vs confirmedItems=%s",
                        eventId, soldSeatIds, confirmedItemSeatIds));
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
     * - No HELD reservation is older than its expires_at plus 90 seconds once quiescent
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

        // Also check orphan active reservation_items whose seat is missing or not matching
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
     * I3 order and payment consistency:
     * - At most one payment per order
     * - An order CONFIRMED has a SUCCEEDED payment and a CONFIRMED reservation
     * - A payment REFUNDED if and only if the order is CANCELLED_REFUNDED
     * - No SUCCEEDED payment belongs to an order in CANCELLED_EXPIRED or CANCELLED_PAYMENT_FAILED once quiescent
     */
    private List<String> checkI3OrderAndPaymentConsistency(
            List<Db.ReservationRow> reservations,
            List<Db.OrderRow> orders,
            List<Db.PaymentRow> payments) {
        List<String> violations = new ArrayList<>();

        Map<UUID, Db.ReservationRow> resById = reservations.stream()
                .collect(Collectors.toMap(Db.ReservationRow::reservationId, Function.identity(), (a, b) -> a));
        Map<UUID, Db.OrderRow> ordersById = orders.stream()
                .collect(Collectors.toMap(Db.OrderRow::orderId, Function.identity(), (a, b) -> a));

        Map<UUID, List<Db.PaymentRow>> paymentsByOrder = new LinkedHashMap<>();
        for (Db.PaymentRow p : payments) {
            paymentsByOrder.computeIfAbsent(p.orderId(), k -> new ArrayList<>()).add(p);
        }

        for (Map.Entry<UUID, List<Db.PaymentRow>> entry : paymentsByOrder.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("Multiple payment rows for orderId=" + entry.getKey() + ": " + entry.getValue());
            }
        }

        for (Db.OrderRow order : orders) {
            UUID orderId = order.orderId();
            List<Db.PaymentRow> orderPayments = paymentsByOrder.getOrDefault(orderId, List.of());
            Db.PaymentRow payment = orderPayments.isEmpty() ? null : orderPayments.get(0);
            Db.ReservationRow res = resById.get(orderId);

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
            }
        }

        return violations;
    }

    /**
     * I4 no duplicate effects:
     * - Per order at most one outbox row each of:
     *   order.payment.requested, order.confirmed, order.refund.requested, payment.succeeded, payment.refunded
     * - Per reservation at most one outbox row each of:
     *   inventory.seat.reserved, inventory.reservation.expired, inventory.seat.confirmed
     */
    private List<String> checkI4NoDuplicateEffects(
            List<Db.OutboxRow> invOutbox,
            List<Db.OutboxRow> ordOutbox,
            List<Db.OutboxRow> payOutbox) {
        List<String> violations = new ArrayList<>();

        Set<String> singlePerOrderTypes = Set.of(
                "order.payment.requested",
                "order.confirmed",
                "order.refund.requested",
                "payment.succeeded",
                "payment.refunded"
        );
        Set<String> singlePerReservationTypes = Set.of(
                "inventory.seat.reserved",
                "inventory.reservation.expired",
                "inventory.seat.confirmed"
        );

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

        return violations;
    }

    /**
     * I5 money:
     * - Payment amount equals order total equals the sum of its items equals the reservation total
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

        for (Db.ReservationRow res : reservations) {
            List<Db.ReservationItemRow> items = resItemsByResId.getOrDefault(res.reservationId(), List.of());
            long sumItems = items.stream().mapToLong(Db.ReservationItemRow::priceCents).sum();
            if (res.totalAmountCents() != sumItems) {
                violations.add(String.format(
                        "Reservation %s totalAmountCents (%d) != sum of reservation_items (%d): items=%s",
                        res.reservationId(), res.totalAmountCents(), sumItems, items));
            }
            for (Db.ReservationItemRow item : items) {
                Db.SeatRow seat = seatById.get(item.seatId());
                if (seat != null && item.priceCents() != seat.priceCents()) {
                    violations.add(String.format(
                            "Reservation item %s priceCents (%d) != seat priceCents (%d)",
                            item, item.priceCents(), seat.priceCents()));
                }
                if (seat != null && !Objects.equals(res.currency(), seat.currency())) {
                    violations.add(String.format(
                            "Reservation %s currency (%s) != seat %s currency (%s)",
                            res.reservationId(), res.currency(), seat.seatId(), seat.currency()));
                }
            }
        }

        for (Db.OrderRow order : orders) {
            List<Db.OrderItemRow> items = orderItemsByOrderId.getOrDefault(order.orderId(), List.of());
            long sumItems = items.stream().mapToLong(Db.OrderItemRow::priceCents).sum();
            if (order.totalAmountCents() != sumItems) {
                violations.add(String.format(
                        "Order %s totalAmountCents (%d) != sum of order_items (%d): items=%s",
                        order.orderId(), order.totalAmountCents(), sumItems, items));
            }
            Db.ReservationRow res = resById.get(order.reservationId());
            if (res != null) {
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
            if (order != null) {
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
            if (res != null && payment.amountCents() != res.totalAmountCents()) {
                violations.add(String.format(
                        "Payment %s amountCents (%d) != reservation %s totalAmountCents (%d)",
                        payment.paymentId(), payment.amountCents(), res.reservationId(), res.totalAmountCents()));
            }
        }

        // Verify refund amounts in outbox events match the payment charge
        List<Db.OutboxRow> refundOutboxes = new ArrayList<>();
        refundOutboxes.addAll(ordOutbox);
        refundOutboxes.addAll(payOutbox);
        for (Db.OutboxRow outbox : refundOutboxes) {
            if ("order.refund.requested".equals(outbox.eventType()) || "payment.refunded".equals(outbox.eventType())) {
                Db.PaymentRow payment = paymentByOrderId.get(outbox.aggregateId());
                if (payment != null && outbox.payloadJson() != null) {
                    try {
                        JsonNode root = objectMapper.readTree(outbox.payloadJson());
                        JsonNode payloadNode = root.has("payload") ? root.get("payload") : root;
                        if (payloadNode != null && payloadNode.hasNonNull("amountCents")) {
                            long refundAmount = payloadNode.get("amountCents").asLong();
                            if (refundAmount != payment.amountCents()) {
                                violations.add(String.format(
                                        "Refund event %s (%s) amountCents (%d) != payment charge amountCents (%d) for order %s",
                                        outbox.eventId(), outbox.eventType(), refundAmount, payment.amountCents(), outbox.aggregateId()));
                            }
                        }
                        if (payloadNode != null && payloadNode.hasNonNull("currency")) {
                            String refundCurrency = payloadNode.get("currency").asText();
                            if (!Objects.equals(refundCurrency, payment.currency())) {
                                violations.add(String.format(
                                        "Refund event %s (%s) currency (%s) != payment currency (%s) for order %s",
                                        outbox.eventId(), outbox.eventType(), refundCurrency, payment.currency(), outbox.aggregateId()));
                            }
                        }
                    } catch (Exception ignored) {
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
     * I7 dead letters: the dead-letter end offsets did not change during the scenario
     * unless the scenario declares an expected number of dead-lettered records.
     */
    private List<String> checkI7DeadLetters(
            KafkaProbe kafkaProbe,
            KafkaProbe.DltSnapshot before,
            long expectedDeadLetters) {
        if (kafkaProbe == null || before == null) {
            return List.of();
        }
        KafkaProbe.DltDiff diff = kafkaProbe.diffDltOffsets(before);
        if (diff.totalDelta() != expectedDeadLetters) {
            return List.of(String.format(
                    "Dead-letter offset growth (%d) != expectedDeadLetters (%d). Per-topic delta: %s",
                    diff.totalDelta(), expectedDeadLetters, diff.deltaByTopic()));
        }
        return List.of();
    }

    /**
     * I8 notifications: the number of notifications sent for the scenario (diff of the
     * notification-service counters) equals the number of distinct order.confirmed and
     * order.cancelled events of the scenario found on order.events, with no duplicates.
     */
    private List<String> checkI8Notifications(
            ScenarioContext ctx,
            Set<UUID> scenarioOrderIds,
            List<Db.OutboxRow> ordOutbox) {
        if (ctx.kafkaProbe() == null || ctx.notificationsBefore() == null || ctx.notificationsAfter() == null) {
            return List.of();
        }
        List<String> violations = new ArrayList<>();

        List<KafkaProbe.ConsumedEvent> orderTopicEvents = ctx.kafkaProbe()
                .readSince(KafkaProbe.TOPIC_ORDER_EVENTS, ctx.orderEventsStartOffsets());

        Set<String> orderIdStrings = scenarioOrderIds.stream().map(UUID::toString).collect(Collectors.toSet());

        Set<UUID> distinctConfirmedOnKafka = new HashSet<>();
        Set<UUID> distinctCancelledOnKafka = new HashSet<>();

        for (KafkaProbe.ConsumedEvent ev : orderTopicEvents) {
            if (ev.key() != null && orderIdStrings.contains(ev.key()) && ev.eventId() != null) {
                if ("order.confirmed".equals(ev.eventType())) {
                    distinctConfirmedOnKafka.add(ev.eventId());
                } else if ("order.cancelled".equals(ev.eventType())) {
                    distinctCancelledOnKafka.add(ev.eventId());
                }
            }
        }

        Set<UUID> distinctConfirmedInOutbox = ordOutbox.stream()
                .filter(r -> "order.confirmed".equals(r.eventType()))
                .map(Db.OutboxRow::eventId)
                .collect(Collectors.toSet());
        Set<UUID> distinctCancelledInOutbox = ordOutbox.stream()
                .filter(r -> "order.cancelled".equals(r.eventType()))
                .map(Db.OutboxRow::eventId)
                .collect(Collectors.toSet());

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

        Notifications.NotificationSnapshot diff =
                Notifications.diff(ctx.notificationsBefore(), ctx.notificationsAfter());

        long sentConfirmed = diff.sent("order.confirmed");
        long sentCancelled = diff.sent("order.cancelled");
        long failedNotifications = diff.totalFailed();

        if (failedNotifications > 0) {
            violations.add("notification-service recorded failed notifications during scenario: count=" + failedNotifications);
        }
        if (sentConfirmed != distinctConfirmedOnKafka.size()) {
            violations.add(String.format(
                    "notification-service sent count for order.confirmed (%d) != distinct order.confirmed events on order.events (%d: %s)",
                    sentConfirmed, distinctConfirmedOnKafka.size(), distinctConfirmedOnKafka));
        }
        if (sentCancelled != distinctCancelledOnKafka.size()) {
            violations.add(String.format(
                    "notification-service sent count for order.cancelled (%d) != distinct order.cancelled events on order.events (%d: %s)",
                    sentCancelled, distinctCancelledOnKafka.size(), distinctCancelledOnKafka));
        }

        return violations;
    }
}
