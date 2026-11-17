package com.ticketing.e2e;

import com.ticketing.e2e.support.Db;
import com.ticketing.e2e.support.InvariantAuditor;
import com.ticketing.e2e.support.KafkaProbe;
import com.ticketing.e2e.support.Notifications;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link InvariantAuditor} using an in-memory fake data source
 * ({@link Db.InMemoryStore}) and injected {@link InvariantAuditor.KafkaAccess} and
 * {@link InvariantAuditor.NotificationAccess} interfaces with deliberately corrupted
 * fixture rows proving that each invariant I1 to I8 detects a violation and passes on valid data.
 */
@Tag("fast")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class InvariantAuditorTest {

    @BeforeAll
    static void printTestMethodsStart() {
        printTestMethods("START");
    }

    @AfterAll
    static void printTestMethodsEnd() {
        printTestMethods("END");
    }

    private static void printTestMethods(String phase) {
        List<String> names = Arrays.stream(InvariantAuditorTest.class.getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Test.class))
                .sorted(Comparator.comparingInt(m -> m.isAnnotationPresent(Order.class)
                        ? m.getAnnotation(Order.class).value() : Integer.MAX_VALUE))
                .map(Method::getName)
                .toList();
        System.out.println("=== [InvariantAuditorTest " + phase + "] Test methods (" + names.size() + "): " + names + " ===");
    }

    private Db.InMemoryStore store;
    private Db db;
    private InvariantAuditor auditor;

    private UUID eventId;
    private UUID seat1Id;
    private UUID seat2Id;
    private UUID user1Id;
    private UUID res1Id;
    private UUID pay1Id;
    private Instant now;
    private Instant exp;

    @BeforeEach
    void setUp() {
        store = new Db.InMemoryStore();
        now = Instant.parse("2026-11-16T12:00:00Z");
        exp = now.plus(10, ChronoUnit.MINUTES);
        store.currentTime = now;

        db = Db.inMemory(store);
        auditor = new InvariantAuditor(db, store.toKafkaAccess(), store.toNotificationAccess());

        eventId = UUID.randomUUID();
        seat1Id = UUID.randomUUID();
        seat2Id = UUID.randomUUID();
        user1Id = UUID.randomUUID();
        res1Id = UUID.randomUUID();
        pay1Id = UUID.randomUUID();

        seedValidConfirmedOrderFixture();
    }

    private void seedValidConfirmedOrderFixture() {
        store.events.add(new Db.EventRow(eventId, "Test Concert", "ON_SALE", now));
        store.seats.add(new Db.SeatRow(seat1Id, eventId, "SEC-A", "01", "001", 15_000L, "USD", "SOLD", 2L));
        store.seats.add(new Db.SeatRow(seat2Id, eventId, "SEC-A", "01", "002", 15_000L, "USD", "AVAILABLE", 0L));

        store.reservations.add(new Db.ReservationRow(
                res1Id, eventId, user1Id, "CONFIRMED", 15_000L, "USD", now, exp, now, null, null));
        store.reservationItems.add(new Db.ReservationItemRow(
                UUID.randomUUID(), res1Id, eventId, seat1Id, 15_000L, "CONFIRMED"));

        store.inventoryOutbox.add(new Db.OutboxRow(
                "inventory_db", UUID.randomUUID(), "RESERVATION", res1Id, "inventory.events",
                "inventory.seat.reserved", UUID.randomUUID(), 1, "{}", now, now));
        store.inventoryOutbox.add(new Db.OutboxRow(
                "inventory_db", UUID.randomUUID(), "RESERVATION", res1Id, "inventory.events",
                "inventory.seat.confirmed", UUID.randomUUID(), 1, "{}", now, now));

        store.orders.add(new Db.OrderRow(
                res1Id, res1Id, user1Id, eventId, "CONFIRMED", 15_000L, "USD",
                "tok_success", pay1Id, null, now, exp, now, now, null, null, now));
        store.orderItems.add(new Db.OrderItemRow(
                UUID.randomUUID(), res1Id, seat1Id, 15_000L));

        store.orderOutbox.add(new Db.OutboxRow(
                "order_db", UUID.randomUUID(), "ORDER", res1Id, "order.events",
                "order.payment.requested", UUID.randomUUID(), 1, "{\"amountCents\":15000,\"currency\":\"USD\"}", now, now));
        store.orderOutbox.add(new Db.OutboxRow(
                "order_db", UUID.randomUUID(), "ORDER", res1Id, "order.events",
                "order.confirmed", UUID.randomUUID(), 1, "{\"totalAmountCents\":15000,\"currency\":\"USD\"}", now, now));

        store.payments.add(new Db.PaymentRow(
                pay1Id, res1Id, res1Id, user1Id, 15_000L, "USD", "tok_success",
                "SUCCEEDED", "mock_ch_1", null, null, null, now, now, null, null));
        store.paymentOutbox.add(new Db.OutboxRow(
                "payment_db", UUID.randomUUID(), "PAYMENT", res1Id, "payment.events",
                "payment.succeeded", UUID.randomUUID(), 1, "{\"amountCents\":15000,\"currency\":\"USD\"}", now, now));
    }

    @Test
    @Order(1)
    @DisplayName("Auditor unit test: valid fixture data passes all invariants I1 to I8")
    void validDataPassesAllInvariants() {
        InvariantAuditor.AuditResult result = auditor.auditDatabaseOnly(eventId);
        assertThat(result.allPassed())
                .as("Expected all invariants to pass, got: %s", result.allViolations())
                .isTrue();
        assertThat(result.violationsByInvariant().keySet())
                .containsExactly("I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8");
    }

    @Test
    @Order(2)
    @DisplayName("Auditor unit test: I1 detects oversell when two active reservation items reference the same seat")
    void i1DetectsOversellDuplicateActiveItems() {
        UUID res2Id = UUID.randomUUID();
        store.reservations.add(new Db.ReservationRow(
                res2Id, eventId, UUID.randomUUID(), "HELD", 15_000L, "USD", now, exp, null, null, null));
        store.reservationItems.add(new Db.ReservationItemRow(
                UUID.randomUUID(), res2Id, eventId, seat1Id, 15_000L, "HELD"));

        InvariantAuditor.AuditResult result = auditor.auditDatabaseOnly(eventId);
        assertThat(result.violationsByInvariant().get("I1"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("Multiple active (HELD/CONFIRMED) reservation_items"));
    }

    @Test
    @Order(3)
    @DisplayName("Auditor unit test: I1 and I2 detect SOLD seat without a CONFIRMED reservation item")
    void i1AndI2DetectSoldSeatWithoutConfirmedItem() {
        store.seats.set(1, new Db.SeatRow(seat2Id, eventId, "SEC-A", "01", "002", 15_000L, "USD", "SOLD", 1L));

        InvariantAuditor.AuditResult result = auditor.auditDatabaseOnly(eventId);
        assertThat(result.violationsByInvariant().get("I1")).isNotEmpty();
        assertThat(result.violationsByInvariant().get("I2")).isNotEmpty();
    }

    @Test
    @Order(4)
    @DisplayName("Auditor unit test: I2 detects AVAILABLE seat with active HELD item and HELD reservation expired > 90s")
    void i2DetectsAvailableSeatWithHeldItemAndStaleExpiredHold() {
        UUID staleResId = UUID.randomUUID();
        Instant createdLongAgo = now.minus(20, ChronoUnit.MINUTES);
        Instant expiredLongAgo = now.minus(5, ChronoUnit.MINUTES);

        store.reservations.add(new Db.ReservationRow(
                staleResId, eventId, UUID.randomUUID(), "HELD", 15_000L, "USD",
                createdLongAgo, expiredLongAgo, null, null, null));
        store.reservationItems.add(new Db.ReservationItemRow(
                UUID.randomUUID(), staleResId, eventId, seat2Id, 15_000L, "HELD"));

        InvariantAuditor.AuditResult result = auditor.auditDatabaseOnly(eventId);
        assertThat(result.violationsByInvariant().get("I2"))
                .hasSizeGreaterThanOrEqualTo(2)
                .anyMatch(msg -> msg.contains("AVAILABLE seat") && msg.contains("active reservation items"))
                .anyMatch(msg -> msg.contains("older than expires_at + 90s"));
    }

    @Test
    @Order(5)
    @DisplayName("Auditor unit test: I3 detects CONFIRMED order without SUCCEEDED payment, REFUNDED mismatch, and SUCCEEDED payment on CANCELLED_EXPIRED order")
    void i3DetectsOrderAndPaymentInconsistencies() {
        // 1. Corrupt payment of CONFIRMED order to REFUNDED
        store.payments.set(0, new Db.PaymentRow(
                pay1Id, res1Id, res1Id, user1Id, 15_000L, "USD", "tok_success",
                "REFUNDED", "mock_ch_1", null, UUID.randomUUID(), "LATE_PAYMENT_EXPIRED_HOLD", now, now, null, now));

        InvariantAuditor.AuditResult result = auditor.auditDatabaseOnly(eventId);
        assertThat(result.violationsByInvariant().get("I3"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("CONFIRMED order") && msg.contains("must have SUCCEEDED payment"))
                .anyMatch(msg -> msg.contains("REFUNDED payment") && msg.contains("requires order in CANCELLED_REFUNDED"));

        // 2. Corrupt order to CANCELLED_EXPIRED while payment is SUCCEEDED
        store.payments.set(0, new Db.PaymentRow(
                pay1Id, res1Id, res1Id, user1Id, 15_000L, "USD", "tok_success",
                "SUCCEEDED", "mock_ch_1", null, null, null, now, now, null, null));
        store.orders.set(0, new Db.OrderRow(
                res1Id, res1Id, user1Id, eventId, "CANCELLED_EXPIRED", 15_000L, "USD",
                "tok_success", pay1Id, "RESERVATION_EXPIRED", now, exp, now, null, now, null, now));

        InvariantAuditor.AuditResult result2 = auditor.auditDatabaseOnly(eventId);
        assertThat(result2.violationsByInvariant().get("I3"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("uncompensated SUCCEEDED payment"));
    }

    @Test
    @Order(6)
    @DisplayName("Auditor unit test: I4 detects duplicate outbox events per order and per reservation")
    void i4DetectsDuplicateOutboxRows() {
        store.orderOutbox.add(new Db.OutboxRow(
                "order_db", UUID.randomUUID(), "ORDER", res1Id, "order.events",
                "order.confirmed", UUID.randomUUID(), 1, "{}", now, now));
        store.inventoryOutbox.add(new Db.OutboxRow(
                "inventory_db", UUID.randomUUID(), "RESERVATION", res1Id, "inventory.events",
                "inventory.seat.confirmed", UUID.randomUUID(), 1, "{}", now, now));

        InvariantAuditor.AuditResult result = auditor.auditDatabaseOnly(eventId);
        assertThat(result.violationsByInvariant().get("I4"))
                .hasSize(2)
                .anyMatch(msg -> msg.contains("order.confirmed"))
                .anyMatch(msg -> msg.contains("inventory.seat.confirmed"));
    }

    @Test
    @Order(7)
    @DisplayName("Auditor unit test: I5 detects money and currency mismatches across seats, reservations, orders, payments, and refunds")
    void i5DetectsMoneyAndCurrencyViolations() {
        store.payments.set(0, new Db.PaymentRow(
                pay1Id, res1Id, res1Id, user1Id, 99_999L, "EUR", "tok_success",
                "SUCCEEDED", "mock_ch_1", null, null, null, now, now, null, null));
        store.paymentOutbox.add(new Db.OutboxRow(
                "payment_db", UUID.randomUUID(), "PAYMENT", res1Id, "payment.events",
                "payment.refunded", UUID.randomUUID(), 1, "{\"amountCents\":12345,\"currency\":\"GBP\"}", now, now));

        InvariantAuditor.AuditResult result = auditor.auditDatabaseOnly(eventId);
        assertThat(result.violationsByInvariant().get("I5"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("amountCents (99999) != order"))
                .anyMatch(msg -> msg.contains("currency (EUR) != order"))
                .anyMatch(msg -> msg.contains("Refund event") && msg.contains("amountCents (12345)"));
    }

    @Test
    @Order(8)
    @DisplayName("Auditor unit test: I1 detects two CONFIRMED order items on one seat and mismatch with SOLD seats")
    void i1DetectsTwoConfirmedOrderItemsOnOneSeat() {
        assertThat(auditor.auditDatabaseOnly(eventId).violationsByInvariant().get("I1")).isEmpty();

        UUID order2Id = UUID.randomUUID();
        store.orders.add(new Db.OrderRow(
                order2Id, order2Id, UUID.randomUUID(), eventId, "CONFIRMED", 15_000L, "USD",
                "tok_success", UUID.randomUUID(), null, now, exp, now, now, null, null, now));
        store.orderItems.add(new Db.OrderItemRow(
                UUID.randomUUID(), order2Id, seat1Id, 15_000L));

        InvariantAuditor.AuditResult corruptedResult = auditor.auditDatabaseOnly(eventId);
        assertThat(corruptedResult.violationsByInvariant().get("I1"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("Multiple CONFIRMED order_items for seatId=" + seat1Id));

        // Also corrupt order_items so CONFIRMED order points to AVAILABLE seat2Id (mismatching SOLD seats)
        store.orderItems.remove(store.orderItems.size() - 1);
        store.orders.remove(store.orders.size() - 1);
        store.orderItems.set(0, new Db.OrderItemRow(UUID.randomUUID(), res1Id, seat2Id, 15_000L));

        InvariantAuditor.AuditResult mismatchResult = auditor.auditDatabaseOnly(eventId);
        assertThat(mismatchResult.violationsByInvariant().get("I1"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("SOLD seats mismatch CONFIRMED order_items"));
    }

    @Test
    @Order(9)
    @DisplayName("Auditor unit test: I3 detects reverse direction violations, non-terminal PAYMENT_PENDING/REFUND_PENDING orders, and seat set mismatch")
    void i3DetectsReverseDirectionAndSeatSetViolations() {
        assertThat(auditor.auditDatabaseOnly(eventId).violationsByInvariant().get("I3")).isEmpty();

        // 1. Reverse direction: CONFIRMED reservation whose order is CANCELLED_EXPIRED (and payment missing)
        store.orders.set(0, new Db.OrderRow(
                res1Id, res1Id, user1Id, eventId, "CANCELLED_EXPIRED", 15_000L, "USD",
                null, null, "RESERVATION_EXPIRED", now, exp, null, null, now, null, now));
        store.payments.clear();
        store.paymentOutbox.clear();

        InvariantAuditor.AuditResult revResult = auditor.auditDatabaseOnly(eventId);
        assertThat(revResult.violationsByInvariant().get("I3"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("CONFIRMED reservation") && msg.contains("must have CONFIRMED order"));

        // 2. Order left in PAYMENT_PENDING after quiescence
        store.orders.set(0, new Db.OrderRow(
                res1Id, res1Id, user1Id, eventId, "PAYMENT_PENDING", 15_000L, "USD",
                "tok_success", null, null, now, exp, now, null, null, null, now));
        InvariantAuditor.AuditResult pendingResult = auditor.auditDatabaseOnly(eventId);
        assertThat(pendingResult.violationsByInvariant().get("I3"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("left in non-terminal status PAYMENT_PENDING"));

        // 3. Order left in undeclared REFUND_PENDING after quiescence fails I3, but passes I3 when declared
        store.reservations.set(0, new Db.ReservationRow(
                res1Id, eventId, user1Id, "EXPIRED", 15_000L, "USD", now, exp, null, null, now));
        store.reservationItems.set(0, new Db.ReservationItemRow(
                UUID.randomUUID(), res1Id, eventId, seat1Id, 15_000L, "EXPIRED"));
        store.seats.set(0, new Db.SeatRow(seat1Id, eventId, "SEC-A", "01", "001", 15_000L, "USD", "AVAILABLE", 2L));
        store.orders.set(0, new Db.OrderRow(
                res1Id, res1Id, user1Id, eventId, "REFUND_PENDING", 15_000L, "USD",
                "tok_refund_decline", pay1Id, "RESERVATION_EXPIRED", now, exp, now, null, now, null, now));
        store.payments.add(new Db.PaymentRow(
                pay1Id, res1Id, res1Id, user1Id, 15_000L, "USD", "tok_refund_decline",
                "SUCCEEDED", "mock_ch_1", null, null, null, now, now, null, null));

        InvariantAuditor.AuditResult undeclaredRefundPending = auditor.auditDatabaseOnly(eventId);
        assertThat(undeclaredRefundPending.violationsByInvariant().get("I3"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("left in REFUND_PENDING after quiescence without being declared"));

        store.allowedRefundPendingOrderIds.add(res1Id);
        InvariantAuditor.AuditResult declaredRefundPending = auditor.auditDatabaseOnly(eventId);
        assertThat(declaredRefundPending.violationsByInvariant().get("I3")).isEmpty();
        store.allowedRefundPendingOrderIds.clear();

        // 4. Seat set mismatch between order_items and reservation_items
        setUp();
        store.orderItems.set(0, new Db.OrderItemRow(UUID.randomUUID(), res1Id, seat2Id, 15_000L));
        InvariantAuditor.AuditResult seatMismatch = auditor.auditDatabaseOnly(eventId);
        assertThat(seatMismatch.violationsByInvariant().get("I3"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("order_items seat set") && msg.contains("!= reservation") && msg.contains("reservation_items seat set"));

        // 5. CONFIRMED reservation missing its corresponding order
        setUp();
        store.orders.clear();
        InvariantAuditor.AuditResult missingOrderForConfirmedRes = auditor.auditDatabaseOnly(eventId);
        assertThat(missingOrderForConfirmedRes.violationsByInvariant().get("I3"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("no corresponding order"));
    }

    @Test
    @Order(10)
    @DisplayName("Auditor unit test: I5 detects missing parent row, REFUNDED payment without payment.refunded outbox row, and malformed JSON payload")
    void i5DetectsMissingParentRowRefundWithoutOutboxAndMalformedJson() {
        assertThat(auditor.auditDatabaseOnly(eventId).violationsByInvariant().get("I5")).isEmpty();

        // 1. Missing parent row: payment references a non-existent order and reservation
        UUID ghostOrderId = UUID.randomUUID();
        store.payments.add(new Db.PaymentRow(
                UUID.randomUUID(), ghostOrderId, ghostOrderId, user1Id, 15_000L, "USD", "tok_success",
                "SUCCEEDED", "mock_ch_ghost", null, null, null, now, now, null, null));

        InvariantAuditor.AuditResult missingParentResult = auditor.auditDatabaseOnly(eventId);
        assertThat(missingParentResult.violationsByInvariant().get("I5"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("references missing parent order " + ghostOrderId))
                .anyMatch(msg -> msg.contains("references missing parent reservation " + ghostOrderId));

        // 2. REFUNDED payment without a payment.refunded outbox row
        setUp();
        store.orders.set(0, new Db.OrderRow(
                res1Id, res1Id, user1Id, eventId, "CANCELLED_REFUNDED", 15_000L, "USD",
                "tok_success", pay1Id, "LATE_PAYMENT_EXPIRED_HOLD", now, exp, now, null, now, now, now));
        store.payments.set(0, new Db.PaymentRow(
                pay1Id, res1Id, res1Id, user1Id, 15_000L, "USD", "tok_success",
                "REFUNDED", "mock_ch_1", null, UUID.randomUUID(), "LATE_PAYMENT_EXPIRED_HOLD", now, now, null, now));
        // Note: no payment.refunded row added to store.paymentOutbox!

        InvariantAuditor.AuditResult missingRefundOutboxResult = auditor.auditDatabaseOnly(eventId);
        assertThat(missingRefundOutboxResult.violationsByInvariant().get("I5"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("REFUNDED payment") && msg.contains("has no payment.refunded outbox row"));

        // 3. Malformed JSON payload on outbox row
        setUp();
        store.paymentOutbox.add(new Db.OutboxRow(
                "payment_db", UUID.randomUUID(), "PAYMENT", res1Id, "payment.events",
                "payment.refunded", UUID.randomUUID(), 1, "{malformed-json", now, now));

        InvariantAuditor.AuditResult malformedJsonResult = auditor.auditDatabaseOnly(eventId);
        assertThat(malformedJsonResult.violationsByInvariant().get("I5"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("Failed to parse JSON payload"));
    }

    @Test
    @Order(11)
    @DisplayName("Auditor unit test: I6 detects unpublished outbox row (published_at == null)")
    void i6DetectsUnpublishedOutboxRow() {
        assertThat(auditor.auditDatabaseOnly(eventId).violationsByInvariant().get("I6")).isEmpty();

        UUID unpublishedEventId = UUID.randomUUID();
        store.orderOutbox.add(new Db.OutboxRow(
                "order_db", unpublishedEventId, "ORDER", res1Id, "order.events",
                "order.cancelled", UUID.randomUUID(), 1, "{\"orderId\":\"" + res1Id + "\"}", now, null));

        InvariantAuditor.AuditResult result = auditor.auditDatabaseOnly(eventId);
        assertThat(result.violationsByInvariant().get("I6"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("Unpublished outbox row in order_db")
                        && msg.contains(unpublishedEventId.toString()));
    }

    @Test
    @Order(12)
    @DisplayName("Auditor unit test: I7 detects unexpected dead-letter queue growth via injected KafkaAccess")
    void i7DetectsUnexpectedDeadLetterGrowth() {
        assertThat(auditor.auditDatabaseOnly(eventId).violationsByInvariant().get("I7")).isEmpty();

        // Inject a KafkaAccess reporting +2 dead-letter messages while expectedDeadLetters is 0
        InvariantAuditor.KafkaAccess corruptedKafka = new InvariantAuditor.KafkaAccess() {
            @Override
            public KafkaProbe.DltDiff diffDltOffsets(KafkaProbe.DltSnapshot before) {
                String dltTopic = "order.events-payment-service-order-cg-dlt";
                return new KafkaProbe.DltDiff(
                        2L,
                        Map.of(dltTopic, 2L),
                        Map.of(new TopicPartition(dltTopic, 0), 2L)
                );
            }

            @Override
            public List<KafkaProbe.ConsumedEvent> readOrderEventsSince(Map<TopicPartition, Long> startOffsets) {
                return store.toKafkaAccess().readOrderEventsSince(startOffsets);
            }
        };

        InvariantAuditor customAuditor = new InvariantAuditor(db, corruptedKafka, store.toNotificationAccess());
        InvariantAuditor.AuditResult corruptedResult = customAuditor.auditDatabaseOnly(eventId);
        assertThat(corruptedResult.violationsByInvariant().get("I7"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("Dead-letter offset growth (2) != expectedDeadLetters (0)"));

        // When expectedDeadLetters matches the 2 DLT records, I7 passes
        store.expectedDeadLetters = 2L;
        InvariantAuditor.AuditResult expectedResult = customAuditor.auditDatabaseOnly(eventId);
        assertThat(expectedResult.violationsByInvariant().get("I7")).isEmpty();
    }

    @Test
    @Order(13)
    @DisplayName("Auditor unit test: I8 detects missing notification, duplicate notification, and counter reset via injected NotificationAccess")
    void i8DetectsMissingNotificationDuplicateNotificationAndCounterReset() {
        assertThat(auditor.auditDatabaseOnly(eventId).violationsByInvariant().get("I8")).isEmpty();

        // 1. Missing notification: 1 order.confirmed outbox/Kafka event exists, but sent counter is 0
        InvariantAuditor missingNotifAuditor = new InvariantAuditor(
                db,
                store.toKafkaAccess(),
                () -> Notifications.NotificationSnapshot.ofCounts(0L, 0L, 0L, 0L)
        );
        InvariantAuditor.AuditResult missingResult = missingNotifAuditor.auditDatabaseOnly(eventId);
        assertThat(missingResult.violationsByInvariant().get("I8"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("sent count for order.confirmed (0) != expected (1)")
                        && msg.contains("missing"));

        // 2. Duplicate notification: 1 order.confirmed outbox/Kafka event exists, but sent counter is 2 (and duplicate counter is 1)
        InvariantAuditor duplicateNotifAuditor = new InvariantAuditor(
                db,
                store.toKafkaAccess(),
                () -> Notifications.NotificationSnapshot.ofCounts(2L, 0L, 1L, 0L)
        );
        InvariantAuditor.AuditResult duplicateResult = duplicateNotifAuditor.auditDatabaseOnly(eventId);
        assertThat(duplicateResult.violationsByInvariant().get("I8"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("sent count for order.confirmed (2) != expected (1)")
                        && msg.contains("duplicate"))
                .anyMatch(msg -> msg.contains("duplicate counter delta (1) != expectedDuplicateNotifications (0)"));

        // 3. Counter reset: notificationsBefore had sent=5, notificationsAfter has sent=1 (decreased!)
        store.notificationsBefore = Notifications.NotificationSnapshot.ofCounts(5L, 0L, 0L, 0L);
        InvariantAuditor resetAuditor = new InvariantAuditor(
                db,
                store.toKafkaAccess(),
                () -> Notifications.NotificationSnapshot.ofCounts(1L, 0L, 0L, 0L)
        );
        InvariantAuditor.AuditResult resetResult = resetAuditor.auditDatabaseOnly(eventId);
        assertThat(resetResult.violationsByInvariant().get("I8"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("counter reset detected")
                        && msg.contains("before=5, after=1"));

        // 4. Raw Kafka duplicate order.confirmed records with the same eventId
        setUp();
        UUID dupEventId = UUID.randomUUID();
        store.useCustomKafkaOrderEvents = true;
        store.kafkaOrderEvents.clear();
        store.kafkaOrderEvents.add(new com.ticketing.e2e.support.KafkaProbe.ConsumedEvent(
                "order.events", 0, 1L, res1Id.toString(), "{}", java.util.Map.of(),
                dupEventId, "order.confirmed", UUID.randomUUID().toString(), null));
        store.kafkaOrderEvents.add(new com.ticketing.e2e.support.KafkaProbe.ConsumedEvent(
                "order.events", 0, 2L, res1Id.toString(), "{}", java.util.Map.of(),
                dupEventId, "order.confirmed", UUID.randomUUID().toString(), null));
        InvariantAuditor dupKafkaAuditor = new InvariantAuditor(db, store.toKafkaAccess(), store.toNotificationAccess());
        InvariantAuditor.AuditResult dupKafkaResult = dupKafkaAuditor.auditDatabaseOnly(eventId);
        assertThat(dupKafkaResult.violationsByInvariant().get("I8"))
                .isNotEmpty()
                .anyMatch(msg -> msg.contains("Kafka order.events duplicate record count mismatch")
                        && msg.contains("rawConfirmed=2 distinctConfirmed=1"));
    }
}
