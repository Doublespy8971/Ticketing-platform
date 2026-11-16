package com.ticketing.e2e;

import com.ticketing.e2e.support.Db;
import com.ticketing.e2e.support.InvariantAuditor;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link InvariantAuditor} using an in-memory fake data source
 * ({@link Db.InMemoryStore}) with deliberately corrupted fixture rows proving that
 * each invariant I1 to I5 detects a violation and passes on valid data.
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

        Db db = Db.inMemory(store);
        auditor = new InvariantAuditor(db);

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
    @DisplayName("Auditor unit test: valid fixture data passes all invariants I1 to I6")
    void validDataPassesAllInvariants() {
        InvariantAuditor.AuditResult result = auditor.auditDatabaseOnly(eventId);
        assertThat(result.allPassed())
                .as("Expected all invariants to pass, got: %s", result.allViolations())
                .isTrue();
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
}
