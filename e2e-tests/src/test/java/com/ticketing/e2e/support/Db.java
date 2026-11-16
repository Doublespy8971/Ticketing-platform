package com.ticketing.e2e.support;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Read-only JDBC helpers for {@code inventory_db}, {@code order_db}, and {@code payment_db}
 * using {@link PreparedStatement}, plus the single documented write helper
 * {@link #forceReservationExpiry(UUID)} used strictly to backdate a reservation's
 * {@code created_at} and {@code expires_at} in {@code inventory_db} so hold-expiry
 * scenarios do not have to wait 10 minutes (600s).
 * <p>
 * Also supports an in-memory fake data source ({@link InMemoryStore}) for unit-testing
 * {@link InvariantAuditor} with deliberately corrupted fixture data.
 */
public final class Db {

    private final String inventoryJdbcUrl;
    private final String orderJdbcUrl;
    private final String paymentJdbcUrl;
    private final String username;
    private final String password;
    private final InMemoryStore memoryStore;

    public Db(E2EConfig config) {
        this.inventoryJdbcUrl = config.inventoryJdbcUrl();
        this.orderJdbcUrl = config.orderJdbcUrl();
        this.paymentJdbcUrl = config.paymentJdbcUrl();
        this.username = config.postgresUser();
        this.password = config.postgresPassword();
        this.memoryStore = null;
    }

    private Db(InMemoryStore memoryStore) {
        this.inventoryJdbcUrl = null;
        this.orderJdbcUrl = null;
        this.paymentJdbcUrl = null;
        this.username = null;
        this.password = null;
        this.memoryStore = memoryStore;
    }

    public static Db inMemory(InMemoryStore store) {
        return new Db(store);
    }

    public static final class InMemoryStore {
        public Instant currentTime = Instant.now();
        public final List<EventRow> events = new ArrayList<>();
        public final List<SeatRow> seats = new ArrayList<>();
        public final List<ReservationRow> reservations = new ArrayList<>();
        public final List<ReservationItemRow> reservationItems = new ArrayList<>();
        public final List<OrderRow> orders = new ArrayList<>();
        public final List<OrderItemRow> orderItems = new ArrayList<>();
        public final List<PaymentRow> payments = new ArrayList<>();
        public final List<OutboxRow> inventoryOutbox = new ArrayList<>();
        public final List<OutboxRow> orderOutbox = new ArrayList<>();
        public final List<OutboxRow> paymentOutbox = new ArrayList<>();
    }

    public record EventRow(UUID eventId, String name, String status, Instant createdAt) {}

    public record SeatRow(
            UUID seatId,
            UUID eventId,
            String sectionCode,
            String rowLabel,
            String seatNumber,
            long priceCents,
            String currency,
            String status,
            long version
    ) {}

    public record ReservationRow(
            UUID reservationId,
            UUID eventId,
            UUID userId,
            String status,
            long totalAmountCents,
            String currency,
            Instant createdAt,
            Instant expiresAt,
            Instant confirmedAt,
            Instant releasedAt,
            Instant expiredAt
    ) {}

    public record ReservationItemRow(
            UUID reservationItemId,
            UUID reservationId,
            UUID eventId,
            UUID seatId,
            long priceCents,
            String status
    ) {}

    public record OrderRow(
            UUID orderId,
            UUID reservationId,
            UUID userId,
            UUID eventId,
            String status,
            long totalAmountCents,
            String currency,
            String paymentMethodToken,
            UUID paymentId,
            String cancellationReason,
            Instant reservedAt,
            Instant expiresAt,
            Instant checkoutAt,
            Instant confirmedAt,
            Instant cancelledAt,
            Instant refundedAt,
            Instant createdAt
    ) {}

    public record OrderItemRow(
            UUID orderItemId,
            UUID orderId,
            UUID seatId,
            long priceCents
    ) {}

    public record PaymentRow(
            UUID paymentId,
            UUID orderId,
            UUID reservationId,
            UUID userId,
            long amountCents,
            String currency,
            String paymentMethodToken,
            String status,
            String providerReference,
            String failureCode,
            UUID refundId,
            String refundReason,
            Instant createdAt,
            Instant paidAt,
            Instant failedAt,
            Instant refundedAt
    ) {}

    public record OutboxRow(
            String dbName,
            UUID eventId,
            String aggregateType,
            UUID aggregateId,
            String topic,
            String eventType,
            UUID correlationId,
            int schemaVersion,
            String payloadJson,
            Instant createdAt,
            Instant publishedAt
    ) {}

    /**
     * THE ONLY WRITE HELPER IN {@link Db}.
     * Sets {@code created_at} to 1 hour ago and {@code expires_at} to 1 minute ago
     * for {@code reservationId} in {@code inventory_db.reservations} so the 1000ms
     * PostgreSQL hold expiry sweeper expires it immediately without waiting 10 minutes.
     */
    public int forceReservationExpiry(UUID reservationId) {
        String sql = """
                UPDATE reservations
                SET created_at = NOW() - INTERVAL '1 hour',
                    expires_at = NOW() - INTERVAL '1 minute',
                    updated_at = NOW()
                WHERE reservation_id = ?
                """;
        try (Connection conn = DriverManager.getConnection(inventoryJdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, reservationId);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to force expiry for reservation " + reservationId, e);
        }
    }

    public void checkConnectivity() {
        checkDb(inventoryJdbcUrl, "inventory_db");
        checkDb(orderJdbcUrl, "order_db");
        checkDb(paymentJdbcUrl, "payment_db");
    }

    private void checkDb(String url, String name) {
        try (Connection conn = openReadOnly(url);
             PreparedStatement ps = conn.prepareStatement("SELECT 1");
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                throw new AssertionError("Database " + name + " returned empty result for SELECT 1 at " + url);
            }
        } catch (SQLException e) {
            throw new AssertionError("Database " + name + " is unreachable at " + url + ": " + e.getMessage(), e);
        }
    }

    public Instant currentTimestampInInventoryDb() {
        if (memoryStore != null) {
            return memoryStore.currentTime;
        }
        try (Connection conn = openReadOnly(inventoryJdbcUrl);
             PreparedStatement ps = conn.prepareStatement("SELECT NOW()");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return toInstant(rs.getTimestamp(1));
            }
            return Instant.now();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read NOW() from inventory_db", e);
        }
    }

    public List<EventRow> findEvents(UUID optionalEventId) {
        if (memoryStore != null) {
            return memoryStore.events.stream()
                    .filter(e -> optionalEventId == null || optionalEventId.equals(e.eventId()))
                    .toList();
        }
        String sql = optionalEventId != null
                ? "SELECT event_id, name, status, created_at FROM events WHERE event_id = ?"
                : "SELECT event_id, name, status, created_at FROM events ORDER BY created_at ASC";
        try (Connection conn = openReadOnly(inventoryJdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (optionalEventId != null) {
                ps.setObject(1, optionalEventId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<EventRow> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(new EventRow(
                            getUuid(rs, "event_id"),
                            rs.getString("name"),
                            rs.getString("status"),
                            toInstant(rs.getTimestamp("created_at"))
                    ));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query events", e);
        }
    }

    public List<SeatRow> findSeats(UUID optionalEventId) {
        if (memoryStore != null) {
            return memoryStore.seats.stream()
                    .filter(s -> optionalEventId == null || optionalEventId.equals(s.eventId()))
                    .toList();
        }
        String sql = optionalEventId != null
                ? "SELECT seat_id, event_id, section_code, row_label, seat_number, price_cents, currency, status, version FROM seats WHERE event_id = ? ORDER BY section_code, row_label, seat_number, seat_id"
                : "SELECT seat_id, event_id, section_code, row_label, seat_number, price_cents, currency, status, version FROM seats ORDER BY event_id, section_code, row_label, seat_number, seat_id";
        try (Connection conn = openReadOnly(inventoryJdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (optionalEventId != null) {
                ps.setObject(1, optionalEventId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<SeatRow> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(new SeatRow(
                            getUuid(rs, "seat_id"),
                            getUuid(rs, "event_id"),
                            rs.getString("section_code"),
                            rs.getString("row_label"),
                            rs.getString("seat_number"),
                            rs.getLong("price_cents"),
                            rs.getString("currency") != null ? rs.getString("currency").trim() : null,
                            rs.getString("status"),
                            rs.getLong("version")
                    ));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query seats", e);
        }
    }

    public List<ReservationRow> findReservations(UUID optionalEventId) {
        if (memoryStore != null) {
            return memoryStore.reservations.stream()
                    .filter(r -> optionalEventId == null || optionalEventId.equals(r.eventId()))
                    .toList();
        }
        String sql = optionalEventId != null
                ? "SELECT reservation_id, event_id, user_id, status, total_amount_cents, currency, created_at, expires_at, confirmed_at, released_at, expired_at FROM reservations WHERE event_id = ? ORDER BY created_at ASC"
                : "SELECT reservation_id, event_id, user_id, status, total_amount_cents, currency, created_at, expires_at, confirmed_at, released_at, expired_at FROM reservations ORDER BY created_at ASC";
        try (Connection conn = openReadOnly(inventoryJdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (optionalEventId != null) {
                ps.setObject(1, optionalEventId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<ReservationRow> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(mapReservation(rs));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query reservations", e);
        }
    }

    public Optional<ReservationRow> findReservationById(UUID reservationId) {
        if (memoryStore != null) {
            return memoryStore.reservations.stream()
                    .filter(r -> reservationId.equals(r.reservationId()))
                    .findFirst();
        }
        String sql = "SELECT reservation_id, event_id, user_id, status, total_amount_cents, currency, created_at, expires_at, confirmed_at, released_at, expired_at FROM reservations WHERE reservation_id = ?";
        try (Connection conn = openReadOnly(inventoryJdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, reservationId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapReservation(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query reservation " + reservationId, e);
        }
    }

    private ReservationRow mapReservation(ResultSet rs) throws SQLException {
        return new ReservationRow(
                getUuid(rs, "reservation_id"),
                getUuid(rs, "event_id"),
                getUuid(rs, "user_id"),
                rs.getString("status"),
                rs.getLong("total_amount_cents"),
                rs.getString("currency") != null ? rs.getString("currency").trim() : null,
                toInstant(rs.getTimestamp("created_at")),
                toInstant(rs.getTimestamp("expires_at")),
                toInstant(rs.getTimestamp("confirmed_at")),
                toInstant(rs.getTimestamp("released_at")),
                toInstant(rs.getTimestamp("expired_at"))
        );
    }

    public List<ReservationItemRow> findReservationItems(UUID optionalEventId) {
        if (memoryStore != null) {
            return memoryStore.reservationItems.stream()
                    .filter(i -> optionalEventId == null || optionalEventId.equals(i.eventId()))
                    .toList();
        }
        String sql = optionalEventId != null
                ? "SELECT reservation_item_id, reservation_id, event_id, seat_id, price_cents, status FROM reservation_items WHERE event_id = ?"
                : "SELECT reservation_item_id, reservation_id, event_id, seat_id, price_cents, status FROM reservation_items";
        try (Connection conn = openReadOnly(inventoryJdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (optionalEventId != null) {
                ps.setObject(1, optionalEventId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<ReservationItemRow> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(new ReservationItemRow(
                            getUuid(rs, "reservation_item_id"),
                            getUuid(rs, "reservation_id"),
                            getUuid(rs, "event_id"),
                            getUuid(rs, "seat_id"),
                            rs.getLong("price_cents"),
                            rs.getString("status")
                    ));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query reservation_items", e);
        }
    }

    public List<OrderRow> findOrders(UUID optionalEventId) {
        if (memoryStore != null) {
            return memoryStore.orders.stream()
                    .filter(o -> optionalEventId == null || optionalEventId.equals(o.eventId()))
                    .toList();
        }
        String sql = optionalEventId != null
                ? "SELECT order_id, reservation_id, user_id, event_id, status, total_amount_cents, currency, payment_method_token, payment_id, cancellation_reason, reserved_at, expires_at, checkout_at, confirmed_at, cancelled_at, refunded_at, created_at FROM orders WHERE event_id = ? ORDER BY created_at ASC"
                : "SELECT order_id, reservation_id, user_id, event_id, status, total_amount_cents, currency, payment_method_token, payment_id, cancellation_reason, reserved_at, expires_at, checkout_at, confirmed_at, cancelled_at, refunded_at, created_at FROM orders ORDER BY created_at ASC";
        try (Connection conn = openReadOnly(orderJdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (optionalEventId != null) {
                ps.setObject(1, optionalEventId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<OrderRow> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(mapOrder(rs));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query orders", e);
        }
    }

    public Optional<OrderRow> findOrderById(UUID orderId) {
        if (memoryStore != null) {
            return memoryStore.orders.stream()
                    .filter(o -> orderId.equals(o.orderId()))
                    .findFirst();
        }
        String sql = "SELECT order_id, reservation_id, user_id, event_id, status, total_amount_cents, currency, payment_method_token, payment_id, cancellation_reason, reserved_at, expires_at, checkout_at, confirmed_at, cancelled_at, refunded_at, created_at FROM orders WHERE order_id = ?";
        try (Connection conn = openReadOnly(orderJdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, orderId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapOrder(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query order " + orderId, e);
        }
    }

    private OrderRow mapOrder(ResultSet rs) throws SQLException {
        return new OrderRow(
                getUuid(rs, "order_id"),
                getUuid(rs, "reservation_id"),
                getUuid(rs, "user_id"),
                getUuid(rs, "event_id"),
                rs.getString("status"),
                rs.getLong("total_amount_cents"),
                rs.getString("currency") != null ? rs.getString("currency").trim() : null,
                rs.getString("payment_method_token"),
                getUuid(rs, "payment_id"),
                rs.getString("cancellation_reason"),
                toInstant(rs.getTimestamp("reserved_at")),
                toInstant(rs.getTimestamp("expires_at")),
                toInstant(rs.getTimestamp("checkout_at")),
                toInstant(rs.getTimestamp("confirmed_at")),
                toInstant(rs.getTimestamp("cancelled_at")),
                toInstant(rs.getTimestamp("refunded_at")),
                toInstant(rs.getTimestamp("created_at"))
        );
    }

    public List<OrderItemRow> findOrderItemsByOrderIds(Collection<UUID> orderIds) {
        if (memoryStore != null) {
            if (orderIds == null) {
                return List.copyOf(memoryStore.orderItems);
            }
            Set<UUID> set = new HashSet<>(orderIds);
            return memoryStore.orderItems.stream()
                    .filter(i -> set.contains(i.orderId()))
                    .toList();
        }
        if (orderIds != null && orderIds.isEmpty()) {
            return List.of();
        }
        String sql;
        List<UUID> idList = orderIds == null ? List.of() : new ArrayList<>(orderIds);
        if (orderIds == null) {
            sql = "SELECT order_item_id, order_id, seat_id, price_cents FROM order_items";
        } else {
            String placeholders = String.join(",", Collections.nCopies(idList.size(), "?"));
            sql = "SELECT order_item_id, order_id, seat_id, price_cents FROM order_items WHERE order_id IN (" + placeholders + ")";
        }
        try (Connection conn = openReadOnly(orderJdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < idList.size(); i++) {
                ps.setObject(i + 1, idList.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<OrderItemRow> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(new OrderItemRow(
                            getUuid(rs, "order_item_id"),
                            getUuid(rs, "order_id"),
                            getUuid(rs, "seat_id"),
                            rs.getLong("price_cents")
                    ));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query order_items", e);
        }
    }

    public List<PaymentRow> findPaymentsByOrderIds(Collection<UUID> orderIds) {
        if (memoryStore != null) {
            if (orderIds == null) {
                return List.copyOf(memoryStore.payments);
            }
            Set<UUID> set = new HashSet<>(orderIds);
            return memoryStore.payments.stream()
                    .filter(p -> set.contains(p.orderId()))
                    .toList();
        }
        if (orderIds != null && orderIds.isEmpty()) {
            return List.of();
        }
        String sql;
        List<UUID> idList = orderIds == null ? List.of() : new ArrayList<>(orderIds);
        if (orderIds == null) {
            sql = "SELECT payment_id, order_id, reservation_id, user_id, amount_cents, currency, payment_method_token, status, provider_reference, failure_code, refund_id, refund_reason, created_at, paid_at, failed_at, refunded_at FROM payments ORDER BY created_at ASC";
        } else {
            String placeholders = String.join(",", Collections.nCopies(idList.size(), "?"));
            sql = "SELECT payment_id, order_id, reservation_id, user_id, amount_cents, currency, payment_method_token, status, provider_reference, failure_code, refund_id, refund_reason, created_at, paid_at, failed_at, refunded_at FROM payments WHERE order_id IN (" + placeholders + ") ORDER BY created_at ASC";
        }
        try (Connection conn = openReadOnly(paymentJdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < idList.size(); i++) {
                ps.setObject(i + 1, idList.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<PaymentRow> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(mapPayment(rs));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query payments", e);
        }
    }

    public Optional<PaymentRow> findPaymentByOrderId(UUID orderId) {
        List<PaymentRow> list = findPaymentsByOrderIds(List.of(orderId));
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    private PaymentRow mapPayment(ResultSet rs) throws SQLException {
        return new PaymentRow(
                getUuid(rs, "payment_id"),
                getUuid(rs, "order_id"),
                getUuid(rs, "reservation_id"),
                getUuid(rs, "user_id"),
                rs.getLong("amount_cents"),
                rs.getString("currency") != null ? rs.getString("currency").trim() : null,
                rs.getString("payment_method_token"),
                rs.getString("status"),
                rs.getString("provider_reference"),
                rs.getString("failure_code"),
                getUuid(rs, "refund_id"),
                rs.getString("refund_reason"),
                toInstant(rs.getTimestamp("created_at")),
                toInstant(rs.getTimestamp("paid_at")),
                toInstant(rs.getTimestamp("failed_at")),
                toInstant(rs.getTimestamp("refunded_at"))
        );
    }

    public List<OutboxRow> findInventoryOutboxByAggregateIds(Collection<UUID> aggregateIds) {
        if (memoryStore != null) {
            return filterMemoryOutbox(memoryStore.inventoryOutbox, aggregateIds);
        }
        return findOutboxByAggregateIds(inventoryJdbcUrl, "inventory_db", aggregateIds);
    }

    public List<OutboxRow> findOrderOutboxByAggregateIds(Collection<UUID> aggregateIds) {
        if (memoryStore != null) {
            return filterMemoryOutbox(memoryStore.orderOutbox, aggregateIds);
        }
        return findOutboxByAggregateIds(orderJdbcUrl, "order_db", aggregateIds);
    }

    public List<OutboxRow> findPaymentOutboxByAggregateIds(Collection<UUID> aggregateIds) {
        if (memoryStore != null) {
            return filterMemoryOutbox(memoryStore.paymentOutbox, aggregateIds);
        }
        return findOutboxByAggregateIds(paymentJdbcUrl, "payment_db", aggregateIds);
    }

    private static List<OutboxRow> filterMemoryOutbox(List<OutboxRow> source, Collection<UUID> aggregateIds) {
        if (aggregateIds == null) {
            return List.copyOf(source);
        }
        Set<UUID> set = new HashSet<>(aggregateIds);
        return source.stream()
                .filter(r -> set.contains(r.aggregateId()))
                .toList();
    }

    public List<OutboxRow> findAllOutboxSince(Instant since) {
        if (memoryStore != null) {
            List<OutboxRow> all = new ArrayList<>();
            all.addAll(memoryStore.inventoryOutbox);
            all.addAll(memoryStore.orderOutbox);
            all.addAll(memoryStore.paymentOutbox);
            if (since == null) {
                return all;
            }
            return all.stream()
                    .filter(r -> r.createdAt() != null && !r.createdAt().isBefore(since))
                    .toList();
        }
        List<OutboxRow> all = new ArrayList<>();
        all.addAll(findOutboxSince(inventoryJdbcUrl, "inventory_db", since));
        all.addAll(findOutboxSince(orderJdbcUrl, "order_db", since));
        all.addAll(findOutboxSince(paymentJdbcUrl, "payment_db", since));
        return all;
    }

    public List<OutboxRow> findUnpublishedOutboxSince(Instant since) {
        return findAllOutboxSince(since).stream()
                .filter(r -> r.publishedAt() == null)
                .toList();
    }

    public long countUnpublishedOutbox(String dbName, UUID eventId) {
        List<UUID> aggIds = new ArrayList<>();
        for (ReservationRow r : findReservations(eventId)) {
            aggIds.add(r.reservationId());
        }
        for (OrderRow o : findOrders(eventId)) {
            if (!aggIds.contains(o.orderId())) {
                aggIds.add(o.orderId());
            }
        }
        List<OutboxRow> rows = switch (dbName) {
            case "inventory_db" -> findInventoryOutboxByAggregateIds(aggIds);
            case "order_db" -> findOrderOutboxByAggregateIds(aggIds);
            case "payment_db" -> findPaymentOutboxByAggregateIds(aggIds);
            default -> throw new IllegalArgumentException("Unknown dbName: " + dbName);
        };
        return rows.stream().filter(r -> r.publishedAt() == null).count();
    }

    private List<OutboxRow> findOutboxByAggregateIds(String jdbcUrl, String dbName, Collection<UUID> aggregateIds) {
        if (aggregateIds != null && aggregateIds.isEmpty()) {
            return List.of();
        }
        String sql;
        List<UUID> idList = aggregateIds == null ? List.of() : new ArrayList<>(aggregateIds);
        if (aggregateIds == null) {
            sql = "SELECT event_id, aggregate_type, aggregate_id, topic, event_type, correlation_id, schema_version, payload::text AS payload, created_at, published_at FROM outbox_events ORDER BY created_at ASC";
        } else {
            String placeholders = String.join(",", Collections.nCopies(idList.size(), "?"));
            sql = "SELECT event_id, aggregate_type, aggregate_id, topic, event_type, correlation_id, schema_version, payload::text AS payload, created_at, published_at FROM outbox_events WHERE aggregate_id IN (" + placeholders + ") ORDER BY created_at ASC";
        }
        try (Connection conn = openReadOnly(jdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < idList.size(); i++) {
                ps.setObject(i + 1, idList.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<OutboxRow> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(mapOutbox(dbName, rs));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query outbox_events in " + dbName, e);
        }
    }

    private List<OutboxRow> findOutboxSince(String jdbcUrl, String dbName, Instant since) {
        String sql = since != null
                ? "SELECT event_id, aggregate_type, aggregate_id, topic, event_type, correlation_id, schema_version, payload::text AS payload, created_at, published_at FROM outbox_events WHERE created_at >= ? ORDER BY created_at ASC"
                : "SELECT event_id, aggregate_type, aggregate_id, topic, event_type, correlation_id, schema_version, payload::text AS payload, created_at, published_at FROM outbox_events ORDER BY created_at ASC";
        try (Connection conn = openReadOnly(jdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (since != null) {
                ps.setTimestamp(1, Timestamp.from(since));
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<OutboxRow> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(mapOutbox(dbName, rs));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query outbox_events in " + dbName, e);
        }
    }

    private OutboxRow mapOutbox(String dbName, ResultSet rs) throws SQLException {
        return new OutboxRow(
                dbName,
                getUuid(rs, "event_id"),
                rs.getString("aggregate_type"),
                getUuid(rs, "aggregate_id"),
                rs.getString("topic"),
                rs.getString("event_type"),
                getUuid(rs, "correlation_id"),
                rs.getInt("schema_version"),
                rs.getString("payload"),
                toInstant(rs.getTimestamp("created_at")),
                toInstant(rs.getTimestamp("published_at"))
        );
    }

    private Connection openReadOnly(String url) throws SQLException {
        Connection conn = DriverManager.getConnection(url, username, password);
        conn.setReadOnly(true);
        return conn;
    }

    private static UUID getUuid(ResultSet rs, String col) throws SQLException {
        Object val = rs.getObject(col);
        if (val == null) {
            return null;
        }
        if (val instanceof UUID u) {
            return u;
        }
        return UUID.fromString(val.toString().trim());
    }

    private static Instant toInstant(Timestamp ts) {
        return ts != null ? ts.toInstant() : null;
    }
}
