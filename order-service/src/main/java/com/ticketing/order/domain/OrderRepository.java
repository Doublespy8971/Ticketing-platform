package com.ticketing.order.domain;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
public class OrderRepository {

    private static final String INSERT_ORDER_SQL = """
            INSERT INTO orders (
                order_id, reservation_id, user_id, event_id, status, total_amount_cents,
                currency, payment_method_token, payment_id, cancellation_reason, version,
                reserved_at, expires_at, checkout_at, paid_at, confirmed_at, cancelled_at,
                refunded_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String INSERT_ORDER_IF_ABSENT_SQL = """
            INSERT INTO orders (
                order_id, reservation_id, user_id, event_id, status, total_amount_cents,
                currency, payment_method_token, payment_id, cancellation_reason, version,
                reserved_at, expires_at, checkout_at, paid_at, confirmed_at, cancelled_at,
                refunded_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, clock_timestamp(), clock_timestamp())
            ON CONFLICT (order_id) DO NOTHING
            RETURNING created_at
            """;

    private static final String INSERT_ITEM_SQL = """
            INSERT INTO order_items (
                order_item_id, order_id, seat_id, section_code, row_label, seat_number, price_cents
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_ORDER_BY_ID_SQL = """
            SELECT order_id, reservation_id, user_id, event_id, status, total_amount_cents,
                   currency, payment_method_token, payment_id, cancellation_reason, version,
                   reserved_at, expires_at, checkout_at, paid_at, confirmed_at, cancelled_at,
                   refunded_at, created_at, updated_at
            FROM orders
            WHERE order_id = ?
            """;

    private static final String SELECT_ORDER_BY_ID_FOR_UPDATE_SQL = """
            SELECT order_id, reservation_id, user_id, event_id, status, total_amount_cents,
                   currency, payment_method_token, payment_id, cancellation_reason, version,
                   reserved_at, expires_at, checkout_at, paid_at, confirmed_at, cancelled_at,
                   refunded_at, created_at, updated_at
            FROM orders
            WHERE order_id = ?
            FOR UPDATE
            """;

    private static final String SELECT_ITEMS_BY_ORDER_ID_SQL = """
            SELECT order_item_id, order_id, seat_id, section_code, row_label, seat_number, price_cents
            FROM order_items
            WHERE order_id = ?
            """;

    private static final String SELECT_ORDER_BY_ID_AND_USER_ID_FOR_CHECKOUT_SQL = """
            SELECT order_id, reservation_id, user_id, event_id, status, total_amount_cents,
                   currency, payment_method_token, payment_id, cancellation_reason, version,
                   reserved_at, expires_at, checkout_at, paid_at, confirmed_at, cancelled_at,
                   refunded_at, created_at, updated_at,
                   clock_timestamp() AS db_clock,
                   (expires_at - clock_timestamp() >= INTERVAL '15 seconds') AS grace_buffer_valid
            FROM orders
            WHERE order_id = ? AND user_id = ?
            FOR UPDATE
            """;

    private static final String UPDATE_ORDER_FOR_CHECKOUT_SQL = """
            UPDATE orders
            SET status = 'PAYMENT_PENDING',
                payment_method_token = ?,
                checkout_at = clock_timestamp(),
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE order_id = ?
              AND status = 'RESERVED'
            RETURNING checkout_at, version, updated_at
            """;

    private static final String CANCEL_EXPIRED_FROM_RESERVED_SQL = """
            UPDATE orders
            SET status = 'CANCELLED_EXPIRED',
                cancellation_reason = 'RESERVATION_EXPIRED',
                cancelled_at = clock_timestamp(),
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE order_id = ?
              AND status = 'RESERVED'
            RETURNING cancelled_at
            """;

    private static final String CANCEL_EXPIRED_FROM_PAYMENT_PENDING_NO_PAYMENT_SQL = """
            UPDATE orders
            SET status = 'CANCELLED_EXPIRED',
                cancellation_reason = 'RESERVATION_EXPIRED',
                cancelled_at = clock_timestamp(),
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE order_id = ?
              AND status = 'PAYMENT_PENDING'
              AND payment_id IS NULL
            RETURNING cancelled_at
            """;

    private static final String CONFIRM_FROM_PAYMENT_PENDING_SQL = """
            UPDATE orders
            SET status = 'CONFIRMED',
                payment_id = COALESCE(payment_id, ?),
                paid_at = COALESCE(paid_at, clock_timestamp()),
                confirmed_at = clock_timestamp(),
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE order_id = ?
              AND status = 'PAYMENT_PENDING'
            RETURNING payment_id, confirmed_at
            """;

    private static final String TRANSITION_TO_REFUND_PENDING_SQL = """
            UPDATE orders
            SET status = 'REFUND_PENDING',
                payment_id = COALESCE(?, payment_id),
                paid_at = COALESCE(paid_at, ?),
                cancellation_reason = ?,
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE order_id = ?
              AND status = ?
            RETURNING updated_at
            """;

    private static final String RECORD_PAYMENT_IN_PAYMENT_PENDING_SQL = """
            UPDATE orders
            SET payment_id = ?,
                paid_at = COALESCE(?, clock_timestamp()),
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE order_id = ?
              AND status = 'PAYMENT_PENDING'
            """;

    private static final String BACKFILL_PAYMENT_IN_CONFIRMED_SQL = """
            UPDATE orders
            SET payment_id = COALESCE(payment_id, ?),
                paid_at = COALESCE(paid_at, ?, clock_timestamp()),
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE order_id = ?
              AND status = 'CONFIRMED'
            """;

    private static final String CANCEL_PAYMENT_FAILED_FROM_PAYMENT_PENDING_SQL = """
            UPDATE orders
            SET status = 'CANCELLED_PAYMENT_FAILED',
                payment_id = COALESCE(payment_id, ?),
                cancellation_reason = 'PAYMENT_FAILED',
                cancelled_at = clock_timestamp(),
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE order_id = ?
              AND status = 'PAYMENT_PENDING'
            RETURNING cancelled_at
            """;

    private static final String CANCEL_REFUNDED_FROM_REFUND_PENDING_SQL = """
            UPDATE orders
            SET status = 'CANCELLED_REFUNDED',
                payment_id = COALESCE(payment_id, ?),
                refunded_at = clock_timestamp(),
                cancelled_at = COALESCE(cancelled_at, clock_timestamp()),
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE order_id = ?
              AND status = 'REFUND_PENDING'
            RETURNING refunded_at
            """;

    public record OrderCheckoutLockResult(
            OrderRecord order,
            Instant dbClock,
            boolean graceBufferValid
    ) {}

    public record OrderCheckoutUpdateResult(
            Instant checkoutAt,
            long version,
            Instant updatedAt
    ) {}

    public record OrderConfirmedUpdateResult(
            UUID paymentId,
            Instant confirmedAt
    ) {}

    private static final RowMapper<OrderRecord> ORDER_ROW_MAPPER = (rs, rowNum) -> new OrderRecord(
            rs.getObject("order_id", UUID.class),
            rs.getObject("reservation_id", UUID.class),
            rs.getObject("user_id", UUID.class),
            rs.getObject("event_id", UUID.class),
            rs.getString("status"),
            rs.getLong("total_amount_cents"),
            rs.getString("currency"),
            rs.getString("payment_method_token"),
            rs.getObject("payment_id", UUID.class),
            rs.getString("cancellation_reason"),
            rs.getLong("version"),
            toInstant(rs.getTimestamp("reserved_at")),
            toInstant(rs.getTimestamp("expires_at")),
            toInstant(rs.getTimestamp("checkout_at")),
            toInstant(rs.getTimestamp("paid_at")),
            toInstant(rs.getTimestamp("confirmed_at")),
            toInstant(rs.getTimestamp("cancelled_at")),
            toInstant(rs.getTimestamp("refunded_at")),
            toInstant(rs.getTimestamp("created_at")),
            toInstant(rs.getTimestamp("updated_at"))
    );

    private static final RowMapper<OrderCheckoutLockResult> CHECKOUT_LOCK_MAPPER = (rs, rowNum) -> {
        OrderRecord order = ORDER_ROW_MAPPER.mapRow(rs, rowNum);
        Instant dbClock = toInstant(rs.getTimestamp("db_clock"));
        boolean graceBufferValid = rs.getBoolean("grace_buffer_valid");
        return new OrderCheckoutLockResult(order, dbClock, graceBufferValid);
    };

    private static final RowMapper<OrderItemRecord> ORDER_ITEM_ROW_MAPPER = (rs, rowNum) -> new OrderItemRecord(
            rs.getObject("order_item_id", UUID.class),
            rs.getObject("order_id", UUID.class),
            rs.getObject("seat_id", UUID.class),
            rs.getString("section_code"),
            rs.getString("row_label"),
            rs.getString("seat_number"),
            rs.getLong("price_cents")
    );

    private final JdbcTemplate jdbcTemplate;

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void insert(OrderRecord order, List<OrderItemRecord> items) {
        Objects.requireNonNull(order, "order must not be null");
        Objects.requireNonNull(items, "items must not be null");

        jdbcTemplate.update(INSERT_ORDER_SQL, ps -> {
            ps.setObject(1, order.orderId());
            ps.setObject(2, order.reservationId());
            ps.setObject(3, order.userId());
            ps.setObject(4, order.eventId());
            ps.setString(5, order.status());
            ps.setLong(6, order.totalAmountCents());
            ps.setString(7, order.currency());
            ps.setString(8, order.paymentMethodToken());
            ps.setObject(9, order.paymentId());
            ps.setString(10, order.cancellationReason());
            ps.setLong(11, order.version());
            setTimestamp(ps, 12, order.reservedAt());
            setTimestamp(ps, 13, order.expiresAt());
            setTimestamp(ps, 14, order.checkoutAt());
            setTimestamp(ps, 15, order.paidAt());
            setTimestamp(ps, 16, order.confirmedAt());
            setTimestamp(ps, 17, order.cancelledAt());
            setTimestamp(ps, 18, order.refundedAt());
            setTimestamp(ps, 19, order.createdAt());
            setTimestamp(ps, 20, order.updatedAt());
        });

        if (!items.isEmpty()) {
            insertItems(items);
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<Instant> insertIfAbsent(OrderRecord order, List<OrderItemRecord> items) {
        Objects.requireNonNull(order, "order must not be null");
        Objects.requireNonNull(items, "items must not be null");

        List<Instant> results = jdbcTemplate.query(
                INSERT_ORDER_IF_ABSENT_SQL,
                ps -> {
                    ps.setObject(1, order.orderId());
                    ps.setObject(2, order.reservationId());
                    ps.setObject(3, order.userId());
                    ps.setObject(4, order.eventId());
                    ps.setString(5, order.status());
                    ps.setLong(6, order.totalAmountCents());
                    ps.setString(7, order.currency());
                    ps.setString(8, order.paymentMethodToken());
                    setUuid(ps, 9, order.paymentId());
                    ps.setString(10, order.cancellationReason());
                    ps.setLong(11, order.version());
                    setTimestamp(ps, 12, order.reservedAt());
                    setTimestamp(ps, 13, order.expiresAt());
                    setTimestamp(ps, 14, order.checkoutAt());
                    setTimestamp(ps, 15, order.paidAt());
                    setTimestamp(ps, 16, order.confirmedAt());
                    setTimestamp(ps, 17, order.cancelledAt());
                    setTimestamp(ps, 18, order.refundedAt());
                },
                (rs, rowNum) -> toInstant(rs.getTimestamp("created_at"))
        );

        if (results.isEmpty()) {
            return Optional.empty();
        }

        if (!items.isEmpty()) {
            insertItems(items);
        }
        return Optional.of(results.get(0));
    }

    private void insertItems(List<OrderItemRecord> items) {
        jdbcTemplate.batchUpdate(INSERT_ITEM_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                OrderItemRecord item = items.get(i);
                ps.setObject(1, item.orderItemId());
                ps.setObject(2, item.orderId());
                ps.setObject(3, item.seatId());
                ps.setString(4, item.sectionCode());
                ps.setString(5, item.rowLabel());
                ps.setString(6, item.seatNumber());
                ps.setLong(7, item.priceCents());
            }

            @Override
            public int getBatchSize() {
                return items.size();
            }
        });
    }

    public Optional<OrderRecord> findById(UUID orderId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        List<OrderRecord> results = jdbcTemplate.query(
                SELECT_ORDER_BY_ID_SQL,
                ORDER_ROW_MAPPER,
                orderId
        );
        return results.stream().findFirst();
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<OrderRecord> findByIdForUpdate(UUID orderId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        List<OrderRecord> results = jdbcTemplate.query(
                SELECT_ORDER_BY_ID_FOR_UPDATE_SQL,
                ORDER_ROW_MAPPER,
                orderId
        );
        return results.stream().findFirst();
    }

    public List<OrderItemRecord> findItemsByOrderId(UUID orderId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        return jdbcTemplate.query(
                SELECT_ITEMS_BY_ORDER_ID_SQL,
                ORDER_ITEM_ROW_MAPPER,
                orderId
        );
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<OrderCheckoutLockResult> findByIdAndUserIdForCheckout(UUID orderId, UUID userId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(userId, "userId must not be null");
        List<OrderCheckoutLockResult> results = jdbcTemplate.query(
                SELECT_ORDER_BY_ID_AND_USER_ID_FOR_CHECKOUT_SQL,
                CHECKOUT_LOCK_MAPPER,
                orderId,
                userId
        );
        return results.stream().findFirst();
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OrderCheckoutUpdateResult updateStatusToPaymentPending(UUID orderId, String paymentMethodToken) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(paymentMethodToken, "paymentMethodToken must not be null");

        List<OrderCheckoutUpdateResult> results = jdbcTemplate.query(
                UPDATE_ORDER_FOR_CHECKOUT_SQL,
                (rs, rowNum) -> new OrderCheckoutUpdateResult(
                        toInstant(rs.getTimestamp("checkout_at")),
                        rs.getLong("version"),
                        toInstant(rs.getTimestamp("updated_at"))
                ),
                paymentMethodToken,
                orderId
        );
        if (results.isEmpty()) {
            throw new IllegalStateException("Failed to transition order " + orderId + " to PAYMENT_PENDING");
        }
        return results.get(0);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Instant cancelExpiredFromReserved(UUID orderId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        List<Instant> results = jdbcTemplate.query(
                CANCEL_EXPIRED_FROM_RESERVED_SQL,
                ps -> ps.setObject(1, orderId),
                (rs, rowNum) -> toInstant(rs.getTimestamp("cancelled_at"))
        );
        if (results.isEmpty()) {
            throw new IllegalStateException("Failed to transition order " + orderId + " from RESERVED to CANCELLED_EXPIRED");
        }
        return results.get(0);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Instant cancelExpiredFromPaymentPendingWithoutPayment(UUID orderId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        List<Instant> results = jdbcTemplate.query(
                CANCEL_EXPIRED_FROM_PAYMENT_PENDING_NO_PAYMENT_SQL,
                ps -> ps.setObject(1, orderId),
                (rs, rowNum) -> toInstant(rs.getTimestamp("cancelled_at"))
        );
        if (results.isEmpty()) {
            throw new IllegalStateException("Failed to transition order " + orderId + " from PAYMENT_PENDING (payment_id IS NULL) to CANCELLED_EXPIRED");
        }
        return results.get(0);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OrderConfirmedUpdateResult confirmFromPaymentPending(UUID orderId, UUID paymentId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        List<OrderConfirmedUpdateResult> results = jdbcTemplate.query(
                CONFIRM_FROM_PAYMENT_PENDING_SQL,
                ps -> {
                    setUuid(ps, 1, paymentId);
                    ps.setObject(2, orderId);
                },
                (rs, rowNum) -> new OrderConfirmedUpdateResult(
                        rs.getObject("payment_id", UUID.class),
                        toInstant(rs.getTimestamp("confirmed_at"))
                )
        );
        if (results.isEmpty()) {
            throw new IllegalStateException("Failed to transition order " + orderId + " from PAYMENT_PENDING to CONFIRMED");
        }
        return results.get(0);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Instant transitionToRefundPending(
            UUID orderId,
            String expectedStatus,
            UUID paymentId,
            Instant paidAt,
            String cancellationReason
    ) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(expectedStatus, "expectedStatus must not be null");
        List<Instant> results = jdbcTemplate.query(
                TRANSITION_TO_REFUND_PENDING_SQL,
                ps -> {
                    setUuid(ps, 1, paymentId);
                    setTimestamp(ps, 2, paidAt);
                    ps.setString(3, cancellationReason);
                    ps.setObject(4, orderId);
                    ps.setString(5, expectedStatus);
                },
                (rs, rowNum) -> toInstant(rs.getTimestamp("updated_at"))
        );
        if (results.isEmpty()) {
            throw new IllegalStateException("Failed to transition order " + orderId + " from " + expectedStatus + " to REFUND_PENDING");
        }
        return results.get(0);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void recordPaymentInPaymentPending(UUID orderId, UUID paymentId, Instant paidAt) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        int updated = jdbcTemplate.update(RECORD_PAYMENT_IN_PAYMENT_PENDING_SQL, ps -> {
            ps.setObject(1, paymentId);
            setTimestamp(ps, 2, paidAt);
            ps.setObject(3, orderId);
        });
        if (updated != 1) {
            throw new IllegalStateException("Failed to record payment in PAYMENT_PENDING for order " + orderId);
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void backfillPaymentInConfirmed(UUID orderId, UUID paymentId, Instant paidAt) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        int updated = jdbcTemplate.update(BACKFILL_PAYMENT_IN_CONFIRMED_SQL, ps -> {
            setUuid(ps, 1, paymentId);
            setTimestamp(ps, 2, paidAt);
            ps.setObject(3, orderId);
        });
        if (updated != 1) {
            throw new IllegalStateException("Failed to backfill payment in CONFIRMED for order " + orderId);
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Instant cancelPaymentFailedFromPaymentPending(UUID orderId, UUID paymentId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        List<Instant> results = jdbcTemplate.query(
                CANCEL_PAYMENT_FAILED_FROM_PAYMENT_PENDING_SQL,
                ps -> {
                    setUuid(ps, 1, paymentId);
                    ps.setObject(2, orderId);
                },
                (rs, rowNum) -> toInstant(rs.getTimestamp("cancelled_at"))
        );
        if (results.isEmpty()) {
            throw new IllegalStateException("Failed to transition order " + orderId + " from PAYMENT_PENDING to CANCELLED_PAYMENT_FAILED");
        }
        return results.get(0);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Instant cancelRefundedFromRefundPending(UUID orderId, UUID paymentId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        List<Instant> results = jdbcTemplate.query(
                CANCEL_REFUNDED_FROM_REFUND_PENDING_SQL,
                ps -> {
                    setUuid(ps, 1, paymentId);
                    ps.setObject(2, orderId);
                },
                (rs, rowNum) -> toInstant(rs.getTimestamp("refunded_at"))
        );
        if (results.isEmpty()) {
            throw new IllegalStateException("Failed to transition order " + orderId + " from REFUND_PENDING to CANCELLED_REFUNDED");
        }
        return results.get(0);
    }

    private static Instant toInstant(Timestamp ts) {
        return ts != null ? ts.toInstant() : null;
    }

    private static void setUuid(PreparedStatement ps, int paramIndex, UUID uuid) throws SQLException {
        if (uuid != null) {
            ps.setObject(paramIndex, uuid);
        } else {
            ps.setNull(paramIndex, Types.OTHER);
        }
    }

    private static void setTimestamp(PreparedStatement ps, int paramIndex, Instant instant) throws SQLException {
        if (instant != null) {
            ps.setTimestamp(paramIndex, Timestamp.from(instant));
        } else {
            ps.setNull(paramIndex, Types.TIMESTAMP_WITH_TIMEZONE);
        }
    }
}

