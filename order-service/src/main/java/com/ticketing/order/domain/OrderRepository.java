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

    private static final String SELECT_ORDER_BY_ID_FOR_CHECKOUT_SQL = """
            SELECT order_id, reservation_id, user_id, event_id, status, total_amount_cents,
                   currency, payment_method_token, payment_id, cancellation_reason, version,
                   reserved_at, expires_at, checkout_at, paid_at, confirmed_at, cancelled_at,
                   refunded_at, created_at, updated_at,
                   clock_timestamp() AS db_clock,
                   (expires_at - clock_timestamp() >= INTERVAL '15 seconds') AS grace_buffer_valid
            FROM orders
            WHERE order_id = ?
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
    public Optional<OrderCheckoutLockResult> findByIdForCheckout(UUID orderId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        List<OrderCheckoutLockResult> results = jdbcTemplate.query(
                SELECT_ORDER_BY_ID_FOR_CHECKOUT_SQL,
                CHECKOUT_LOCK_MAPPER,
                orderId
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

    private static Instant toInstant(Timestamp ts) {
        return ts != null ? ts.toInstant() : null;
    }

    private static void setTimestamp(PreparedStatement ps, int paramIndex, Instant instant) throws SQLException {
        if (instant != null) {
            ps.setTimestamp(paramIndex, Timestamp.from(instant));
        } else {
            ps.setNull(paramIndex, Types.TIMESTAMP_WITH_TIMEZONE);
        }
    }
}

