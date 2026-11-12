package com.ticketing.payment.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
public class PaymentRepository {

    private static final RowMapper<PaymentRecord> PAYMENT_ROW_MAPPER = PaymentRepository::mapRow;

    private static final String FIND_BY_ORDER_ID_SQL = """
            SELECT payment_id, order_id, reservation_id, user_id, amount_cents, currency,
                   payment_method_token, status, provider_reference, failure_code, failure_message,
                   refund_id, refund_reason, created_at, paid_at, failed_at, refunded_at, updated_at
            FROM payments
            WHERE order_id = ?
            """;

    private static final String FIND_BY_ID_SQL = """
            SELECT payment_id, order_id, reservation_id, user_id, amount_cents, currency,
                   payment_method_token, status, provider_reference, failure_code, failure_message,
                   refund_id, refund_reason, created_at, paid_at, failed_at, refunded_at, updated_at
            FROM payments
            WHERE payment_id = ?
            """;

    private static final String FIND_BY_ID_FOR_UPDATE_SQL = """
            SELECT payment_id, order_id, reservation_id, user_id, amount_cents, currency,
                   payment_method_token, status, provider_reference, failure_code, failure_message,
                   refund_id, refund_reason, created_at, paid_at, failed_at, refunded_at, updated_at
            FROM payments
            WHERE payment_id = ?
            FOR UPDATE
            """;

    private static final String INSERT_PAYMENT_SQL = """
            INSERT INTO payments (
                payment_id,
                order_id,
                reservation_id,
                user_id,
                amount_cents,
                currency,
                payment_method_token,
                status,
                provider_reference,
                failure_code,
                failure_message,
                created_at,
                paid_at,
                failed_at,
                updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                clock_timestamp(),
                CASE WHEN ? = 'SUCCEEDED' THEN clock_timestamp() ELSE NULL END,
                CASE WHEN ? = 'FAILED' THEN clock_timestamp() ELSE NULL END,
                clock_timestamp()
            )
            ON CONFLICT (order_id) DO NOTHING
            RETURNING payment_id, order_id, reservation_id, user_id, amount_cents, currency,
                      payment_method_token, status, provider_reference, failure_code, failure_message,
                      refund_id, refund_reason, created_at, paid_at, failed_at, refunded_at, updated_at
            """;

    private static final String UPDATE_REFUNDED_SQL = """
            UPDATE payments
            SET status = 'REFUNDED',
                refund_id = ?,
                refund_reason = ?,
                refunded_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE payment_id = ?
              AND status = 'SUCCEEDED'
            RETURNING refunded_at
            """;

    private final JdbcTemplate jdbcTemplate;

    public PaymentRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    }

    public Optional<PaymentRecord> findByOrderId(UUID orderId) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        List<PaymentRecord> list = jdbcTemplate.query(FIND_BY_ORDER_ID_SQL, PAYMENT_ROW_MAPPER, orderId);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    public Optional<PaymentRecord> findById(UUID paymentId) {
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        List<PaymentRecord> list = jdbcTemplate.query(FIND_BY_ID_SQL, PAYMENT_ROW_MAPPER, paymentId);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<PaymentRecord> findByIdForUpdate(UUID paymentId) {
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        List<PaymentRecord> list = jdbcTemplate.query(FIND_BY_ID_FOR_UPDATE_SQL, PAYMENT_ROW_MAPPER, paymentId);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<PaymentRecord> insertPayment(PaymentRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        String statusStr = record.status().name();
        List<PaymentRecord> results = jdbcTemplate.query(
                INSERT_PAYMENT_SQL,
                PAYMENT_ROW_MAPPER,
                record.paymentId(),
                record.orderId(),
                record.reservationId(),
                record.userId(),
                record.amountCents(),
                record.currency(),
                record.paymentMethodToken(),
                statusStr,
                record.providerReference(),
                record.failureCode(),
                record.failureMessage(),
                statusStr,
                statusStr
        );
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<Instant> updateRefunded(UUID paymentId, UUID refundId, String refundReason) {
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        Objects.requireNonNull(refundId, "refundId must not be null");
        List<Instant> results = jdbcTemplate.query(
                UPDATE_REFUNDED_SQL,
                (rs, rowNum) -> toInstant(rs.getTimestamp("refunded_at")),
                refundId,
                refundReason,
                paymentId
        );
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    private static PaymentRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new PaymentRecord(
                rs.getObject("payment_id", UUID.class),
                rs.getObject("order_id", UUID.class),
                rs.getObject("reservation_id", UUID.class),
                rs.getObject("user_id", UUID.class),
                rs.getLong("amount_cents"),
                rs.getString("currency"),
                rs.getString("payment_method_token"),
                PaymentStatus.valueOf(rs.getString("status")),
                rs.getString("provider_reference"),
                rs.getString("failure_code"),
                rs.getString("failure_message"),
                rs.getObject("refund_id", UUID.class),
                rs.getString("refund_reason"),
                toInstant(rs.getTimestamp("created_at")),
                toInstant(rs.getTimestamp("paid_at")),
                toInstant(rs.getTimestamp("failed_at")),
                toInstant(rs.getTimestamp("refunded_at")),
                toInstant(rs.getTimestamp("updated_at"))
        );
    }

    private static Instant toInstant(Timestamp ts) {
        return ts != null ? ts.toInstant() : null;
    }
}

