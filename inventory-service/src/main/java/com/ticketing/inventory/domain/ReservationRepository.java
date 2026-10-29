package com.ticketing.inventory.domain;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ReservationRepository {

    private static final String INSERT_RESERVATION_SQL = """
            INSERT INTO reservations (
                reservation_id, event_id, user_id, status, total_amount_cents, currency,
                created_at, expires_at, confirmed_at, released_at, expired_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String INSERT_ITEM_SQL = """
            INSERT INTO reservation_items (
                reservation_item_id, reservation_id, event_id, seat_id, section_code,
                row_label, seat_number, price_cents, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_RESERVATION_BY_ID_SQL = """
            SELECT reservation_id, event_id, user_id, status, total_amount_cents, currency,
                   created_at, expires_at, confirmed_at, released_at, expired_at, updated_at
            FROM reservations
            WHERE reservation_id = ?
            """;

    private static final String SELECT_ITEMS_BY_RESERVATION_ID_SQL = """
            SELECT reservation_item_id, reservation_id, event_id, seat_id, section_code,
                   row_label, seat_number, price_cents, status, created_at, updated_at
            FROM reservation_items
            WHERE reservation_id = ?
            """;

    private static final String SELECT_ACTIVE_HELD_SEATS_BY_EVENT_SQL = """
            SELECT ri.event_id,
                   ri.seat_id,
                   r.reservation_id,
                   GREATEST(1, FLOOR(EXTRACT(EPOCH FROM (r.expires_at - NOW()))))::bigint AS remaining_sec
            FROM reservation_items ri
            JOIN reservations r ON r.reservation_id = ri.reservation_id
            WHERE ri.event_id = ?
              AND r.status = 'HELD'
              AND r.expires_at > NOW()
            """;

    private static final String SELECT_ALL_ACTIVE_HELD_SEATS_SQL = """
            SELECT ri.event_id,
                   ri.seat_id,
                   r.reservation_id,
                   GREATEST(1, FLOOR(EXTRACT(EPOCH FROM (r.expires_at - NOW()))))::bigint AS remaining_sec
            FROM reservation_items ri
            JOIN reservations r ON r.reservation_id = ri.reservation_id
            WHERE r.status = 'HELD'
              AND r.expires_at > NOW()
            """;

    public record HeldSeatLock(UUID eventId, UUID seatId, UUID reservationId, long remainingSeconds) {}

    private static final RowMapper<HeldSeatLock> HELD_SEAT_LOCK_ROW_MAPPER = (rs, rowNum) -> new HeldSeatLock(
            rs.getObject("event_id", UUID.class),
            rs.getObject("seat_id", UUID.class),
            rs.getObject("reservation_id", UUID.class),
            rs.getLong("remaining_sec")
    );

    private static final RowMapper<ReservationRecord> RESERVATION_ROW_MAPPER = (rs, rowNum) -> new ReservationRecord(
            rs.getObject("reservation_id", UUID.class),
            rs.getObject("event_id", UUID.class),
            rs.getObject("user_id", UUID.class),
            rs.getString("status"),
            rs.getLong("total_amount_cents"),
            rs.getString("currency"),
            toInstant(rs.getObject("created_at", OffsetDateTime.class)),
            toInstant(rs.getObject("expires_at", OffsetDateTime.class)),
            toInstant(rs.getObject("confirmed_at", OffsetDateTime.class)),
            toInstant(rs.getObject("released_at", OffsetDateTime.class)),
            toInstant(rs.getObject("expired_at", OffsetDateTime.class)),
            toInstant(rs.getObject("updated_at", OffsetDateTime.class))
    );

    private static final RowMapper<ReservationItemRecord> ITEM_ROW_MAPPER = (rs, rowNum) -> new ReservationItemRecord(
            rs.getObject("reservation_item_id", UUID.class),
            rs.getObject("reservation_id", UUID.class),
            rs.getObject("event_id", UUID.class),
            rs.getObject("seat_id", UUID.class),
            rs.getString("section_code"),
            rs.getString("row_label"),
            rs.getString("seat_number"),
            rs.getLong("price_cents"),
            rs.getString("status"),
            toInstant(rs.getObject("created_at", OffsetDateTime.class)),
            toInstant(rs.getObject("updated_at", OffsetDateTime.class))
    );

    private final JdbcTemplate jdbcTemplate;

    public ReservationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    }

    public Optional<ReservationRecord> findById(UUID reservationId) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        return jdbcTemplate.query(SELECT_RESERVATION_BY_ID_SQL, RESERVATION_ROW_MAPPER, reservationId)
                .stream()
                .findFirst();
    }

    public List<ReservationItemRecord> findItemsByReservationId(UUID reservationId) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        return jdbcTemplate.query(SELECT_ITEMS_BY_RESERVATION_ID_SQL, ITEM_ROW_MAPPER, reservationId);
    }

    public List<HeldSeatLock> findActiveHeldSeatsByEventId(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        return jdbcTemplate.query(SELECT_ACTIVE_HELD_SEATS_BY_EVENT_SQL, HELD_SEAT_LOCK_ROW_MAPPER, eventId);
    }

    public List<HeldSeatLock> findAllActiveHeldSeats() {
        return jdbcTemplate.query(SELECT_ALL_ACTIVE_HELD_SEATS_SQL, HELD_SEAT_LOCK_ROW_MAPPER);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void insert(ReservationRecord reservation, List<ReservationItemRecord> items) {
        Objects.requireNonNull(reservation, "reservation must not be null");
        Objects.requireNonNull(items, "items must not be null");

        Instant now = Instant.now();
        Instant createdAt = reservation.createdAt() != null ? reservation.createdAt() : now;
        Instant updatedAt = reservation.updatedAt() != null ? reservation.updatedAt() : now;
        String currency = reservation.currency() != null ? reservation.currency() : "USD";
        String status = reservation.status() != null ? reservation.status() : "HELD";

        jdbcTemplate.update(
                INSERT_RESERVATION_SQL,
                new Object[]{
                        reservation.reservationId(),
                        reservation.eventId(),
                        reservation.userId(),
                        status,
                        reservation.totalAmountCents(),
                        currency,
                        toOffsetDateTime(createdAt),
                        toOffsetDateTime(reservation.expiresAt()),
                        toOffsetDateTime(reservation.confirmedAt()),
                        toOffsetDateTime(reservation.releasedAt()),
                        toOffsetDateTime(reservation.expiredAt()),
                        toOffsetDateTime(updatedAt)
                },
                new int[]{
                        Types.OTHER,
                        Types.OTHER,
                        Types.OTHER,
                        Types.VARCHAR,
                        Types.BIGINT,
                        Types.CHAR,
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.TIMESTAMP_WITH_TIMEZONE
                }
        );

        if (!items.isEmpty()) {
            jdbcTemplate.batchUpdate(INSERT_ITEM_SQL, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ReservationItemRecord item = items.get(i);
                    Instant itemCreatedAt = item.createdAt() != null ? item.createdAt() : now;
                    Instant itemUpdatedAt = item.updatedAt() != null ? item.updatedAt() : now;
                    String itemStatus = item.status() != null ? item.status() : "HELD";

                    ps.setObject(1, item.reservationItemId(), Types.OTHER);
                    ps.setObject(2, item.reservationId(), Types.OTHER);
                    ps.setObject(3, item.eventId(), Types.OTHER);
                    ps.setObject(4, item.seatId(), Types.OTHER);
                    ps.setString(5, item.sectionCode());
                    ps.setString(6, item.rowLabel());
                    ps.setString(7, item.seatNumber());
                    ps.setLong(8, item.priceCents());
                    ps.setString(9, itemStatus);
                    ps.setObject(10, toOffsetDateTime(itemCreatedAt), Types.TIMESTAMP_WITH_TIMEZONE);
                    ps.setObject(11, toOffsetDateTime(itemUpdatedAt), Types.TIMESTAMP_WITH_TIMEZONE);
                }

                @Override
                public int getBatchSize() {
                    return items.size();
                }
            });
        }
    }

    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant toInstant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }
}
