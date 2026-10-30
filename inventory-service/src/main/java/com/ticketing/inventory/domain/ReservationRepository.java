package com.ticketing.inventory.domain;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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

    public record ReservationTimestamps(Instant createdAt, Instant expiresAt) {}

    public record LockedReservation(
            UUID reservationId,
            UUID eventId,
            UUID userId,
            String status,
            boolean expired
    ) {}

    public record ConfirmedReservation(
            UUID reservationId,
            UUID eventId,
            UUID userId,
            List<UUID> seatIds,
            Instant confirmedAt
    ) {}

    private static final String SELECT_EXPIRED_HELD_IDS_FOR_UPDATE_SKIP_LOCKED_SQL = """
            SELECT reservation_id
            FROM reservations
            WHERE status = 'HELD'
              AND expires_at <= NOW()
            ORDER BY expires_at ASC
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            """;

    private static final String LOCK_RESERVATION_FOR_UPDATE_SQL = """
            SELECT reservation_id,
                   event_id,
                   user_id,
                   status,
                   (expires_at <= NOW()) AS is_expired
            FROM reservations
            WHERE reservation_id = ?
            FOR UPDATE
            """;

    private static final String MARK_EXPIRED_WITH_DB_CLOCK_SQL = """
            UPDATE reservations
            SET status = 'EXPIRED',
                expired_at = date_trunc('microseconds', NOW()),
                updated_at = date_trunc('microseconds', NOW())
            WHERE reservation_id = ?
              AND status = 'HELD'
            RETURNING expired_at
            """;

    private static final String EXPIRE_HELD_ITEMS_SQL = """
            WITH updated_items AS (
                UPDATE reservation_items
                SET status = 'EXPIRED',
                    updated_at = date_trunc('microseconds', NOW())
                WHERE reservation_id = ?
                  AND status = 'HELD'
                RETURNING seat_id
            )
            SELECT seat_id
            FROM updated_items
            ORDER BY seat_id ASC
            """;

    private static final String MARK_CONFIRMED_WITH_DB_CLOCK_SQL = """
            UPDATE reservations
            SET status = 'CONFIRMED',
                confirmed_at = date_trunc('microseconds', NOW()),
                updated_at = date_trunc('microseconds', NOW())
            WHERE reservation_id = ?
              AND status = 'HELD'
            RETURNING confirmed_at
            """;

    private static final String CONFIRM_HELD_ITEMS_SQL = """
            WITH updated_items AS (
                UPDATE reservation_items
                SET status = 'CONFIRMED',
                    updated_at = date_trunc('microseconds', NOW())
                WHERE reservation_id = ?
                  AND status = 'HELD'
                RETURNING seat_id
            )
            SELECT seat_id
            FROM updated_items
            ORDER BY seat_id ASC
            """;

    private static final String CONFIRM_HELD_SEATS_SQL = """
            UPDATE seats
            SET status = 'SOLD',
                version = version + 1,
                updated_at = NOW()
            WHERE seat_id IN (
                SELECT seat_id
                FROM seats
                WHERE event_id = ?
                  AND seat_id = ANY(?)
                  AND status = 'HELD'
                ORDER BY seat_id ASC
                FOR UPDATE
            )
            """;

    private static final String OLDEST_OVERDUE_HOLD_SECONDS_SQL = """
            SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(expires_at))), 0.0)
            FROM reservations
            WHERE status = 'HELD'
              AND expires_at <= NOW()
            """;

    private static final String INSERT_HELD_WITH_DB_CLOCK_SQL = """
            WITH ts AS (
                SELECT date_trunc('microseconds', clock_timestamp()) AS now_ts
            )
            INSERT INTO reservations (
                reservation_id, event_id, user_id, status, total_amount_cents, currency,
                created_at, expires_at, updated_at
            )
            SELECT ?, ?, ?, 'HELD', ?, ?,
                   ts.now_ts,
                   ts.now_ts + (? * INTERVAL '1 second'),
                   ts.now_ts
            FROM ts
            RETURNING created_at, expires_at
            """;

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

    public Optional<String> findStatusById(UUID reservationId) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        String sql = "SELECT status FROM reservations WHERE reservation_id = ?";
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("status"), reservationId)
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
            insertItems(items, now);
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public ReservationTimestamps insertHeldReservationWithDbClock(
            UUID reservationId,
            UUID eventId,
            UUID userId,
            long totalAmountCents,
            String currency,
            long holdTtlSeconds) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(userId, "userId must not be null");
        String curr = currency != null ? currency : "USD";

        ReservationTimestamps timestamps = jdbcTemplate.queryForObject(
                INSERT_HELD_WITH_DB_CLOCK_SQL,
                new Object[]{reservationId, eventId, userId, totalAmountCents, curr, holdTtlSeconds},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.BIGINT, Types.CHAR, Types.BIGINT},
                (rs, rowNum) -> new ReservationTimestamps(
                        toInstant(rs.getObject("created_at", OffsetDateTime.class)),
                        toInstant(rs.getObject("expires_at", OffsetDateTime.class))
                )
        );
        return Objects.requireNonNull(timestamps, "timestamps must not be null");
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void insertItems(List<ReservationItemRecord> items, Instant defaultTimestamp) {
        Objects.requireNonNull(items, "items must not be null");
        if (items.isEmpty()) {
            return;
        }
        Instant now = defaultTimestamp != null ? defaultTimestamp : Instant.now();
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

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean updateStatus(UUID reservationId, String newStatus) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        Objects.requireNonNull(newStatus, "newStatus must not be null");
        String sql = """
                UPDATE reservations
                SET status = ?, updated_at = ?
                WHERE reservation_id = ?
                """;
        int updated = jdbcTemplate.update(
                sql,
                new Object[]{newStatus, toOffsetDateTime(Instant.now()), reservationId},
                new int[]{Types.VARCHAR, Types.TIMESTAMP_WITH_TIMEZONE, Types.OTHER}
        );
        return updated > 0;
    }

    /**
     * Selects up to {@code limit} reservation IDs currently in {@code HELD} status whose
     * {@code expires_at} is earlier than or equal to the PostgreSQL database clock ({@code NOW()}),
     * ordered by {@code expires_at ASC}, using {@code FOR UPDATE SKIP LOCKED} so concurrent sweeper
     * instances do not pick rows already locked by another transaction.
     */
    public List<UUID> findExpiredHeldReservationIds(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return jdbcTemplate.query(
                SELECT_EXPIRED_HELD_IDS_FOR_UPDATE_SKIP_LOCKED_SQL,
                (rs, rowNum) -> rs.getObject("reservation_id", UUID.class),
                limit
        );
    }

    /**
     * Acquires an exclusive row-level lock ({@code FOR UPDATE}) on the reservation row in PostgreSQL
     * and evaluates {@code expires_at <= NOW()} using the database clock.
     *
     * <p>Both reservation confirmation ({@link #confirmIfHeld(UUID)}) and reservation expiry
     * ({@code ExpirySweeper.expireReservation(UUID)}) acquire this exact row lock on {@code reservations}
     * first before inspecting {@code status} or mutating child rows. Consequently, concurrent confirm
     * and expire operations on the same reservation strictly serialize on the {@code reservations}
     * primary-key row lock so that at most one observes {@code status = 'HELD'} and wins.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<LockedReservation> lockForUpdate(UUID reservationId) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        return jdbcTemplate.query(
                LOCK_RESERVATION_FOR_UPDATE_SQL,
                (rs, rowNum) -> new LockedReservation(
                        rs.getObject("reservation_id", UUID.class),
                        rs.getObject("event_id", UUID.class),
                        rs.getObject("user_id", UUID.class),
                        rs.getString("status"),
                        rs.getBoolean("is_expired")
                ),
                reservationId
        ).stream().findFirst();
    }

    /**
     * Sets the reservation status from {@code HELD} to {@code EXPIRED} with {@code expired_at}
     * and {@code updated_at} populated from the PostgreSQL database clock.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<Instant> markExpiredWithDbClock(UUID reservationId) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        return jdbcTemplate.query(
                MARK_EXPIRED_WITH_DB_CLOCK_SQL,
                (rs, rowNum) -> toInstant(rs.getObject("expired_at", OffsetDateTime.class)),
                reservationId
        ).stream().findFirst();
    }

    /**
     * Transitions all {@code HELD} items of the reservation to {@code EXPIRED} and returns their
     * {@code seat_id}s in deterministic ascending order.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public List<UUID> expireHeldItems(UUID reservationId) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        return jdbcTemplate.query(
                EXPIRE_HELD_ITEMS_SQL,
                (rs, rowNum) -> rs.getObject("seat_id", UUID.class),
                reservationId
        );
    }

    /**
     * Confirmation guard for the {@code payment.succeeded} handler (docs/arch/02-saga.md, Section 2.3 & 2.4.2).
     *
     * <p>Concurrency guarantee: This method and {@code ExpirySweeper.expireReservation(UUID)} both acquire
     * {@code SELECT ... FROM reservations WHERE reservation_id = ? FOR UPDATE} ({@link #lockForUpdate(UUID)})
     * as their first step inside a PostgreSQL transaction. Because both operations serialize on the
     * {@code reservations} row lock and require {@code status = 'HELD'} to proceed:
     * <ul>
     *   <li>If {@code confirmIfHeld} acquires the lock first, it transitions the reservation to
     *       {@code CONFIRMED}, its items to {@code CONFIRMED}, and its seats to {@code SOLD}; when
     *       {@code expireReservation} unblocks, it observes {@code status = 'CONFIRMED'} and performs a no-op.</li>
     *   <li>If {@code expireReservation} acquires the lock first, it transitions the reservation to
     *       {@code EXPIRED}, its items to {@code EXPIRED}, and its seats back to {@code AVAILABLE}; when
     *       {@code confirmIfHeld} unblocks, it observes {@code status = 'EXPIRED'} and returns
     *       {@link Optional#empty()} so the caller can emit {@code inventory.seat.confirmation_rejected}.</li>
     * </ul>
     * Exactly one of confirm or expire wins.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<ConfirmedReservation> confirmIfHeld(UUID reservationId) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");

        Optional<LockedReservation> lockedOpt = lockForUpdate(reservationId);
        if (lockedOpt.isEmpty()) {
            return Optional.empty();
        }
        LockedReservation locked = lockedOpt.get();
        if (!"HELD".equals(locked.status())) {
            return Optional.empty();
        }

        Optional<Instant> confirmedAtOpt = jdbcTemplate.query(
                MARK_CONFIRMED_WITH_DB_CLOCK_SQL,
                (rs, rowNum) -> toInstant(rs.getObject("confirmed_at", OffsetDateTime.class)),
                reservationId
        ).stream().findFirst();
        if (confirmedAtOpt.isEmpty()) {
            return Optional.empty();
        }

        List<UUID> seatIds = jdbcTemplate.query(
                CONFIRM_HELD_ITEMS_SQL,
                (rs, rowNum) -> rs.getObject("seat_id", UUID.class),
                reservationId
        );
        if (seatIds.isEmpty()) {
            throw new IllegalStateException(
                    "Confirmed reservation " + reservationId + " had no HELD reservation_items");
        }

        UUID[] idArray = seatIds.toArray(new UUID[0]);
        int updatedSeats = jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(CONFIRM_HELD_SEATS_SQL);
            ps.setObject(1, locked.eventId(), Types.OTHER);
            Array sqlArray = con.createArrayOf("uuid", idArray);
            ps.setArray(2, sqlArray);
            return ps;
        });
        if (updatedSeats != seatIds.size()) {
            throw new IllegalStateException(
                    "Seat update count mismatch when confirming reservation " + reservationId
                            + ": expected " + seatIds.size() + " HELD seats for event " + locked.eventId()
                            + ", but updated " + updatedSeats);
        }

        return Optional.of(new ConfirmedReservation(
                reservationId,
                locked.eventId(),
                locked.userId(),
                seatIds,
                confirmedAtOpt.get()
        ));
    }

    /**
     * Queries the age in seconds of the oldest overdue {@code HELD} reservation using the PostgreSQL
     * database clock ({@code NOW() - MIN(expires_at)}), or {@code 0.0} when none are overdue.
     */
    public double queryOldestOverdueHoldSeconds() {
        try {
            Double seconds = jdbcTemplate.queryForObject(OLDEST_OVERDUE_HOLD_SECONDS_SQL, Double.class);
            return seconds == null || seconds < 0.0 ? 0.0 : seconds;
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC);
    }

    private static Instant toInstant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant().truncatedTo(ChronoUnit.MICROS);
    }
}
