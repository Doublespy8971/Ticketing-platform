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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
public class SeatRepository {

    private static final String INSERT_SQL = """
            INSERT INTO seats (
                seat_id, event_id, section_code, row_label, seat_number,
                price_cents, currency, status, version, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BY_ID_SQL = """
            SELECT seat_id, event_id, section_code, row_label, seat_number,
                   price_cents, currency, status, version, created_at, updated_at
            FROM seats
            WHERE seat_id = ?
            """;

    private static final String SELECT_BY_EVENT_AND_IDS_SQL = """
            SELECT seat_id, event_id, section_code, row_label, seat_number,
                   price_cents, currency, status, version, created_at, updated_at
            FROM seats
            WHERE event_id = ? AND seat_id = ANY(?)
            ORDER BY seat_id ASC
            """;

    private static final String HOLD_AVAILABLE_SEATS_SQL = """
            UPDATE seats
            SET status = 'HELD',
                version = version + 1,
                updated_at = NOW()
            WHERE seat_id IN (
                SELECT seat_id
                FROM seats
                WHERE event_id = ?
                  AND seat_id = ANY(?)
                  AND status = 'AVAILABLE'
                ORDER BY seat_id ASC
                FOR UPDATE
            )
            """;

    private static final String RELEASE_HELD_SEATS_SQL = """
            UPDATE seats
            SET status = 'AVAILABLE',
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

    public record SoldSeat(UUID eventId, UUID seatId) {}

    private static final String SELECT_SOLD_SEAT_IDS_BY_EVENT_SQL = """
            SELECT seat_id
            FROM seats
            WHERE event_id = ? AND status = 'SOLD'
            """;

    private static final String SELECT_SOLD_SEATS_PAGE_FIRST_SQL = """
            SELECT event_id, seat_id
            FROM seats
            WHERE event_id = ? AND status = 'SOLD'
            ORDER BY seat_id ASC
            LIMIT ?
            """;

    private static final String SELECT_SOLD_SEATS_PAGE_NEXT_SQL = """
            SELECT event_id, seat_id
            FROM seats
            WHERE event_id = ? AND status = 'SOLD' AND seat_id > ?
            ORDER BY seat_id ASC
            LIMIT ?
            """;

    private static final RowMapper<SoldSeat> SOLD_SEAT_ROW_MAPPER = (rs, rowNum) -> new SoldSeat(
            rs.getObject("event_id", UUID.class),
            rs.getObject("seat_id", UUID.class)
    );

    private static final String SELECT_ALL_SOLD_SEATS_SQL = """
            SELECT seat_id, event_id, section_code, row_label, seat_number,
                   price_cents, currency, status, version, created_at, updated_at
            FROM seats
            WHERE status = 'SOLD'
            """;

    private static final RowMapper<SeatRecord> ROW_MAPPER = (rs, rowNum) -> new SeatRecord(
            rs.getObject("seat_id", UUID.class),
            rs.getObject("event_id", UUID.class),
            rs.getString("section_code"),
            rs.getString("row_label"),
            rs.getString("seat_number"),
            rs.getLong("price_cents"),
            rs.getString("currency"),
            rs.getString("status"),
            rs.getLong("version"),
            toInstant(rs.getObject("created_at", OffsetDateTime.class)),
            toInstant(rs.getObject("updated_at", OffsetDateTime.class))
    );

    private final JdbcTemplate jdbcTemplate;

    public SeatRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void bulkInsert(List<SeatRecord> seats) {
        Objects.requireNonNull(seats, "seats must not be null");
        if (seats.isEmpty()) {
            return;
        }
        Instant now = Instant.now();
        jdbcTemplate.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                SeatRecord s = seats.get(i);
                Instant createdAt = s.createdAt() != null ? s.createdAt() : now;
                Instant updatedAt = s.updatedAt() != null ? s.updatedAt() : now;
                String currency = s.currency() != null ? s.currency() : "USD";
                String status = s.status() != null ? s.status() : "AVAILABLE";

                ps.setObject(1, s.seatId(), Types.OTHER);
                ps.setObject(2, s.eventId(), Types.OTHER);
                ps.setString(3, s.sectionCode());
                ps.setString(4, s.rowLabel());
                ps.setString(5, s.seatNumber());
                ps.setLong(6, s.priceCents());
                ps.setString(7, currency);
                ps.setString(8, status);
                ps.setLong(9, s.version());
                ps.setObject(10, toOffsetDateTime(createdAt), Types.TIMESTAMP_WITH_TIMEZONE);
                ps.setObject(11, toOffsetDateTime(updatedAt), Types.TIMESTAMP_WITH_TIMEZONE);
            }

            @Override
            public int getBatchSize() {
                return seats.size();
            }
        });
    }

    public Optional<SeatRecord> findById(UUID seatId) {
        Objects.requireNonNull(seatId, "seatId must not be null");
        return jdbcTemplate.query(SELECT_BY_ID_SQL, ROW_MAPPER, seatId)
                .stream()
                .findFirst();
    }

    public List<SeatRecord> findByEventIdAndSeatIds(UUID eventId, Collection<UUID> seatIds) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(seatIds, "seatIds must not be null");
        if (seatIds.isEmpty()) {
            return List.of();
        }
        UUID[] idArray = seatIds.toArray(new UUID[0]);
        return jdbcTemplate.query(con -> {
            PreparedStatement ps = con.prepareStatement(SELECT_BY_EVENT_AND_IDS_SQL);
            ps.setObject(1, eventId, Types.OTHER);
            Array sqlArray = con.createArrayOf("uuid", idArray);
            ps.setArray(2, sqlArray);
            return ps;
        }, ROW_MAPPER);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public int holdAvailableSeats(UUID eventId, Collection<UUID> seatIds) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(seatIds, "seatIds must not be null");
        if (seatIds.isEmpty()) {
            return 0;
        }
        UUID[] idArray = seatIds.toArray(new UUID[0]);
        return jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(HOLD_AVAILABLE_SEATS_SQL);
            ps.setObject(1, eventId, Types.OTHER);
            Array sqlArray = con.createArrayOf("uuid", idArray);
            ps.setArray(2, sqlArray);
            return ps;
        });
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public int releaseHeldSeats(UUID eventId, Collection<UUID> seatIds) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(seatIds, "seatIds must not be null");
        if (seatIds.isEmpty()) {
            return 0;
        }
        UUID[] idArray = seatIds.toArray(new UUID[0]);
        return jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(RELEASE_HELD_SEATS_SQL);
            ps.setObject(1, eventId, Types.OTHER);
            Array sqlArray = con.createArrayOf("uuid", idArray);
            ps.setArray(2, sqlArray);
            return ps;
        });
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public int confirmHeldSeats(UUID eventId, Collection<UUID> seatIds) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(seatIds, "seatIds must not be null");
        if (seatIds.isEmpty()) {
            return 0;
        }
        UUID[] idArray = seatIds.toArray(new UUID[0]);
        return jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(CONFIRM_HELD_SEATS_SQL);
            ps.setObject(1, eventId, Types.OTHER);
            Array sqlArray = con.createArrayOf("uuid", idArray);
            ps.setArray(2, sqlArray);
            return ps;
        });
    }

    public List<UUID> findSoldSeatIdsByEventId(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        return jdbcTemplate.query(
                SELECT_SOLD_SEAT_IDS_BY_EVENT_SQL,
                (rs, rowNum) -> rs.getObject("seat_id", UUID.class),
                eventId
        );
    }

    public List<SoldSeat> findSoldSeatsByEventIdKeyset(UUID eventId, UUID afterSeatId, int limit) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        if (afterSeatId == null) {
            return jdbcTemplate.query(
                    SELECT_SOLD_SEATS_PAGE_FIRST_SQL,
                    SOLD_SEAT_ROW_MAPPER,
                    eventId,
                    limit
            );
        } else {
            return jdbcTemplate.query(
                    SELECT_SOLD_SEATS_PAGE_NEXT_SQL,
                    SOLD_SEAT_ROW_MAPPER,
                    eventId,
                    afterSeatId,
                    limit
            );
        }
    }

    public List<SeatRecord> findAllSoldSeats() {
        return jdbcTemplate.query(SELECT_ALL_SOLD_SEATS_SQL, ROW_MAPPER);
    }

    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant toInstant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }
}

