package com.ticketing.inventory.domain;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

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

    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant toInstant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }
}

