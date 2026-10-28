package com.ticketing.inventory.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
public class EventRepository {

    private static final String INSERT_SQL = """
            INSERT INTO events (
                event_id, name, venue_name, event_time, drop_time, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BY_ID_SQL = """
            SELECT event_id, name, venue_name, event_time, drop_time, status, created_at, updated_at
            FROM events
            WHERE event_id = ?
            """;

    private static final RowMapper<EventRecord> ROW_MAPPER = (rs, rowNum) -> new EventRecord(
            rs.getObject("event_id", UUID.class),
            rs.getString("name"),
            rs.getString("venue_name"),
            toInstant(rs.getObject("event_time", OffsetDateTime.class)),
            toInstant(rs.getObject("drop_time", OffsetDateTime.class)),
            rs.getString("status"),
            toInstant(rs.getObject("created_at", OffsetDateTime.class)),
            toInstant(rs.getObject("updated_at", OffsetDateTime.class))
    );

    private final JdbcTemplate jdbcTemplate;

    public EventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    }

    public void insert(EventRecord event) {
        Objects.requireNonNull(event, "event must not be null");
        Instant now = Instant.now();
        Instant createdAt = event.createdAt() != null ? event.createdAt() : now;
        Instant updatedAt = event.updatedAt() != null ? event.updatedAt() : now;
        String status = event.status() != null ? event.status() : "SCHEDULED";

        jdbcTemplate.update(
                INSERT_SQL,
                new Object[]{
                        event.eventId(),
                        event.name(),
                        event.venueName(),
                        toOffsetDateTime(event.eventTime()),
                        toOffsetDateTime(event.dropTime()),
                        status,
                        toOffsetDateTime(createdAt),
                        toOffsetDateTime(updatedAt)
                },
                new int[]{
                        Types.OTHER,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.VARCHAR,
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.TIMESTAMP_WITH_TIMEZONE
                }
        );
    }

    public Optional<EventRecord> findById(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        return jdbcTemplate.query(SELECT_BY_ID_SQL, ROW_MAPPER, eventId)
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

