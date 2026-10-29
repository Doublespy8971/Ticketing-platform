package com.ticketing.inventory.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
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

    private static final String SELECT_BY_ID_FOR_UPDATE_SQL = """
            SELECT event_id, name, venue_name, event_time, drop_time, status, created_at, updated_at
            FROM events
            WHERE event_id = ?
            FOR UPDATE
            """;

    private static final String UPDATE_STATUS_SQL = """
            UPDATE events
            SET status = ?, updated_at = ?
            WHERE event_id = ?
            """;

    private static final String SELECT_ALL_SQL = """
            SELECT event_id, name, venue_name, event_time, drop_time, status, created_at, updated_at
            FROM events
            """;

    private static final String SELECT_REWARM_ELIGIBLE_SQL = """
            SELECT event_id, name, venue_name, event_time, drop_time, status, created_at, updated_at
            FROM events
            WHERE status IN ('SCHEDULED', 'ON_SALE', 'SOLD_OUT')
            ORDER BY created_at ASC
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

    @Transactional(propagation = Propagation.MANDATORY)
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

    public Optional<EventRecord> findByIdForUpdate(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        return jdbcTemplate.query(SELECT_BY_ID_FOR_UPDATE_SQL, ROW_MAPPER, eventId)
                .stream()
                .findFirst();
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public int updateStatus(UUID eventId, String status, Instant updatedAt) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        return jdbcTemplate.update(
                UPDATE_STATUS_SQL,
                new Object[]{status, toOffsetDateTime(updatedAt), eventId},
                new int[]{Types.VARCHAR, Types.TIMESTAMP_WITH_TIMEZONE, Types.OTHER}
        );
    }

    public List<EventRecord> findAll() {
        return jdbcTemplate.query(SELECT_ALL_SQL, ROW_MAPPER);
    }

    public List<EventRecord> findRewarmEligibleEvents() {
        return jdbcTemplate.query(SELECT_REWARM_ELIGIBLE_SQL, ROW_MAPPER);
    }

    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant toInstant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }
}

