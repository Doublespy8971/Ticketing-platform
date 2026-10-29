package com.ticketing.inventory.idempotency;

import com.ticketing.inventory.api.IdempotencyRequestInProgressException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
public class IdempotencyKeyRepository {

    public static final String STATUS_IN_PROGRESS = "IN_PROGRESS";
    public static final String STATUS_COMPLETED = "COMPLETED";

    private static final String CLAIM_SQL = """
            INSERT INTO idempotency_keys (user_id, idempotency_key, endpoint, request_hash, status, created_at)
            VALUES (?, ?, ?, ?, 'IN_PROGRESS', date_trunc('microseconds', clock_timestamp()))
            ON CONFLICT (user_id, idempotency_key) DO NOTHING
            RETURNING created_at
            """;

    private static final String SELECT_BY_PK_SQL = """
            SELECT user_id, idempotency_key, endpoint, request_hash, status,
                   response_status, response_body::text AS response_body,
                   created_at, completed_at, expires_at
            FROM idempotency_keys
            WHERE user_id = ? AND idempotency_key = ?
            """;

    private static final String MARK_COMPLETED_SQL = """
            UPDATE idempotency_keys
            SET status = 'COMPLETED',
                response_status = ?,
                response_body = ?::jsonb,
                completed_at = NOW()
            WHERE user_id = ?
              AND idempotency_key = ?
              AND status = 'IN_PROGRESS'
              AND created_at = ?
            """;

    private static final String DELETE_IN_PROGRESS_SQL = """
            DELETE FROM idempotency_keys
            WHERE user_id = ?
              AND idempotency_key = ?
              AND status = 'IN_PROGRESS'
              AND created_at = ?
            """;

    private static final String RECLAIM_STALE_IN_PROGRESS_SQL = """
            UPDATE idempotency_keys
            SET created_at = date_trunc('microseconds', clock_timestamp())
            WHERE user_id = ?
              AND idempotency_key = ?
              AND status = 'IN_PROGRESS'
              AND created_at <= NOW() - INTERVAL '30 seconds'
            RETURNING created_at
            """;

    private static final RowMapper<IdempotencyKeyRecord> ROW_MAPPER = (rs, rowNum) -> {
        String rawHash = rs.getString("request_hash");
        Integer responseStatus = rs.getObject("response_status", Integer.class);
        return new IdempotencyKeyRecord(
                rs.getObject("user_id", UUID.class),
                rs.getObject("idempotency_key", UUID.class),
                rs.getString("endpoint"),
                rawHash != null ? rawHash.trim() : null,
                rs.getString("status"),
                responseStatus,
                rs.getString("response_body"),
                toInstant(rs.getObject("created_at", OffsetDateTime.class)),
                toInstant(rs.getObject("completed_at", OffsetDateTime.class)),
                toInstant(rs.getObject("expires_at", OffsetDateTime.class))
        );
    };

    private static final RowMapper<Instant> CREATED_AT_MAPPER = (rs, rowNum) ->
            toInstant(rs.getObject("created_at", OffsetDateTime.class));

    private final JdbcTemplate jdbcTemplate;

    public IdempotencyKeyRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    }

    public Optional<Instant> tryClaim(UUID userId, UUID idempotencyKey, String endpoint, String requestHash) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(requestHash, "requestHash must not be null");

        return jdbcTemplate.query(
                CLAIM_SQL,
                new Object[]{userId, idempotencyKey, endpoint, requestHash},
                new int[]{Types.OTHER, Types.OTHER, Types.VARCHAR, Types.CHAR},
                CREATED_AT_MAPPER
        ).stream().findFirst();
    }

    public Optional<IdempotencyKeyRecord> findByUserAndKey(UUID userId, UUID idempotencyKey) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        return jdbcTemplate.query(SELECT_BY_PK_SQL, ROW_MAPPER, userId, idempotencyKey)
                .stream()
                .findFirst();
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void markCompleted(
            UUID userId,
            UUID idempotencyKey,
            Instant claimedAt,
            int responseStatus,
            String responseBodyJson) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(claimedAt, "claimedAt must not be null");
        Objects.requireNonNull(responseBodyJson, "responseBodyJson must not be null");

        int updated = jdbcTemplate.update(
                MARK_COMPLETED_SQL,
                new Object[]{
                        responseStatus,
                        responseBodyJson,
                        userId,
                        idempotencyKey,
                        toOffsetDateTime(claimedAt)
                },
                new int[]{
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.OTHER,
                        Types.OTHER,
                        Types.TIMESTAMP_WITH_TIMEZONE
                }
        );
        if (updated != 1) {
            throw new IdempotencyRequestInProgressException(
                    "Idempotency claim for key " + idempotencyKey + " was lost or reclaimed by a newer request.");
        }
    }

    public Optional<Instant> reclaimStaleInProgress(UUID userId, UUID idempotencyKey) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        return jdbcTemplate.query(
                RECLAIM_STALE_IN_PROGRESS_SQL,
                new Object[]{userId, idempotencyKey},
                new int[]{Types.OTHER, Types.OTHER},
                CREATED_AT_MAPPER
        ).stream().findFirst();
    }

    public int deleteInProgress(UUID userId, UUID idempotencyKey, Instant claimedAt) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(claimedAt, "claimedAt must not be null");
        return jdbcTemplate.update(
                DELETE_IN_PROGRESS_SQL,
                new Object[]{userId, idempotencyKey, toOffsetDateTime(claimedAt)},
                new int[]{Types.OTHER, Types.OTHER, Types.TIMESTAMP_WITH_TIMEZONE}
        );
    }

    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC);
    }

    private static Instant toInstant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant().truncatedTo(ChronoUnit.MICROS);
    }
}
