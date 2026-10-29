package com.ticketing.inventory.idempotency;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
public class IdempotencyKeyRepository {

    public static final String STATUS_IN_PROGRESS = "IN_PROGRESS";
    public static final String STATUS_COMPLETED = "COMPLETED";

    private static final String CLAIM_SQL = """
            INSERT INTO idempotency_keys (user_id, idempotency_key, endpoint, request_hash, status)
            VALUES (?, ?, ?, ?, 'IN_PROGRESS')
            ON CONFLICT (user_id, idempotency_key) DO NOTHING
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
            WHERE user_id = ? AND idempotency_key = ?
            """;

    private static final String DELETE_IN_PROGRESS_SQL = """
            DELETE FROM idempotency_keys
            WHERE user_id = ? AND idempotency_key = ? AND status = 'IN_PROGRESS'
            """;

    private static final String RECLAIM_STALE_IN_PROGRESS_SQL = """
            UPDATE idempotency_keys
            SET created_at = NOW()
            WHERE user_id = ?
              AND idempotency_key = ?
              AND status = 'IN_PROGRESS'
              AND created_at <= NOW() - INTERVAL '30 seconds'
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

    private final JdbcTemplate jdbcTemplate;

    public IdempotencyKeyRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    }

    public boolean tryClaim(UUID userId, UUID idempotencyKey, String endpoint, String requestHash) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(requestHash, "requestHash must not be null");

        int inserted = jdbcTemplate.update(
                CLAIM_SQL,
                new Object[]{userId, idempotencyKey, endpoint, requestHash},
                new int[]{Types.OTHER, Types.OTHER, Types.VARCHAR, Types.CHAR}
        );
        return inserted == 1;
    }

    public Optional<IdempotencyKeyRecord> findByUserAndKey(UUID userId, UUID idempotencyKey) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        return jdbcTemplate.query(SELECT_BY_PK_SQL, ROW_MAPPER, userId, idempotencyKey)
                .stream()
                .findFirst();
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void markCompleted(UUID userId, UUID idempotencyKey, int responseStatus, String responseBodyJson) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(responseBodyJson, "responseBodyJson must not be null");

        int updated = jdbcTemplate.update(
                MARK_COMPLETED_SQL,
                new Object[]{responseStatus, responseBodyJson, userId, idempotencyKey},
                new int[]{Types.INTEGER, Types.VARCHAR, Types.OTHER, Types.OTHER}
        );
        if (updated != 1) {
            throw new IllegalStateException("Expected 1 idempotency_keys row updated to COMPLETED, but updated " + updated);
        }
    }

    public boolean reclaimStaleInProgress(UUID userId, UUID idempotencyKey) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        int updated = jdbcTemplate.update(
                RECLAIM_STALE_IN_PROGRESS_SQL,
                new Object[]{userId, idempotencyKey},
                new int[]{Types.OTHER, Types.OTHER}
        );
        return updated == 1;
    }

    public void deleteInProgress(UUID userId, UUID idempotencyKey) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        jdbcTemplate.update(
                DELETE_IN_PROGRESS_SQL,
                new Object[]{userId, idempotencyKey},
                new int[]{Types.OTHER, Types.OTHER}
        );
    }

    private static Instant toInstant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }
}

