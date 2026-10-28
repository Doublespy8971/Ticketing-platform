package com.ticketing.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * Scheduled retention cleaner that deletes published {@code outbox_events} rows older than the configured
 * retention duration (72 hours by default) in bounded batches using {@code FOR UPDATE SKIP LOCKED}
 * (docs/arch/05-outbox-idempotency.md, section 5.3).
 */
public class OutboxRetentionCleaner {

    private static final Logger log = LoggerFactory.getLogger(OutboxRetentionCleaner.class);

    public static final int MAX_CLEANUP_ITERATIONS = 100;

    private static final String DELETE_EXPIRED_SQL = """
            DELETE FROM outbox_events
            WHERE event_id IN (
                SELECT event_id
                FROM outbox_events
                WHERE published_at IS NOT NULL
                  AND published_at < ?
                ORDER BY published_at ASC
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            """;

    private final JdbcTemplate jdbcTemplate;
    private final OutboxProperties properties;

    public OutboxRetentionCleaner(JdbcTemplate jdbcTemplate, OutboxProperties properties) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.properties.validate();
    }

    @Scheduled(cron = "${ticketing.outbox.cleanup-cron:0 0 * * * *}")
    public int purgeExpired() {
        Instant cutoff = Instant.now().minus(properties.getRetention()).truncatedTo(ChronoUnit.MICROS);
        OffsetDateTime cutoffOdt = OffsetDateTime.ofInstant(cutoff, ZoneOffset.UTC);
        int batchSize = properties.getCleanupBatchSize();
        int totalDeleted = 0;

        for (int i = 0; i < MAX_CLEANUP_ITERATIONS; i++) {
            int deleted = jdbcTemplate.update(
                    DELETE_EXPIRED_SQL,
                    new Object[]{cutoffOdt, batchSize},
                    new int[]{Types.TIMESTAMP_WITH_TIMEZONE, Types.INTEGER}
            );
            totalDeleted += deleted;
            if (deleted < batchSize) {
                break;
            }
        }
        if (totalDeleted > 0) {
            log.info("Purged {} published outbox_events rows older than {}", totalDeleted, cutoff);
        }
        return totalDeleted;
    }
}
