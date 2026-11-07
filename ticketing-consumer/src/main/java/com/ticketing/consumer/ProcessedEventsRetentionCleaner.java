package com.ticketing.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Objects;

/**
 * Scheduled retention cleaner that deletes {@code processed_events} rows older than the configured
 * retention period (192 hours / 8 days by default, longer than the 7-day Kafka topic retention)
 * in bounded batches of {@code batchSize} (5000 by default) using {@code FOR UPDATE SKIP LOCKED},
 * looping until fewer rows than {@code batchSize} are deleted up to at most 100 iterations per run
 * (docs/arch/05-outbox-idempotency.md, section 5.5).
 */
public class ProcessedEventsRetentionCleaner {

    private static final Logger log = LoggerFactory.getLogger(ProcessedEventsRetentionCleaner.class);

    public static final int MAX_CLEANUP_ITERATIONS = 100;

    private static final String DELETE_EXPIRED_SQL = """
            DELETE FROM processed_events
            WHERE (event_id, consumer_group) IN (
                SELECT event_id, consumer_group
                FROM processed_events
                WHERE processed_at < clock_timestamp() - (? * INTERVAL '1 hour')
                ORDER BY processed_at ASC
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ConsumerCleanupProperties properties;

    public ProcessedEventsRetentionCleaner(JdbcTemplate jdbcTemplate, ConsumerCleanupProperties properties) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.properties.validate();
    }

    @Scheduled(
            cron = "${ticketing.consumer.cleanup.cron:0 0 * * * *}",
            scheduler = "ticketingConsumerCleanupScheduler"
    )
    public int purgeExpired() {
        int retentionHours = properties.getRetentionHours();
        int batchSize = properties.getBatchSize();
        int totalDeleted = 0;

        for (int i = 0; i < MAX_CLEANUP_ITERATIONS; i++) {
            int deleted = jdbcTemplate.update(DELETE_EXPIRED_SQL, retentionHours, batchSize);
            totalDeleted += deleted;
            if (deleted < batchSize) {
                break;
            }
        }
        if (totalDeleted > 0) {
            log.info("Purged {} expired processed_events rows older than {} hours", totalDeleted, retentionHours);
        }
        return totalDeleted;
    }
}

