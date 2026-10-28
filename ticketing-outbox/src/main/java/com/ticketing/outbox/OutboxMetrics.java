package com.ticketing.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Outbox Micrometer metrics conforming to {@code docs/arch/09-conventions.md} and fallback names:
 * <ul>
 *     <li>{@code ticketing_outbox_unpublished_records} and {@code outbox_pending_rows} (Gauge, cached 5s)</li>
 *     <li>{@code ticketing_outbox_publish_latency_seconds} (Timer per topic)</li>
 *     <li>{@code outbox_published_total} (Counter)</li>
 *     <li>{@code outbox_publish_failures_total} (Counter)</li>
 *     <li>{@code outbox_oldest_pending_age_seconds} (Gauge, cached 5s)</li>
 * </ul>
 */
public class OutboxMetrics {

    static final long CACHE_TTL_NANOS = TimeUnit.SECONDS.toNanos(5);

    private static final String COUNT_UNPUBLISHED_SQL =
            "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL";

    private static final String OLDEST_AGE_SQL =
            "SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0.0) FROM outbox_events WHERE published_at IS NULL";

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;
    private final Counter publishedTotal;
    private final Counter publishFailuresTotal;
    private final ConcurrentMap<String, Timer> latencyTimers = new ConcurrentHashMap<>();

    private double cachedPendingRows;
    private long pendingRowsUpdatedAtNanos;
    private boolean pendingRowsInitialized;

    private double cachedOldestAgeSeconds;
    private long oldestAgeUpdatedAtNanos;
    private boolean oldestAgeInitialized;

    public OutboxMetrics(JdbcTemplate jdbcTemplate, MeterRegistry meterRegistry) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");

        Gauge.builder("ticketing_outbox_unpublished_records", this, OutboxMetrics::getCachedPendingRows)
                .description("Count of rows in outbox_events where published_at IS NULL")
                .register(meterRegistry);

        Gauge.builder("outbox_pending_rows", this, OutboxMetrics::getCachedPendingRows)
                .description("Count of rows in outbox_events where published_at IS NULL")
                .register(meterRegistry);

        Gauge.builder("outbox_oldest_pending_age_seconds", this, OutboxMetrics::getCachedOldestPendingAgeSeconds)
                .description("Age in seconds of the oldest unpublished outbox row")
                .register(meterRegistry);

        this.publishedTotal = Counter.builder("outbox_published_total")
                .description("Total outbox events successfully published to Kafka")
                .register(meterRegistry);

        this.publishFailuresTotal = Counter.builder("outbox_publish_failures_total")
                .description("Total failed attempts to publish an outbox event to Kafka")
                .register(meterRegistry);
    }

    public void recordPublished(String topic, Instant createdAt, Instant publishedAt) {
        publishedTotal.increment();
        Duration latency = Duration.between(createdAt, publishedAt);
        if (!latency.isNegative()) {
            latencyTimers.computeIfAbsent(topic, t ->
                    Timer.builder("ticketing_outbox_publish_latency_seconds")
                            .tag("topic", t)
                            .description("Elapsed time between outbox_events.created_at and published_at")
                            .register(meterRegistry)
            ).record(latency);
        }
    }

    public void recordFailure() {
        publishFailuresTotal.increment();
    }

    synchronized double getCachedPendingRows() {
        long now = System.nanoTime();
        if (!pendingRowsInitialized || (now - pendingRowsUpdatedAtNanos) >= CACHE_TTL_NANOS) {
            cachedPendingRows = queryPendingRows();
            pendingRowsUpdatedAtNanos = now;
            pendingRowsInitialized = true;
        }
        return cachedPendingRows;
    }

    synchronized double getCachedOldestPendingAgeSeconds() {
        long now = System.nanoTime();
        if (!oldestAgeInitialized || (now - oldestAgeUpdatedAtNanos) >= CACHE_TTL_NANOS) {
            cachedOldestAgeSeconds = queryOldestPendingAgeSeconds();
            oldestAgeUpdatedAtNanos = now;
            oldestAgeInitialized = true;
        }
        return cachedOldestAgeSeconds;
    }

    double queryPendingRows() {
        try {
            Long count = jdbcTemplate.queryForObject(COUNT_UNPUBLISHED_SQL, Long.class);
            return count == null ? 0.0 : count.doubleValue();
        } catch (Exception e) {
            return 0.0;
        }
    }

    double queryOldestPendingAgeSeconds() {
        try {
            Double age = jdbcTemplate.queryForObject(OLDEST_AGE_SQL, Double.class);
            return age == null || age < 0.0 ? 0.0 : age;
        } catch (Exception e) {
            return 0.0;
        }
    }
}
