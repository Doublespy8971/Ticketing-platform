package com.ticketing.outbox;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.Objects;

/** Configuration properties for the transactional outbox poller and retention cleaner. */
@ConfigurationProperties(prefix = "ticketing.outbox")
public class OutboxProperties implements EnvironmentAware, InitializingBean {

    public static final int MIN_BATCH_SIZE = 1;
    public static final int MAX_BATCH_SIZE = 1000;

    private Environment environment;

    /** Enables the scheduled outbox poller and retention cleaner. */
    private boolean enabled = false;

    /** Fixed delay in milliseconds between poller runs (docs/arch/05-outbox-idempotency.md: 200 ms). */
    private long pollIntervalMs = 200L;

    /** Maximum number of unpublished rows fetched per poll cycle (docs/arch/05-outbox-idempotency.md: 100). */
    private int batchSize = 100;

    /** Broker acknowledgement timeout per record send (5000 ms). */
    private Duration sendTimeout = Duration.ofMillis(5000);

    /** Fixed PostgreSQL session advisory lock ID held on the leader's dedicated connection. */
    private long advisoryLockId = 71001L;

    /** Retention duration for published outbox rows (docs/arch/05-outbox-idempotency.md: 72 hours). */
    private Duration retention = Duration.ofHours(72);

    /** Maximum number of published rows deleted per retention cleanup batch (1..1000, default 1000). */
    private int cleanupBatchSize = 1000;

    /** Cron expression for retention cleanup (hourly by default). */
    private String cleanupCron = "0 0 * * * *";

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void afterPropertiesSet() {
        if (environment != null) {
            if (!environment.containsProperty("ticketing.outbox.batch-size")
                    && !environment.containsProperty("ticketing.outbox.batchSize")
                    && environment.containsProperty("OUTBOX_BATCH_SIZE")) {
                this.batchSize = Integer.parseInt(
                        Objects.requireNonNull(environment.getProperty("OUTBOX_BATCH_SIZE")).trim());
            }
            if (!environment.containsProperty("ticketing.outbox.poll-interval-ms")
                    && !environment.containsProperty("ticketing.outbox.pollIntervalMs")
                    && environment.containsProperty("OUTBOX_POLL_INTERVAL_MS")) {
                this.pollIntervalMs = Long.parseLong(
                        Objects.requireNonNull(environment.getProperty("OUTBOX_POLL_INTERVAL_MS")).trim());
            }
        }
        validate();
    }

    public void validate() {
        if (batchSize < MIN_BATCH_SIZE || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "ticketing.outbox.batch-size must be between " + MIN_BATCH_SIZE + " and " + MAX_BATCH_SIZE
                            + ", but was: " + batchSize);
        }
        if (cleanupBatchSize < MIN_BATCH_SIZE || cleanupBatchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "ticketing.outbox.cleanup-batch-size must be between " + MIN_BATCH_SIZE + " and " + MAX_BATCH_SIZE
                            + ", but was: " + cleanupBatchSize);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Duration getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(Duration sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public long getAdvisoryLockId() {
        return advisoryLockId;
    }

    public void setAdvisoryLockId(long advisoryLockId) {
        this.advisoryLockId = advisoryLockId;
    }

    public Duration getRetention() {
        return retention;
    }

    public void setRetention(Duration retention) {
        this.retention = retention;
    }

    public int getCleanupBatchSize() {
        return cleanupBatchSize;
    }

    public void setCleanupBatchSize(int cleanupBatchSize) {
        this.cleanupBatchSize = cleanupBatchSize;
    }

    public String getCleanupCron() {
        return cleanupCron;
    }

    public void setCleanupCron(String cleanupCron) {
        this.cleanupCron = cleanupCron;
    }
}
