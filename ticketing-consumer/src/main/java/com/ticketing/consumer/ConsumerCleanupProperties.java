package com.ticketing.consumer;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the scheduled {@code processed_events} retention cleaner
 * (docs/arch/05-outbox-idempotency.md, section 5.5).
 */
@ConfigurationProperties(prefix = "ticketing.consumer.cleanup")
public class ConsumerCleanupProperties implements InitializingBean {

    public static final int MIN_RETENTION_HOURS = 192;
    public static final int MIN_BATCH_SIZE = 1;
    public static final int MAX_BATCH_SIZE = 10_000;
    public static final String DEFAULT_CRON = "0 0 * * * *";

    /** Enables the scheduled processed_events retention cleaner (default true). */
    private boolean enabled = true;

    /** Retention window in hours before processed_events rows are eligible for deletion (minimum 192h / 8 days). */
    private int retentionHours = MIN_RETENTION_HOURS;

    /** Maximum number of rows deleted per batch statement (1..10000, default 5000). */
    private int batchSize = 5000;

    /** Cron expression for retention cleanup on ticketingConsumerCleanupScheduler. */
    private String cron = DEFAULT_CRON;

    @Override
    public void afterPropertiesSet() {
        validate();
    }

    public void validate() {
        if (retentionHours < MIN_RETENTION_HOURS) {
            throw new IllegalArgumentException(
                    "ticketing.consumer.cleanup.retention-hours must be at least " + MIN_RETENTION_HOURS
                            + " (8 days, exceeding the 7-day Kafka topic retention), but was: " + retentionHours);
        }
        if (batchSize < MIN_BATCH_SIZE || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "ticketing.consumer.cleanup.batch-size must be between " + MIN_BATCH_SIZE
                            + " and " + MAX_BATCH_SIZE + ", but was: " + batchSize);
        }
        if (cron == null || cron.isBlank()) {
            throw new IllegalArgumentException("ticketing.consumer.cleanup.cron must not be null or blank");
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getRetentionHours() {
        return retentionHours;
    }

    public void setRetentionHours(int retentionHours) {
        this.retentionHours = retentionHours;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }
}

