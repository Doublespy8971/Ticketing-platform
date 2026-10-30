package com.ticketing.inventory.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ticketing.sweeper")
public class ExpirySweeperProperties {

    public static final int DEFAULT_BATCH_SIZE = 500;
    public static final long DEFAULT_INTERVAL_MS = 1000L;

    /** Whether scheduled background execution of the expiry sweeper is enabled. */
    private boolean enabled = true;

    /** Fixed delay in milliseconds between expiry sweep runs (default 1000 ms). */
    private long intervalMs = DEFAULT_INTERVAL_MS;

    /** Maximum number of expired HELD reservations selected per sweep (default 500). */
    private int batchSize = DEFAULT_BATCH_SIZE;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be positive");
        }
        this.intervalMs = intervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = batchSize;
    }
}

