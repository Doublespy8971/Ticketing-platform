package com.ticketing.consumer;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Consumer group session timeout and heartbeat properties for rapid crash-recovery failover
 * (docs/arch/03-kafka.md and docs/arch/09-conventions.md).
 */
@ConfigurationProperties(prefix = "ticketing.consumer")
public class ConsumerTimeoutProperties implements InitializingBean {

    public static final int DEFAULT_SESSION_TIMEOUT_MS = 15_000;
    public static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 3_000;
    public static final int MIN_SESSION_TIMEOUT_MS = 6_000;
    public static final int MAX_SESSION_TIMEOUT_MS = 60_000;
    public static final int DEFAULT_CONCURRENCY = 1;
    public static final int MIN_CONCURRENCY = 1;
    public static final int MAX_CONCURRENCY = 12;

    /**
     * Kafka consumer session timeout in milliseconds (between 6,000 and 60,000 ms, default 15,000 ms).
     */
    private int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;

    /**
     * Kafka consumer heartbeat interval in milliseconds (positive, at most one third of session timeout, default 3,000 ms).
     */
    private int heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;

    /**
     * Listener container concurrency (number of consumer threads per listener container;
     * between 1 and 12 matching the topic partition count, default 1).
     */
    private int concurrency = DEFAULT_CONCURRENCY;

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    @Override
    public void afterPropertiesSet() {
        validate();
    }

    public void validate() {
        if (sessionTimeoutMs < MIN_SESSION_TIMEOUT_MS || sessionTimeoutMs > MAX_SESSION_TIMEOUT_MS) {
            throw new IllegalArgumentException(
                    "ticketing.consumer.session-timeout-ms must be between " + MIN_SESSION_TIMEOUT_MS
                            + " and " + MAX_SESSION_TIMEOUT_MS + ", but was: " + sessionTimeoutMs);
        }
        if (heartbeatIntervalMs <= 0 || heartbeatIntervalMs > sessionTimeoutMs / 3) {
            throw new IllegalArgumentException(
                    "ticketing.consumer.heartbeat-interval-ms must be positive and at most one third of session-timeout-ms ("
                            + (sessionTimeoutMs / 3) + "), but was: " + heartbeatIntervalMs);
        }
        if (concurrency < MIN_CONCURRENCY || concurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException(
                    "ticketing.consumer.concurrency must be between " + MIN_CONCURRENCY
                            + " and " + MAX_CONCURRENCY + ", but was: " + concurrency);
        }
    }
}
