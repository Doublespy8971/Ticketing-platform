package com.ticketing.inventory.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "ticketing.redis")
public class InventoryRedisProperties {

    private long seatHoldTtlSeconds = RedisKeys.DEFAULT_SEAT_HOLD_TTL_SECONDS;
    private long eventDropTsTtlSeconds = RedisKeys.EVENT_DROP_TS_TTL_SECONDS;
    private long holdTtlSafetyMarginSeconds = RedisKeys.HOLD_TTL_SAFETY_MARGIN_SECONDS;
    private boolean rewarmOnStartup = false;
    private int pipelineBatchSize = 500;
    private int soldSeatsPageSize = 1000;
    private Duration commandTimeout = Duration.ofMillis(500);

    public long getSeatHoldTtlSeconds() {
        return seatHoldTtlSeconds;
    }

    public void setSeatHoldTtlSeconds(long seatHoldTtlSeconds) {
        this.seatHoldTtlSeconds = seatHoldTtlSeconds;
    }

    public long getEventDropTsTtlSeconds() {
        return eventDropTsTtlSeconds;
    }

    public void setEventDropTsTtlSeconds(long eventDropTsTtlSeconds) {
        this.eventDropTsTtlSeconds = eventDropTsTtlSeconds;
    }

    public long getHoldTtlSafetyMarginSeconds() {
        return holdTtlSafetyMarginSeconds;
    }

    public void setHoldTtlSafetyMarginSeconds(long holdTtlSafetyMarginSeconds) {
        this.holdTtlSafetyMarginSeconds = holdTtlSafetyMarginSeconds;
    }

    public boolean isRewarmOnStartup() {
        return rewarmOnStartup;
    }

    public void setRewarmOnStartup(boolean rewarmOnStartup) {
        this.rewarmOnStartup = rewarmOnStartup;
    }

    public int getPipelineBatchSize() {
        return pipelineBatchSize;
    }

    public void setPipelineBatchSize(int pipelineBatchSize) {
        this.pipelineBatchSize = pipelineBatchSize;
    }

    public int getSoldSeatsPageSize() {
        return soldSeatsPageSize;
    }

    public void setSoldSeatsPageSize(int soldSeatsPageSize) {
        this.soldSeatsPageSize = soldSeatsPageSize;
    }

    public Duration getCommandTimeout() {
        return commandTimeout;
    }

    public void setCommandTimeout(Duration commandTimeout) {
        this.commandTimeout = commandTimeout;
    }
}

