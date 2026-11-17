package com.ticketing.inventory.redis;

public class RedisUnavailableException extends RuntimeException {

    private boolean metricRecorded;
    private boolean compensationAttempted;

    public RedisUnavailableException(String message) {
        super(message);
    }

    public RedisUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public boolean isMetricRecorded() {
        return metricRecorded;
    }

    public void setMetricRecorded(boolean metricRecorded) {
        this.metricRecorded = metricRecorded;
    }

    public boolean isCompensationAttempted() {
        return compensationAttempted;
    }

    public void setCompensationAttempted(boolean compensationAttempted) {
        this.compensationAttempted = compensationAttempted;
    }
}
