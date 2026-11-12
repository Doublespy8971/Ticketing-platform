package com.ticketing.payment.service.provider;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ticketing.payment.provider.mock")
public class MockPaymentProviderProperties {

    /** Probability (0.0 to 1.0) of random charge failure when using non-deterministic token. */
    private double declineRate = 0.05;

    /** Minimum simulated latency in ms. */
    private long minLatencyMs = 50;

    /** Maximum simulated latency in ms. */
    private long maxLatencyMs = 300;

    public double getDeclineRate() {
        return declineRate;
    }

    public void setDeclineRate(double declineRate) {
        this.declineRate = declineRate;
    }

    public long getMinLatencyMs() {
        return minLatencyMs;
    }

    public void setMinLatencyMs(long minLatencyMs) {
        this.minLatencyMs = minLatencyMs;
    }

    public long getMaxLatencyMs() {
        return maxLatencyMs;
    }

    public void setMaxLatencyMs(long maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }
}

