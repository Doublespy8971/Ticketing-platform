package com.ticketing.payment.service.provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock implementation of {@link PaymentProvider}.
 * <p>
 * In production, an external payment gateway (e.g., Stripe, Adyen) guarantees idempotency
 * per {@code Idempotency-Key} natively: re-submitting an authorization or refund with the
 * same key returns the original cached response and never performs a duplicate charge or refund.
 * In this mock implementation, we provide that same guarantee using in-memory maps keyed by
 * the request's idempotency key.
 */
@Component
public class MockPaymentProvider implements PaymentProvider {

    private static final Logger log = LoggerFactory.getLogger(MockPaymentProvider.class);

    public static final String MAGIC_TOKEN_SUCCESS = "tok_success";
    public static final String MAGIC_TOKEN_DECLINE = "tok_decline";
    public static final String MAGIC_TOKEN_ERROR = "tok_error";

    public static final String FAILURE_CODE_DECLINED = "CARD_DECLINED";
    public static final String FAILURE_MESSAGE_DECLINED = "Payment declined by card issuer";

    private final ConcurrentMap<UUID, PaymentResult> chargeResults = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, PaymentResult> refundResults = new ConcurrentHashMap<>();
    private final Set<UUID> errorTokensSeen = ConcurrentHashMap.newKeySet();

    private final AtomicInteger chargeCount = new AtomicInteger();
    private final AtomicInteger refundCount = new AtomicInteger();

    private volatile Random random;
    private volatile double declineRate;
    private volatile long minLatencyMs;
    private volatile long maxLatencyMs;

    @Autowired
    public MockPaymentProvider(MockPaymentProviderProperties properties) {
        this(properties, new Random());
    }

    public MockPaymentProvider(MockPaymentProviderProperties properties, Random random) {
        Objects.requireNonNull(properties, "properties must not be null");
        this.random = Objects.requireNonNull(random, "random must not be null");
        this.declineRate = properties.getDeclineRate();
        this.minLatencyMs = properties.getMinLatencyMs();
        this.maxLatencyMs = properties.getMaxLatencyMs();
    }

    @Override
    public PaymentResult charge(UUID idempotencyKey, long amountCents, String currency, String paymentMethodToken)
            throws ProviderUnavailableException {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(paymentMethodToken, "paymentMethodToken must not be null");

        // Idempotency: repeating a call with the same key returns the exact same result and never counts as a second charge
        PaymentResult existing = chargeResults.get(idempotencyKey);
        if (existing != null) {
            log.debug("MockPaymentProvider returning cached charge result for idempotencyKey={}", idempotencyKey);
            return existing;
        }

        // Magic token: tok_error throws ProviderUnavailableException once per idempotency key and succeeds afterwards
        if (MAGIC_TOKEN_ERROR.equals(paymentMethodToken)) {
            if (errorTokensSeen.add(idempotencyKey)) {
                log.info("MockPaymentProvider throwing ProviderUnavailableException for token on first attempt for key={}",
                        idempotencyKey);
                throw new ProviderUnavailableException("Mock payment gateway temporarily unreachable");
            }
        }

        simulateLatency();

        final PaymentResult result;
        if (MAGIC_TOKEN_SUCCESS.equals(paymentMethodToken)) {
            String reference = "mock_ch_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            result = PaymentResult.succeeded(reference);
        } else if (MAGIC_TOKEN_DECLINE.equals(paymentMethodToken)) {
            result = PaymentResult.declined(FAILURE_CODE_DECLINED, FAILURE_MESSAGE_DECLINED);
        } else if (MAGIC_TOKEN_ERROR.equals(paymentMethodToken)) {
            String reference = "mock_ch_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            result = PaymentResult.succeeded(reference);
        } else if (declineRate > 0.0 && random.nextDouble() < declineRate) {
            result = PaymentResult.declined(FAILURE_CODE_DECLINED, FAILURE_MESSAGE_DECLINED);
        } else {
            String reference = "mock_ch_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            result = PaymentResult.succeeded(reference);
        }

        chargeResults.put(idempotencyKey, result);
        chargeCount.incrementAndGet();
        return result;
    }

    @Override
    public PaymentResult refund(UUID idempotencyKey, String providerReference, long amountCents, String currency)
            throws ProviderUnavailableException {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(currency, "currency must not be null");

        PaymentResult existing = refundResults.get(idempotencyKey);
        if (existing != null) {
            log.debug("MockPaymentProvider returning cached refund result for idempotencyKey={}", idempotencyKey);
            return existing;
        }

        simulateLatency();

        String refundRef = "mock_ref_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        PaymentResult result = PaymentResult.succeeded(refundRef);
        refundResults.put(idempotencyKey, result);
        refundCount.incrementAndGet();
        return result;
    }

    private void simulateLatency() {
        if (maxLatencyMs <= 0) {
            return;
        }
        long delay;
        if (maxLatencyMs > minLatencyMs) {
            delay = minLatencyMs + (long) (random.nextDouble() * (maxLatencyMs - minLatencyMs));
        } else {
            delay = minLatencyMs;
        }
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // Exposed for tests
    public int getEffectiveChargeCount() {
        return chargeCount.get();
    }

    public int getEffectiveRefundCount() {
        return refundCount.get();
    }

    public void reset() {
        chargeResults.clear();
        refundResults.clear();
        errorTokensSeen.clear();
        chargeCount.set(0);
        refundCount.set(0);
    }

    public void setRandom(Random random) {
        this.random = Objects.requireNonNull(random, "random must not be null");
    }

    public void setDeclineRate(double declineRate) {
        this.declineRate = declineRate;
    }

    public void setMinLatencyMs(long minLatencyMs) {
        this.minLatencyMs = minLatencyMs;
    }

    public void setMaxLatencyMs(long maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }
}

