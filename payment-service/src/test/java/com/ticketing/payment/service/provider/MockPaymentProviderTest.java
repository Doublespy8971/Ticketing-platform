package com.ticketing.payment.service.provider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockPaymentProviderTest {

    private MockPaymentProviderProperties properties;
    private MockPaymentProvider provider;

    @BeforeEach
    void setUp() {
        properties = new MockPaymentProviderProperties();
        properties.setMinLatencyMs(0);
        properties.setMaxLatencyMs(0);
        provider = new MockPaymentProvider(properties);
    }

    @Test
    @DisplayName("With decline rate set to 1.0 (always decline), tok_success still succeeds")
    void withDeclineRate1_tokSuccessStillSucceeds() throws Exception {
        provider.setDeclineRate(1.0);

        UUID key = UUID.randomUUID();
        PaymentResult result = provider.charge(key, 10000, "USD", MockPaymentProvider.MAGIC_TOKEN_SUCCESS);

        assertThat(result.outcome()).isEqualTo(PaymentResult.Outcome.SUCCEEDED);
        assertThat(result.providerReference()).isNotBlank().startsWith("mock_ch_");
        assertThat(result.failureCode()).isNull();
        assertThat(result.failureMessage()).isNull();
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(1);

        // Repeating with the same idempotency key returns the exact same cached result and does not increment count
        PaymentResult second = provider.charge(key, 10000, "USD", MockPaymentProvider.MAGIC_TOKEN_SUCCESS);
        assertThat(second).isSameAs(result);
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("With decline rate set to 1.0 (always decline), a normal token declines")
    void withDeclineRate1_normalTokenDeclines() throws Exception {
        provider.setDeclineRate(1.0);

        UUID key = UUID.randomUUID();
        PaymentResult result = provider.charge(key, 10000, "USD", "normal_random_token_123");

        assertThat(result.outcome()).isEqualTo(PaymentResult.Outcome.DECLINED);
        assertThat(result.failureCode()).isEqualTo(MockPaymentProvider.FAILURE_CODE_DECLINED);
        assertThat(result.failureMessage()).isEqualTo(MockPaymentProvider.FAILURE_MESSAGE_DECLINED);
        assertThat(result.providerReference()).isNull();
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("With decline rate set to 1.0 (always decline), tok_decline declines with documented code")
    void withDeclineRate1_tokDeclineDeclinesWithDocumentedCode() throws Exception {
        provider.setDeclineRate(1.0);

        UUID key = UUID.randomUUID();
        PaymentResult result = provider.charge(key, 10000, "USD", MockPaymentProvider.MAGIC_TOKEN_DECLINE);

        assertThat(result.outcome()).isEqualTo(PaymentResult.Outcome.DECLINED);
        assertThat(result.failureCode()).isEqualTo(MockPaymentProvider.FAILURE_CODE_DECLINED);
        assertThat(result.failureMessage()).isEqualTo(MockPaymentProvider.FAILURE_MESSAGE_DECLINED);
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("With decline rate set to 0.0, a normal token succeeds")
    void withDeclineRate0_normalTokenSucceeds() throws Exception {
        provider.setDeclineRate(0.0);

        UUID key = UUID.randomUUID();
        PaymentResult result = provider.charge(key, 15000, "USD", "normal_token_abc");

        assertThat(result.outcome()).isEqualTo(PaymentResult.Outcome.SUCCEEDED);
        assertThat(result.providerReference()).isNotBlank().startsWith("mock_ch_");
        assertThat(result.failureCode()).isNull();
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("tok_error throws on first attempt and succeeds on retry")
    void tokErrorThrowsOnceThenSucceeds() throws Exception {
        provider.setDeclineRate(1.0); // even if decline rate is 1.0, tok_error is a magic token that succeeds on retry

        UUID key = UUID.randomUUID();
        assertThatThrownBy(() -> provider.charge(key, 20000, "USD", MockPaymentProvider.MAGIC_TOKEN_ERROR))
                .isInstanceOf(ProviderUnavailableException.class);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(1);
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(0);

        PaymentResult result = provider.charge(key, 20000, "USD", MockPaymentProvider.MAGIC_TOKEN_ERROR);
        assertThat(result.outcome()).isEqualTo(PaymentResult.Outcome.SUCCEEDED);
        assertThat(result.providerReference()).isNotBlank().startsWith("mock_ch_");
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("tok_refund_decline succeeds on charge and deterministically declines on refund")
    void tokRefundDeclineSucceedsOnChargeAndDeclinesOnRefund() throws Exception {
        UUID chargeKey = UUID.randomUUID();
        PaymentResult chargeResult = provider.charge(chargeKey, 10000, "USD", MockPaymentProvider.MAGIC_TOKEN_REFUND_DECLINE);

        assertThat(chargeResult.outcome()).isEqualTo(PaymentResult.Outcome.SUCCEEDED);
        assertThat(chargeResult.providerReference()).isNotBlank().startsWith("mock_ch_");
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(1);

        UUID refundKey = UUID.randomUUID();
        PaymentResult refundResult = provider.refund(refundKey, chargeResult.providerReference(), 10000, "USD");

        assertThat(refundResult.outcome()).isEqualTo(PaymentResult.Outcome.DECLINED);
        assertThat(refundResult.failureCode()).isEqualTo(MockPaymentProvider.FAILURE_CODE_REFUND_DECLINED);
        assertThat(refundResult.failureMessage()).isEqualTo(MockPaymentProvider.FAILURE_MESSAGE_REFUND_DECLINED);
        assertThat(provider.getEffectiveRefundCount()).isEqualTo(1);
        assertThat(provider.getRawRefundInvocationCount()).isEqualTo(1);

        // Repeat refund with same idempotency key returns identical declined result
        PaymentResult secondRefund = provider.refund(refundKey, chargeResult.providerReference(), 10000, "USD");
        assertThat(secondRefund).isEqualTo(refundResult);
        assertThat(provider.getEffectiveRefundCount()).isEqualTo(1);
        assertThat(provider.getRawRefundInvocationCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("Concurrent charge calls execute atomically once and increment effective count once")
    void concurrentChargesAtomicIdempotency() throws Exception {
        int threads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        UUID key = UUID.randomUUID();

        List<Future<PaymentResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(executor.submit(() -> {
                ready.countDown();
                start.await();
                return provider.charge(key, 12000, "USD", MockPaymentProvider.MAGIC_TOKEN_SUCCESS);
            }));
        }

        ready.await(5, TimeUnit.SECONDS);
        start.countDown();

        PaymentResult first = futures.get(0).get(5, TimeUnit.SECONDS);
        for (Future<PaymentResult> f : futures) {
            assertThat(f.get(5, TimeUnit.SECONDS).providerReference()).isEqualTo(first.providerReference());
        }
        executor.shutdown();

        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(threads);
    }

    @Test
    @DisplayName("Concurrent charges with tok_error: exactly one throws and retry succeeds")
    void concurrentChargesWithTokError() throws Exception {
        UUID key = UUID.randomUUID();

        // First call fails
        assertThatThrownBy(() -> provider.charge(key, 12000, "USD", MockPaymentProvider.MAGIC_TOKEN_ERROR))
                .isInstanceOf(ProviderUnavailableException.class);

        // Subsequent concurrent calls all succeed
        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);

        List<Future<PaymentResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(executor.submit(() -> {
                ready.countDown();
                start.await();
                return provider.charge(key, 12000, "USD", MockPaymentProvider.MAGIC_TOKEN_ERROR);
            }));
        }

        ready.await(5, TimeUnit.SECONDS);
        start.countDown();

        for (Future<PaymentResult> f : futures) {
            assertThat(f.get(5, TimeUnit.SECONDS).outcome()).isEqualTo(PaymentResult.Outcome.SUCCEEDED);
        }
        executor.shutdown();

        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
        assertThat(provider.getRawChargeInvocationCount()).isEqualTo(threads + 1);
    }
}
