package com.ticketing.payment.service.provider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

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

        // Repeating with the same idempotency key returns the exact same cached result and does not increment count
        PaymentResult second = provider.charge(key, 10000, "USD", MockPaymentProvider.MAGIC_TOKEN_SUCCESS);
        assertThat(second).isSameAs(result);
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
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
    }

    @Test
    @DisplayName("tok_error throws on first attempt and succeeds on retry")
    void tokErrorThrowsOnceThenSucceeds() throws Exception {
        provider.setDeclineRate(1.0); // even if decline rate is 1.0, tok_error is a magic token that succeeds on retry

        UUID key = UUID.randomUUID();
        assertThatThrownBy(() -> provider.charge(key, 20000, "USD", MockPaymentProvider.MAGIC_TOKEN_ERROR))
                .isInstanceOf(ProviderUnavailableException.class);

        PaymentResult result = provider.charge(key, 20000, "USD", MockPaymentProvider.MAGIC_TOKEN_ERROR);
        assertThat(result.outcome()).isEqualTo(PaymentResult.Outcome.SUCCEEDED);
        assertThat(result.providerReference()).isNotBlank().startsWith("mock_ch_");
        assertThat(provider.getEffectiveChargeCount()).isEqualTo(1);
    }
}

