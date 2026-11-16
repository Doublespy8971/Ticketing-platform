package com.ticketing.payment.service.provider;

import java.util.UUID;

public interface PaymentProvider {

    PaymentResult charge(UUID idempotencyKey, long amountCents, String currency, String paymentMethodToken)
            throws ProviderUnavailableException;

    PaymentResult refund(UUID idempotencyKey, String providerReference, long amountCents, String currency)
            throws ProviderUnavailableException;

    default PaymentResult refund(UUID idempotencyKey, String providerReference, long amountCents, String currency, String paymentMethodToken)
            throws ProviderUnavailableException {
        return refund(idempotencyKey, providerReference, amountCents, currency);
    }
}
