package com.ticketing.payment.service.provider;

public record PaymentResult(
        Outcome outcome,
        String providerReference,
        String failureCode,
        String failureMessage
) {
    public enum Outcome {
        SUCCEEDED,
        DECLINED
    }

    public static PaymentResult succeeded(String providerReference) {
        return new PaymentResult(Outcome.SUCCEEDED, providerReference, null, null);
    }

    public static PaymentResult declined(String failureCode, String failureMessage) {
        return new PaymentResult(Outcome.DECLINED, null, failureCode, failureMessage);
    }
}

