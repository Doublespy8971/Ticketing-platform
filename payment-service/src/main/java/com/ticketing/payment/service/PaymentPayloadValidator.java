package com.ticketing.payment.service;

import com.ticketing.common.events.PaymentRequestedPayload;
import com.ticketing.common.events.RefundRequestedPayload;
import com.ticketing.consumer.NonRetryableEventException;

import java.util.Objects;

/**
 * Shared validator enforcing database constraints from {@code V1__payment_schema.sql}
 * on incoming order events before external payment provider calls or local database writes.
 */
public final class PaymentPayloadValidator {

    public static final int MAX_PAYMENT_METHOD_TOKEN_LENGTH = 128;
    public static final int MAX_REFUND_REASON_LENGTH = 64;
    public static final String DEFAULT_REFUND_REASON = "LATE_PAYMENT_EXPIRED_HOLD";

    private PaymentPayloadValidator() {
    }

    /**
     * Validates an {@code order.payment.requested} payload against the {@code payments} table constraints:
     * <ul>
     *     <li>Payload is non-null</li>
     *     <li>{@code orderId}, {@code reservationId}, and {@code userId} are non-null</li>
     *     <li>{@code reservationId} equals {@code orderId} (chk_payments_order_equals_res)</li>
     *     <li>{@code amountCents > 0} (chk_payments_amount_positive)</li>
     *     <li>{@code currency} is exactly 3 letters (ISO 4217, CHAR(3))</li>
     *     <li>{@code paymentMethodToken} is non-blank and at most 128 characters (VARCHAR(128))</li>
     * </ul>
     *
     * @throws NonRetryableEventException if any constraint is violated
     */
    public static void validatePaymentRequested(PaymentRequestedPayload payload) {
        if (payload == null) {
            throw new NonRetryableEventException("order.payment.requested payload must not be null");
        }
        if (payload.orderId() == null) {
            throw new NonRetryableEventException("order.payment.requested orderId must not be null");
        }
        if (payload.reservationId() == null) {
            throw new NonRetryableEventException("order.payment.requested reservationId must not be null");
        }
        if (payload.userId() == null) {
            throw new NonRetryableEventException("order.payment.requested userId must not be null");
        }
        if (!Objects.equals(payload.orderId(), payload.reservationId())) {
            throw new NonRetryableEventException(
                    "order.payment.requested reservationId (" + payload.reservationId()
                            + ") must equal orderId (" + payload.orderId() + ")");
        }
        if (payload.amountCents() <= 0) {
            throw new NonRetryableEventException(
                    "order.payment.requested amountCents must be positive (> 0), got: " + payload.amountCents());
        }
        if (!isValidCurrency(payload.currency())) {
            throw new NonRetryableEventException(
                    "order.payment.requested currency must be exactly 3 letters, got: " + payload.currency());
        }
        if (payload.paymentMethodToken() == null || payload.paymentMethodToken().isBlank()) {
            throw new NonRetryableEventException("order.payment.requested paymentMethodToken must not be blank");
        }
        if (payload.paymentMethodToken().length() > MAX_PAYMENT_METHOD_TOKEN_LENGTH) {
            throw new NonRetryableEventException(
                    "order.payment.requested paymentMethodToken exceeds maximum length of "
                            + MAX_PAYMENT_METHOD_TOKEN_LENGTH + " characters (length: "
                            + payload.paymentMethodToken().length() + ")");
        }
    }

    /**
     * Validates an {@code order.refund.requested} payload against {@code payments} table constraints:
     * <ul>
     *     <li>Payload is non-null</li>
     *     <li>{@code orderId}, {@code paymentId}, and {@code userId} are non-null</li>
     *     <li>{@code reservationId} if present must equal {@code orderId}</li>
     *     <li>{@code amountCents > 0}</li>
     *     <li>{@code currency} is exactly 3 letters (ISO 4217, CHAR(3))</li>
     * </ul>
     *
     * @throws NonRetryableEventException if any constraint is violated
     */
    public static void validateRefundRequested(RefundRequestedPayload payload) {
        if (payload == null) {
            throw new NonRetryableEventException("order.refund.requested payload must not be null");
        }
        if (payload.orderId() == null) {
            throw new NonRetryableEventException("order.refund.requested orderId must not be null");
        }
        if (payload.paymentId() == null) {
            throw new NonRetryableEventException("order.refund.requested paymentId must not be null");
        }
        if (payload.userId() == null) {
            throw new NonRetryableEventException("order.refund.requested userId must not be null");
        }
        if (payload.reservationId() != null && !Objects.equals(payload.orderId(), payload.reservationId())) {
            throw new NonRetryableEventException(
                    "order.refund.requested reservationId (" + payload.reservationId()
                            + ") must equal orderId (" + payload.orderId() + ")");
        }
        if (payload.amountCents() <= 0) {
            throw new NonRetryableEventException(
                    "order.refund.requested amountCents must be positive (> 0), got: " + payload.amountCents());
        }
        if (!isValidCurrency(payload.currency())) {
            throw new NonRetryableEventException(
                    "order.refund.requested currency must be exactly 3 letters, got: " + payload.currency());
        }
    }

    /**
     * Replaces a null or blank refund reason with the documented default ({@value #DEFAULT_REFUND_REASON})
     * and truncates it to the column length of {@code refund_reason} ({@value #MAX_REFUND_REASON_LENGTH}).
     *
     * @param reason input refund reason
     * @return sanitized and truncated refund reason (at most 64 characters)
     */
    public static String sanitizeRefundReason(String reason) {
        if (reason == null || reason.isBlank()) {
            return DEFAULT_REFUND_REASON;
        }
        String trimmed = reason.trim();
        if (trimmed.length() > MAX_REFUND_REASON_LENGTH) {
            return trimmed.substring(0, MAX_REFUND_REASON_LENGTH);
        }
        return trimmed;
    }

    /**
     * Checks if a currency string is exactly 3 alphabetic letters.
     */
    public static boolean isValidCurrency(String currency) {
        if (currency == null) {
            return false;
        }
        String trimmed = currency.trim();
        if (trimmed.length() != 3) {
            return false;
        }
        for (int i = 0; i < 3; i++) {
            if (!Character.isLetter(trimmed.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
