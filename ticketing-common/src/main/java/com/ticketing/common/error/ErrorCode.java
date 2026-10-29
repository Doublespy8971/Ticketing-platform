package com.ticketing.common.error;

/** Canonical errorCode enum (docs/arch/06-api.md, section 7.2). */
public enum ErrorCode {

    VALIDATION_ERROR(400),
    MISSING_IDEMPOTENCY_KEY(400),
    UNAUTHORIZED(401),
    FORBIDDEN(403),
    RESOURCE_NOT_FOUND(404),
    EVENT_NOT_OPEN(409),
    SEAT_ALREADY_TAKEN(409),
    ORDER_NOT_READY(409),
    RESERVATION_EXPIRED(409),
    INVALID_ORDER_STATE(409),
    IDEMPOTENCY_REQUEST_IN_PROGRESS(409),
    INVALID_STATE_TRANSITION(409),
    IDEMPOTENCY_KEY_PAYLOAD_MISMATCH(422),
    RATE_LIMIT_EXCEEDED(429),
    INTERNAL_SERVER_ERROR(500);

    private final int httpStatus;

    ErrorCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public int httpStatus() {
        return httpStatus;
    }

    /** Human readable title, e.g. {@code ORDER_NOT_READY} becomes {@code Order Not Ready}. */
    public String title() {
        StringBuilder sb = new StringBuilder();
        for (String part : name().split("_")) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(part.charAt(0)).append(part.substring(1).toLowerCase());
        }
        return sb.toString();
    }

    /** Problem type slug, e.g. {@code ORDER_NOT_READY} becomes {@code order-not-ready}. */
    public String slug() {
        return name().toLowerCase().replace('_', '-');
    }
}
