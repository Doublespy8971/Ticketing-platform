package com.ticketing.gateway.auth;

/**
 * Thrown when JWT validation fails.
 */
public class JwtValidationException extends RuntimeException {

    public enum Reason {
        MISSING,
        INVALID,
        EXPIRED,
        FORBIDDEN
    }

    private final Reason reason;

    public JwtValidationException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public JwtValidationException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}

