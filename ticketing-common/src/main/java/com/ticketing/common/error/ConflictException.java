package com.ticketing.common.error;

/** Maps to a {@code 409 Conflict} response carrying one of the 409 error codes. */
public class ConflictException extends RuntimeException {

    private final ErrorCode errorCode;

    public ConflictException(ErrorCode errorCode, String message) {
        super(message);
        if (errorCode.httpStatus() != 409) {
            throw new IllegalArgumentException("ErrorCode " + errorCode + " is not a 409 code");
        }
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
