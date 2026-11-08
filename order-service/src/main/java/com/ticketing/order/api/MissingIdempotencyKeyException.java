package com.ticketing.order.api;

public class MissingIdempotencyKeyException extends RuntimeException {
    public MissingIdempotencyKeyException(String message) {
        super(message);
    }
}

