package com.ticketing.order.api;

public class IdempotencyPayloadMismatchException extends RuntimeException {
    public IdempotencyPayloadMismatchException(String message) {
        super(message);
    }
}

