package com.ticketing.inventory.api;

public class IdempotencyPayloadMismatchException extends RuntimeException {

    public IdempotencyPayloadMismatchException(String message) {
        super(message);
    }
}

