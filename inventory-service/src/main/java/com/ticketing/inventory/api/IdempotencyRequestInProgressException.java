package com.ticketing.inventory.api;

public class IdempotencyRequestInProgressException extends RuntimeException {

    public IdempotencyRequestInProgressException(String message) {
        super(message);
    }
}

