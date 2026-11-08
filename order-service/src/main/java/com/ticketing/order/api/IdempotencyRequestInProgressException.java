package com.ticketing.order.api;

public class IdempotencyRequestInProgressException extends RuntimeException {
    public IdempotencyRequestInProgressException(String message) {
        super(message);
    }
}

