package com.ticketing.common.error;

/** Maps to {@code 404 RESOURCE_NOT_FOUND}. */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
