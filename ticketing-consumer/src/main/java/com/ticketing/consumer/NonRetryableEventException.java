package com.ticketing.consumer;

/**
 * Thrown when an incoming Kafka record cannot be parsed as a valid {@code EventEnvelope}, fails schema version
 * or event-type validation, or is explicitly classified as a non-retryable poison message.
 * <p>
 * Records failing with {@code NonRetryableEventException} bypass {@code -retry-0} and {@code -retry-1}
 * and route directly to the dead-letter topic ({@code -dlt}) without stalling the partition.
 */
public class NonRetryableEventException extends RuntimeException {

    public NonRetryableEventException(String message) {
        super(message);
    }

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}

