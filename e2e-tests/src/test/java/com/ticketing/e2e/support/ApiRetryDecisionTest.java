package com.ticketing.e2e.support;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("fast")
class ApiRetryDecisionTest {

    @Test
    void retriesGetRequestsOnConnectionResetOrClosed() {
        IOException noBytes = new IOException("HTTP/1.1 header parser received no bytes");
        IOException connReset = new IOException("Connection reset by peer");
        EOFException eof = new EOFException("Reached EOF prematurely");

        assertThat(Api.shouldRetry("GET", Map.of(), noBytes)).isTrue();
        assertThat(Api.shouldRetry("get", null, connReset)).isTrue();
        assertThat(Api.shouldRetry("GET", Map.of("Accept", "application/json"), eof)).isTrue();
    }

    @Test
    void retriesIdempotencyKeyRequestsOnConnectionResetOrClosed() {
        String key = UUID.randomUUID().toString();
        IOException noBytes = new IOException("HTTP/1.1 header parser received no bytes");

        assertThat(Api.shouldRetry("POST", Map.of("Idempotency-Key", key), noBytes)).isTrue();
        assertThat(Api.shouldRetry("POST", Map.of("idempotency-key", key), noBytes)).isTrue();
        assertThat(Api.shouldRetry("PATCH", Map.of("Idempotency-Key", key), noBytes)).isTrue();
    }

    @Test
    void neverRetriesPostWithoutIdempotencyKey() {
        IOException noBytes = new IOException("HTTP/1.1 header parser received no bytes");

        assertThat(Api.shouldRetry("POST", Map.of(), noBytes)).isFalse();
        assertThat(Api.shouldRetry("POST", null, noBytes)).isFalse();
        assertThat(Api.shouldRetry("POST", Map.of("Authorization", "Bearer token"), noBytes)).isFalse();
        assertThat(Api.shouldRetry("POST", Map.of("Idempotency-Key", "   "), noBytes)).isFalse();
    }

    @Test
    void neverRetriesOnTimeoutErrors() {
        String key = UUID.randomUUID().toString();
        HttpTimeoutException reqTimeout = new HttpTimeoutException("request timed out");
        HttpConnectTimeoutException connTimeout = new HttpConnectTimeoutException("connect timed out");

        assertThat(Api.shouldRetry("GET", Map.of(), reqTimeout)).isFalse();
        assertThat(Api.shouldRetry("GET", Map.of(), connTimeout)).isFalse();
        assertThat(Api.shouldRetry("POST", Map.of("Idempotency-Key", key), reqTimeout)).isFalse();
        assertThat(Api.shouldRetry("POST", Map.of("Idempotency-Key", key), connTimeout)).isFalse();
    }

    @Test
    void neverRetriesOnNonIoExceptions() {
        String key = UUID.randomUUID().toString();
        IllegalStateException runtime = new IllegalStateException("HTTP/1.1 header parser received no bytes");

        assertThat(Api.shouldRetry("GET", Map.of(), runtime)).isFalse();
        assertThat(Api.shouldRetry("POST", Map.of("Idempotency-Key", key), runtime)).isFalse();
    }
}
