package com.ticketing.inventory.api;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.inventory.redis.RedisUnavailableException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryExceptionHandlerTest {

    private MeterRegistry meterRegistry;
    private InventoryExceptionHandler handler;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
        beanFactory.addBean("meterRegistry", meterRegistry);
        handler = new InventoryExceptionHandler(beanFactory.getBeanProvider(MeterRegistry.class));
        MDC.put(CorrelationConstants.MDC_KEY, "corr-test-123");
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void handleRedisUnavailableOnHoldPathReturns503WithRetryAfterAndStandardProblemDetails() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/reservations");
        RedisUnavailableException ex = new RedisUnavailableException("Simulated Redis timeout",
                new RedisCommandTimeoutException("Command timed out after 2000ms"));

        ResponseEntity<ProblemDetails> response = handler.handleRedisUnavailable(ex, request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");

        ProblemDetails body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo(503);
        assertThat(body.errorCode()).isEqualTo(ErrorCode.SERVICE_UNAVAILABLE.name());
        assertThat(body.detail()).isEqualTo("Inventory concurrency gate is temporarily unavailable.");
        assertThat(body.instance()).isEqualTo("/api/v1/reservations");
        assertThat(body.correlationId()).isEqualTo("corr-test-123");

        double holdTimeoutMetric = meterRegistry.get("inventory_redis_unavailable_total")
                .tag("service", "inventory-service")
                .tag("operation", "hold")
                .tag("cause", "timeout")
                .counter()
                .count();
        assertThat(holdTimeoutMetric).isEqualTo(1.0);
    }

    @Test
    void handleRedisConnectionExceptionOnHoldPathMapsToConnectionCause() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/reservations");
        RedisConnectionException ex = new RedisConnectionException("Unable to connect to Redis");

        ResponseEntity<ProblemDetails> response = handler.handleRedisUnavailable(ex, request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");

        double holdConnMetric = meterRegistry.get("inventory_redis_unavailable_total")
                .tag("service", "inventory-service")
                .tag("operation", "hold")
                .tag("cause", "connection")
                .counter()
                .count();
        assertThat(holdConnMetric).isEqualTo(1.0);
    }

    @Test
    void handleRedisUnavailableDoesNotDoubleCountIfAlreadyRecorded() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/reservations");
        RedisUnavailableException ex = new RedisUnavailableException("Already recorded");
        ex.setMetricRecorded(true);

        ResponseEntity<ProblemDetails> response = handler.handleRedisUnavailable(ex, request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(meterRegistry.find("inventory_redis_unavailable_total").counters()).isEmpty();
    }

    @Test
    void handleRedisUnavailableOnNonHoldPathReturns500InternalServerError() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/v1/events/123/seats");
        RedisUnavailableException ex = new RedisUnavailableException("Redis failed");

        ResponseEntity<ProblemDetails> response = handler.handleRedisUnavailable(ex, request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
        ProblemDetails body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo(500);
        assertThat(body.errorCode()).isEqualTo(ErrorCode.INTERNAL_SERVER_ERROR.name());
        assertThat(body.detail()).isEqualTo("An unexpected error occurred.");
    }

    @Test
    void handleUnexpectedNonRedisExceptionReturns500WithGenericDetail() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/reservations");
        NullPointerException ex = new NullPointerException("Simulated NPE");

        ResponseEntity<ProblemDetails> response = handler.handleUnexpected(ex, request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
        ProblemDetails body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo(500);
        assertThat(body.errorCode()).isEqualTo(ErrorCode.INTERNAL_SERVER_ERROR.name());
        assertThat(body.detail()).isEqualTo("An unexpected error occurred.");
        assertThat(body.correlationId()).isEqualTo("corr-test-123");
    }
}
