package com.ticketing.inventory.api;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.BaseRestControllerAdvice;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.inventory.redis.RedisSeatGate;
import com.ticketing.inventory.redis.RedisUnavailableException;
import io.lettuce.core.RedisException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class InventoryExceptionHandler extends BaseRestControllerAdvice {

    private static final Logger log = LoggerFactory.getLogger(InventoryExceptionHandler.class);

    private static final String SERVICE_NAME = "inventory-service";
    private static final String METRIC_REDIS_UNAVAILABLE = "inventory_redis_unavailable_total";

    private final MeterRegistry meterRegistry;

    public InventoryExceptionHandler(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        this.meterRegistry = meterRegistryProvider != null ? meterRegistryProvider.getIfAvailable() : null;
    }

    @ExceptionHandler(UnauthorizedException.class)
    public ResponseEntity<ProblemDetails> handleUnauthorized(UnauthorizedException ex, HttpServletRequest request) {
        ProblemDetails body = ProblemDetails.of(
                ErrorCode.UNAUTHORIZED,
                ex.getMessage(),
                request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY),
                null
        );
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(body);
    }

    @ExceptionHandler(ForbiddenException.class)
    public ResponseEntity<ProblemDetails> handleForbidden(ForbiddenException ex, HttpServletRequest request) {
        ProblemDetails body = ProblemDetails.of(
                ErrorCode.FORBIDDEN,
                ex.getMessage(),
                request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY),
                null
        );
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(body);
    }

    @ExceptionHandler(MissingIdempotencyKeyException.class)
    public ResponseEntity<ProblemDetails> handleMissingIdempotencyKey(
            MissingIdempotencyKeyException ex, HttpServletRequest request) {
        ProblemDetails body = ProblemDetails.of(
                ErrorCode.MISSING_IDEMPOTENCY_KEY,
                ex.getMessage(),
                request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY),
                null
        );
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(body);
    }

    @ExceptionHandler(IdempotencyPayloadMismatchException.class)
    public ResponseEntity<ProblemDetails> handleIdempotencyPayloadMismatch(
            IdempotencyPayloadMismatchException ex, HttpServletRequest request) {
        ProblemDetails body = ProblemDetails.of(
                ErrorCode.IDEMPOTENCY_KEY_PAYLOAD_MISMATCH,
                ex.getMessage(),
                request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY),
                null
        );
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(body);
    }

    @ExceptionHandler(IdempotencyRequestInProgressException.class)
    public ResponseEntity<ProblemDetails> handleIdempotencyInProgress(
            IdempotencyRequestInProgressException ex, HttpServletRequest request) {
        ProblemDetails body = ProblemDetails.of(
                ErrorCode.IDEMPOTENCY_REQUEST_IN_PROGRESS,
                ex.getMessage(),
                request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY),
                null
        );
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(body);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ProblemDetails> handleIllegalArgument(
            IllegalArgumentException ex, HttpServletRequest request) {
        ProblemDetails body = ProblemDetails.of(
                ErrorCode.VALIDATION_ERROR,
                ex.getMessage(),
                request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY),
                null
        );
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(body);
    }

    @ExceptionHandler({
            RedisUnavailableException.class,
            RedisException.class,
            RedisConnectionFailureException.class,
            RedisSystemException.class
    })
    public ResponseEntity<ProblemDetails> handleRedisUnavailable(
            Exception ex, HttpServletRequest request) {
        Throwable rootCause = ex;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        String correlationId = MDC.get(CorrelationConstants.MDC_KEY);
        log.error("Redis unavailable while handling {} [correlationId={}]: {}: {}",
                request.getRequestURI(), correlationId, rootCause.getClass().getName(), rootCause.getMessage());

        boolean isHold = isHoldPath(request);
        boolean alreadyRecorded = false;
        if (ex instanceof RedisUnavailableException rue && rue.isMetricRecorded()) {
            alreadyRecorded = true;
        }

        if (!alreadyRecorded && meterRegistry != null) {
            String cause = RedisSeatGate.resolveCause(ex);
            String operation = isHold ? "hold" : "other";
            Counter.builder(METRIC_REDIS_UNAVAILABLE)
                    .tag("service", SERVICE_NAME)
                    .tag("operation", operation)
                    .tag("cause", cause)
                    .register(meterRegistry)
                    .increment();
            if (ex instanceof RedisUnavailableException rue) {
                rue.setMetricRecorded(true);
            }
        }

        if (isHold) {
            ProblemDetails body = ProblemDetails.of(
                    ErrorCode.SERVICE_UNAVAILABLE,
                    "Inventory concurrency gate is temporarily unavailable.",
                    request.getRequestURI(),
                    correlationId,
                    null
            );
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .header(HttpHeaders.RETRY_AFTER, "1")
                    .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                    .body(body);
        }

        ProblemDetails body = ProblemDetails.of(
                ErrorCode.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred.",
                request.getRequestURI(),
                correlationId,
                null
        );
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(body);
    }

    private boolean isHoldPath(HttpServletRequest request) {
        if (request == null) {
            return false;
        }
        String method = request.getMethod();
        String uri = request.getRequestURI();
        return "POST".equalsIgnoreCase(method) && uri != null &&
                (uri.equals("/api/v1/reservations") || uri.endsWith("/api/v1/reservations") || uri.endsWith("/api/v1/reservations/"));
    }
}
