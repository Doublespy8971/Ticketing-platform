package com.ticketing.inventory.api;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.BaseRestControllerAdvice;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.inventory.redis.RedisUnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class InventoryExceptionHandler extends BaseRestControllerAdvice {

    private static final Logger log = LoggerFactory.getLogger(InventoryExceptionHandler.class);

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

    @ExceptionHandler(RedisUnavailableException.class)
    public ResponseEntity<ProblemDetails> handleRedisUnavailable(
            RedisUnavailableException ex, HttpServletRequest request) {
        log.error("Redis unavailable while handling {}: {}", request.getRequestURI(), ex.getMessage(), ex);
        ProblemDetails body = ProblemDetails.of(
                ErrorCode.INTERNAL_SERVER_ERROR,
                "Inventory concurrency gate is temporarily unavailable.",
                request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY),
                null
        );
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(body);
    }
}
