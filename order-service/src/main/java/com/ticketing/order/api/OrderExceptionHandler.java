package com.ticketing.order.api;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.BaseRestControllerAdvice;
import com.ticketing.common.error.ConflictException;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.common.error.ResourceNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.MDC;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class OrderExceptionHandler extends BaseRestControllerAdvice {

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

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<ProblemDetails> handleConflict(ConflictException ex, HttpServletRequest request) {
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.CONFLICT)
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE));
        if (ex.errorCode() == ErrorCode.ORDER_NOT_READY || ex.errorCode() == ErrorCode.IDEMPOTENCY_REQUEST_IN_PROGRESS) {
            builder.header(HttpHeaders.RETRY_AFTER, "1");
        }
        ProblemDetails body = ProblemDetails.of(
                ex.errorCode(),
                ex.getMessage(),
                request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY),
                null
        );
        return builder.body(body);
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ProblemDetails> handleNotFound(ResourceNotFoundException ex, HttpServletRequest request) {
        ProblemDetails body = ProblemDetails.of(
                ErrorCode.RESOURCE_NOT_FOUND,
                ex.getMessage(),
                request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY),
                null
        );
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
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
}

