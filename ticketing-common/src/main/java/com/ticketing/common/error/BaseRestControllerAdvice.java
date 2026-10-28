package com.ticketing.common.error;

import com.ticketing.common.correlation.CorrelationConstants;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

/** Servlet-only base advice producing RFC 7807 {@code application/problem+json} responses. */
@RestControllerAdvice
public class BaseRestControllerAdvice {

    private static final Logger log = LoggerFactory.getLogger(BaseRestControllerAdvice.class);

    @ExceptionHandler(BindException.class)
    public ResponseEntity<ProblemDetails> handleValidation(BindException ex, HttpServletRequest request) {
        List<ProblemDetails.FieldViolation> violations = ex.getBindingResult().getAllErrors().stream()
                .map(e -> new ProblemDetails.FieldViolation(
                        e instanceof FieldError fe ? fe.getField() : e.getObjectName(),
                        e.getDefaultMessage()))
                .toList();
        return respond(ErrorCode.VALIDATION_ERROR, "Request validation failed.", request, violations);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ProblemDetails> handleUnreadable(HttpMessageNotReadableException ex,
                                                           HttpServletRequest request) {
        return respond(ErrorCode.VALIDATION_ERROR, "Malformed request body.", request, null);
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ProblemDetails> handleNotFound(ResourceNotFoundException ex, HttpServletRequest request) {
        return respond(ErrorCode.RESOURCE_NOT_FOUND, ex.getMessage(), request, null);
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<ProblemDetails> handleConflict(ConflictException ex, HttpServletRequest request) {
        return respond(ex.errorCode(), ex.getMessage(), request, null);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProblemDetails> handleUnexpected(Exception ex, HttpServletRequest request) {
        if (ex instanceof ErrorResponse er) {
            // Spring MVC framework exceptions (404 for unknown paths, 405, 415, ...) keep their status.
            HttpStatusCode status = er.getStatusCode();
            ErrorCode code = status.value() == 404 ? ErrorCode.RESOURCE_NOT_FOUND
                    : status.is4xxClientError() ? ErrorCode.VALIDATION_ERROR : ErrorCode.INTERNAL_SERVER_ERROR;
            ProblemDetails body = ProblemDetails.of(code, er.getBody().getDetail(), request.getRequestURI(),
                    MDC.get(CorrelationConstants.MDC_KEY), null);
            return build(new ProblemDetails(body.type(), body.title(), status.value(), body.detail(),
                    body.instance(), body.errorCode(), body.correlationId(), body.timestamp(), body.errors()));
        }
        log.error("Unhandled exception for {}", request.getRequestURI(), ex);
        return respond(ErrorCode.INTERNAL_SERVER_ERROR, "An unexpected error occurred.", request, null);
    }

    private ResponseEntity<ProblemDetails> respond(ErrorCode code, String detail, HttpServletRequest request,
                                                   List<ProblemDetails.FieldViolation> errors) {
        return build(ProblemDetails.of(code, detail, request.getRequestURI(),
                MDC.get(CorrelationConstants.MDC_KEY), errors));
    }

    private ResponseEntity<ProblemDetails> build(ProblemDetails body) {
        return ResponseEntity.status(body.status())
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(body);
    }
}
