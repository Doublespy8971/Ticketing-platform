package com.ticketing.inventory.api;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.BaseRestControllerAdvice;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class InventoryExceptionHandler extends BaseRestControllerAdvice {

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
}

