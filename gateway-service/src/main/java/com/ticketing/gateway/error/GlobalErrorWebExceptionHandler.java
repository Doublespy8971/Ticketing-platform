package com.ticketing.gateway.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.gateway.filter.CorrelationIdWebFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Global reactive error handler producing RFC 7807 Problem Details (application/problem+json).
 * Maps UnknownHostException, PrematureCloseException, connection reset, and transport IOExceptions to 502 BAD_GATEWAY.
 * Maps timeouts to 504 GATEWAY_TIMEOUT.
 * Fixed safe detail strings per status; never leaks hostnames, stack traces, or request-derived text.
 */
@Component
@Order(-2)
public class GlobalErrorWebExceptionHandler implements ErrorWebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalErrorWebExceptionHandler.class);

    private static final byte[] FALLBACK_JSON =
            ("{\"type\":\"https://api.ticketing.internal/errors/internal-server-error\","
                    + "\"title\":\"Internal Server Error\",\"status\":500,\"detail\":\"An unexpected error occurred\"}")
                    .getBytes(StandardCharsets.UTF_8);

    private final ObjectMapper objectMapper;

    public GlobalErrorWebExceptionHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        ServerHttpResponse response = exchange.getResponse();
        if (response.isCommitted()) {
            return Mono.error(ex);
        }

        String correlationId = (String) exchange.getAttributes().getOrDefault(
                CorrelationIdWebFilter.CORRELATION_ID_ATTR, UUID.randomUUID().toString());

        HttpStatus status;
        String errorCode;
        String title;
        String detail;
        boolean is429 = false;

        if (isTimeoutException(ex)) {
            status = HttpStatus.GATEWAY_TIMEOUT;
            errorCode = "GATEWAY_TIMEOUT";
            title = "Gateway Timeout";
            detail = "Downstream service timed out";
        } else if (isConnectionOrTransportException(ex)) {
            status = HttpStatus.BAD_GATEWAY;
            errorCode = "BAD_GATEWAY";
            title = "Bad Gateway";
            detail = "Downstream service is unavailable";
        } else if (ex instanceof ResponseStatusException rse) {
            int code = rse.getStatusCode().value();
            status = HttpStatus.resolve(code) != null ? HttpStatus.resolve(code) : HttpStatus.INTERNAL_SERVER_ERROR;
            switch (code) {
                case 400 -> {
                    errorCode = "VALIDATION_ERROR";
                    title = "Validation Error";
                    detail = "Validation failed";
                }
                case 401 -> {
                    errorCode = "UNAUTHORIZED";
                    title = "Unauthorized";
                    detail = "Authentication required";
                }
                case 403 -> {
                    errorCode = "FORBIDDEN";
                    title = "Forbidden";
                    detail = "Access denied";
                }
                case 404 -> {
                    errorCode = "RESOURCE_NOT_FOUND";
                    title = "Not Found";
                    detail = "Resource not found";
                }
                case 429 -> {
                    errorCode = "RATE_LIMIT_EXCEEDED";
                    title = "Rate Limit Exceeded";
                    detail = "Rate limit exceeded. Please retry after 1 second.";
                    is429 = true;
                }
                case 502 -> {
                    errorCode = "BAD_GATEWAY";
                    title = "Bad Gateway";
                    detail = "Downstream service is unavailable";
                }
                case 503 -> {
                    errorCode = "SERVICE_UNAVAILABLE";
                    title = "Service Unavailable";
                    detail = "Service temporarily unavailable";
                }
                case 504 -> {
                    errorCode = "GATEWAY_TIMEOUT";
                    title = "Gateway Timeout";
                    detail = "Downstream service timed out";
                }
                default -> {
                    if (status.is5xxServerError()) {
                        errorCode = "INTERNAL_SERVER_ERROR";
                        title = "Internal Server Error";
                        detail = "An unexpected error occurred";
                    } else {
                        errorCode = "VALIDATION_ERROR";
                        title = "Validation Error";
                        detail = "Validation failed";
                    }
                }
            }
        } else {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
            errorCode = "INTERNAL_SERVER_ERROR";
            title = "Internal Server Error";
            detail = "An unexpected error occurred";
        }

        log.error("Handling gateway error for instance '{}' [{} {}]: {}",
                exchange.getRequest().getPath().value(), status.value(), errorCode, ex.getClass().getSimpleName());

        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE));
        response.getHeaders().set(CorrelationIdWebFilter.CORRELATION_ID_HEADER, correlationId);
        if (is429) {
            response.getHeaders().set("Retry-After", "1");
        }

        String slug = errorCode.toLowerCase().replace('_', '-');
        String type = ProblemDetails.TYPE_BASE + slug;
        String instance = (status == HttpStatus.NOT_FOUND) ? "/" : exchange.getRequest().getPath().value();

        ProblemDetails problem = new ProblemDetails(
                type,
                title,
                status.value(),
                detail,
                instance,
                errorCode,
                correlationId,
                Instant.now(),
                List.of()
        );

        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(problem);
        } catch (Exception jsonEx) {
            bytes = FALLBACK_JSON;
        }

        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }

    private boolean isTimeoutException(Throwable ex) {
        if (ex == null) return false;
        if (ex instanceof TimeoutException) {
            return true;
        }
        String name = ex.getClass().getName();
        if (name.contains("Timeout") || (ex.getMessage() != null && ex.getMessage().toLowerCase().contains("timeout"))) {
            return true;
        }
        return isTimeoutException(ex.getCause());
    }

    private boolean isConnectionOrTransportException(Throwable ex) {
        if (ex == null) return false;
        if (ex instanceof UnknownHostException
                || ex instanceof ConnectException
                || ex instanceof ClosedChannelException) {
            return true;
        }
        String name = ex.getClass().getName();
        if (name.contains("PrematureCloseException")
                || name.contains("Connect")
                || name.contains("Connection")) {
            return true;
        }
        String msg = ex.getMessage();
        if (msg != null) {
            String lower = msg.toLowerCase();
            if (lower.contains("connection refused")
                    || lower.contains("connection reset")
                    || lower.contains("premature close")
                    || lower.contains("broken pipe")) {
                return true;
            }
        }
        if (ex instanceof IOException) {
            return true;
        }
        return isConnectionOrTransportException(ex.getCause());
    }
}
