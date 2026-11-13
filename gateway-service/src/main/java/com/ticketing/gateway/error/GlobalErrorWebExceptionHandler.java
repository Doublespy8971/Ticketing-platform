package com.ticketing.gateway.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.gateway.filter.CorrelationIdGlobalFilter;
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

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Global reactive error handler producing RFC 7807 Problem Details (application/problem+json).
 * Handles 401, 403, 404, 429, 502, 503, 504 and 500 without leaking stack traces or downstream URLs.
 */
@Component
@Order(-2)
public class GlobalErrorWebExceptionHandler implements ErrorWebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalErrorWebExceptionHandler.class);

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
                CorrelationIdGlobalFilter.CORRELATION_ID_ATTR, UUID.randomUUID().toString());

        HttpStatus status;
        String errorCode;
        String title;
        String detail;

        if (ex instanceof ResponseStatusException rse) {
            int code = rse.getStatusCode().value();
            status = HttpStatus.resolve(code) != null ? HttpStatus.resolve(code) : HttpStatus.INTERNAL_SERVER_ERROR;
            switch (code) {
                case 401 -> {
                    errorCode = "UNAUTHORIZED";
                    title = "Unauthorized";
                    detail = rse.getReason() != null ? rse.getReason() : "Authentication required";
                }
                case 403 -> {
                    errorCode = "FORBIDDEN";
                    title = "Forbidden";
                    detail = rse.getReason() != null ? rse.getReason() : "Access denied";
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
                    errorCode = status.is5xxServerError() ? "INTERNAL_SERVER_ERROR" : "VALIDATION_ERROR";
                    title = status.getReasonPhrase();
                    detail = rse.getReason() != null ? rse.getReason() : status.getReasonPhrase();
                }
            }
        } else if (isTimeoutException(ex)) {
            status = HttpStatus.GATEWAY_TIMEOUT;
            errorCode = "GATEWAY_TIMEOUT";
            title = "Gateway Timeout";
            detail = "Downstream service timed out";
        } else if (isConnectionException(ex)) {
            status = HttpStatus.BAD_GATEWAY;
            errorCode = "BAD_GATEWAY";
            title = "Bad Gateway";
            detail = "Downstream service is unavailable";
        } else {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
            errorCode = "INTERNAL_SERVER_ERROR";
            title = "Internal Server Error";
            detail = "An unexpected error occurred";
        }

        log.error("Handling gateway error for instance '{}' [{} {}]: {}",
                exchange.getRequest().getPath().value(), status.value(), errorCode, ex.getMessage());

        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE));
        response.getHeaders().set("X-Correlation-Id", correlationId);

        String slug = errorCode.toLowerCase().replace('_', '-');
        String type = ProblemDetails.TYPE_BASE + slug;
        String instance = exchange.getRequest().getPath().value();

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
            bytes = ("{\"status\":" + status.value() + ",\"title\":\"" + title + "\",\"detail\":\"" + detail + "\"}").getBytes(StandardCharsets.UTF_8);
        }

        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }

    private boolean isTimeoutException(Throwable ex) {
        if (ex instanceof TimeoutException) {
            return true;
        }
        String name = ex.getClass().getName();
        if (name.contains("Timeout") || (ex.getMessage() != null && ex.getMessage().toLowerCase().contains("timeout"))) {
            return true;
        }
        return ex.getCause() != null && isTimeoutException(ex.getCause());
    }

    private boolean isConnectionException(Throwable ex) {
        if (ex instanceof ConnectException || ex instanceof ClosedChannelException) {
            return true;
        }
        String name = ex.getClass().getName();
        if (name.contains("Connect") || name.contains("Connection") || (ex.getMessage() != null && ex.getMessage().toLowerCase().contains("connection refused"))) {
            return true;
        }
        return ex.getCause() != null && isConnectionException(ex.getCause());
    }
}
