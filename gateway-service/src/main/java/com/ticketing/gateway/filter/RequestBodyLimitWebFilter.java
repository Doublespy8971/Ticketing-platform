package com.ticketing.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.error.ProblemDetails;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Enforces request body limits per route:
 * - GET requests must have no body (Content-Length > 0 or Transfer-Encoding returns 400 VALIDATION_ERROR).
 * - POST and PATCH requests require a Content-Length header (chunked or missing returns 411 LENGTH_REQUIRED).
 * - Body size limit: 64KB default, 2MB for the bulk seat creation route. Requests exceeding the limit return 413 PAYLOAD_TOO_LARGE.
 */
@Component
public class RequestBodyLimitWebFilter implements WebFilter, Ordered {

    public static final long DEFAULT_MAX_BODY_BYTES = 64L * 1024L; // 64KB
    public static final long BULK_SEATS_MAX_BODY_BYTES = 2L * 1024L * 1024L; // 2MB

    private final ObjectMapper objectMapper;

    public RequestBodyLimitWebFilter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 2;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        HttpMethod method = exchange.getRequest().getMethod();
        HttpHeaders headers = exchange.getRequest().getHeaders();
        String path = exchange.getRequest().getPath().value();
        String correlationId = (String) exchange.getAttributes().getOrDefault(
                CorrelationIdWebFilter.CORRELATION_ID_ATTR, UUID.randomUUID().toString());

        boolean hasTransferEncoding = headers.get(HttpHeaders.TRANSFER_ENCODING) != null
                && !headers.get(HttpHeaders.TRANSFER_ENCODING).isEmpty();
        long contentLength = headers.getContentLength();

        if (method == HttpMethod.GET || method == HttpMethod.HEAD) {
            if (contentLength > 0 || hasTransferEncoding) {
                return writeProblemResponse(exchange, HttpStatus.BAD_REQUEST,
                        "VALIDATION_ERROR", "Validation Error",
                        "GET requests must not contain a request body", correlationId);
            }
        } else if (method == HttpMethod.POST || method == HttpMethod.PATCH) {
            if (hasTransferEncoding || contentLength < 0) {
                return writeProblemResponse(exchange, HttpStatus.LENGTH_REQUIRED,
                        "LENGTH_REQUIRED", "Length Required",
                        "Content-Length header is required", correlationId);
            }

            long maxLimit = path.contains("/seats/bulk") ? BULK_SEATS_MAX_BODY_BYTES : DEFAULT_MAX_BODY_BYTES;
            if (contentLength > maxLimit) {
                return writeProblemResponse(exchange, HttpStatus.PAYLOAD_TOO_LARGE,
                        "PAYLOAD_TOO_LARGE", "Payload Too Large",
                        "Request payload exceeds maximum allowed size", correlationId);
            }
        }

        return chain.filter(exchange);
    }

    private Mono<Void> writeProblemResponse(ServerWebExchange exchange,
                                            HttpStatus status,
                                            String errorCode,
                                            String title,
                                            String detail,
                                            String correlationId) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE));
        response.getHeaders().set(CorrelationIdWebFilter.CORRELATION_ID_HEADER, correlationId);

        String slug = errorCode.toLowerCase().replace('_', '-');
        String type = ProblemDetails.TYPE_BASE + slug;

        ProblemDetails problem = new ProblemDetails(
                type,
                title,
                status.value(),
                detail,
                "/",
                errorCode,
                correlationId,
                Instant.now(),
                List.of()
        );

        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(problem);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize problem details", e);
        }

        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }
}

