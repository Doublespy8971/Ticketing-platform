package com.ticketing.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * Enforces strict path hygiene at highest precedence.
 * Rejects with 400 VALIDATION_ERROR any request whose raw path contains:
 * - percent sign (%)
 * - semicolon (;)
 * - backslash (\)
 * - two consecutive slashes (//)
 * - dot-dot segment (..)
 * - NUL (\0)
 * - non-ASCII characters
 */
@Component
public class PathHygieneWebFilter implements WebFilter, Ordered {

    private final ObjectMapper objectMapper;

    public PathHygieneWebFilter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String rawPath = exchange.getRequest().getURI().getRawPath();
        String pathValue = exchange.getRequest().getPath().value();

        if (isInvalidPath(rawPath) || isInvalidPath(pathValue)) {
            String correlationId = (String) exchange.getAttributes().getOrDefault(
                    CorrelationIdWebFilter.CORRELATION_ID_ATTR, UUID.randomUUID().toString());
            return writeBadRequestResponse(exchange, correlationId);
        }

        return chain.filter(exchange);
    }

    private boolean isInvalidPath(String path) {
        if (path == null) {
            return false;
        }
        if (path.contains("%")
                || path.contains(";")
                || path.contains("\\")
                || path.contains("//")
                || path.contains("..")
                || path.indexOf('\0') != -1) {
            return true;
        }
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c < 32 || c > 126) {
                return true;
            }
        }
        return false;
    }

    private Mono<Void> writeBadRequestResponse(ServerWebExchange exchange, String correlationId) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        response.getHeaders().setContentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE));
        response.getHeaders().set(CorrelationIdWebFilter.CORRELATION_ID_HEADER, correlationId);

        ProblemDetails problem = ProblemDetails.of(
                ErrorCode.VALIDATION_ERROR,
                "Invalid request path",
                "/",
                correlationId,
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

