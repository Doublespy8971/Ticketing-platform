package com.ticketing.gateway.filter;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Ensures every request and response carries a valid UUIDv4 X-Correlation-Id.
 * Executes at Ordered.HIGHEST_PRECEDENCE.
 */
@Component
public class CorrelationIdGlobalFilter implements GlobalFilter, Ordered {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String CORRELATION_ID_ATTR = "correlationId";

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String correlationId = exchange.getRequest().getHeaders().getFirst(CORRELATION_ID_HEADER);
        if (!isValidUuid(correlationId)) {
            correlationId = UUID.randomUUID().toString();
        }

        // Store in exchange attributes for downstream filters and error handler
        exchange.getAttributes().put(CORRELATION_ID_ATTR, correlationId);

        // Echo in outgoing response headers
        exchange.getResponse().getHeaders().set(CORRELATION_ID_HEADER, correlationId);

        // Mutate request to ensure downstream receives the valid correlation ID
        ServerHttpRequest mutatedRequest = exchange.getRequest().mutate()
                .header(CORRELATION_ID_HEADER, correlationId)
                .build();

        return chain.filter(exchange.mutate().request(mutatedRequest).build());
    }

    private boolean isValidUuid(String value) {
        if (value == null || value.trim().length() != 36) {
            return false;
        }
        try {
            UUID.fromString(value.trim());
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}

