package com.ticketing.gateway.filter;

import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Ensures every incoming and outgoing HTTP request/response carries a valid canonical X-Correlation-Id.
 * Executes as a WebFilter at Ordered.HIGHEST_PRECEDENCE so it applies to all requests
 * (gateway routes, controller endpoints, actuator, 404s, and rejections).
 */
@Component
public class CorrelationIdWebFilter implements WebFilter, Ordered {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String CORRELATION_ID_ATTR = "correlationId";

    private static final Pattern CANONICAL_UUID_PATTERN =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        List<String> headers = exchange.getRequest().getHeaders().get(CORRELATION_ID_HEADER);

        String correlationId;
        if (headers != null && headers.size() == 1) {
            String candidate = headers.get(0).trim();
            if (CANONICAL_UUID_PATTERN.matcher(candidate).matches()) {
                correlationId = candidate;
            } else {
                correlationId = UUID.randomUUID().toString();
            }
        } else {
            // Missing, duplicate, or empty headers -> generate new UUIDv4
            correlationId = UUID.randomUUID().toString();
        }

        // Store in exchange attributes for subsequent filters and error handlers
        exchange.getAttributes().put(CORRELATION_ID_ATTR, correlationId);

        final String finalCorrelationId = correlationId;

        // Echo in outgoing response headers via beforeCommit to guarantee presence on all responses
        exchange.getResponse().beforeCommit(() -> {
            exchange.getResponse().getHeaders().set(CORRELATION_ID_HEADER, finalCorrelationId);
            return Mono.empty();
        });

        // Forward to downstream services using set (never append)
        ServerHttpRequest mutatedRequest = exchange.getRequest().mutate()
                .headers(h -> h.set(CORRELATION_ID_HEADER, finalCorrelationId))
                .build();

        return chain.filter(exchange.mutate().request(mutatedRequest).build());
    }
}

