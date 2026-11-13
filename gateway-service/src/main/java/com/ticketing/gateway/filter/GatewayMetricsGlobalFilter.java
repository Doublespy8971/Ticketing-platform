package com.ticketing.gateway.filter;

import com.ticketing.gateway.metrics.GatewayMetrics;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.regex.Pattern;

/**
 * Records request metrics for all requests passing through the gateway.
 * Tagged with route (bounded set) and status_class (2xx, 4xx, 5xx).
 */
@Component
public class GatewayMetricsGlobalFilter implements GlobalFilter, Ordered {

    private static final Pattern PUBLIC_SEATS_PATTERN = Pattern.compile("^/api/v1/events/[^/]+/seats/?$");

    private final GatewayMetrics gatewayMetrics;

    public GatewayMetricsGlobalFilter(GatewayMetrics gatewayMetrics) {
        this.gatewayMetrics = gatewayMetrics;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        String route = resolveRouteName(path);

        return chain.filter(exchange).doFinally(signalType -> {
            HttpStatusCode statusCode = exchange.getResponse().getStatusCode();
            int code = statusCode != null ? statusCode.value() : 500;
            String statusClass = GatewayMetrics.computeStatusClass(code);
            gatewayMetrics.recordRequest(route, statusClass);
        });
    }

    private String resolveRouteName(String path) {
        if (PUBLIC_SEATS_PATTERN.matcher(path).matches()) return "inventory-seats";
        if (path.startsWith("/api/v1/admin")) return "inventory-admin";
        if (path.startsWith("/api/v1/reservations")) return "inventory-reservations";
        if (path.contains("/checkout")) return "order-checkout";
        if (path.startsWith("/api/v1/orders")) return "order-get";
        if (path.startsWith("/api/v1/auth")) return "auth-token";
        if (path.startsWith("/actuator")) return "actuator";
        return "unknown";
    }
}

