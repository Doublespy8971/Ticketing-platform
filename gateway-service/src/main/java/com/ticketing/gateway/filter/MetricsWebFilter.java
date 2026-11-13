package com.ticketing.gateway.filter;

import com.ticketing.gateway.config.GatewayRoutesConfig;
import com.ticketing.gateway.metrics.GatewayMetrics;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * WebFilter recording gateway_requests_total metrics via a beforeCommit hook.
 * Captures all requests: routed, token endpoint, actuator, unmatched (404), and errors.
 * Route tag is derived strictly from matched Route ID or bounded endpoint identifiers.
 */
@Component
public class MetricsWebFilter implements WebFilter, Ordered {

    private final GatewayMetrics gatewayMetrics;

    public MetricsWebFilter(GatewayMetrics gatewayMetrics) {
        this.gatewayMetrics = gatewayMetrics;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 2;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        exchange.getResponse().beforeCommit(() -> {
            Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
            String path = exchange.getRequest().getPath().value();

            String routeTag;
            if (route != null) {
                routeTag = route.getId();
            } else if (path.startsWith("/api/v1/auth")) {
                routeTag = GatewayRoutesConfig.ROUTE_AUTH_TOKEN;
            } else if (path.startsWith("/actuator")) {
                routeTag = GatewayRoutesConfig.ROUTE_ACTUATOR;
            } else {
                routeTag = GatewayRoutesConfig.ROUTE_UNMATCHED;
            }

            HttpStatusCode statusCode = exchange.getResponse().getStatusCode();
            int code = statusCode != null ? statusCode.value() : 500;
            String statusClass = GatewayMetrics.computeStatusClass(code);

            gatewayMetrics.recordRequest(routeTag, statusClass);
            return Mono.empty();
        });

        return chain.filter(exchange);
    }
}

