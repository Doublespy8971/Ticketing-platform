package com.ticketing.gateway.filter;

import com.ticketing.gateway.config.GatewayRoutesConfig;
import com.ticketing.gateway.metrics.GatewayMetrics;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WebFilter recording gateway_requests_total metrics.
 * Runs before all other filters (at HIGHEST_PRECEDENCE) so path hygiene rejections
 * and early rejections are counted.
 *
 * Employs a beforeCommit hook and a doFinally fallback guarded by an AtomicBoolean,
 * ensuring cancelled or aborted requests are counted once with status_class "cancelled".
 *
 * Route tags are strictly derived from the matched Route ID, plus fixed values:
 * - auth-token (exact path /api/v1/auth/token only, and only when the endpoint is registered)
 * - actuator (exact paths /actuator/health, /actuator/info, /actuator/prometheus)
 * - unmatched (for all other paths, including /api/v1/auth/foo and /actuator/env)
 */
@Component
public class MetricsWebFilter implements WebFilter, Ordered {

    private final GatewayMetrics gatewayMetrics;
    private final ApplicationContext applicationContext;

    public MetricsWebFilter(GatewayMetrics gatewayMetrics, ApplicationContext applicationContext) {
        this.gatewayMetrics = gatewayMetrics;
        this.applicationContext = applicationContext;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        AtomicBoolean recorded = new AtomicBoolean(false);

        exchange.getResponse().beforeCommit(() -> {
            if (recorded.compareAndSet(false, true)) {
                recordMetric(exchange, null);
            }
            return Mono.empty();
        });

        return chain.filter(exchange)
                .doFinally(signalType -> {
                    if (signalType == SignalType.CANCEL) {
                        if (recorded.compareAndSet(false, true)) {
                            recordMetric(exchange, "cancelled");
                        }
                    } else {
                        if (recorded.compareAndSet(false, true)) {
                            recordMetric(exchange, null);
                        }
                    }
                });
    }

    private void recordMetric(ServerWebExchange exchange, String overrideStatusClass) {
        String routeTag = deriveRouteTag(exchange);
        String statusClass;
        if ("cancelled".equalsIgnoreCase(overrideStatusClass)) {
            statusClass = "cancelled";
        } else {
            HttpStatusCode statusCode = exchange.getResponse().getStatusCode();
            int code = statusCode != null ? statusCode.value() : 500;
            statusClass = GatewayMetrics.computeStatusClass(code);
        }
        gatewayMetrics.recordRequest(routeTag, statusClass);
    }

    private String deriveRouteTag(ServerWebExchange exchange) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        if (route != null) {
            return route.getId();
        }
        String path = exchange.getRequest().getPath().value();
        if ("/api/v1/auth/token".equals(path) && isTokenEndpointRegistered()) {
            return GatewayRoutesConfig.ROUTE_AUTH_TOKEN;
        }
        if ("/actuator/health".equals(path) || "/actuator/info".equals(path) || "/actuator/prometheus".equals(path)) {
            return GatewayRoutesConfig.ROUTE_ACTUATOR;
        }
        return GatewayRoutesConfig.ROUTE_UNMATCHED;
    }

    private boolean isTokenEndpointRegistered() {
        try {
            return applicationContext.containsBean("devTokenController");
        } catch (Exception e) {
            return false;
        }
    }
}

