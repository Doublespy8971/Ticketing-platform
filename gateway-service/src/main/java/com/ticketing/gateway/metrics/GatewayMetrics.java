package com.ticketing.gateway.metrics;

import com.ticketing.gateway.config.GatewayRoutesConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Custom metrics for gateway-service:
 * - gateway_requests_total (tags: route, status_class)
 * - gateway_rate_limited_total (tags: route)
 * - gateway_auth_failures_total (tags: reason: missing, invalid, expired, forbidden)
 *
 * Route tags are strictly bounded:
 * inventory-seats, inventory-reservations, inventory-admin, order-checkout, order-get,
 * payment-service, auth-token, actuator, unmatched.
 *
 * Security: Tokens and Authorization headers are NEVER logged or used as tags.
 */
@Component
public class GatewayMetrics {

    public static final Set<String> ALLOWED_ROUTES = Set.of(
            GatewayRoutesConfig.ROUTE_INVENTORY_SEATS,
            GatewayRoutesConfig.ROUTE_INVENTORY_RESERVATIONS,
            GatewayRoutesConfig.ROUTE_INVENTORY_ADMIN,
            GatewayRoutesConfig.ROUTE_ORDER_CHECKOUT,
            GatewayRoutesConfig.ROUTE_ORDER_GET,
            GatewayRoutesConfig.ROUTE_PAYMENT,
            GatewayRoutesConfig.ROUTE_AUTH_TOKEN,
            GatewayRoutesConfig.ROUTE_ACTUATOR,
            GatewayRoutesConfig.ROUTE_UNMATCHED
    );

    private final MeterRegistry meterRegistry;

    public GatewayMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void recordRequest(String route, String statusClass) {
        meterRegistry.counter("gateway_requests_total",
                "route", normalizeRoute(route),
                "status_class", statusClass != null ? statusClass : "unknown"
        ).increment();
    }

    public void recordRateLimited(String route) {
        meterRegistry.counter("gateway_rate_limited_total",
                "route", normalizeRoute(route)
        ).increment();
    }

    public void recordAuthFailure(String reason) {
        meterRegistry.counter("gateway_auth_failures_total",
                "reason", normalizeReason(reason)
        ).increment();
    }

    public static String computeStatusClass(int statusCode) {
        if (statusCode >= 200 && statusCode < 300) {
            return "2xx";
        } else if (statusCode >= 400 && statusCode < 500) {
            return "4xx";
        } else if (statusCode >= 500 && statusCode < 600) {
            return "5xx";
        }
        return "other";
    }

    public static String normalizeRoute(String route) {
        if (route == null || route.isBlank()) {
            return GatewayRoutesConfig.ROUTE_UNMATCHED;
        }
        if (ALLOWED_ROUTES.contains(route)) {
            return route;
        }
        return GatewayRoutesConfig.ROUTE_UNMATCHED;
    }

    public static String normalizeReason(String reason) {
        if (reason == null) return "invalid";
        return switch (reason.toLowerCase()) {
            case "missing" -> "missing";
            case "expired" -> "expired";
            case "forbidden" -> "forbidden";
            default -> "invalid";
        };
    }
}
