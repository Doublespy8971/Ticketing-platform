package com.ticketing.gateway.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Custom metrics for gateway-service:
 * - gateway_requests_total (tags: route, status_class)
 * - gateway_rate_limited_total (tags: route)
 * - gateway_auth_failures_total (tags: reason: missing, invalid, expired, forbidden)
 *
 * Security: Tokens and Authorization headers are NEVER logged or used as tags.
 */
@Component
public class GatewayMetrics {

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
            return "unknown";
        }
        return switch (route) {
            case "inventory-seats", "inventory-reservations", "inventory-admin",
                 "order-checkout", "order-get", "auth-token", "actuator" -> route;
            default -> {
                if (route.contains("seats")) yield "inventory-seats";
                if (route.contains("reservations")) yield "inventory-reservations";
                if (route.contains("admin")) yield "inventory-admin";
                if (route.contains("checkout")) yield "order-checkout";
                if (route.contains("orders")) yield "order-get";
                if (route.contains("auth")) yield "auth-token";
                if (route.contains("actuator")) yield "actuator";
                yield "unknown";
            }
        };
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

