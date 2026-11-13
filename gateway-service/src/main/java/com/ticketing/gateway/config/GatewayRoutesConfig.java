package com.ticketing.gateway.config;

import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;

/**
 * Route definitions for routing traffic through gateway-service.
 * Enforces explicit HTTP method predicates matching docs/arch/06-api.md.
 * Undocumented routes (such as payments) are not routed; unmatched requests return 404.
 */
@Configuration
public class GatewayRoutesConfig {

    public static final String ROUTE_INVENTORY_SEATS = "inventory-seats";
    public static final String ROUTE_INVENTORY_RESERVATIONS = "inventory-reservations";
    public static final String ROUTE_INVENTORY_ADMIN = "inventory-admin";
    public static final String ROUTE_ORDER_CHECKOUT = "order-checkout";
    public static final String ROUTE_ORDER_GET = "order-get";
    public static final String ROUTE_PAYMENT = "payment-service";
    public static final String ROUTE_AUTH_TOKEN = "auth-token";
    public static final String ROUTE_ACTUATOR = "actuator";
    public static final String ROUTE_UNMATCHED = "unmatched";

    @Bean
    public RouteLocator customRouteLocator(RouteLocatorBuilder builder, GatewayRoutesProperties routesProperties) {
        return builder.routes()
                // Inventory service routes
                .route(ROUTE_INVENTORY_SEATS, r -> r
                        .path("/api/v1/events/*/seats")
                        .and().method(HttpMethod.GET)
                        .uri(routesProperties.getInventoryServiceUrl()))
                .route(ROUTE_INVENTORY_RESERVATIONS, r -> r
                        .path("/api/v1/reservations")
                        .and().method(HttpMethod.POST)
                        .uri(routesProperties.getInventoryServiceUrl()))
                .route(ROUTE_INVENTORY_ADMIN, r -> r
                        .path("/api/v1/admin/**")
                        .and().method(HttpMethod.POST, HttpMethod.PATCH)
                        .uri(routesProperties.getInventoryServiceUrl()))

                // Order service routes
                .route(ROUTE_ORDER_CHECKOUT, r -> r
                        .path("/api/v1/orders/*/checkout")
                        .and().method(HttpMethod.POST)
                        .uri(routesProperties.getOrderServiceUrl()))
                .route(ROUTE_ORDER_GET, r -> r
                        .path("/api/v1/orders/*")
                        .and().method(HttpMethod.GET)
                        .uri(routesProperties.getOrderServiceUrl()))
                .build();
    }
}
