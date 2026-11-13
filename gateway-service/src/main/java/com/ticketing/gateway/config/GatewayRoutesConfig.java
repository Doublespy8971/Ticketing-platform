package com.ticketing.gateway.config;

import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;

/**
 * Route definitions for routing traffic through gateway-service:
 * - inventory-service (:8081): /api/v1/events/{eventId}/seats, /api/v1/reservations, /api/v1/admin/**
 * - order-service (:8082): /api/v1/orders/{orderId}/checkout, /api/v1/orders/{orderId}
 * - payment-service (:8083): /api/v1/payments/**
 */
@Configuration
public class GatewayRoutesConfig {

    @Bean
    public RouteLocator customRouteLocator(RouteLocatorBuilder builder, GatewayRoutesProperties routesProperties) {
        return builder.routes()
                // Inventory service routes
                .route("inventory-seats", r -> r
                        .path("/api/v1/events/*/seats", "/api/v1/events/*/seats/**")
                        .and().method(HttpMethod.GET)
                        .uri(routesProperties.getInventoryServiceUrl()))
                .route("inventory-reservations", r -> r
                        .path("/api/v1/reservations", "/api/v1/reservations/**")
                        .uri(routesProperties.getInventoryServiceUrl()))
                .route("inventory-admin", r -> r
                        .path("/api/v1/admin/**")
                        .uri(routesProperties.getInventoryServiceUrl()))

                // Order service routes
                .route("order-checkout", r -> r
                        .path("/api/v1/orders/*/checkout")
                        .and().method(HttpMethod.POST)
                        .uri(routesProperties.getOrderServiceUrl()))
                .route("order-get", r -> r
                        .path("/api/v1/orders/*", "/api/v1/orders/**")
                        .and().method(HttpMethod.GET)
                        .uri(routesProperties.getOrderServiceUrl()))

                // Payment service routes
                .route("payment-service", r -> r
                        .path("/api/v1/payments/**")
                        .uri(routesProperties.getPaymentServiceUrl()))
                .build();
    }
}

