package com.ticketing.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.gateway.auth.JwtTokenService;
import com.ticketing.gateway.auth.JwtValidationException;
import com.ticketing.gateway.auth.ParsedJwt;
import com.ticketing.gateway.auth.SecurityConstants;
import com.ticketing.gateway.config.GatewayRoutesConfig;
import com.ticketing.gateway.metrics.GatewayMetrics;
import com.ticketing.gateway.ratelimit.ClientAddressResolver;
import com.ticketing.gateway.ratelimit.RedisRateLimiterService;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * Global filter enforcing authentication, role rules, header hygiene, and rate limiting.
 * Decisions are made strictly from the matched Gateway Route ID (ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR)
 * and never from path strings.
 */
@Component
public class SecurityAndHygieneGlobalFilter implements GlobalFilter, Ordered {

    private final JwtTokenService jwtTokenService;
    private final RedisRateLimiterService rateLimiterService;
    private final ClientAddressResolver clientAddressResolver;
    private final GatewayMetrics gatewayMetrics;
    private final ObjectMapper objectMapper;

    public SecurityAndHygieneGlobalFilter(JwtTokenService jwtTokenService,
                                          RedisRateLimiterService rateLimiterService,
                                          ClientAddressResolver clientAddressResolver,
                                          GatewayMetrics gatewayMetrics,
                                          ObjectMapper objectMapper) {
        this.jwtTokenService = jwtTokenService;
        this.rateLimiterService = rateLimiterService;
        this.clientAddressResolver = clientAddressResolver;
        this.gatewayMetrics = gatewayMetrics;
        this.objectMapper = objectMapper;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 10;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        if (route == null) {
            // Unmatched route - allow chain to complete so WebFlux returns 404
            return chain.filter(exchange);
        }

        String routeId = route.getId();
        String correlationId = (String) exchange.getAttributes().getOrDefault(
                CorrelationIdWebFilter.CORRELATION_ID_ATTR, UUID.randomUUID().toString());

        // 1. Public route: inventory-seats
        if (GatewayRoutesConfig.ROUTE_INVENTORY_SEATS.equals(routeId)) {
            String clientIp = clientAddressResolver.resolveClientIp(exchange);
            return rateLimiterService.isAllowedIp(clientIp)
                    .flatMap(allowed -> {
                        if (!allowed) {
                            gatewayMetrics.recordRateLimited(routeId);
                            return writeProblemResponse(exchange, HttpStatus.TOO_MANY_REQUESTS,
                                    ErrorCode.RATE_LIMIT_EXCEEDED, "Rate limit exceeded. Please retry after 1 second.",
                                    "1", correlationId);
                        }
                        return forwardWithCleanedHeaders(exchange, chain, null, null);
                    });
        }

        // 2. Protected routes: extract Authorization header
        String authHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (authHeader == null || !SecurityConstants.hasBearerPrefix(authHeader)) {
            gatewayMetrics.recordAuthFailure("missing");
            return writeProblemResponse(exchange, HttpStatus.UNAUTHORIZED,
                    ErrorCode.UNAUTHORIZED, "Authentication required", null, correlationId);
        }

        // 3. Validate JWT token
        ParsedJwt parsedJwt;
        try {
            parsedJwt = jwtTokenService.validateAndParseToken(authHeader);
        } catch (JwtValidationException e) {
            String reason = e.getReason().name().toLowerCase();
            gatewayMetrics.recordAuthFailure(reason);
            return writeProblemResponse(exchange, HttpStatus.UNAUTHORIZED,
                    ErrorCode.UNAUTHORIZED, "Authentication required", null, correlationId);
        }

        // 4. Role rules: admin route requires ADMIN role
        if (GatewayRoutesConfig.ROUTE_INVENTORY_ADMIN.equals(routeId)) {
            if (!parsedJwt.roles().contains(SecurityConstants.ROLE_ADMIN)) {
                gatewayMetrics.recordAuthFailure("forbidden");
                return writeProblemResponse(exchange, HttpStatus.FORBIDDEN,
                        ErrorCode.FORBIDDEN, "Access denied", null, correlationId);
            }
        }

        // 5. Rate limit authenticated requests by user ID (10 burst, 5 refill/s)
        return rateLimiterService.isAllowedUser(parsedJwt.userId().toString())
                .flatMap(allowed -> {
                    if (!allowed) {
                        gatewayMetrics.recordRateLimited(routeId);
                        return writeProblemResponse(exchange, HttpStatus.TOO_MANY_REQUESTS,
                                ErrorCode.RATE_LIMIT_EXCEEDED, "Rate limit exceeded. Please retry after 1 second.",
                                "1", correlationId);
                    }
                    return forwardWithCleanedHeaders(exchange, chain, parsedJwt.userId(), parsedJwt.roles());
                });
    }

    private Mono<Void> forwardWithCleanedHeaders(ServerWebExchange exchange,
                                                 GatewayFilterChain chain,
                                                 UUID userId,
                                                 List<String> roles) {
        ServerHttpRequest.Builder reqBuilder = exchange.getRequest().mutate();
        reqBuilder.headers(headers -> {
            // Strip any header whose normalized name is in STRIPPED_NORMALIZED_HEADERS
            java.util.List<String> keysToRemove = new java.util.ArrayList<>();
            for (String headerName : headers.keySet()) {
                if (SecurityConstants.STRIPPED_NORMALIZED_HEADERS.contains(
                        SecurityConstants.normalizeHeaderName(headerName))) {
                    keysToRemove.add(headerName);
                }
            }
            for (String key : keysToRemove) {
                headers.remove(key);
            }

            // Inject verified identity headers if authenticated
            if (userId != null) {
                headers.set("X-User-Id", userId.toString());
            }
            if (roles != null && !roles.isEmpty()) {
                headers.set("X-User-Roles", String.join(",", roles));
            }
        });

        return chain.filter(exchange.mutate().request(reqBuilder.build()).build());
    }

    private Mono<Void> writeProblemResponse(ServerWebExchange exchange,
                                            HttpStatus status,
                                            ErrorCode errorCode,
                                            String detail,
                                            String retryAfter,
                                            String correlationId) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE));
        response.getHeaders().set(CorrelationIdWebFilter.CORRELATION_ID_HEADER, correlationId);
        if (retryAfter != null) {
            response.getHeaders().set("Retry-After", retryAfter);
        }

        ProblemDetails problem = ProblemDetails.of(
                errorCode,
                detail,
                exchange.getRequest().getPath().value(),
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
