package com.ticketing.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.gateway.auth.JwtTokenService;
import com.ticketing.gateway.auth.JwtValidationException;
import com.ticketing.gateway.auth.ParsedJwt;
import com.ticketing.gateway.metrics.GatewayMetrics;
import com.ticketing.gateway.ratelimit.RedisRateLimiterService;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
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
import java.util.regex.Pattern;

/**
 * Global filter enforcing authentication, role rules, header hygiene, and rate limiting.
 * Ordered after CorrelationIdGlobalFilter.
 */
@Component
public class SecurityAndHygieneGlobalFilter implements GlobalFilter, Ordered {

    private static final Pattern PUBLIC_SEATS_PATTERN = Pattern.compile("^/api/v1/events/[^/]+/seats/?$");

    private final JwtTokenService jwtTokenService;
    private final RedisRateLimiterService rateLimiterService;
    private final GatewayMetrics gatewayMetrics;
    private final ObjectMapper objectMapper;

    public SecurityAndHygieneGlobalFilter(JwtTokenService jwtTokenService,
                                          RedisRateLimiterService rateLimiterService,
                                          GatewayMetrics gatewayMetrics,
                                          ObjectMapper objectMapper) {
        this.jwtTokenService = jwtTokenService;
        this.rateLimiterService = rateLimiterService;
        this.gatewayMetrics = gatewayMetrics;
        this.objectMapper = objectMapper;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 10;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        HttpMethod method = exchange.getRequest().getMethod();
        String correlationId = (String) exchange.getAttributes().getOrDefault(
                CorrelationIdGlobalFilter.CORRELATION_ID_ATTR, UUID.randomUUID().toString());

        boolean isSeatsPublic = (method == HttpMethod.GET && PUBLIC_SEATS_PATTERN.matcher(path).matches());
        boolean isAuthPublic = path.startsWith("/api/v1/auth");
        boolean isActuator = path.startsWith("/actuator");
        boolean isPublic = isSeatsPublic || isAuthPublic || isActuator;

        if (isPublic) {
            if (isSeatsPublic) {
                // Rate limit unauthenticated seats endpoint by client IP (20 burst, 10 refill/s)
                String clientIp = resolveClientIp(exchange);
                return rateLimiterService.isAllowedIp(clientIp)
                        .flatMap(allowed -> {
                            if (!allowed) {
                                gatewayMetrics.recordRateLimited("inventory-seats");
                                return writeProblemResponse(exchange, HttpStatus.TOO_MANY_REQUESTS,
                                        ErrorCode.RATE_LIMIT_EXCEEDED, "Rate limit exceeded. Please retry after 1 second.",
                                        "1", correlationId);
                            }
                            return forwardWithCleanedHeaders(exchange, chain, null, null, correlationId);
                        });
            }
            // Other public endpoints (auth/token, actuator)
            return forwardWithCleanedHeaders(exchange, chain, null, null, correlationId);
        }

        // Protected endpoint - extract Authorization header
        String authHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (authHeader == null || !authHeader.toLowerCase().startsWith("bearer ")) {
            gatewayMetrics.recordAuthFailure("missing");
            return writeProblemResponse(exchange, HttpStatus.UNAUTHORIZED,
                    ErrorCode.UNAUTHORIZED, "Missing or invalid Authorization header", null, correlationId);
        }

        // Validate JWT token
        ParsedJwt parsedJwt;
        try {
            parsedJwt = jwtTokenService.validateAndParseToken(authHeader);
        } catch (JwtValidationException e) {
            String reason = e.getReason().name().toLowerCase();
            gatewayMetrics.recordAuthFailure(reason);
            return writeProblemResponse(exchange, HttpStatus.UNAUTHORIZED,
                    ErrorCode.UNAUTHORIZED, e.getMessage(), null, correlationId);
        }

        // Role rules: admin paths require ADMIN role
        boolean isAdminRoute = path.startsWith("/api/v1/admin");
        if (isAdminRoute && !parsedJwt.roles().contains("ADMIN")) {
            gatewayMetrics.recordAuthFailure("forbidden");
            return writeProblemResponse(exchange, HttpStatus.FORBIDDEN,
                    ErrorCode.FORBIDDEN, "Access denied: ADMIN role required", null, correlationId);
        }

        // Rate limit authenticated requests by user ID (10 burst, 5 refill/s)
        String route = resolveRouteName(path);
        return rateLimiterService.isAllowedUser(parsedJwt.userId().toString())
                .flatMap(allowed -> {
                    if (!allowed) {
                        gatewayMetrics.recordRateLimited(route);
                        return writeProblemResponse(exchange, HttpStatus.TOO_MANY_REQUESTS,
                                ErrorCode.RATE_LIMIT_EXCEEDED, "Rate limit exceeded. Please retry after 1 second.",
                                "1", correlationId);
                    }

                    // Forward downstream with validated identity headers injected and Authorization stripped
                    return forwardWithCleanedHeaders(exchange, chain, parsedJwt.userId(), parsedJwt.roles(), correlationId);
                });
    }

    private Mono<Void> forwardWithCleanedHeaders(ServerWebExchange exchange,
                                                 GatewayFilterChain chain,
                                                 UUID userId,
                                                 List<String> roles,
                                                 String correlationId) {
        ServerHttpRequest.Builder reqBuilder = exchange.getRequest().mutate();
        reqBuilder.headers(headers -> {
            // Header hygiene: NEVER forward client-supplied identity headers or Authorization
            headers.remove("X-User-Id");
            headers.remove("x-user-id");
            headers.remove("X-User-Roles");
            headers.remove("x-user-roles");
            headers.remove(HttpHeaders.AUTHORIZATION);
            headers.remove("authorization");

            // Inject verified identity headers if authenticated
            if (userId != null) {
                headers.set("X-User-Id", userId.toString());
            }
            if (roles != null && !roles.isEmpty()) {
                headers.set("X-User-Roles", String.join(",", roles));
            }
            // Ensure X-Correlation-Id is present
            headers.set("X-Correlation-Id", correlationId);
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
        response.getHeaders().set("X-Correlation-Id", correlationId);
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
            bytes = ("{\"status\":" + status.value() + ",\"detail\":\"" + detail + "\"}").getBytes(StandardCharsets.UTF_8);
        }
        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }

    private String resolveClientIp(ServerWebExchange exchange) {
        String xForwardedFor = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        if (xForwardedFor != null && !xForwardedFor.isBlank()) {
            int commaIndex = xForwardedFor.indexOf(',');
            return (commaIndex > 0 ? xForwardedFor.substring(0, commaIndex) : xForwardedFor).trim();
        }
        if (exchange.getRequest().getRemoteAddress() != null && exchange.getRequest().getRemoteAddress().getAddress() != null) {
            return exchange.getRequest().getRemoteAddress().getAddress().getHostAddress();
        }
        return "127.0.0.1";
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

