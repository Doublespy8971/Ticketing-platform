package com.ticketing.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.gateway.config.GatewayRoutesConfig;
import com.ticketing.gateway.metrics.GatewayMetrics;
import com.ticketing.gateway.ratelimit.ClientAddressResolver;
import com.ticketing.gateway.ratelimit.RedisRateLimiterService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * Applies IP-based rate limiting to the dev token endpoint (/api/v1/auth/token).
 * Token bucket: capacity 10, refill 1 req/s.
 * Returns 429 RATE_LIMIT_EXCEEDED with Retry-After: 1 on breach.
 * Active ONLY under 'dev' or 'local' profile AND when ticketing.gateway.dev-token.enabled=true,
 * and applies only to POST requests.
 */
@Component
@Profile({"dev", "local"})
@ConditionalOnProperty(name = "ticketing.gateway.dev-token.enabled", havingValue = "true")
public class DevTokenRateLimiterWebFilter implements WebFilter, Ordered {

    private final RedisRateLimiterService rateLimiterService;
    private final ClientAddressResolver clientAddressResolver;
    private final GatewayMetrics gatewayMetrics;
    private final ObjectMapper objectMapper;

    public DevTokenRateLimiterWebFilter(RedisRateLimiterService rateLimiterService,
                                        ClientAddressResolver clientAddressResolver,
                                        GatewayMetrics gatewayMetrics,
                                        ObjectMapper objectMapper) {
        this.rateLimiterService = rateLimiterService;
        this.clientAddressResolver = clientAddressResolver;
        this.gatewayMetrics = gatewayMetrics;
        this.objectMapper = objectMapper;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 4;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        HttpMethod method = exchange.getRequest().getMethod();
        if (!path.equals("/api/v1/auth/token") || method != HttpMethod.POST) {
            return chain.filter(exchange);
        }

        String clientIp = clientAddressResolver.resolveClientIp(exchange);
        return rateLimiterService.isAllowedAuthTokenIp(clientIp)
                .flatMap(allowed -> {
                    if (!allowed) {
                        gatewayMetrics.recordRateLimited(GatewayRoutesConfig.ROUTE_AUTH_TOKEN);
                        String correlationId = (String) exchange.getAttributes().getOrDefault(
                                CorrelationIdWebFilter.CORRELATION_ID_ATTR, UUID.randomUUID().toString());
                        return writeRateLimitResponse(exchange, path, correlationId);
                    }
                    return chain.filter(exchange);
                });
    }

    private Mono<Void> writeRateLimitResponse(ServerWebExchange exchange, String instance, String correlationId) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        response.getHeaders().setContentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE));
        response.getHeaders().set("Retry-After", "1");
        response.getHeaders().set(CorrelationIdWebFilter.CORRELATION_ID_HEADER, correlationId);

        ProblemDetails problem = ProblemDetails.of(
                ErrorCode.RATE_LIMIT_EXCEEDED,
                "Rate limit exceeded. Please retry after 1 second.",
                instance,
                correlationId,
                List.of()
        );

        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(problem);
        } catch (Exception e) {
            bytes = "{\"status\":429,\"errorCode\":\"RATE_LIMIT_EXCEEDED\",\"title\":\"Rate Limit Exceeded\",\"detail\":\"Rate limit exceeded. Please retry after 1 second.\"}"
                    .getBytes(StandardCharsets.UTF_8);
        }
        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }
}

