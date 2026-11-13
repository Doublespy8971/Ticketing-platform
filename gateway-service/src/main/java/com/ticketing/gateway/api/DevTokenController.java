package com.ticketing.gateway.api;

import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.gateway.auth.JwtTokenService;
import com.ticketing.gateway.config.DevTokenProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Dev-only token issuance endpoint: POST /api/v1/auth/token.
 * Active when ticketing.gateway.dev-token.enabled=true (default true locally).
 * MUST NEVER be enabled outside development environments.
 */
@RestController
@RequestMapping("/api/v1/auth")
@ConditionalOnProperty(name = "ticketing.gateway.dev-token.enabled", havingValue = "true", matchIfMissing = true)
public class DevTokenController {

    private final JwtTokenService jwtTokenService;
    private final DevTokenProperties devTokenProperties;

    public DevTokenController(JwtTokenService jwtTokenService, DevTokenProperties devTokenProperties) {
        this.jwtTokenService = jwtTokenService;
        this.devTokenProperties = devTokenProperties;
    }

    @PostMapping(value = "/token", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> issueToken(@RequestBody(required = false) DevTokenRequest request,
                                        ServerWebExchange exchange) {
        String correlationId = (String) exchange.getAttributes().getOrDefault("correlationId", UUID.randomUUID().toString());
        String instance = exchange.getRequest().getPath().value();

        if (request == null) {
            return validationError("Request body is required", instance, correlationId);
        }

        if (request.userId() == null || request.userId().trim().isEmpty()) {
            return validationError("userId is required", instance, correlationId);
        }

        UUID userUuid;
        try {
            userUuid = UUID.fromString(request.userId().trim());
        } catch (IllegalArgumentException e) {
            return validationError("userId must be a valid UUID", instance, correlationId);
        }

        if (request.roles() == null || request.roles().isEmpty()) {
            return validationError("roles must not be empty", instance, correlationId);
        }

        long ttlSeconds = request.ttlSeconds() != null ? request.ttlSeconds() : 3600;
        if (ttlSeconds < 60 || ttlSeconds > 86400) {
            return validationError("ttlSeconds must be between 60 and 86400", instance, correlationId);
        }

        boolean hasAdmin = request.roles().stream().anyMatch("ADMIN"::equalsIgnoreCase);
        if (hasAdmin && !devTokenProperties.isAllowAdmin()) {
            ProblemDetails problem = ProblemDetails.of(
                    ErrorCode.FORBIDDEN,
                    "Dev token issuance for ADMIN role is disabled",
                    instance,
                    correlationId,
                    List.of()
            );
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                    .body(problem);
        }

        String token = jwtTokenService.generateToken(userUuid, request.roles(), ttlSeconds);
        DevTokenResponse response = new DevTokenResponse(
                token,
                "Bearer",
                ttlSeconds,
                Instant.now().toString()
        );

        return ResponseEntity.ok(response);
    }

    private ResponseEntity<ProblemDetails> validationError(String detail, String instance, String correlationId) {
        ProblemDetails problem = ProblemDetails.of(
                ErrorCode.VALIDATION_ERROR,
                detail,
                instance,
                correlationId,
                List.of()
        );
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .contentType(MediaType.parseMediaType(ProblemDetails.MEDIA_TYPE))
                .body(problem);
    }
}

