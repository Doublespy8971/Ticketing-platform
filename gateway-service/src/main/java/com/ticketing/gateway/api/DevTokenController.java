package com.ticketing.gateway.api;

import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ProblemDetails;
import com.ticketing.gateway.auth.JwtTokenService;
import com.ticketing.gateway.auth.SecurityConstants;
import com.ticketing.gateway.config.DevTokenProperties;
import com.ticketing.gateway.filter.CorrelationIdWebFilter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
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
import java.util.regex.Pattern;

/**
 * Dev-only token issuance endpoint: POST /api/v1/auth/token.
 * Active ONLY under 'dev' or 'local' profile AND when ticketing.gateway.dev-token.enabled=true.
 * MUST NEVER be active in production.
 */
@RestController
@RequestMapping("/api/v1/auth")
@Profile({"dev", "local"})
@ConditionalOnProperty(name = "ticketing.gateway.dev-token.enabled", havingValue = "true")
public class DevTokenController {

    private static final Pattern CANONICAL_UUID_PATTERN =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private final JwtTokenService jwtTokenService;
    private final DevTokenProperties devTokenProperties;

    public DevTokenController(JwtTokenService jwtTokenService, DevTokenProperties devTokenProperties) {
        this.jwtTokenService = jwtTokenService;
        this.devTokenProperties = devTokenProperties;
    }

    @PostMapping(value = "/token", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> issueToken(@RequestBody(required = false) DevTokenRequest request,
                                        ServerWebExchange exchange) {
        String correlationId = (String) exchange.getAttributes().getOrDefault(
                CorrelationIdWebFilter.CORRELATION_ID_ATTR, UUID.randomUUID().toString());
        String instance = exchange.getRequest().getPath().value();

        if (request == null) {
            return validationError("Request body is required", instance, correlationId);
        }

        if (request.userId() == null || request.userId().trim().isEmpty()) {
            return validationError("userId is required", instance, correlationId);
        }

        String userIdStr = request.userId().trim();
        if (!CANONICAL_UUID_PATTERN.matcher(userIdStr).matches()) {
            return validationError("userId must be a valid canonical UUID", instance, correlationId);
        }
        UUID userUuid = UUID.fromString(userIdStr);

        if (request.roles() == null || request.roles().isEmpty()) {
            return validationError("roles must not be empty", instance, correlationId);
        }

        for (String role : request.roles()) {
            if (role == null || role.isBlank()) {
                return validationError("role must not be blank", instance, correlationId);
            }
            if (role.contains(",") || role.contains(" ") || !role.equals(role.trim())) {
                return validationError("role must not contain comma or whitespace", instance, correlationId);
            }
            if (!SecurityConstants.ALLOWED_ROLES.contains(role)) {
                return validationError("role is not allowlisted: " + role, instance, correlationId);
            }
        }

        long ttlSeconds = request.ttlSeconds() != null ? request.ttlSeconds() : 3600;
        if (ttlSeconds < 60 || ttlSeconds > 86400) {
            return validationError("ttlSeconds must be between 60 and 86400", instance, correlationId);
        }

        boolean hasAdmin = request.roles().contains(SecurityConstants.ROLE_ADMIN);
        if (hasAdmin && !devTokenProperties.isAllowAdmin()) {
            ProblemDetails problem = ProblemDetails.of(
                    ErrorCode.FORBIDDEN,
                    "Access denied",
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
