package com.ticketing.order.api;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.order.api.dto.CheckoutRequest;
import com.ticketing.order.api.dto.CheckoutResponse;
import com.ticketing.order.api.dto.OrderDetailsResponse;
import com.ticketing.order.service.CheckoutService;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

@RestController
@RequestMapping(path = "/api/v1/orders", produces = MediaType.APPLICATION_JSON_VALUE)
public class OrderController {

    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    public static final String IDEMPOTENT_REPLAY_HEADER = "X-Idempotent-Replay";
    public static final String X_USER_ROLES_HEADER = "X-User-Roles";

    private final CheckoutService checkoutService;

    public OrderController(CheckoutService checkoutService) {
        this.checkoutService = Objects.requireNonNull(checkoutService, "checkoutService must not be null");
    }

    @PostMapping(path = "/{orderId}/checkout", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<CheckoutResponse> checkout(
            @PathVariable("orderId") UUID orderId,
            @RequestHeader(value = CorrelationConstants.USER_ID_HEADER, required = false) String userIdHeader,
            @RequestHeader(value = X_USER_ROLES_HEADER, required = false) String userRolesHeader,
            @RequestHeader(value = IDEMPOTENCY_KEY_HEADER, required = false) String idempotencyKeyHeader,
            @Valid @RequestBody CheckoutRequest request) {

        UUID userId = parseAndValidateUserId(userIdHeader, userRolesHeader);
        UUID idempotencyKey = parseAndValidateIdempotencyKey(idempotencyKeyHeader);

        CheckoutService.CheckoutExecutionResult result =
                checkoutService.checkout(orderId, userId, idempotencyKey, request);

        ResponseEntity.BodyBuilder builder = ResponseEntity.status(result.httpStatus())
                .contentType(MediaType.APPLICATION_JSON);
        if (result.idempotentReplay()) {
            builder.header(IDEMPOTENT_REPLAY_HEADER, "true");
        }
        return builder.body(result.response());
    }

    @GetMapping(path = "/{orderId}")
    public ResponseEntity<OrderDetailsResponse> getOrder(
            @PathVariable("orderId") UUID orderId,
            @RequestHeader(value = CorrelationConstants.USER_ID_HEADER, required = false) String userIdHeader,
            @RequestHeader(value = X_USER_ROLES_HEADER, required = false) String userRolesHeader) {

        UUID userId = parseAndValidateUserId(userIdHeader, userRolesHeader);
        OrderDetailsResponse response = checkoutService.getOrder(orderId, userId);
        return ResponseEntity.ok(response);
    }

    public static UUID parseAndValidateUserId(String userIdHeader, String userRolesHeader) {
        if (userIdHeader == null || userIdHeader.isBlank()) {
            throw new UnauthorizedException("Missing authentication credentials (" + CorrelationConstants.USER_ID_HEADER + ").");
        }
        final UUID userId;
        try {
            userId = UUID.fromString(userIdHeader.trim());
        } catch (IllegalArgumentException ex) {
            throw new UnauthorizedException("Invalid " + CorrelationConstants.USER_ID_HEADER + " header: must be a valid UUID.");
        }

        if (userRolesHeader != null && !userRolesHeader.isBlank()) {
            boolean allowed = Arrays.stream(userRolesHeader.split(","))
                    .map(String::trim)
                    .anyMatch(role -> role.equalsIgnoreCase("USER") || role.equalsIgnoreCase("ADMIN"));
            if (!allowed) {
                throw new ForbiddenException("Caller lacks required USER or ADMIN role.");
            }
        }
        return userId;
    }

    public static UUID parseAndValidateIdempotencyKey(String idempotencyKeyHeader) {
        if (idempotencyKeyHeader == null || idempotencyKeyHeader.isBlank()) {
            throw new MissingIdempotencyKeyException(
                    "Idempotency-Key header (UUID v4 or v7) is required on POST /api/v1/orders/{orderId}/checkout.");
        }
        try {
            return UUID.fromString(idempotencyKeyHeader.trim());
        } catch (IllegalArgumentException ex) {
            throw new MissingIdempotencyKeyException(
                    "Idempotency-Key header must be a valid UUID.");
        }
    }
}

