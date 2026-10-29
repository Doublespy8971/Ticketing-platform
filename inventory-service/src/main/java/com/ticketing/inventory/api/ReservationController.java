package com.ticketing.inventory.api;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.inventory.api.dto.CreateReservationRequest;
import com.ticketing.inventory.api.dto.ReservationResponse;
import com.ticketing.inventory.service.ReservationService;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

@RestController
@RequestMapping(path = "/api/v1/reservations", produces = MediaType.APPLICATION_JSON_VALUE)
public class ReservationController {

    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    public static final String IDEMPOTENT_REPLAY_HEADER = "X-Idempotent-Replay";

    private final ReservationService reservationService;

    public ReservationController(ReservationService reservationService) {
        this.reservationService = Objects.requireNonNull(reservationService, "reservationService must not be null");
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ReservationResponse> createReservation(
            @RequestHeader(value = CorrelationConstants.USER_ID_HEADER, required = false) String userIdHeader,
            @RequestHeader(value = AdminRoleInterceptor.X_USER_ROLES_HEADER, required = false) String userRolesHeader,
            @RequestHeader(value = IDEMPOTENCY_KEY_HEADER, required = false) String idempotencyKeyHeader,
            @Valid @RequestBody CreateReservationRequest request) {

        UUID userId = parseAndValidateUserId(userIdHeader, userRolesHeader);
        UUID idempotencyKey = parseAndValidateIdempotencyKey(idempotencyKeyHeader);

        ReservationService.HoldExecutionResult result =
                reservationService.createReservation(userId, idempotencyKey, request);

        ResponseEntity.BodyBuilder builder = ResponseEntity.status(result.httpStatus())
                .contentType(MediaType.APPLICATION_JSON);
        if (result.idempotentReplay()) {
            builder.header(IDEMPOTENT_REPLAY_HEADER, "true");
        }
        return builder.body(result.response());
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
                    "Idempotency-Key header (UUID v4 or v7) is required on POST /api/v1/reservations.");
        }
        try {
            return UUID.fromString(idempotencyKeyHeader.trim());
        } catch (IllegalArgumentException ex) {
            throw new MissingIdempotencyKeyException(
                    "Idempotency-Key header must be a valid UUID.");
        }
    }
}
