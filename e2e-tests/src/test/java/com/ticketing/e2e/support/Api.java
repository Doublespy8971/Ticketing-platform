package com.ticketing.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * HTTP client wrapper targeting {@code gateway-service} (:8080) using {@link java.net.http.HttpClient}.
 * Never throws on non-2xx responses and never fabricates HTTP status codes (such as 502/504)
 * when a local connection/transport error occurs; instead returns {@code status=0} with
 * {@code transportError=true}.
 */
public final class Api {

    private final E2EConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public Api(E2EConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public ObjectMapper objectMapper() {
        return objectMapper;
    }

    public record SeatSpec(String sectionCode, String rowLabel, String seatNumber, long priceCents) {}

    public record ApiResponse(
            int status,
            HttpHeaders headers,
            String body,
            JsonNode json,
            boolean transportError,
            String transportErrorMessage
    ) {
        public ApiResponse(int status, HttpHeaders headers, String body, JsonNode json) {
            this(status, headers, body, json, false, null);
        }

        public Optional<String> header(String name) {
            return headers != null ? headers.firstValue(name) : Optional.empty();
        }

        public String headerValue(String name) {
            return header(name).orElse("");
        }

        public String errorCode() {
            if (json != null && json.hasNonNull("errorCode")) {
                return json.get("errorCode").asText();
            }
            return "";
        }
    }

    /**
     * Calls {@code POST /api/v1/auth/token} on the gateway.
     */
    public ApiResponse requestToken(UUID userId, List<String> roles, long ttlSeconds) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("userId", userId.toString());
        payload.put("roles", roles);
        payload.put("ttlSeconds", ttlSeconds);
        return postJson("/api/v1/auth/token", payload, Map.of());
    }

    /**
     * Obtains a Bearer JWT from the gateway's dev token endpoint ({@code POST /api/v1/auth/token}).
     * If the dev token endpoint's IP rate limiter (10 burst, 1 req/s) returns 429, falls back to
     * signing the identical HS256 JWT locally with {@code JWT_SECRET}.
     */
    public String obtainToken(UUID userId, List<String> roles) {
        ApiResponse resp = requestToken(userId, roles, 3600);
        if (resp.status() == 200 && resp.json() != null && resp.json().hasNonNull("accessToken")) {
            return resp.json().get("accessToken").asText();
        }
        if (resp.status() == 429) {
            return mintHs256Jwt(userId, roles, 3600);
        }
        throw new IllegalStateException("Failed to obtain token from gateway /api/v1/auth/token: HTTP "
                + resp.status() + " body=" + resp.body()
                + (resp.transportError() ? " transportError=" + resp.transportErrorMessage() : ""));
    }

    public String mintHs256Jwt(UUID userId, List<String> roles, long ttlSeconds) {
        try {
            long now = Instant.now().getEpochSecond();
            long exp = now + ttlSeconds;
            Map<String, Object> header = Map.of("typ", "JWT", "alg", "HS256");
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("sub", userId.toString());
            payload.put("roles", roles);
            payload.put("iat", now);
            payload.put("exp", exp);

            Base64.Encoder urlEncoder = Base64.getUrlEncoder().withoutPadding();
            String h = urlEncoder.encodeToString(objectMapper.writeValueAsBytes(header));
            String p = urlEncoder.encodeToString(objectMapper.writeValueAsBytes(payload));
            String signingInput = h + "." + p;

            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(config.jwtSecret().getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            String sig = urlEncoder.encodeToString(mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8)));
            return signingInput + "." + sig;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to sign HS256 JWT", e);
        }
    }

    public ApiResponse createEvent(String bearerToken, String name, String venueName, Instant dropTime, Instant eventTime) {
        return createEventWithHeaders(bearerToken, name, venueName, dropTime, eventTime, Map.of());
    }

    public ApiResponse createEventWithHeaders(
            String bearerToken,
            String name,
            String venueName,
            Instant dropTime,
            Instant eventTime,
            Map<String, String> extraHeaders) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("venueName", venueName);
        body.put("dropTime", dropTime.toString());
        body.put("eventTime", eventTime.toString());
        return postJson("/api/v1/admin/events", body, authHeaders(bearerToken, null, extraHeaders));
    }

    public ApiResponse setEventStatus(String bearerToken, UUID eventId, String status) {
        return setEventStatusWithHeaders(bearerToken, eventId, status, Map.of());
    }

    public ApiResponse setEventStatusWithHeaders(
            String bearerToken,
            UUID eventId,
            String status,
            Map<String, String> extraHeaders) {
        Map<String, Object> body = Map.of("status", status);
        return patchJson("/api/v1/admin/events/" + eventId + "/status", body, authHeaders(bearerToken, null, extraHeaders));
    }

    public ApiResponse setEventOnSale(String bearerToken, UUID eventId) {
        return setEventStatus(bearerToken, eventId, "ON_SALE");
    }

    public ApiResponse bulkCreateSeats(String bearerToken, UUID eventId, List<SeatSpec> seats) {
        Map<String, Object> body = Map.of("seats", seats);
        return postJson("/api/v1/admin/events/" + eventId + "/seats/bulk", body, authHeaders(bearerToken, null, Map.of()));
    }

    public ApiResponse getSeats(UUID eventId, String rawQuery) {
        String path = "/api/v1/events/" + eventId + "/seats";
        if (rawQuery != null && !rawQuery.isBlank()) {
            path += (rawQuery.startsWith("?") ? rawQuery : "?" + rawQuery);
        }
        return rawRequest("GET", path, null, Map.of());
    }

    public ApiResponse getSeats(UUID eventId, String section, String status, Integer limit, UUID cursor) {
        List<String> params = new java.util.ArrayList<>();
        if (section != null && !section.isBlank()) params.add("section=" + section);
        if (status != null && !status.isBlank()) params.add("status=" + status);
        if (limit != null) params.add("limit=" + limit);
        if (cursor != null) params.add("cursor=" + cursor);
        return getSeats(eventId, String.join("&", params));
    }

    public ApiResponse holdSeats(String userToken, UUID eventId, List<UUID> seatIds, UUID idempotencyKey) {
        return hold(userToken, idempotencyKey, eventId, seatIds);
    }

    public ApiResponse hold(String userToken, UUID idempotencyKey, UUID eventId, List<UUID> seatIds) {
        return holdWithHeaders(userToken, idempotencyKey, eventId, seatIds, Map.of());
    }

    public ApiResponse holdWithHeaders(
            String userToken,
            UUID idempotencyKey,
            UUID eventId,
            List<UUID> seatIds,
            Map<String, String> extraHeaders) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("eventId", eventId.toString());
        body.put("seatIds", seatIds.stream().map(UUID::toString).toList());
        return postJson("/api/v1/reservations", body, authHeaders(userToken, idempotencyKey, extraHeaders));
    }

    public ApiResponse checkout(String userToken, UUID orderId, UUID idempotencyKey, String paymentMethodToken) {
        return checkoutWithHeaders(userToken, orderId, idempotencyKey, paymentMethodToken, Map.of());
    }

    public ApiResponse checkoutWithSpoofedHeaders(
            String userToken,
            UUID orderId,
            UUID idempotencyKey,
            String paymentMethodToken,
            Map<String, String> extraHeaders) {
        return checkoutWithHeaders(userToken, orderId, idempotencyKey, paymentMethodToken, extraHeaders);
    }

    public ApiResponse checkoutWithHeaders(
            String userToken,
            UUID orderId,
            UUID idempotencyKey,
            String paymentMethodToken,
            Map<String, String> extraHeaders) {
        Map<String, Object> body = Map.of("paymentMethodToken", paymentMethodToken);
        return postJson("/api/v1/orders/" + orderId + "/checkout", body, authHeaders(userToken, idempotencyKey, extraHeaders));
    }

    public ApiResponse getOrder(String userToken, UUID orderId) {
        return getOrderWithHeaders(userToken, orderId, Map.of());
    }

    public ApiResponse getOrderWithHeaders(String userToken, UUID orderId, Map<String, String> extraHeaders) {
        return rawRequest("GET", "/api/v1/orders/" + orderId, null, authHeaders(userToken, null, extraHeaders));
    }

    public ApiResponse postJson(String path, Object payload, Map<String, String> headers) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            return rawRequest("POST", path, json, headers);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize JSON request body", e);
        }
    }

    public ApiResponse patchJson(String path, Object payload, Map<String, String> headers) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            return rawRequest("PATCH", path, json, headers);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize JSON request body", e);
        }
    }

    public ApiResponse rawRequest(String method, String pathAndQuery, String jsonBody, Map<String, String> headers) {
        String fullUrl = config.gatewayBaseUrl() + (pathAndQuery.startsWith("/") ? pathAndQuery : "/" + pathAndQuery);
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(fullUrl))
                .timeout(Duration.ofSeconds(12))
                .header("Accept", "application/json, application/problem+json");

        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    builder.header(entry.getKey(), entry.getValue());
                }
            }
        }

        if (jsonBody != null) {
            builder.header("Content-Type", "application/json");
            builder.method(method, HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8));
        } else {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        }

        try {
            HttpResponse<String> resp = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            String respBody = resp.body() != null ? resp.body() : "";
            JsonNode parsed = null;
            if (!respBody.isBlank()) {
                try {
                    parsed = objectMapper.readTree(respBody);
                } catch (Exception ignored) {
                }
            }
            return new ApiResponse(resp.statusCode(), resp.headers(), respBody, parsed, false, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ApiResponse(0, null, "", null, true, e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (Exception e) {
            return new ApiResponse(0, null, "", null, true, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static Map<String, String> authHeaders(String bearerToken, UUID idempotencyKey, Map<String, String> extra) {
        Map<String, String> map = new LinkedHashMap<>();
        if (bearerToken != null && !bearerToken.isBlank()) {
            map.put("Authorization", "Bearer " + bearerToken);
        }
        if (idempotencyKey != null) {
            map.put("Idempotency-Key", idempotencyKey.toString());
        }
        if (extra != null) {
            map.putAll(extra);
        }
        return map;
    }
}
