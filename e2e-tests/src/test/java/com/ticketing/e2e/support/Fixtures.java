package com.ticketing.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test fixture factory for events, seats, users, and mock payment tokens.
 */
public final class Fixtures {

    public static final String TOK_SUCCESS = "tok_success";
    public static final String TOK_DECLINE = "tok_decline";
    public static final String TOK_ERROR = "tok_error";
    public static final String TOK_REFUND_DECLINE = "tok_refund_decline";

    public static final long DEFAULT_SEAT_PRICE_CENTS = 15_000L;

    private final Api api;
    private final Set<UUID> createdEventIds = ConcurrentHashMap.newKeySet();

    public Fixtures(Api api) {
        this.api = api;
    }

    public record EventFixture(UUID eventId, List<UUID> seatIds, long priceCents) {}

    public record UserFixture(UUID userId, String token) {
        public String bearerToken() {
            return token;
        }
    }

    public static UserFixture newUser(Api api) {
        return new Fixtures(api).newUser();
    }

    public static EventFixture createOnSaleEvent(Api api, int seatCount, long priceCents, String currency) {
        return new Fixtures(api).createOnSaleEvent(seatCount, priceCents);
    }

    public Set<UUID> createdEventIds() {
        return Collections.unmodifiableSet(createdEventIds);
    }

    public UserFixture newUser() {
        UUID userId = UUID.randomUUID();
        String token = api.obtainToken(userId, List.of("USER"));
        return new UserFixture(userId, token);
    }

    public UserFixture newAdmin() {
        UUID adminId = UUID.randomUUID();
        String token = api.obtainToken(adminId, List.of("ADMIN"));
        return new UserFixture(adminId, token);
    }

    public EventFixture createOnSaleEvent(int seatCount) {
        return createOnSaleEvent(seatCount, DEFAULT_SEAT_PRICE_CENTS);
    }

    public EventFixture createOnSaleEvent(int seatCount, long priceCents) {
        UserFixture admin = newAdmin();
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Instant dropTime = now.minus(1, ChronoUnit.HOURS);
        Instant eventTime = now.plus(7, ChronoUnit.DAYS);

        Api.ApiResponse createResp = api.createEvent(
                admin.token(),
                "E2E Event " + UUID.randomUUID().toString().substring(0, 8),
                "E2E Arena",
                dropTime,
                eventTime
        );
        if (createResp.status() != 201 || createResp.json() == null || !createResp.json().hasNonNull("eventId")) {
            throw new IllegalStateException("Failed to create event via gateway: HTTP "
                    + createResp.status() + " body=" + createResp.body());
        }
        UUID eventId = UUID.fromString(createResp.json().get("eventId").asText());
        createdEventIds.add(eventId);

        List<Api.SeatSpec> specs = new ArrayList<>(seatCount);
        for (int i = 1; i <= seatCount; i++) {
            String row = String.format("%02d", ((i - 1) / 20) + 1);
            String seatNum = String.format("%03d", i);
            specs.add(new Api.SeatSpec("SEC-A", row, seatNum, priceCents));
        }

        Api.ApiResponse seatsResp = api.bulkCreateSeats(admin.token(), eventId, specs);
        if (seatsResp.status() != 201 || seatsResp.json() == null || !seatsResp.json().hasNonNull("seats")) {
            throw new IllegalStateException("Failed to bulk-create seats via gateway: HTTP "
                    + seatsResp.status() + " body=" + seatsResp.body());
        }
        List<UUID> seatIds = new ArrayList<>(seatCount);
        for (JsonNode seatNode : seatsResp.json().get("seats")) {
            seatIds.add(UUID.fromString(seatNode.get("seatId").asText()));
        }

        Api.ApiResponse onSaleResp = api.setEventOnSale(admin.token(), eventId);
        if (onSaleResp.status() != 200) {
            throw new IllegalStateException("Failed to set event ON_SALE via gateway: HTTP "
                    + onSaleResp.status() + " body=" + onSaleResp.body());
        }

        return new EventFixture(eventId, Collections.unmodifiableList(seatIds), priceCents);
    }
}
