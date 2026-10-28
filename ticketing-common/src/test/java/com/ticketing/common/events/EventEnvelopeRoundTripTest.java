package com.ticketing.common.events;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.json.ObjectMapperFactory;
import com.ticketing.common.messaging.EventTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class EventEnvelopeRoundTripTest {

    private static final ObjectMapper MAPPER = ObjectMapperFactory.create();
    private static final UUID ID1 = UUID.fromString("01926b54-1111-7000-8000-000000000001");
    private static final UUID USER = UUID.fromString("a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d");
    private static final UUID EVENT = UUID.fromString("f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0");
    private static final UUID PAY = UUID.fromString("01926b55-9999-7000-8000-000000000099");
    private static final UUID SEAT1 = UUID.fromString("11111111-2222-4333-8444-555555555501");
    private static final UUID SEAT2 = UUID.fromString("11111111-2222-4333-8444-555555555502");
    private static final Instant T = Instant.parse("2026-10-27T22:01:05.450000Z");
    private static final List<SeatLine> SEATS = List.of(
            new SeatLine(SEAT1, "FLOOR-A", "1", "101", 15000L),
            new SeatLine(SEAT2, "FLOOR-A", "1", "102", 15000L));
    private static final List<UUID> SEAT_IDS = List.of(SEAT1, SEAT2);

    static Stream<Arguments> payloads() {
        return Stream.of(
                Arguments.of(EventTypes.INVENTORY_SEAT_RESERVED, new SeatReservedPayload(ID1, ID1, USER, EVENT, SEATS,
                        30000L, "USD", Instant.parse("2026-10-27T22:00:00Z"), Instant.parse("2026-10-27T22:10:00Z"))),
                Arguments.of(EventTypes.INVENTORY_SEAT_RELEASED, new SeatReleasedPayload(ID1, ID1, EVENT, SEAT_IDS,
                        "PAYMENT_FAILED", T)),
                Arguments.of(EventTypes.INVENTORY_RESERVATION_EXPIRED, new ReservationExpiredPayload(ID1, ID1, USER,
                        EVENT, SEAT_IDS, T)),
                Arguments.of(EventTypes.INVENTORY_SEAT_CONFIRMED, new SeatConfirmedPayload(ID1, ID1, PAY, EVENT,
                        SEAT_IDS, T)),
                Arguments.of(EventTypes.INVENTORY_SEAT_CONFIRMATION_REJECTED, new SeatConfirmationRejectedPayload(ID1,
                        ID1, PAY, USER, EVENT, "RESERVATION_ALREADY_EXPIRED", T)),
                Arguments.of(EventTypes.ORDER_PAYMENT_REQUESTED, new PaymentRequestedPayload(ID1, ID1, USER, EVENT,
                        30000L, "USD", "pm_tok_visa_success", T, Instant.parse("2026-10-27T22:10:00Z"))),
                Arguments.of(EventTypes.ORDER_REFUND_REQUESTED, new RefundRequestedPayload(ID1, ID1, PAY, USER, 30000L,
                        "USD", "LATE_PAYMENT_EXPIRED_HOLD", T)),
                Arguments.of(EventTypes.ORDER_CONFIRMED, new OrderConfirmedPayload(ID1, ID1, USER, EVENT, PAY, SEATS,
                        30000L, "USD", T)),
                Arguments.of(EventTypes.ORDER_CANCELLED, new OrderCancelledPayload(ID1, ID1, USER, EVENT,
                        "CANCELLED_PAYMENT_FAILED", "PAYMENT_FAILED", T)),
                Arguments.of(EventTypes.PAYMENT_SUCCEEDED, new PaymentSucceededPayload(PAY, ID1, ID1, USER, 30000L,
                        "USD", "mock_ch_987654321", T)),
                Arguments.of(EventTypes.PAYMENT_FAILED, new PaymentFailedPayload(PAY, ID1, ID1, USER, 30000L, "USD",
                        "CARD_DECLINED", "Mock payment provider declined the charge.", T)),
                Arguments.of(EventTypes.PAYMENT_REFUNDED, new PaymentRefundedPayload(UUID.randomUUID(), PAY, ID1, ID1,
                        USER, 30000L, "USD", "LATE_PAYMENT_EXPIRED_HOLD", T)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("payloads")
    void roundTripsInsideEnvelope(String eventType, Object payload) throws Exception {
        EventEnvelope<Object> envelope = new EventEnvelope<>(UUID.fromString("01926b54-7c8a-7000-8000-1a2b3c4d5e6f"),
                eventType, Instant.parse("2026-10-27T22:00:00.123456Z"), "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e", 1,
                payload);

        String json = MAPPER.writeValueAsString(envelope);
        JavaType type = MAPPER.getTypeFactory().constructParametricType(EventEnvelope.class, payload.getClass());
        EventEnvelope<Object> restored = MAPPER.readValue(json, type);

        assertThat(restored).isEqualTo(envelope);
        JsonNode node = MAPPER.readTree(json);
        assertThat(node.get("eventType").asText()).isEqualTo(eventType);
        assertThat(node.get("occurredAt").asText()).isEqualTo("2026-10-27T22:00:00.123456Z");
        assertThat(node.get("schemaVersion").asInt()).isEqualTo(1);
    }

    @Test
    void ignoresUnknownPropertiesAndSerializesMoneyAsIntegers() throws Exception {
        String json = "{\"paymentId\":\"" + PAY + "\",\"orderId\":\"" + ID1 + "\",\"reservationId\":\"" + ID1
                + "\",\"userId\":\"" + USER + "\",\"amountCents\":30000,\"currency\":\"USD\","
                + "\"providerReference\":\"x\",\"paidAt\":\"2026-10-27T22:01:05.100000Z\",\"extra\":true}";
        PaymentSucceededPayload payload = MAPPER.readValue(json, PaymentSucceededPayload.class);
        assertThat(payload.amountCents()).isEqualTo(30000L);
        assertThat(MAPPER.readTree(MAPPER.writeValueAsString(payload)).get("amountCents").isIntegralNumber()).isTrue();
    }
}
