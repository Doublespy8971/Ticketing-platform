package events

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseEnvelopeValid(t *testing.T) {
	raw := `{
		"eventId": "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
		"eventType": "order.confirmed",
		"occurredAt": "2026-10-27T22:00:00.123456Z",
		"correlationId": "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
		"schemaVersion": 1,
		"payload": {"orderId": "01926b54-1111-7000-8000-000000000001"}
	}`

	env, err := ParseEnvelope([]byte(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if env.EventID != "01926b54-7c8a-7000-8000-1a2b3c4d5e6f" {
		t.Errorf("unexpected eventId: %s", env.EventID)
	}
	if env.EventType != "order.confirmed" {
		t.Errorf("unexpected eventType: %s", env.EventType)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("unexpected schemaVersion: %d", env.SchemaVersion)
	}
	if !IsHandledEventType(env.EventType) {
		t.Errorf("expected order.confirmed to be handled")
	}
}

func TestParseEnvelopeMissingFields(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{"empty", ""},
		{"missing eventId", `{"eventType":"order.confirmed","occurredAt":"2026-10-27T22:00:00Z","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":1,"payload":{}}`},
		{"invalid eventId not uuid", `{"eventId":"not-a-uuid","eventType":"order.confirmed","occurredAt":"2026-10-27T22:00:00Z","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":1,"payload":{}}`},
		{"nil eventId", `{"eventId":"00000000-0000-0000-0000-000000000000","eventType":"order.confirmed","occurredAt":"2026-10-27T22:00:00Z","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":1,"payload":{}}`},
		{"missing eventType", `{"eventId":"01926b54-7c8a-7000-8000-1a2b3c4d5e6f","occurredAt":"2026-10-27T22:00:00Z","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":1,"payload":{}}`},
		{"missing occurredAt", `{"eventId":"01926b54-7c8a-7000-8000-1a2b3c4d5e6f","eventType":"order.confirmed","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":1,"payload":{}}`},
		{"invalid occurredAt", `{"eventId":"01926b54-7c8a-7000-8000-1a2b3c4d5e6f","eventType":"order.confirmed","occurredAt":"not-a-timestamp","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":1,"payload":{}}`},
		{"missing correlationId", `{"eventId":"01926b54-7c8a-7000-8000-1a2b3c4d5e6f","eventType":"order.confirmed","occurredAt":"2026-10-27T22:00:00Z","schemaVersion":1,"payload":{}}`},
		{"schemaVersion 2", `{"eventId":"01926b54-7c8a-7000-8000-1a2b3c4d5e6f","eventType":"order.confirmed","occurredAt":"2026-10-27T22:00:00Z","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":2,"payload":{}}`},
		{"null payload", `{"eventId":"01926b54-7c8a-7000-8000-1a2b3c4d5e6f","eventType":"order.confirmed","occurredAt":"2026-10-27T22:00:00Z","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":1,"payload":null}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseEnvelope([]byte(tc.json))
			if err == nil {
				t.Fatalf("expected error for case %q, got nil", tc.name)
			}
		})
	}
}

func TestParseEnvelopeWrongTypes(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{"eventId is int", `{"eventId":12345,"eventType":"order.confirmed","occurredAt":"2026-10-27T22:00:00Z","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":1,"payload":{}}`},
		{"schemaVersion is string", `{"eventId":"01926b54-7c8a-7000-8000-1a2b3c4d5e6f","eventType":"order.confirmed","occurredAt":"2026-10-27T22:00:00Z","correlationId":"c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e","schemaVersion":"1","payload":{}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseEnvelope([]byte(tc.json))
			if err == nil {
				t.Fatalf("expected error for wrong type case %q, got nil", tc.name)
			}
		})
	}
}

func TestParseEnvelopeUnknownType(t *testing.T) {
	raw := `{
		"eventId": "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
		"eventType": "order.payment.requested",
		"occurredAt": "2026-10-27T22:00:00Z",
		"correlationId": "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
		"schemaVersion": 1,
		"payload": {"amountCents": 1000}
	}`

	env, err := ParseEnvelope([]byte(raw))
	if err != nil {
		t.Fatalf("unexpected error parsing envelope with unknown eventType: %v", err)
	}

	if IsHandledEventType(env.EventType) {
		t.Fatalf("expected order.payment.requested to NOT be handled")
	}
}

func TestParseEnvelopeHugePayload(t *testing.T) {
	// Construct a 2MB payload
	hugeString := strings.Repeat("A", 2*1024*1024)
	payloadObj := map[string]string{"data": hugeString}
	payloadBytes, _ := json.Marshal(payloadObj)

	envObj := map[string]any{
		"eventId":       "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
		"eventType":     "order.confirmed",
		"occurredAt":    "2026-10-27T22:00:00Z",
		"correlationId": "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
		"schemaVersion": 1,
		"payload":       json.RawMessage(payloadBytes),
	}
	envBytes, _ := json.Marshal(envObj)

	env, err := ParseEnvelope(envBytes)
	if err != nil {
		t.Fatalf("failed to parse huge payload: %v", err)
	}
	if len(env.Payload) < 2*1024*1024 {
		t.Fatalf("payload length too small: %d", len(env.Payload))
	}
}

func TestParseOrderConfirmedPayload(t *testing.T) {
	raw := `{
		"orderId": "01926b54-1111-7000-8000-000000000001",
		"reservationId": "01926b54-1111-7000-8000-000000000001",
		"userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
		"eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
		"paymentId": "01926b55-9999-7000-8000-000000000099",
		"seats": [
			{
				"seatId": "11111111-2222-4333-8444-555555555501",
				"sectionCode": "FLOOR-A",
				"rowLabel": "1",
				"seatNumber": "101",
				"priceCents": 15000
			}
		],
		"totalAmountCents": 15000,
		"currency": "USD",
		"confirmedAt": "2026-10-27T22:01:05.650000Z"
	}`

	payload, err := ParseOrderConfirmedPayload([]byte(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payload.OrderID != "01926b54-1111-7000-8000-000000000001" {
		t.Errorf("wrong orderId: %s", payload.OrderID)
	}
	if len(payload.Seats) != 1 {
		t.Errorf("expected 1 seat, got %d", len(payload.Seats))
	}

	// Test missing seat
	badRaw := `{"orderId":"01926b54-1111-7000-8000-000000000001","reservationId":"01926b54-1111-7000-8000-000000000001","userId":"a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d","eventId":"f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0","paymentId":"01926b55-9999-7000-8000-000000000099","seats":[],"totalAmountCents":15000,"currency":"USD","confirmedAt":"2026-10-27T22:01:05Z"}`
	if _, err := ParseOrderConfirmedPayload([]byte(badRaw)); err == nil {
		t.Fatalf("expected error for empty seats, got nil")
	}
}

func TestParseOrderCancelledPayload(t *testing.T) {
	raw := `{
		"orderId": "01926b54-1111-7000-8000-000000000001",
		"reservationId": "01926b54-1111-7000-8000-000000000001",
		"userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
		"eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
		"finalStatus": "CANCELLED_PAYMENT_FAILED",
		"cancellationReason": "PAYMENT_FAILED",
		"cancelledAt": "2026-10-27T22:01:15.350000Z"
	}`

	payload, err := ParseOrderCancelledPayload([]byte(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payload.FinalStatus != "CANCELLED_PAYMENT_FAILED" {
		t.Errorf("wrong finalStatus: %s", payload.FinalStatus)
	}
	if payload.CancellationReason != "PAYMENT_FAILED" {
		t.Errorf("wrong cancellationReason: %s", payload.CancellationReason)
	}

	// Test invalid finalStatus
	badRaw := `{"orderId":"01926b54-1111-7000-8000-000000000001","reservationId":"01926b54-1111-7000-8000-000000000001","userId":"a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d","eventId":"f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0","finalStatus":"UNKNOWN_STATUS","cancellationReason":"PAYMENT_FAILED","cancelledAt":"2026-10-27T22:01:15Z"}`
	if _, err := ParseOrderCancelledPayload([]byte(badRaw)); err == nil {
		t.Fatalf("expected error for invalid finalStatus, got nil")
	}
}
