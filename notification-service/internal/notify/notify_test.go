package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ticketing-platform/notification-service/internal/events"
)

func TestMapEventToNotification(t *testing.T) {
	// 1. order.confirmed
	confirmedEnv := &events.Envelope{
		EventID:       "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
		EventType:     events.EventTypeOrderConfirmed,
		CorrelationID: "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
		SchemaVersion: 1,
		Payload: []byte(`{
			"orderId": "01926b54-1111-7000-8000-000000000001",
			"reservationId": "01926b54-1111-7000-8000-000000000001",
			"userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
			"eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
			"paymentId": "01926b55-9999-7000-8000-000000000099",
			"seats": [{"seatId": "11111111-2222-4333-8444-555555555501", "sectionCode": "A", "rowLabel": "1", "seatNumber": "10", "priceCents": 10000}],
			"totalAmountCents": 10000,
			"currency": "USD",
			"confirmedAt": "2026-10-27T22:01:05.650000Z"
		}`),
	}

	notif, handled, err := MapEventToNotification(confirmedEnv)
	if err != nil {
		t.Fatalf("unexpected error for order.confirmed: %v", err)
	}
	if !handled {
		t.Fatalf("expected order.confirmed to be handled")
	}
	if notif.NotificationKind != KindConfirmation {
		t.Errorf("expected kind %s, got %s", KindConfirmation, notif.NotificationKind)
	}
	if notif.OrderID != "01926b54-1111-7000-8000-000000000001" {
		t.Errorf("expected orderId 01926b54-1111-7000-8000-000000000001, got %s", notif.OrderID)
	}

	// 2. order.cancelled
	cancelledEnv := &events.Envelope{
		EventID:       "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
		EventType:     events.EventTypeOrderCancelled,
		CorrelationID: "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
		SchemaVersion: 1,
		Payload: []byte(`{
			"orderId": "01926b54-1111-7000-8000-000000000001",
			"reservationId": "01926b54-1111-7000-8000-000000000001",
			"userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
			"eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
			"finalStatus": "CANCELLED_PAYMENT_FAILED",
			"cancellationReason": "PAYMENT_FAILED",
			"cancelledAt": "2026-10-27T22:01:15.350000Z"
		}`),
	}

	notif2, handled2, err := MapEventToNotification(cancelledEnv)
	if err != nil {
		t.Fatalf("unexpected error for order.cancelled: %v", err)
	}
	if !handled2 {
		t.Fatalf("expected order.cancelled to be handled")
	}
	if notif2.NotificationKind != KindCancellation {
		t.Errorf("expected kind %s, got %s", KindCancellation, notif2.NotificationKind)
	}
	if notif2.FinalStatus != "CANCELLED_PAYMENT_FAILED" {
		t.Errorf("expected finalStatus CANCELLED_PAYMENT_FAILED, got %s", notif2.FinalStatus)
	}
	if notif2.CancellationReason != "PAYMENT_FAILED" {
		t.Errorf("expected cancellationReason PAYMENT_FAILED, got %s", notif2.CancellationReason)
	}

	// 3. Unhandled event
	otherEnv := &events.Envelope{
		EventID:   "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
		EventType: "order.payment.requested",
		Payload:   []byte(`{"orderId": "123"}`),
	}
	notif3, handled3, err := MapEventToNotification(otherEnv)
	if err != nil {
		t.Fatalf("unexpected error for unhandled event: %v", err)
	}
	if handled3 || notif3 != nil {
		t.Fatalf("expected unhandled event to return handled=false and nil notif")
	}

	// 4. Invalid payload
	badEnv := &events.Envelope{
		EventID:   "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
		EventType: events.EventTypeOrderConfirmed,
		Payload:   []byte(`{"invalid": true}`),
	}
	_, _, err = MapEventToNotification(badEnv)
	if err == nil {
		t.Fatalf("expected error for invalid payload")
	}
}

func TestLogSenderJSONOutput(t *testing.T) {
	var buf bytes.Buffer
	sender := NewLogSender(&buf)

	notif := Notification{
		EventID:            "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
		EventType:          events.EventTypeOrderCancelled,
		OrderID:            "01926b54-1111-7000-8000-000000000001",
		NotificationKind:   KindCancellation,
		FinalStatus:        "CANCELLED_PAYMENT_FAILED",
		CancellationReason: "PAYMENT_FAILED",
		CorrelationID:      "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
	}

	err := sender.Send(context.Background(), notif)
	if err != nil {
		t.Fatalf("unexpected error from LogSender: %v", err)
	}

	line := strings.TrimSpace(buf.String())
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("LogSender did not output valid JSON: %v, output=%s", err, line)
	}

	if decoded["service"] != "notification-service" {
		t.Errorf("expected service notification-service, got %v", decoded["service"])
	}
	if decoded["level"] != "INFO" {
		t.Errorf("expected level INFO, got %v", decoded["level"])
	}
	if decoded["eventId"] != notif.EventID {
		t.Errorf("expected eventId %s, got %v", notif.EventID, decoded["eventId"])
	}
	if decoded["eventType"] != notif.EventType {
		t.Errorf("expected eventType %s, got %v", notif.EventType, decoded["eventType"])
	}
	if decoded["orderId"] != notif.OrderID {
		t.Errorf("expected orderId %s, got %v", notif.OrderID, decoded["orderId"])
	}
	if decoded["notificationKind"] != notif.NotificationKind {
		t.Errorf("expected notificationKind %s, got %v", notif.NotificationKind, decoded["notificationKind"])
	}

	// Verify absence of sensitive words
	for _, forbidden := range []string{"tok_", "pm_", "password", "email", "secret"} {
		if strings.Contains(strings.ToLower(line), forbidden) {
			t.Errorf("log line contains sensitive word %q: %s", forbidden, line)
		}
	}
}

func TestFailingSender(t *testing.T) {
	ctx := context.Background()
	notif := Notification{OrderID: "order-1"}

	// Fails twice, then succeeds
	sender := NewFailingSender(2, errors.New("temporary error"))
	if err := sender.Send(ctx, notif); err == nil {
		t.Fatalf("expected error on attempt 1")
	}
	if err := sender.Send(ctx, notif); err == nil {
		t.Fatalf("expected error on attempt 2")
	}
	if err := sender.Send(ctx, notif); err != nil {
		t.Fatalf("expected success on attempt 3, got %v", err)
	}
	if sender.TotalAttempts() != 3 {
		t.Errorf("expected 3 attempts, got %d", sender.TotalAttempts())
	}

	// Always failing
	alwaysFail := NewAlwaysFailingSender(errors.New("fatal"))
	for i := 0; i < 5; i++ {
		if err := alwaysFail.Send(ctx, notif); err == nil {
			t.Fatalf("expected error on attempt %d", i+1)
		}
	}
	if alwaysFail.TotalAttempts() != 5 {
		t.Errorf("expected 5 attempts, got %d", alwaysFail.TotalAttempts())
	}
}

func TestSlowSender(t *testing.T) {
	sender := NewSlowSender(10 * time.Millisecond)
	start := time.Now()
	err := sender.Send(context.Background(), Notification{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if time.Since(start) < 10*time.Millisecond {
		t.Errorf("expected SlowSender to take at least 10ms")
	}
}
