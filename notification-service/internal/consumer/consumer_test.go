package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/ticketing-platform/notification-service/internal/dedup"
	"github.com/ticketing-platform/notification-service/internal/metrics"
	"github.com/ticketing-platform/notification-service/internal/notify"
)



var validConfirmedJSON = []byte(`{
	"eventId": "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
	"eventType": "order.confirmed",
	"occurredAt": "2026-10-27T22:00:00.123456Z",
	"correlationId": "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
	"schemaVersion": 1,
	"payload": {
		"orderId": "01926b54-1111-7000-8000-000000000001",
		"reservationId": "01926b54-1111-7000-8000-000000000001",
		"userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
		"eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
		"paymentId": "01926b55-9999-7000-8000-000000000099",
		"seats": [{"seatId": "11111111-2222-4333-8444-555555555501", "sectionCode": "A", "rowLabel": "1", "seatNumber": "1", "priceCents": 1000}],
		"totalAmountCents": 1000,
		"currency": "USD",
		"confirmedAt": "2026-10-27T22:01:05.650000Z"
	}
}`)

func TestProcessMessageValidConfirmed(t *testing.T) {
	reader := NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{
		MaxAttempts:    3,
		InitialBackoff: 0,
		MaxBackoff:     0,
	})

	msg := kafka.Message{
		Topic:     "order.events",
		Partition: 0,
		Offset:    10,
		Value:     validConfirmedJSON,
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reader.CommittedCount() != 1 {
		t.Fatalf("expected 1 committed message, got %d", reader.CommittedCount())
	}
	if sender.TotalAttempts() != 1 {
		t.Fatalf("expected 1 sender attempt, got %d", sender.TotalAttempts())
	}
}

func TestProcessMessageDuplicateEvent(t *testing.T) {
	reader := NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{
		MaxAttempts:    3,
		InitialBackoff: 0,
		MaxBackoff:     0,
	})

	msg := kafka.Message{
		Topic:     "order.events",
		Partition: 0,
		Offset:    10,
		Value:     validConfirmedJSON,
	}

	// First delivery
	if err := c.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}
	// Second delivery with same eventId
	msg.Offset = 11
	if err := c.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("second delivery failed: %v", err)
	}

	if reader.CommittedCount() != 2 {
		t.Fatalf("expected both messages to be committed, got %d", reader.CommittedCount())
	}
	// Sender should only have been called ONCE
	if sender.TotalAttempts() != 1 {
		t.Fatalf("expected sender to be called only once, got %d", sender.TotalAttempts())
	}
}

func TestProcessMessageSenderRetriesThenSucceeds(t *testing.T) {
	reader := NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(2, errors.New("temporary send error"))
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	msg := kafka.Message{
		Topic:     "order.events",
		Partition: 0,
		Offset:    20,
		Value:     validConfirmedJSON,
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sender.TotalAttempts() != 3 {
		t.Fatalf("expected 3 sender attempts, got %d", sender.TotalAttempts())
	}
	if reader.CommittedCount() != 1 {
		t.Fatalf("expected 1 committed message, got %d", reader.CommittedCount())
	}
}

func TestProcessMessageSenderAlwaysFails(t *testing.T) {
	reader := NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewAlwaysFailingSender(errors.New("downstream email service down"))
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	msg := kafka.Message{
		Topic:     "order.events",
		Partition: 0,
		Offset:    30,
		Value:     validConfirmedJSON,
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("ProcessMessage should NOT return error when sender fails (offset must be committed), got: %v", err)
	}

	if sender.TotalAttempts() != 3 {
		t.Fatalf("expected 3 sender attempts, got %d", sender.TotalAttempts())
	}
	if reader.CommittedCount() != 1 {
		t.Fatalf("expected offset to be committed despite sender failure, got %d", reader.CommittedCount())
	}

	// Check dedup key was released so replay is possible
	claimed, err := d.Claim(context.Background(), "01926b54-7c8a-7000-8000-1a2b3c4d5e6f")
	if err != nil {
		t.Fatalf("unexpected dedup error: %v", err)
	}
	if !claimed {
		t.Fatalf("expected dedup key to be deleted after sender exhaustion to allow manual replay")
	}
}

func TestProcessMessageMalformedAndIgnored(t *testing.T) {
	reader := NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{})

	// 1. Malformed JSON
	malformedMsg := kafka.Message{
		Topic:     "order.events",
		Partition: 0,
		Offset:    40,
		Value:     []byte(`{not-json}`),
	}
	if err := c.ProcessMessage(context.Background(), malformedMsg); err != nil {
		t.Fatalf("unexpected error on malformed message: %v", err)
	}

	// 2. Unhandled event type (order.payment.requested)
	unhandledMsg := kafka.Message{
		Topic:     "order.events",
		Partition: 0,
		Offset:    41,
		Value: []byte(`{
			"eventId": "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
			"eventType": "order.payment.requested",
			"occurredAt": "2026-10-27T22:00:00Z",
			"correlationId": "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
			"schemaVersion": 1,
			"payload": {"amountCents": 500}
		}`),
	}
	if err := c.ProcessMessage(context.Background(), unhandledMsg); err != nil {
		t.Fatalf("unexpected error on unhandled message: %v", err)
	}

	if reader.CommittedCount() != 2 {
		t.Fatalf("expected both malformed and unhandled messages to be committed, got %d", reader.CommittedCount())
	}
	if sender.TotalAttempts() != 0 {
		t.Fatalf("sender should not have been called, got %d", sender.TotalAttempts())
	}
}

func TestProcessMessageRedisUnavailableStopsConsumer(t *testing.T) {
	reader := NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	d.SetFailClaim(errors.New("redis: connection refused"))
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{
		MaxPollInterval: 10 * time.Millisecond,
	})

	msg := kafka.Message{
		Topic:     "order.events",
		Partition: 0,
		Offset:    50,
		Value:     validConfirmedJSON,
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err == nil {
		t.Fatalf("expected error when Redis is unavailable, got nil")
	}

	// Offset must NOT be committed!
	if reader.CommittedCount() != 0 {
		t.Fatalf("offset must NOT be committed when Redis is unavailable, got %d", reader.CommittedCount())
	}
}
