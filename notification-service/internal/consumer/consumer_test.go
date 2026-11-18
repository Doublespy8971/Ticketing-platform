package consumer

import (
	"context"
	"errors"
	"sync"
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

type panickingSender struct{}

func (p *panickingSender) Send(ctx context.Context, notif notify.Notification) error {
	panic("unexpected poison pill in sender!")
}

func TestProcessMessagePanicRecovery(t *testing.T) {
	reader := NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, &panickingSender{}, m, Options{})

	msg := kafka.Message{
		Topic:     "order.events",
		Partition: 0,
		Offset:    60,
		Value:     validConfirmedJSON,
	}

	// Should NOT panic or terminate process
	err := c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected panic to be recovered without returning fatal error, got %v", err)
	}

	// Offset must be committed to avoid poison pill partition stall
	if reader.CommittedCount() != 1 {
		t.Fatalf("expected offset to be committed after panic recovery, got %d", reader.CommittedCount())
	}

	// Held claim must be released so key is not orphaned
	claimed, err := d.Claim(context.Background(), "01926b54-7c8a-7000-8000-1a2b3c4d5e6f")
	if err != nil || !claimed {
		t.Fatalf("expected claim to have been released after panic, claimed=%v, err=%v", claimed, err)
	}
}

func TestProcessMessageCancelledReleasesClaim(t *testing.T) {
	reader := NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(5, errors.New("temporary error"))
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{
		MaxAttempts:    3,
		InitialBackoff: 200 * time.Millisecond,
		MaxBackoff:     400 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after 50ms (during retry backoff)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	msg := kafka.Message{
		Topic:     "order.events",
		Partition: 0,
		Offset:    70,
		Value:     validConfirmedJSON,
	}

	err := c.ProcessMessage(ctx, msg)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Claim MUST be released with fresh context so key is not orphaned
	claimed, err := d.Claim(context.Background(), "01926b54-7c8a-7000-8000-1a2b3c4d5e6f")
	if err != nil || !claimed {
		t.Fatalf("expected claim to have been released on cancellation with fresh context, claimed=%v, err=%v", claimed, err)
	}

	// Offset must NOT be committed when cancelled without processing
	if reader.CommittedCount() != 0 {
		t.Fatalf("offset must not be committed on cancellation, got %d", reader.CommittedCount())
	}
}

type transientErrorReader struct {
	err error
}

func (r *transientErrorReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if ctx.Err() != nil {
		return kafka.Message{}, ctx.Err()
	}
	return kafka.Message{}, r.err
}

func (r *transientErrorReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return nil
}

func (r *transientErrorReader) Close() error {
	return nil
}

// Requirement 3: transient error forever with 10s backoff -> stops within 500 ms after StopFetching
func TestRun_TransientFetchErrorInterruptibleByStopFetching(t *testing.T) {
	reader := &transientErrorReader{err: errors.New("kafka: not leader for partition")}
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{})

	runDone := make(chan error, 1)
	ctx := context.Background()

	go func() {
		runDone <- c.Run(ctx)
	}()

	// Wait briefly so reader fails fetch and enters backoff timer
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	c.StopFetching()

	select {
	case err := <-runDone:
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("expected nil error on stop fetching, got %v", err)
		}
		if elapsed > 500*time.Millisecond {
			t.Fatalf("expected Run to return within 500ms, took %v", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for Run to return after StopFetching")
	}
}

type failingCommitReader struct {
	mu           sync.Mutex
	messages     []kafka.Message
	commitCalled chan struct{}
	commitOnce   sync.Once
}

func (r *failingCommitReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.messages) == 0 {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := r.messages[0]
	r.messages = r.messages[1:]
	return msg, nil
}

func (r *failingCommitReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	r.commitOnce.Do(func() {
		close(r.commitCalled)
	})
	return errors.New("kafka: commit failure")
}

func (r *failingCommitReader) Close() error {
	return nil
}

// Requirement 3: same during commit retry -> stops within 500 ms after StopFetching
func TestRun_CommitRetryInterruptibleByStopFetching(t *testing.T) {
	commitCalled := make(chan struct{})
	reader := &failingCommitReader{
		messages: []kafka.Message{{
			Topic:     "order.events",
			Partition: 0,
			Offset:    100,
			Value:     validConfirmedJSON,
		}},
		commitCalled: commitCalled,
	}

	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{})

	runDone := make(chan error, 1)
	ctx := context.Background()

	go func() {
		runDone <- c.Run(ctx)
	}()

	// Wait until commit is attempted
	select {
	case <-commitCalled:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for commit to be called")
	}

	// Consumer is now in commitWithRetry backoff sleep (500ms..10s)
	start := time.Now()
	c.StopFetching()

	select {
	case err := <-runDone:
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("expected nil error on stop fetching, got %v", err)
		}
		if elapsed > 500*time.Millisecond {
			t.Fatalf("expected Run to return within 500ms during commit retry, took %v", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for Run to return during commit retry")
	}
}

type blockingClaimDeduplicator struct {
	claimCalled chan struct{}
	claimOnce   sync.Once
}

func (b *blockingClaimDeduplicator) Claim(ctx context.Context, eventID string) (bool, error) {
	b.claimOnce.Do(func() {
		close(b.claimCalled)
	})
	<-ctx.Done()
	return false, ctx.Err()
}

func (b *blockingClaimDeduplicator) Promote(ctx context.Context, eventID string) error {
	return nil
}

func (b *blockingClaimDeduplicator) Release(ctx context.Context, eventID string, reasons ...string) error {
	return nil
}

func (b *blockingClaimDeduplicator) Key(eventID string) string {
	return "test-key"
}

// Requirement 3: same while waiting on another worker's claim -> stops within 500 ms after StopFetching
func TestRun_ClaimWaitInterruptibleByStopFetching(t *testing.T) {
	claimCalled := make(chan struct{})
	reader := NewMockMessageReader([]kafka.Message{{
		Topic:     "order.events",
		Partition: 0,
		Offset:    200,
		Value:     validConfirmedJSON,
	}})

	d := &blockingClaimDeduplicator{claimCalled: claimCalled}
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := NewConsumer(reader, d, sender, m, Options{})

	runDone := make(chan error, 1)
	ctx := context.Background()

	go func() {
		runDone <- c.Run(ctx)
	}()

	// Wait until claim is called
	select {
	case <-claimCalled:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for Claim to be called")
	}

	start := time.Now()
	c.StopFetching()

	select {
	case err := <-runDone:
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("expected nil error on stop fetching during claim wait, got %v", err)
		}
		if elapsed > 500*time.Millisecond {
			t.Fatalf("expected Run to return within 500ms while waiting on claim, took %v", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for Run to return while waiting on claim")
	}
}
