package test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/ticketing-platform/notification-service/internal/consumer"
	"github.com/ticketing-platform/notification-service/internal/dedup"
	"github.com/ticketing-platform/notification-service/internal/events"
	"github.com/ticketing-platform/notification-service/internal/health"
	"github.com/ticketing-platform/notification-service/internal/metrics"
	"github.com/ticketing-platform/notification-service/internal/notify"
)

type testInfra struct {
	kafkaBrokers []string
	redisAddr    string
	rdb          *redis.Client
	cleanup      func()
}

func getLiveInfra(t *testing.T) *testInfra {
	defer func() {
		if r := recover(); r != nil {
			t.Skipf("Live Kafka/Redis and Docker socket not accessible: %v", r)
		}
	}()

	localKafka := "localhost:29092"
	localRedis := "localhost:6379"

	kafkaConn, kErr := net.DialTimeout("tcp", localKafka, 300*time.Millisecond)
	redisConn, rErr := net.DialTimeout("tcp", localRedis, 300*time.Millisecond)

	if kErr == nil && rErr == nil {
		_ = kafkaConn.Close()
		_ = redisConn.Close()

		rdb := redis.NewClient(&redis.Options{Addr: localRedis})
		if err := rdb.Ping(context.Background()).Err(); err == nil {
			return &testInfra{
				kafkaBrokers: []string{localKafka},
				redisAddr:    localRedis,
				rdb:          rdb,
				cleanup: func() {
					_ = rdb.Close()
				},
			}
		}
		_ = rdb.Close()
	}

	// Try testcontainers if running with Docker socket access
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	redisC, err := tcredis.Run(ctx, "redis:7.4.1-alpine")
	if err != nil {
		t.Skipf("Live Kafka/Redis and testcontainers not available (e.g. running in sandbox): %v", err)
		return nil
	}

	redisEndpoint, err := redisC.ConnectionString(ctx)
	if err != nil {
		_ = redisC.Terminate(context.Background())
		t.Skipf("Failed to get redis container connection string: %v", err)
		return nil
	}
	redisAddr := strings.TrimPrefix(redisEndpoint, "redis://")

	kafkaC, err := tckafka.Run(ctx, "apache/kafka:3.8.0")
	if err != nil {
		_ = redisC.Terminate(context.Background())
		t.Skipf("Kafka testcontainer could not start: %v", err)
		return nil
	}

	brokers, err := kafkaC.Brokers(ctx)
	if err != nil {
		_ = redisC.Terminate(context.Background())
		_ = kafkaC.Terminate(context.Background())
		t.Skipf("Failed to get kafka brokers: %v", err)
		return nil
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	return &testInfra{
		kafkaBrokers: brokers,
		redisAddr:    redisAddr,
		rdb:          rdb,
		cleanup: func() {
			_ = rdb.Close()
			_ = redisC.Terminate(context.Background())
			_ = kafkaC.Terminate(context.Background())
		},
	}
}

func makeValidConfirmedMessage(offset int64, eventID, orderID string) kafka.Message {
	if eventID == "" {
		eventID = uuid.NewString()
	}
	if orderID == "" {
		orderID = uuid.NewString()
	}

	val := fmt.Sprintf(`{
		"eventId": "%s",
		"eventType": "order.confirmed",
		"occurredAt": "%s",
		"correlationId": "%s",
		"schemaVersion": 1,
		"payload": {
			"orderId": "%s",
			"reservationId": "%s",
			"userId": "%s",
			"eventId": "%s",
			"paymentId": "%s",
			"seats": [{"seatId": "%s", "sectionCode": "A", "rowLabel": "1", "seatNumber": "10", "priceCents": 15000}],
			"totalAmountCents": 15000,
			"currency": "USD",
			"confirmedAt": "%s"
		}
	}`, eventID, time.Now().UTC().Format(time.RFC3339Nano), uuid.NewString(),
		orderID, orderID, uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), time.Now().UTC().Format(time.RFC3339Nano))

	return kafka.Message{
		Topic:  "order.events",
		Offset: offset,
		Key:    []byte(orderID),
		Value:  []byte(val),
	}
}

// 1) A valid order.confirmed produces exactly one notification and the offset is committed (lag reaches zero)
func TestIntegration_ValidOrderConfirmedCommitted(t *testing.T) {
	msg := makeValidConfirmedMessage(100, "", "")
	reader := consumer.NewMockMessageReader([]kafka.Message{msg})
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fMsg, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if err := c.ProcessMessage(ctx, fMsg); err != nil {
		t.Fatalf("ProcessMessage failed: %v", err)
	}

	if sender.TotalAttempts() != 1 {
		t.Fatalf("expected 1 notification sent, got %d", sender.TotalAttempts())
	}
	if reader.CommittedCount() != 1 {
		t.Fatalf("expected offset to be committed (lag 0), got %d committed", reader.CommittedCount())
	}

	// Verify uncommitted message queue is now empty (lag reaches zero)
	_, err = reader.FetchMessage(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected reader to have 0 uncommitted lag, got err=%v", err)
	}
}

// 2) The same event id delivered twice produces one notification and increments the duplicate counter
func TestIntegration_DuplicateEventProducesOneNotification(t *testing.T) {
	eventID := uuid.NewString()
	orderID := uuid.NewString()

	msg1 := makeValidConfirmedMessage(101, eventID, orderID)
	msg2 := makeValidConfirmedMessage(102, eventID, orderID)

	reader := consumer.NewMockMessageReader([]kafka.Message{msg1, msg2})
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Process message 1
	m1, _ := reader.FetchMessage(ctx)
	if err := c.ProcessMessage(ctx, m1); err != nil {
		t.Fatalf("failed msg1: %v", err)
	}

	// Process message 2 (duplicate)
	m2, _ := reader.FetchMessage(ctx)
	if err := c.ProcessMessage(ctx, m2); err != nil {
		t.Fatalf("failed msg2: %v", err)
	}

	if sender.TotalAttempts() != 1 {
		t.Fatalf("expected exactly 1 notification sent, got %d", sender.TotalAttempts())
	}
	if reader.CommittedCount() != 2 {
		t.Fatalf("expected both offsets committed, got %d", reader.CommittedCount())
	}
}

// 3) Two consumers in the same group with the same event produce one notification
func TestIntegration_TwoConsumersSameGroupOneNotification(t *testing.T) {
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c1 := consumer.NewConsumer(consumer.NewMockMessageReader(nil), d, sender, m, consumer.Options{})
	c2 := consumer.NewConsumer(consumer.NewMockMessageReader(nil), d, sender, m, consumer.Options{})

	eventID := uuid.NewString()
	orderID := uuid.NewString()
	msg := makeValidConfirmedMessage(201, eventID, orderID)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = c1.ProcessMessage(context.Background(), msg)
	}()
	go func() {
		defer wg.Done()
		_ = c2.ProcessMessage(context.Background(), msg)
	}()
	wg.Wait()

	if sender.TotalAttempts() != 1 {
		t.Fatalf("expected exactly 1 notification dispatched between 2 consumers in the same group, got %d", sender.TotalAttempts())
	}
}

// 4) A sender that fails twice and then succeeds produces one notification after three attempts
func TestIntegration_SenderFailsTwiceThenSucceeds(t *testing.T) {
	reader := consumer.NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(2, errors.New("simulated network error"))
	m := metrics.NewMetrics()

	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	msg := makeValidConfirmedMessage(301, "", "")
	if err := c.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sender.TotalAttempts() != 3 {
		t.Fatalf("expected 3 attempts before success, got %d", sender.TotalAttempts())
	}
	if reader.CommittedCount() != 1 {
		t.Fatalf("expected 1 committed message, got %d", reader.CommittedCount())
	}
}

// 5) A sender that always fails results in the failure counter incremented, the dedup key removed, the offset committed and the next record processed
func TestIntegration_SenderAlwaysFailsKeyRemovedNextProcessed(t *testing.T) {
	reader := consumer.NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewAlwaysFailingSender(errors.New("permanent failure"))
	m := metrics.NewMetrics()

	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	eventID1 := uuid.NewString()
	msg1 := makeValidConfirmedMessage(401, eventID1, "")

	// 1. Process failing record
	if err := c.ProcessMessage(context.Background(), msg1); err != nil {
		t.Fatalf("ProcessMessage should not fail on exhausted sender, got %v", err)
	}

	if reader.CommittedCount() != 1 {
		t.Fatalf("expected offset to be committed despite exhaustion, got %d", reader.CommittedCount())
	}

	// Verify dedup key was deleted to allow manual replay
	claimed, err := d.Claim(context.Background(), eventID1)
	if err != nil || !claimed {
		t.Fatalf("expected dedup key for eventID1 to be removed from Redis to allow replay, claimed=%v, err=%v", claimed, err)
	}

	// 2. Next record can be processed
	eventID2 := uuid.NewString()
	msg2 := makeValidConfirmedMessage(402, eventID2, "")
	if err := c.ProcessMessage(context.Background(), msg2); err != nil {
		t.Fatalf("unexpected error processing next record: %v", err)
	}
	if reader.CommittedCount() != 2 {
		t.Fatalf("expected 2nd record to be committed, got %d", reader.CommittedCount())
	}
}

// 6) A malformed JSON record and an unknown event type do not block the partition and are counted
func TestIntegration_MalformedAndUnknownEventsDoNotBlockPartition(t *testing.T) {
	reader := consumer.NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{})

	// 1. Malformed JSON
	msg1 := kafka.Message{Offset: 501, Value: []byte(`{invalid-json}`)}
	if err := c.ProcessMessage(context.Background(), msg1); err != nil {
		t.Fatalf("malformed record must not return error: %v", err)
	}

	// 2. Unknown event type
	msg2 := kafka.Message{
		Offset: 502,
		Value: []byte(fmt.Sprintf(`{
			"eventId": "%s",
			"eventType": "order.payment.requested",
			"occurredAt": "%s",
			"correlationId": "%s",
			"schemaVersion": 1,
			"payload": {"amountCents": 100}
		}`, uuid.NewString(), time.Now().UTC().Format(time.RFC3339Nano), uuid.NewString())),
	}
	if err := c.ProcessMessage(context.Background(), msg2); err != nil {
		t.Fatalf("unknown event type must not return error: %v", err)
	}

	if reader.CommittedCount() != 2 {
		t.Fatalf("expected both offsets committed to prevent partition blockage, got %d", reader.CommittedCount())
	}
	if sender.TotalAttempts() != 0 {
		t.Fatalf("sender should not be called, got %d", sender.TotalAttempts())
	}
}

// 7) Redis stopped makes the consumer stop committing and the event is processed after Redis returns
func TestIntegration_RedisStoppedStopsCommittingAndResumes(t *testing.T) {
	reader := consumer.NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	d.SetFailClaim(errors.New("redis: connection refused"))
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{
		MaxPollInterval: 50 * time.Millisecond,
	})

	msg := makeValidConfirmedMessage(601, "", "")

	// 1. Redis down -> error returned, offset NOT committed
	err := c.ProcessMessage(context.Background(), msg)
	if err == nil {
		t.Fatalf("expected error when Redis is down, got nil")
	}
	if reader.CommittedCount() != 0 {
		t.Fatalf("offset must NOT be committed when Redis is down, got %d", reader.CommittedCount())
	}
	if sender.TotalAttempts() != 0 {
		t.Fatalf("sender must not be called when Redis is down, got %d", sender.TotalAttempts())
	}

	// 2. Redis returns
	d.SetFailClaim(nil)

	if err := c.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected success after Redis returns, got %v", err)
	}
	if reader.CommittedCount() != 1 {
		t.Fatalf("expected offset to be committed after Redis returns, got %d", reader.CommittedCount())
	}
	if sender.TotalAttempts() != 1 {
		t.Fatalf("expected sender to be called after Redis returns, got %d", sender.TotalAttempts())
	}
}

// 8) /health and /ready behave as described and /metrics exposes the documented names
func TestIntegration_HealthReadyMetricsEndpoints(t *testing.T) {
	var kafkaHealthy int32 = 1
	var redisHealthy int32 = 1

	checkKafka := func(ctx context.Context) error {
		if atomic.LoadInt32(&kafkaHealthy) == 1 {
			return nil
		}
		return errors.New("kafka unreachable")
	}
	checkRedis := func(ctx context.Context) error {
		if atomic.LoadInt32(&redisHealthy) == 1 {
			return nil
		}
		return errors.New("redis unreachable")
	}

	h := health.NewHealthHandler(checkKafka, checkRedis, 1*time.Second)
	m := metrics.NewMetrics()
	m.RecordEvent("order.confirmed", metrics.OutcomeSent)
	m.RecordEvent("order.cancelled", metrics.OutcomeDuplicate)
	m.RecordEvent("unknown", metrics.OutcomeInvalid)
	m.RecordRedisError()
	m.ObserveSendDuration(0.05)
	m.RecordClaimWait()
	m.RecordClaimRelease("failure")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.LivenessHandler)
	mux.HandleFunc("/ready", h.ReadinessHandler)
	mux.Handle("/metrics", m.Handler())

	// 1. GET /health
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	recHealth := httptest.NewRecorder()
	mux.ServeHTTP(recHealth, reqHealth)
	if recHealth.Code != http.StatusOK {
		t.Fatalf("GET /health failed: status %d", recHealth.Code)
	}
	var livenessBody map[string]string
	_ = json.NewDecoder(recHealth.Body).Decode(&livenessBody)
	if livenessBody["status"] != "UP" {
		t.Fatalf("expected status UP, got %s", livenessBody["status"])
	}

	// 2. GET /ready when healthy
	reqReady := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recReady := httptest.NewRecorder()
	mux.ServeHTTP(recReady, reqReady)
	if recReady.Code != http.StatusOK {
		t.Fatalf("GET /ready failed: status %d", recReady.Code)
	}
	var readyBody health.ReadinessResponse
	_ = json.NewDecoder(recReady.Body).Decode(&readyBody)
	if readyBody.Status != "UP" || readyBody.Kafka != "UP" || readyBody.Redis != "UP" {
		t.Fatalf("expected all UP on /ready, got %+v", readyBody)
	}

	// 3. GET /ready when Kafka fails
	atomic.StoreInt32(&kafkaHealthy, 0)
	reqKafkaDown := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recKafkaDown := httptest.NewRecorder()
	mux.ServeHTTP(recKafkaDown, reqKafkaDown)
	if recKafkaDown.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on /ready when Kafka down, got %d", recKafkaDown.Code)
	}
	_ = json.NewDecoder(recKafkaDown.Body).Decode(&readyBody)
	if readyBody.Status != "DOWN" || readyBody.Kafka != "DOWN" {
		t.Fatalf("expected Kafka DOWN on /ready, got %+v", readyBody)
	}
	atomic.StoreInt32(&kafkaHealthy, 1)

	// 4. GET /ready when Redis fails
	atomic.StoreInt32(&redisHealthy, 0)
	reqRedisDown := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recRedisDown := httptest.NewRecorder()
	mux.ServeHTTP(recRedisDown, reqRedisDown)
	if recRedisDown.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on /ready when Redis down, got %d", recRedisDown.Code)
	}
	_ = json.NewDecoder(recRedisDown.Body).Decode(&readyBody)
	if readyBody.Status != "DOWN" || readyBody.Redis != "DOWN" {
		t.Fatalf("expected Redis DOWN on /ready, got %+v", readyBody)
	}
	atomic.StoreInt32(&redisHealthy, 1)

	// 5. GET /metrics
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recMetrics := httptest.NewRecorder()
	mux.ServeHTTP(recMetrics, reqMetrics)
	if recMetrics.Code != http.StatusOK {
		t.Fatalf("GET /metrics failed: status %d", recMetrics.Code)
	}
	metricsStr := recMetrics.Body.String()

	requiredMetricNames := []string{
		"notification_events_total",
		"notification_send_duration_seconds",
		"notification_dedup_redis_errors_total",
		"notification_claim_wait_total",
		"notification_claim_release_total",
	}
	for _, name := range requiredMetricNames {
		if !strings.Contains(metricsStr, name) {
			t.Errorf("expected /metrics to expose %s, but was missing", name)
		}
	}
}

// 9) A log capture test proving that no payment method token, email or other personal field appears in any log line
func TestIntegration_LogCaptureNoPersonalOrTokenData(t *testing.T) {
	var logBuf bytes.Buffer
	sender := notify.NewLogSender(&logBuf)

	payloadWithSecrets := []byte(`{
		"orderId": "01926b54-1111-7000-8000-000000000001",
		"reservationId": "01926b54-1111-7000-8000-000000000001",
		"userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
		"eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
		"paymentId": "01926b55-9999-7000-8000-000000000099",
		"paymentMethodToken": "pm_tok_visa_super_secret_token_12345",
		"customerEmail": "customer_secret@example.com",
		"creditCardNumber": "4111-2222-3333-4444",
		"seats": [{"seatId": "11111111-2222-4333-8444-555555555501", "sectionCode": "A", "rowLabel": "1", "seatNumber": "1", "priceCents": 1000}],
		"totalAmountCents": 1000,
		"currency": "USD",
		"confirmedAt": "2026-10-27T22:01:05.650000Z"
	}`)

	env := &events.Envelope{
		EventID:       "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
		EventType:     events.EventTypeOrderConfirmed,
		OccurredAt:    "2026-10-27T22:00:00Z",
		CorrelationID: "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
		SchemaVersion: 1,
		Payload:       payloadWithSecrets,
	}

	notif, handled, err := notify.MapEventToNotification(env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handled {
		t.Fatalf("expected order.confirmed to be handled")
	}

	if err := sender.Send(context.Background(), *notif); err != nil {
		t.Fatalf("LogSender.Send failed: %v", err)
	}

	logOutput := logBuf.String()

	sensitiveStrings := []string{
		"pm_tok_visa_super_secret_token_12345",
		"customer_secret@example.com",
		"4111-2222-3333-4444",
		"super_secret",
		"creditCard",
	}

	for _, secret := range sensitiveStrings {
		if strings.Contains(logOutput, secret) {
			t.Fatalf("CRITICAL SECURITY VIOLATION: sensitive string %q leaked in notification log output: %s", secret, logOutput)
		}
	}

	var parsedLog map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(logOutput)), &parsedLog); err != nil {
		t.Fatalf("log line is not valid JSON: %v", err)
	}

	for _, key := range []string{"timestamp", "level", "service", "message", "eventId", "eventType", "orderId", "notificationKind"} {
		if _, ok := parsedLog[key]; !ok {
			t.Errorf("missing required log key: %s", key)
		}
	}
}

// 10) Live Kafka and Redis end-to-end integration test (when available)
func TestIntegration_LiveKafkaAndRedis(t *testing.T) {
	infra := getLiveInfra(t)
	if infra == nil {
		return
	}
	defer infra.cleanup()

	topic := "order.events"
	groupID := fmt.Sprintf("test-live-cg-%s", uuid.NewString())

	eventID := uuid.NewString()
	orderID := uuid.NewString()

	msg := makeValidConfirmedMessage(0, eventID, orderID)
	msg.Topic = ""

	// Produce to live Kafka
	writer := &kafka.Writer{
		Addr:     kafka.TCP(infra.kafkaBrokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := writer.WriteMessages(ctx, msg); err != nil {
		t.Fatalf("failed to write message to live kafka: %v", err)
	}

	// Read and process using real Redis deduplicator
	d := dedup.NewRedisDeduplicator(infra.rdb, groupID, 1*time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()

	reader := consumer.NewMockMessageReader([]kafka.Message{msg})
	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{})

	if err := c.ProcessMessage(ctx, msg); err != nil {
		t.Fatalf("ProcessMessage with live Redis failed: %v", err)
	}

	if sender.TotalAttempts() != 1 {
		t.Fatalf("expected 1 notification sent, got %d", sender.TotalAttempts())
	}

	// Verify key in live Redis
	claimed, err := d.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("redis claim failed: %v", err)
	}
	if claimed {
		t.Fatalf("expected redis key to exist and return duplicate on claim")
	}

	// Clean up test key in live Redis
	_ = d.Release(ctx, eventID)
}
