package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/ticketing-platform/notification-service/internal/consumer"
	"github.com/ticketing-platform/notification-service/internal/dedup"
	"github.com/ticketing-platform/notification-service/internal/health"
	"github.com/ticketing-platform/notification-service/internal/metrics"
	"github.com/ticketing-platform/notification-service/internal/notify"
)

var (
	sharedInfraOnce      sync.Once
	sharedInfraErr       error
	sharedKafkaContainer *tckafka.KafkaContainer
	sharedRedisContainer *tcredis.RedisContainer
	sharedKafkaBrokers   []string
	sharedRedisAddr      string
)

type customSender struct {
	fn func(ctx context.Context, n notify.Notification) error
}

func (s *customSender) Send(ctx context.Context, n notify.Notification) error {
	if s.fn != nil {
		return s.fn(ctx, n)
	}
	return nil
}

func requireDockerAndInfra(t *testing.T) ([]string, string, *redis.Client) {
	t.Helper()

	sharedInfraOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		// Verify Docker availability; must FAIL, not skip
		cli, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			sharedInfraErr = fmt.Errorf("Docker is required for real infrastructure integration tests: %w", err)
			return
		}
		if _, err := cli.Ping(ctx); err != nil {
			sharedInfraErr = fmt.Errorf("Docker daemon is not responsive for real infrastructure integration tests: %w", err)
			return
		}

		redisC, err := tcredis.Run(ctx, "redis:7.4.1-alpine")
		if err != nil {
			sharedInfraErr = fmt.Errorf("failed to start testcontainers redis: %w", err)
			return
		}
		sharedRedisContainer = redisC

		kafkaC, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.6.1", tckafka.WithClusterID("4L622nShTZaIOqiTmIGCyA"))
		if err != nil {
			sharedInfraErr = fmt.Errorf("failed to start testcontainers kafka: %w", err)
			return
		}
		sharedKafkaContainer = kafkaC

		brokers, err := kafkaC.Brokers(ctx)
		if err != nil {
			sharedInfraErr = fmt.Errorf("failed to get kafka brokers: %w", err)
			return
		}
		sharedKafkaBrokers = brokers

		redisConnStr, err := redisC.ConnectionString(ctx)
		if err != nil {
			sharedInfraErr = fmt.Errorf("failed to get redis connection string: %w", err)
			return
		}
		redisOpt, err := redis.ParseURL(redisConnStr)
		if err != nil {
			sharedInfraErr = fmt.Errorf("failed to parse redis url: %w", err)
			return
		}
		sharedRedisAddr = redisOpt.Addr
	})

	if sharedInfraErr != nil {
		t.Fatalf("Real infrastructure test failed to initialize: %v", sharedInfraErr)
	}

	rdb := redis.NewClient(&redis.Options{Addr: sharedRedisAddr})
	return sharedKafkaBrokers, sharedRedisAddr, rdb
}

func createRealTopic(ctx context.Context, t *testing.T, brokers []string, topic string, numPartitions int) {
	t.Helper()
	client := &kafka.Client{
		Addr: kafka.TCP(brokers...),
	}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     numPartitions,
				ReplicationFactor: 1,
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create topic %s: %v", topic, err)
	}
	if topicErr, ok := resp.Errors[topic]; ok && topicErr != nil {
		t.Fatalf("broker returned error creating topic %s: %v", topic, topicErr)
	}
}

func makeOrderConfirmedJSON(eventID, orderID string) []byte {
	return []byte(fmt.Sprintf(`{
		"eventId": "%s",
		"eventType": "order.confirmed",
		"occurredAt": "2026-10-27T22:00:00Z",
		"correlationId": "%s",
		"schemaVersion": 1,
		"payload": {
			"orderId": "%s",
			"reservationId": "%s",
			"userId": "%s",
			"eventId": "%s",
			"paymentId": "%s",
			"seats": [
				{"seatId": "%s", "sectionCode": "A", "rowLabel": "1", "seatNumber": "101", "priceCents": 10000}
			],
			"totalAmountCents": 10000,
			"currency": "USD",
			"confirmedAt": "2026-10-27T22:00:00Z"
		}
	}`, eventID, uuid.NewString(), orderID, orderID, uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()))
}

func waitForLagZero(ctx context.Context, t *testing.T, brokers []string, groupID, topic string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &kafka.Client{
		Addr: kafka.TCP(brokers...),
	}

	for time.Now().Before(deadline) {
		meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
			Topics: []string{topic},
		})
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var partitions []int
		for _, top := range meta.Topics {
			if top.Name == topic {
				for _, p := range top.Partitions {
					partitions = append(partitions, p.ID)
				}
			}
		}

		if len(partitions) == 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		offResp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
			GroupID: groupID,
			Topics: map[string][]int{
				topic: partitions,
			},
		})
		if err != nil || offResp.Error != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		partReq := make([]kafka.OffsetRequest, len(partitions))
		for i, p := range partitions {
			partReq[i] = kafka.LastOffsetOf(p)
		}

		listResp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
			Topics: map[string][]kafka.OffsetRequest{
				topic: partReq,
			},
		})
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		committedMap := make(map[int]int64)
		for _, cp := range offResp.Topics[topic] {
			committedMap[cp.Partition] = cp.CommittedOffset
		}

		allZero := true
		for _, lp := range listResp.Topics[topic] {
			latest := lp.LastOffset
			committed, ok := committedMap[lp.Partition]
			if !ok || committed < latest {
				allZero = false
				break
			}
		}

		if allZero && len(committedMap) > 0 {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("timeout waiting for consumer group %s lag to reach zero on topic %s", groupID, topic)
}

// 1. End to end delivery with offset commit verified by consumer group lag reaching zero on the broker
func TestRealInfra_EndToEndLagZero(t *testing.T) {
	brokers, _, rdb := requireDockerAndInfra(t)
	defer rdb.Close()

	topic := fmt.Sprintf("topic-lag-%s", uuid.NewString())
	groupID := fmt.Sprintf("cg-lag-%s", uuid.NewString())
	workerID := uuid.NewString()
	eventID := uuid.NewString()
	orderID := uuid.NewString()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	createRealTopic(ctx, t, brokers, topic, 1)

	// Produce 1 message
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	msg := kafka.Message{
		Key:   []byte(orderID),
		Value: makeOrderConfirmedJSON(eventID, orderID),
	}
	if err := writer.WriteMessages(ctx, msg); err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	// Real deduplicator and consumer
	m := metrics.NewMetrics()
	d := dedup.NewRedisDeduplicator(rdb, groupID, 1*time.Hour,
		dedup.WithRedisWorkerID(workerID),
		dedup.WithRedisClaimTTL(60*time.Second),
		dedup.WithRedisMetrics(m),
	)

	delivered := make(chan struct{}, 1)
	sender := &customSender{fn: func(ctx context.Context, n notify.Notification) error {
		select {
		case delivered <- struct{}{}:
		default:
		}
		return nil
	}}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0,
	})
	defer reader.Close()

	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{})

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- c.Run(consumerCtx)
	}()

	select {
	case <-delivered:
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for notification delivery")
	}

	// Verify consumer group lag on broker reaches zero (which occurs after promotion to SENT and offset commit)
	waitForLagZero(ctx, t, brokers, groupID, topic, 20*time.Second)

	// Verify key in Redis is promoted to "SENT"
	key := fmt.Sprintf("notif:dedup:%s:%s", groupID, eventID)
	deadline := time.Now().Add(5 * time.Second)
	var val string
	for time.Now().Before(deadline) {
		res, err := rdb.Get(ctx, key).Result()
		if err == nil && res == "SENT" {
			val = res
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if val != "SENT" {
		t.Fatalf("expected key value 'SENT', got %q", val)
	}

	consumerCancel()
	<-consumerDone
}

// 2. Two consumer instances in one group with distinct worker ids over a topic with at least 3 partitions and 30 distinct events deliver each event exactly once in total
func TestRealInfra_TwoConsumersThreePartitionsThirtyEvents(t *testing.T) {
	brokers, _, rdb := requireDockerAndInfra(t)
	defer rdb.Close()

	topic := fmt.Sprintf("topic-3p-%s", uuid.NewString())
	groupID := fmt.Sprintf("cg-3p-%s", uuid.NewString())

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	createRealTopic(ctx, t, brokers, topic, 3)

	const totalEvents = 30
	var deliveredIDs sync.Map
	var deliveredCount int64

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	for i := 0; i < totalEvents; i++ {
		eventID := uuid.NewString()
		orderID := uuid.NewString()
		msg := kafka.Message{
			Key:   []byte(orderID),
			Value: makeOrderConfirmedJSON(eventID, orderID),
		}
		if err := writer.WriteMessages(ctx, msg); err != nil {
			t.Fatalf("failed to produce message %d: %v", i, err)
		}
	}

	allDelivered := make(chan struct{})
	onSend := func(n notify.Notification) {
		if _, loaded := deliveredIDs.LoadOrStore(n.EventID, true); loaded {
			t.Errorf("duplicate delivery for event %s!", n.EventID)
		}
		if atomic.AddInt64(&deliveredCount, 1) == totalEvents {
			close(allDelivered)
		}
	}

	startConsumer := func(workerID string) (*consumer.Consumer, *kafka.Reader, context.CancelFunc, chan error) {
		m := metrics.NewMetrics()
		d := dedup.NewRedisDeduplicator(rdb, groupID, 1*time.Hour,
			dedup.WithRedisWorkerID(workerID),
			dedup.WithRedisClaimTTL(60*time.Second),
			dedup.WithRedisMetrics(m),
		)
		s := &customSender{fn: func(ctx context.Context, n notify.Notification) error {
			onSend(n)
			return nil
		}}
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			GroupID:        groupID,
			Topic:          topic,
			MinBytes:       1,
			MaxBytes:       10e6,
			MaxWait:        200 * time.Millisecond,
			CommitInterval: 0,
		})
		c := consumer.NewConsumer(r, d, s, m, consumer.Options{})
		cCtx, cCancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- c.Run(cCtx)
		}()
		return c, r, cCancel, done
	}

	_, r1, cancel1, done1 := startConsumer("worker-1")
	defer r1.Close()
	defer cancel1()

	_, r2, cancel2, done2 := startConsumer("worker-2")
	defer r2.Close()
	defer cancel2()

	select {
	case <-allDelivered:
	case <-time.After(25 * time.Second):
		t.Fatalf("timed out waiting for 30 distinct events; delivered: %d", atomic.LoadInt64(&deliveredCount))
	}

	if atomic.LoadInt64(&deliveredCount) != totalEvents {
		t.Fatalf("expected %d events, got %d", totalEvents, atomic.LoadInt64(&deliveredCount))
	}

	waitForLagZero(ctx, t, brokers, groupID, topic, 20*time.Second)

	cancel1()
	cancel2()
	<-done1
	<-done2
}

// 3. The F-01 regression: a first consumer is cancelled in the middle of a slow send (sender blocked on a latch),
// a second consumer then delivers that event exactly once and the Redis key ends as SENT
func TestRealInfra_F01Regression_CancelledConsumerSecondDelivers(t *testing.T) {
	brokers, _, rdb := requireDockerAndInfra(t)
	defer rdb.Close()

	topic := fmt.Sprintf("topic-f01-%s", uuid.NewString())
	groupID := fmt.Sprintf("cg-f01-%s", uuid.NewString())
	eventID := uuid.NewString()
	orderID := uuid.NewString()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createRealTopic(ctx, t, brokers, topic, 1)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(orderID),
		Value: makeOrderConfirmedJSON(eventID, orderID),
	}); err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	// Consumer 1: blocks on latch
	inSender1 := make(chan struct{})
	blockSender1 := make(chan struct{})

	var inSenderOnce sync.Once
	sender1 := &customSender{fn: func(ctx context.Context, n notify.Notification) error {
		inSenderOnce.Do(func() {
			close(inSender1)
		})
		<-blockSender1
		return ctx.Err()
	}}

	m1 := metrics.NewMetrics()
	d1 := dedup.NewRedisDeduplicator(rdb, groupID, 1*time.Hour,
		dedup.WithRedisWorkerID("worker-1"),
		dedup.WithRedisClaimTTL(60*time.Second),
		dedup.WithRedisMetrics(m1),
	)

	r1 := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0,
	})

	c1 := consumer.NewConsumer(r1, d1, sender1, m1, consumer.Options{})
	c1Ctx, c1Cancel := context.WithCancel(ctx)

	c1Done := make(chan error, 1)
	go func() {
		c1Done <- c1.Run(c1Ctx)
	}()

	select {
	case <-inSender1:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for consumer 1 to enter sender")
	}

	// Cancel consumer 1 mid-send and unblock sender
	c1.StopFetching()
	c1Cancel()
	close(blockSender1)
	<-c1Done
	_ = r1.Close()

	// Consumer 2: should pick up uncommitted message, claim key, send, promote to SENT
	delivered2 := make(chan struct{}, 1)
	sender2 := &customSender{fn: func(ctx context.Context, n notify.Notification) error {
		delivered2 <- struct{}{}
		return nil
	}}

	m2 := metrics.NewMetrics()
	d2 := dedup.NewRedisDeduplicator(rdb, groupID, 1*time.Hour,
		dedup.WithRedisWorkerID("worker-2"),
		dedup.WithRedisClaimTTL(60*time.Second),
		dedup.WithRedisMetrics(m2),
	)

	r2 := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0,
	})
	defer r2.Close()

	c2 := consumer.NewConsumer(r2, d2, sender2, m2, consumer.Options{})
	c2Ctx, c2Cancel := context.WithCancel(ctx)
	defer c2Cancel()

	c2Done := make(chan error, 1)
	go func() {
		c2Done <- c2.Run(c2Ctx)
	}()

	select {
	case <-delivered2:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for consumer 2 to deliver message")
	}

	waitForLagZero(ctx, t, brokers, groupID, topic, 20*time.Second)

	key := fmt.Sprintf("notif:dedup:%s:%s", groupID, eventID)
	deadline := time.Now().Add(5 * time.Second)
	var val string
	for time.Now().Before(deadline) {
		res, err := rdb.Get(ctx, key).Result()
		if err == nil && res == "SENT" {
			val = res
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if val != "SENT" {
		t.Fatalf("expected key value 'SENT', got %q", val)
	}

	c2Cancel()
	<-c2Done
}

// 4. A claim left IN_PROGRESS by a dead worker (create the key directly with another worker id and a 2 second TTL)
// is taken over by a live worker once it expires and the event is delivered
func TestRealInfra_ExpiredInProgressClaimTakenOverByLiveWorker(t *testing.T) {
	brokers, _, rdb := requireDockerAndInfra(t)
	defer rdb.Close()

	topic := fmt.Sprintf("topic-exp-%s", uuid.NewString())
	groupID := fmt.Sprintf("cg-exp-%s", uuid.NewString())
	eventID := uuid.NewString()
	orderID := uuid.NewString()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	createRealTopic(ctx, t, brokers, topic, 1)

	// Pre-create the key in Redis with 2-second TTL owned by a "dead worker"
	key := fmt.Sprintf("notif:dedup:%s:%s", groupID, eventID)
	if err := rdb.Set(ctx, key, "dead-worker-id:IN_PROGRESS", 2*time.Second).Err(); err != nil {
		t.Fatalf("failed to create dead-worker key: %v", err)
	}

	// Produce 1 message
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(orderID),
		Value: makeOrderConfirmedJSON(eventID, orderID),
	}); err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	delivered := make(chan struct{}, 1)
	sender := &customSender{fn: func(ctx context.Context, n notify.Notification) error {
		delivered <- struct{}{}
		return nil
	}}

	m := metrics.NewMetrics()
	d := dedup.NewRedisDeduplicator(rdb, groupID, 1*time.Hour,
		dedup.WithRedisWorkerID("live-worker"),
		dedup.WithRedisClaimTTL(60*time.Second),
		dedup.WithRedisMetrics(m),
	)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0,
	})
	defer r.Close()

	c := consumer.NewConsumer(r, d, sender, m, consumer.Options{})
	cCtx, cCancel := context.WithCancel(ctx)
	defer cCancel()

	cDone := make(chan error, 1)
	go func() {
		cDone <- c.Run(cCtx)
	}()

	select {
	case <-delivered:
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for live worker to deliver event after claim expired")
	}

	waitForLagZero(ctx, t, brokers, groupID, topic, 20*time.Second)

	deadline := time.Now().Add(5 * time.Second)
	var val string
	for time.Now().Before(deadline) {
		res, err := rdb.Get(ctx, key).Result()
		if err == nil && res == "SENT" {
			val = res
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if val != "SENT" {
		t.Fatalf("expected key value 'SENT', got %q", val)
	}

	cCancel()
	<-cDone
}

// 5. A stale worker whose claim expired and was taken by another worker can neither promote nor delete
// the new owner's claim (call the promote and release scripts through the real client and assert on the key value)
func TestRealInfra_StaleWorkerCannotPromoteOrDeleteNewOwnerClaim(t *testing.T) {
	_, _, rdb := requireDockerAndInfra(t)
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := fmt.Sprintf("notif:dedup:test-cg:%s", uuid.NewString())

	// New owner holds claim
	if err := rdb.Set(ctx, key, "new-owner-worker:IN_PROGRESS", 60*time.Second).Err(); err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	// Stale worker (stale-worker) tries to promote via Lua CAS
	promoteScript := `
local val = redis.call('GET', KEYS[1])
if val == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
    return 1
else
    return 0
end`
	resPromote, err := rdb.Eval(ctx, promoteScript, []string{key}, "stale-worker:IN_PROGRESS", "SENT", 604800).Int()
	if err != nil {
		t.Fatalf("eval promoteScript failed: %v", err)
	}
	if resPromote != 0 {
		t.Fatalf("expected promoteScript to return 0 for stale worker, got %d", resPromote)
	}

	// Assert key value is still new owner's IN_PROGRESS
	val, err := rdb.Get(ctx, key).Result()
	if err != nil || val != "new-owner-worker:IN_PROGRESS" {
		t.Fatalf("key was modified by stale promote! val=%q, err=%v", val, err)
	}

	// Stale worker tries to release via Lua CAD
	releaseScript := `
local val = redis.call('GET', KEYS[1])
if val == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end`
	resRelease, err := rdb.Eval(ctx, releaseScript, []string{key}, "stale-worker:IN_PROGRESS").Int()
	if err != nil {
		t.Fatalf("eval releaseScript failed: %v", err)
	}
	if resRelease != 0 {
		t.Fatalf("expected releaseScript to return 0 for stale worker, got %d", resRelease)
	}

	// Assert key value is STILL new owner's IN_PROGRESS
	val, err = rdb.Get(ctx, key).Result()
	if err != nil || val != "new-owner-worker:IN_PROGRESS" {
		t.Fatalf("key was deleted by stale release! val=%q, err=%v", val, err)
	}

	// True owner promotes successfully
	resOwner, err := rdb.Eval(ctx, promoteScript, []string{key}, "new-owner-worker:IN_PROGRESS", "SENT", 604800).Int()
	if err != nil {
		t.Fatalf("owner promote failed: %v", err)
	}
	if resOwner != 1 {
		t.Fatalf("expected owner promote to succeed with 1, got %d", resOwner)
	}

	val, err = rdb.Get(ctx, key).Result()
	if err != nil || val != "SENT" {
		t.Fatalf("expected key value 'SENT', got %q", val)
	}
}

// 6. A sender that panics once does not kill the process, the record is counted as invalid and the next record is delivered
func TestRealInfra_SenderPanicContinuesAndCountsInvalid(t *testing.T) {
	brokers, _, rdb := requireDockerAndInfra(t)
	defer rdb.Close()

	topic := fmt.Sprintf("topic-panic-%s", uuid.NewString())
	groupID := fmt.Sprintf("cg-panic-%s", uuid.NewString())
	eventID1 := uuid.NewString()
	eventID2 := uuid.NewString()
	orderID := uuid.NewString()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	createRealTopic(ctx, t, brokers, topic, 1)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	// Write message 1 (will panic), then message 2 (will succeed)
	if err := writer.WriteMessages(ctx,
		kafka.Message{Key: []byte(orderID), Value: makeOrderConfirmedJSON(eventID1, orderID)},
		kafka.Message{Key: []byte(orderID), Value: makeOrderConfirmedJSON(eventID2, orderID)},
	); err != nil {
		t.Fatalf("failed to write messages: %v", err)
	}

	var panickedOnce atomic.Bool
	deliveredMsg2 := make(chan struct{}, 1)

	sender := &customSender{fn: func(ctx context.Context, n notify.Notification) error {
		if n.EventID == eventID1 {
			panickedOnce.Store(true)
			panic("simulated panic in notification sender")
		}
		if n.EventID == eventID2 {
			deliveredMsg2 <- struct{}{}
		}
		return nil
	}}

	m := metrics.NewMetrics()
	d := dedup.NewRedisDeduplicator(rdb, groupID, 1*time.Hour,
		dedup.WithRedisWorkerID("worker-panic-test"),
		dedup.WithRedisClaimTTL(60*time.Second),
		dedup.WithRedisMetrics(m),
	)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0,
	})
	defer r.Close()

	c := consumer.NewConsumer(r, d, sender, m, consumer.Options{})
	cCtx, cCancel := context.WithCancel(ctx)
	defer cCancel()

	cDone := make(chan error, 1)
	go func() {
		cDone <- c.Run(cCtx)
	}()

	select {
	case <-deliveredMsg2:
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for message 2 to be delivered after message 1 panicked")
	}

	if !panickedOnce.Load() {
		t.Fatalf("expected message 1 to have panicked")
	}

	waitForLagZero(ctx, t, brokers, groupID, topic, 20*time.Second)

	cCancel()
	<-cDone
}

// Reader wrapper for test 7
type readerWrapperWithFailures struct {
	*kafka.Reader
	fetchCount  int64
	commitCount int64
}

func (w *readerWrapperWithFailures) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if atomic.AddInt64(&w.fetchCount, 1) == 1 {
		return kafka.Message{}, errors.New("transient network error on fetch")
	}
	return w.Reader.FetchMessage(ctx)
}

func (w *readerWrapperWithFailures) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if atomic.AddInt64(&w.commitCount, 1) <= 2 {
		return errors.New("transient commit failure")
	}
	return w.Reader.CommitMessages(ctx, msgs...)
}

// 7. A thin wrapper around the real reader (used only in this test) that returns a transient error on the first FetchMessage
// and fails the first two commits does not stop the loop and the event is delivered once
func TestRealInfra_TransientFetchAndCommitErrorsDoNotStopLoop(t *testing.T) {
	brokers, _, rdb := requireDockerAndInfra(t)
	defer rdb.Close()

	topic := fmt.Sprintf("topic-transient-%s", uuid.NewString())
	groupID := fmt.Sprintf("cg-transient-%s", uuid.NewString())
	eventID := uuid.NewString()
	orderID := uuid.NewString()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	createRealTopic(ctx, t, brokers, topic, 1)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(orderID),
		Value: makeOrderConfirmedJSON(eventID, orderID),
	}); err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	realReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0,
	})
	defer realReader.Close()

	wrappedReader := &readerWrapperWithFailures{Reader: realReader}

	delivered := make(chan struct{}, 1)
	sender := &customSender{fn: func(ctx context.Context, n notify.Notification) error {
		delivered <- struct{}{}
		return nil
	}}

	m := metrics.NewMetrics()
	d := dedup.NewRedisDeduplicator(rdb, groupID, 1*time.Hour,
		dedup.WithRedisWorkerID("worker-transient"),
		dedup.WithRedisClaimTTL(60*time.Second),
		dedup.WithRedisMetrics(m),
	)

	c := consumer.NewConsumer(wrappedReader, d, sender, m, consumer.Options{})
	cCtx, cCancel := context.WithCancel(ctx)
	defer cCancel()

	cDone := make(chan error, 1)
	go func() {
		cDone <- c.Run(cCtx)
	}()

	select {
	case <-delivered:
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for event delivery with transient fetch/commit errors")
	}

	waitForLagZero(ctx, t, brokers, groupID, topic, 20*time.Second)

	if atomic.LoadInt64(&wrappedReader.fetchCount) < 2 {
		t.Fatalf("expected at least 2 fetch calls, got %d", atomic.LoadInt64(&wrappedReader.fetchCount))
	}
	if atomic.LoadInt64(&wrappedReader.commitCount) < 3 {
		t.Fatalf("expected at least 3 commit attempts (2 failed + 1 successful), got %d", atomic.LoadInt64(&wrappedReader.commitCount))
	}

	cCancel()
	<-cDone
}

// 8. Cancellation during a 500 ms sender completes the in-flight notification, commits its offset (lag zero)
// and only then does Run return, and the reader and the Redis client are closed strictly after Run returned
func TestRealInfra_CancellationDuringSendCompletesAndClosesInOrder(t *testing.T) {
	brokers, _, rdb := requireDockerAndInfra(t)

	topic := fmt.Sprintf("topic-order-%s", uuid.NewString())
	groupID := fmt.Sprintf("cg-order-%s", uuid.NewString())
	eventID := uuid.NewString()
	orderID := uuid.NewString()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	createRealTopic(ctx, t, brokers, topic, 1)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(orderID),
		Value: makeOrderConfirmedJSON(eventID, orderID),
	}); err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	sendStarted := make(chan struct{})
	sender := &customSender{fn: func(ctx context.Context, n notify.Notification) error {
		close(sendStarted)
		time.Sleep(500 * time.Millisecond)
		return nil
	}}

	m := metrics.NewMetrics()
	d := dedup.NewRedisDeduplicator(rdb, groupID, 1*time.Hour,
		dedup.WithRedisWorkerID("worker-close-order"),
		dedup.WithRedisClaimTTL(60*time.Second),
		dedup.WithRedisMetrics(m),
	)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0,
	})

	c := consumer.NewConsumer(r, d, sender, m, consumer.Options{})

	var eventsMu sync.Mutex
	var orderEvents []string
	record := func(name string) {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		orderEvents = append(orderEvents, name)
	}

	cCtx, cCancel := context.WithCancel(ctx)
	defer cCancel()

	cDone := make(chan error, 1)
	go func() {
		err := c.Run(cCtx)
		record("RunReturned")
		cDone <- err
	}()

	select {
	case <-sendStarted:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for sender to start")
	}

	// Signal graceful shutdown during send: stop fetching new messages
	c.StopFetching()

	// Wait for Run to return naturally after completing in-flight message
	select {
	case err := <-cDone:
		if err != nil {
			t.Fatalf("unexpected error from Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for Run to finish in-flight message")
	}

	// Strictly after Run returned, close reader and rdb
	_ = r.Close()
	record("ReaderClosed")

	_ = rdb.Close()
	record("RedisClosed")

	eventsMu.Lock()
	defer eventsMu.Unlock()

	if len(orderEvents) != 3 {
		t.Fatalf("expected 3 lifecycle events, got %v", orderEvents)
	}
	if orderEvents[0] != "RunReturned" || orderEvents[1] != "ReaderClosed" || orderEvents[2] != "RedisClosed" {
		t.Fatalf("unexpected lifecycle order: got %v; expected [RunReturned, ReaderClosed, RedisClosed]", orderEvents)
	}

	// Offset was committed during in-flight completion: verify lag reached zero
	waitForLagZero(ctx, t, brokers, groupID, topic, 20*time.Second)
}

func getFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// 9. Redis stopped and restarted (container stop and start) during processing delays the event but neither loses nor duplicates it
func TestRealInfra_RedisRestartDelaysWithoutLossOrDuplicate(t *testing.T) {
	brokers, _, _ := requireDockerAndInfra(t)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	freePort, err := getFreePort()
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}

	// Dedicated Redis container with fixed host port so stop/start re-binds to identical host port
	redisC, err := tcredis.Run(ctx, "redis:7.4.1-alpine",
		testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
			req.ExposedPorts = []string{fmt.Sprintf("%d:6379/tcp", freePort)}
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("failed to start dedicated redis container: %v", err)
	}
	defer redisC.Terminate(ctx)

	rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", freePort)})
	defer rdb.Close()

	topic := fmt.Sprintf("topic-restart-%s", uuid.NewString())
	groupID := fmt.Sprintf("cg-restart-%s", uuid.NewString())
	eventID := uuid.NewString()
	orderID := uuid.NewString()

	createRealTopic(ctx, t, brokers, topic, 1)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(orderID),
		Value: makeOrderConfirmedJSON(eventID, orderID),
	}); err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	var sendCount int64
	delivered := make(chan struct{}, 1)
	sender := &customSender{fn: func(ctx context.Context, n notify.Notification) error {
		if atomic.AddInt64(&sendCount, 1) == 1 {
			delivered <- struct{}{}
		}
		return nil
	}}

	m := metrics.NewMetrics()
	d := dedup.NewRedisDeduplicator(rdb, groupID, 1*time.Hour,
		dedup.WithRedisWorkerID("worker-restart"),
		dedup.WithRedisClaimTTL(60*time.Second),
		dedup.WithRedisMetrics(m),
	)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0,
	})
	defer r.Close()

	c := consumer.NewConsumer(r, d, sender, m, consumer.Options{
		MaxPollInterval: 30 * time.Second,
	})

	// Stop Redis container before starting consumer loop
	stopTimeout := 5 * time.Second
	if err := redisC.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("failed to stop redis container: %v", err)
	}

	cCtx, cCancel := context.WithCancel(ctx)
	defer cCancel()

	cDone := make(chan error, 1)
	go func() {
		cDone <- c.Run(cCtx)
	}()

	// Wait 2 seconds while consumer retries Redis connection
	time.Sleep(2 * time.Second)

	// Restart Redis container
	if err := redisC.Start(ctx); err != nil {
		t.Fatalf("failed to restart redis container: %v", err)
	}

	// Verify event is delivered once Redis recovers
	select {
	case <-delivered:
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for event delivery after Redis restart")
	}

	if atomic.LoadInt64(&sendCount) != 1 {
		t.Fatalf("expected exactly 1 delivery, got %d", atomic.LoadInt64(&sendCount))
	}

	waitForLagZero(ctx, t, brokers, groupID, topic, 20*time.Second)

	cCancel()
	<-cDone
}

// 10. A Redis container started with requirepass works with REDIS_PASSWORD and fails readiness without it
func TestRealInfra_RedisRequirePassValidationAndReadiness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	redisPassword := "secretRedisPass123"

	// Dedicated Redis container started with requirepass
	redisC, err := tcredis.Run(ctx, "redis:7.4.1-alpine",
		testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
			req.Cmd = []string{"redis-server", "--requirepass", redisPassword}
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("failed to start requirepass redis container: %v", err)
	}
	defer redisC.Terminate(ctx)

	endpoint, err := redisC.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %v", err)
	}

	// Dummy Kafka checker that succeeds
	alwaysUpKafka := func(ctx context.Context) error { return nil }

	// 1. Client WITHOUT password: Ping fails with NOAUTH
	rdbNoAuth := redis.NewClient(&redis.Options{Addr: endpoint})
	defer rdbNoAuth.Close()

	checkRedisNoAuth := func(ctx context.Context) error {
		return rdbNoAuth.Ping(ctx).Err()
	}

	hNoAuth := health.NewHealthHandler(alwaysUpKafka, checkRedisNoAuth, 2*time.Second)
	req := httptest.NewRequest("GET", "/ready", nil)
	rec := httptest.NewRecorder()
	hNoAuth.ReadinessHandler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503 DOWN when Redis password is missing, got %d", rec.Code)
	}

	// 2. Client WITH correct REDIS_PASSWORD: Ping succeeds
	rdbAuth := redis.NewClient(&redis.Options{
		Addr:     endpoint,
		Password: redisPassword,
	})
	defer rdbAuth.Close()

	checkRedisAuth := func(ctx context.Context) error {
		return rdbAuth.Ping(ctx).Err()
	}

	hAuth := health.NewHealthHandler(alwaysUpKafka, checkRedisAuth, 2*time.Second)
	req2 := httptest.NewRequest("GET", "/ready", nil)
	rec2 := httptest.NewRecorder()
	hAuth.ReadinessHandler(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 UP when Redis password is provided, got %d", rec2.Code)
	}

	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &body); err != nil || body.Status != "UP" {
		t.Fatalf("expected status UP, got body: %s", rec2.Body.String())
	}
}

// 11. /metrics shows order.payment.requested with its real event_type label and the new claim counters
func TestRealInfra_MetricsAllowlistAndClaimCounters(t *testing.T) {
	m := metrics.NewMetrics()

	// Record canonical unhandled event type
	m.RecordEvent("order.payment.requested", metrics.OutcomeIgnored)

	// Record claim wait and release counters
	m.RecordClaimWait()
	m.RecordClaimRelease(metrics.ReleaseReasonFailure)
	m.RecordClaimRelease(metrics.ReleaseReasonCancelled)
	m.RecordClaimRelease(metrics.ReleaseReasonPanic)
	m.RecordClaimRelease(metrics.ReleaseReasonCommitError)

	server := httptest.NewServer(m.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("failed to scrape metrics endpoint: %v", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read metrics body: %v", err)
	}
	body := string(bodyBytes)

	// Verify order.payment.requested is displayed with its real event_type label, not 'unknown'
	expectedMetric := `notification_events_total{event_type="order.payment.requested",outcome="ignored",service="notification-service"} 1`
	if !strings.Contains(body, expectedMetric) {
		t.Fatalf("expected metrics to contain %q, got:\n%s", expectedMetric, body)
	}

	// Verify claim wait counter
	if !strings.Contains(body, `notification_claim_wait_total{service="notification-service"} 1`) {
		t.Fatalf("expected metrics to contain notification_claim_wait_total, got:\n%s", body)
	}

	// Verify all claim release reasons
	expectedReasons := []string{
		`notification_claim_release_total{reason="failure",service="notification-service"} 1`,
		`notification_claim_release_total{reason="cancelled",service="notification-service"} 1`,
		`notification_claim_release_total{reason="panic",service="notification-service"} 1`,
		`notification_claim_release_total{reason="commit_error",service="notification-service"} 1`,
	}
	for _, expected := range expectedReasons {
		if !strings.Contains(body, expected) {
			t.Fatalf("expected metrics to contain %q, got:\n%s", expected, body)
		}
	}
}
