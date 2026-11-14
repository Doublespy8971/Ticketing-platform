package dedup

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/ticketing-platform/notification-service/internal/metrics"
)

const (
	StatusInProgress = "IN_PROGRESS"
	StatusSent       = "SENT"

	DefaultClaimTTL = 60 * time.Second
	DefaultSentTTL  = 7 * 24 * time.Hour
)

// Lua script for Compare-And-Set promotion to SENT with sentTTL.
// KEYS[1]: dedup key
// ARGV[1]: expected token (<workerId>:IN_PROGRESS)
// ARGV[2]: new value ("SENT")
// ARGV[3]: TTL in seconds
var promoteLuaScript = redis.NewScript(`
local val = redis.call('GET', KEYS[1])
if val == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
    return 1
else
    return 0
end
`)

// Lua script for Compare-And-Delete release of in-progress claim.
// KEYS[1]: dedup key
// ARGV[1]: expected token (<workerId>:IN_PROGRESS)
var releaseLuaScript = redis.NewScript(`
local val = redis.call('GET', KEYS[1])
if val == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
`)

type Deduplicator interface {
	Claim(ctx context.Context, eventID string) (claimed bool, err error)
	Promote(ctx context.Context, eventID string) error
	Release(ctx context.Context, eventID string, reasons ...string) error
	Key(eventID string) string
}

type RedisDedupOption func(*RedisDeduplicator)

func WithRedisWorkerID(workerID string) RedisDedupOption {
	return func(r *RedisDeduplicator) {
		if workerID != "" {
			r.workerID = workerID
		}
	}
}

func WithRedisClaimTTL(claimTTL time.Duration) RedisDedupOption {
	return func(r *RedisDeduplicator) {
		if claimTTL > 0 {
			r.claimTTL = claimTTL
		}
	}
}

func WithRedisMetrics(m *metrics.Metrics) RedisDedupOption {
	return func(r *RedisDeduplicator) {
		r.metrics = m
	}
}

type RedisDeduplicator struct {
	client        redis.Cmdable
	consumerGroup string
	workerID      string
	claimTTL      time.Duration
	sentTTL       time.Duration
	metrics       *metrics.Metrics
}

func NewRedisDeduplicator(client redis.Cmdable, consumerGroup string, sentTTL time.Duration, opts ...RedisDedupOption) *RedisDeduplicator {
	if sentTTL <= 0 {
		sentTTL = DefaultSentTTL
	}
	r := &RedisDeduplicator{
		client:        client,
		consumerGroup: consumerGroup,
		workerID:      uuid.NewString(),
		claimTTL:      DefaultClaimTTL,
		sentTTL:       sentTTL,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (d *RedisDeduplicator) Key(eventID string) string {
	return fmt.Sprintf("notif:dedup:%s:%s", d.consumerGroup, eventID)
}

func (d *RedisDeduplicator) token() string {
	return fmt.Sprintf("%s:%s", d.workerID, StatusInProgress)
}

func (d *RedisDeduplicator) Claim(ctx context.Context, eventID string) (bool, error) {
	key := d.Key(eventID)
	myToken := d.token()

	// 1. Initial SET key <workerId>:IN_PROGRESS NX EX claimTTL
	res, err := d.client.SetArgs(ctx, key, myToken, redis.SetArgs{
		Mode: "NX",
		TTL:  d.claimTTL,
	}).Result()

	if err == nil && res == "OK" {
		return true, nil
	}
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("redis dedup claim error: %w", err)
	}

	// 2. Claim failed -> GET current value
	val, err := d.client.Get(ctx, key).Result()
	if err == redis.Nil {
		// Key expired between SetArgs and Get, retry claim once
		res, err := d.client.SetArgs(ctx, key, myToken, redis.SetArgs{
			Mode: "NX",
			TTL:  d.claimTTL,
		}).Result()
		if err == nil && res == "OK" {
			return true, nil
		}
		if err != nil && err != redis.Nil {
			return false, fmt.Errorf("redis dedup claim retry error: %w", err)
		}
	} else if err != nil {
		return false, fmt.Errorf("redis dedup get error: %w", err)
	}

	if val == StatusSent || val == "1" {
		// Already sent -> duplicate
		return false, nil
	}

	// 3. Owned by another worker in IN_PROGRESS: poll every 500ms up to claimTTL + 5s
	deadline := time.Now().Add(d.claimTTL + 5*time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-ticker.C:
		}

		if time.Now().After(deadline) {
			break
		}

		if d.metrics != nil {
			d.metrics.RecordClaimWait()
		}

		val, err := d.client.Get(ctx, key).Result()
		if err == redis.Nil {
			// Disappeared! Retry claim
			res, err := d.client.SetArgs(ctx, key, myToken, redis.SetArgs{
				Mode: "NX",
				TTL:  d.claimTTL,
			}).Result()
			if err == nil && res == "OK" {
				return true, nil
			}
			if err != nil && err != redis.Nil {
				return false, fmt.Errorf("redis dedup poll set error: %w", err)
			}
		} else if err != nil {
			return false, fmt.Errorf("redis dedup poll get error: %w", err)
		} else if val == StatusSent || val == "1" {
			// Became SENT
			return false, nil
		}
	}

	return false, fmt.Errorf("timeout waiting for in-progress claim on event %s to resolve", eventID)
}

func (d *RedisDeduplicator) Promote(ctx context.Context, eventID string) error {
	key := d.Key(eventID)
	myToken := d.token()
	ttlSec := int64(d.sentTTL.Seconds())
	if ttlSec <= 0 {
		ttlSec = int64(DefaultSentTTL.Seconds())
	}

	res, err := promoteLuaScript.Run(ctx, d.client, []string{key}, myToken, StatusSent, ttlSec).Result()
	if err != nil {
		return fmt.Errorf("redis dedup promote script error: %w", err)
	}

	intRes, _ := res.(int64)
	if intRes == 0 {
		return fmt.Errorf("redis dedup promote CAS failed: token mismatch or key expired for event %s", eventID)
	}
	return nil
}

func (d *RedisDeduplicator) Release(ctx context.Context, eventID string, reasons ...string) error {
	key := d.Key(eventID)
	myToken := d.token()

	reason := "failure"
	if len(reasons) > 0 && reasons[0] != "" {
		reason = reasons[0]
	}
	if d.metrics != nil {
		d.metrics.RecordClaimRelease(reason)
	}

	res, err := releaseLuaScript.Run(ctx, d.client, []string{key}, myToken).Result()
	if err != nil {
		return fmt.Errorf("redis dedup release script error: %w", err)
	}

	intRes, _ := res.(int64)
	if intRes == 0 {
		// Key was either already released, expired, or claimed by someone else; safe no-op
		return nil
	}
	return nil
}

// -----------------------------------------------------------------------------
// MemoryDeduplicator for unit testing
// -----------------------------------------------------------------------------

type memEntry struct {
	value     string
	expiresAt time.Time
}

type MemoryDedupOption func(*MemoryDeduplicator)

func WithMemoryWorkerID(workerID string) MemoryDedupOption {
	return func(m *MemoryDeduplicator) {
		if workerID != "" {
			m.workerID = workerID
		}
	}
}

func WithMemoryClaimTTL(claimTTL time.Duration) MemoryDedupOption {
	return func(m *MemoryDeduplicator) {
		if claimTTL > 0 {
			m.claimTTL = claimTTL
		}
	}
}

func WithMemoryMetrics(met *metrics.Metrics) MemoryDedupOption {
	return func(m *MemoryDeduplicator) {
		m.metrics = met
	}
}

type MemoryDeduplicator struct {
	mu          sync.Mutex
	keys        map[string]memEntry
	workerID    string
	claimTTL    time.Duration
	sentTTL     time.Duration
	failClaim   error
	failRelease error
	metrics     *metrics.Metrics
}

func NewMemoryDeduplicator(sentTTL time.Duration, opts ...MemoryDedupOption) *MemoryDeduplicator {
	if sentTTL <= 0 {
		sentTTL = DefaultSentTTL
	}
	m := &MemoryDeduplicator{
		keys:     make(map[string]memEntry),
		workerID: uuid.NewString(),
		claimTTL: DefaultClaimTTL,
		sentTTL:  sentTTL,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *MemoryDeduplicator) Key(eventID string) string {
	return fmt.Sprintf("notif:dedup:memory:%s", eventID)
}

func (m *MemoryDeduplicator) token() string {
	return fmt.Sprintf("%s:%s", m.workerID, StatusInProgress)
}

func (m *MemoryDeduplicator) SetFailClaim(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failClaim = err
}

func (m *MemoryDeduplicator) SetFailRelease(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failRelease = err
}

func (m *MemoryDeduplicator) Claim(ctx context.Context, eventID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failClaim != nil {
		return false, m.failClaim
	}

	now := time.Now()
	entry, exists := m.keys[eventID]
	if exists && now.Before(entry.expiresAt) {
		return false, nil
	}

	// Claim as in-progress
	m.keys[eventID] = memEntry{
		value:     m.token(),
		expiresAt: now.Add(m.claimTTL),
	}
	return true, nil
}

func (m *MemoryDeduplicator) Promote(ctx context.Context, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	entry, exists := m.keys[eventID]
	if !exists || now.After(entry.expiresAt) {
		return fmt.Errorf("cannot promote missing or expired claim for event %s", eventID)
	}

	if entry.value != m.token() && !strings.HasSuffix(entry.value, ":"+StatusInProgress) {
		return fmt.Errorf("cannot promote claim not owned by worker for event %s", eventID)
	}

	m.keys[eventID] = memEntry{
		value:     StatusSent,
		expiresAt: now.Add(m.sentTTL),
	}
	return nil
}

func (m *MemoryDeduplicator) Release(ctx context.Context, eventID string, reasons ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failRelease != nil {
		return m.failRelease
	}

	reason := "failure"
	if len(reasons) > 0 && reasons[0] != "" {
		reason = reasons[0]
	}
	if m.metrics != nil {
		m.metrics.RecordClaimRelease(reason)
	}

	entry, exists := m.keys[eventID]
	if exists {
		// Only delete if it's this worker's IN_PROGRESS token or in-progress
		if entry.value == m.token() || strings.HasSuffix(entry.value, ":"+StatusInProgress) || entry.value == "1" {
			delete(m.keys, eventID)
		}
	}
	return nil
}
