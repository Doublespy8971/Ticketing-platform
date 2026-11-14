package dedup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Deduplicator interface {
	Claim(ctx context.Context, eventID string) (claimed bool, err error)
	Release(ctx context.Context, eventID string) error
}

type RedisDeduplicator struct {
	client        redis.Cmdable
	consumerGroup string
	ttl           time.Duration
}

func NewRedisDeduplicator(client redis.Cmdable, consumerGroup string, ttl time.Duration) *RedisDeduplicator {
	return &RedisDeduplicator{
		client:        client,
		consumerGroup: consumerGroup,
		ttl:           ttl,
	}
}

func (d *RedisDeduplicator) Key(eventID string) string {
	return fmt.Sprintf("notif:dedup:%s:%s", d.consumerGroup, eventID)
}

func (d *RedisDeduplicator) Claim(ctx context.Context, eventID string) (bool, error) {
	key := d.Key(eventID)
	// SET key "1" NX EX ttl
	res, err := d.client.SetArgs(ctx, key, "1", redis.SetArgs{
		Mode: "NX",
		TTL:  d.ttl,
	}).Result()

	if err == redis.Nil {
		// Key already exists -> not claimed (duplicate)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("redis dedup claim error: %w", err)
	}

	return res == "OK", nil
}

func (d *RedisDeduplicator) Release(ctx context.Context, eventID string) error {
	key := d.Key(eventID)
	if err := d.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis dedup release error: %w", err)
	}
	return nil
}

// MemoryDeduplicator is an in-memory implementation for unit testing.
type MemoryDeduplicator struct {
	mu          sync.Mutex
	keys        map[string]time.Time
	ttl         time.Duration
	failClaim   error
	failRelease error
}

func NewMemoryDeduplicator(ttl time.Duration) *MemoryDeduplicator {
	return &MemoryDeduplicator{
		keys: make(map[string]time.Time),
		ttl:  ttl,
	}
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
	if exp, exists := m.keys[eventID]; exists && now.Before(exp) {
		return false, nil
	}

	m.keys[eventID] = now.Add(m.ttl)
	return true, nil
}

func (m *MemoryDeduplicator) Release(ctx context.Context, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failRelease != nil {
		return m.failRelease
	}

	delete(m.keys, eventID)
	return nil
}
