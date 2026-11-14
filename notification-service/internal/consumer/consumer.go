package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/ticketing-platform/notification-service/internal/dedup"
	"github.com/ticketing-platform/notification-service/internal/events"
	"github.com/ticketing-platform/notification-service/internal/metrics"
	"github.com/ticketing-platform/notification-service/internal/notify"
)

type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Consumer struct {
	reader          MessageReader
	dedup           dedup.Deduplicator
	sender          notify.Sender
	metrics         *metrics.Metrics
	logger          *slog.Logger
	maxAttempts     int
	initialBackoff  time.Duration
	maxBackoff      time.Duration
	maxPollInterval time.Duration

	stopping      atomic.Bool
	stopFetchChan chan struct{}
}

type Options struct {
	MaxAttempts     int
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
	MaxPollInterval time.Duration
	Logger          *slog.Logger
}

func NewConsumer(
	reader MessageReader,
	d dedup.Deduplicator,
	s notify.Sender,
	m *metrics.Metrics,
	opts Options,
) *Consumer {
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	initialBackoff := opts.InitialBackoff
	if initialBackoff < 0 {
		initialBackoff = 200 * time.Millisecond
	}
	maxBackoff := opts.MaxBackoff
	if maxBackoff < initialBackoff {
		maxBackoff = 800 * time.Millisecond
	}
	maxPollInterval := opts.MaxPollInterval
	if maxPollInterval <= 0 {
		maxPollInterval = 300 * time.Second
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Consumer{
		reader:          reader,
		dedup:           d,
		sender:          s,
		metrics:         m,
		logger:          logger,
		maxAttempts:     maxAttempts,
		initialBackoff:  initialBackoff,
		maxBackoff:      maxBackoff,
		maxPollInterval: maxPollInterval,
		stopFetchChan:   make(chan struct{}),
	}
}

func (c *Consumer) StopFetching() {
	if c.stopping.CompareAndSwap(false, true) {
		close(c.stopFetchChan)
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	fetchBackoff := 500 * time.Millisecond
	maxFetchBackoff := 10 * time.Second

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.stopFetchChan:
			return nil
		default:
		}

		if c.stopping.Load() {
			return nil
		}

		// Create a fetch context that cancels if ctx cancels or StopFetching is called
		fetchCtx, fetchCancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-c.stopFetchChan:
				fetchCancel()
			case <-fetchCtx.Done():
			}
		}()

		msg, err := c.reader.FetchMessage(fetchCtx)
		fetchCancel()

		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil || c.stopping.Load() {
				return nil
			}
			c.logger.Warn("Transient Kafka fetch error, backing off",
				slog.String("error", err.Error()),
				slog.Duration("backoff", fetchBackoff),
			)
			select {
			case <-time.After(fetchBackoff):
				fetchBackoff = min(fetchBackoff*2, maxFetchBackoff)
				continue
			case <-ctx.Done():
				return nil
			case <-c.stopFetchChan:
				return nil
			}
		}

		// Reset fetch backoff on successful fetch
		fetchBackoff = 500 * time.Millisecond

		if err := c.ProcessMessage(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			c.logger.Warn("Error processing message, continuing", slog.String("error", err.Error()))
		}
	}
}

func (c *Consumer) ProcessMessage(ctx context.Context, msg kafka.Message) (err error) {
	var claimedEventID string

	// Panic recovery: catch panic, log topic/partition/offset and panic type, release claim, commit offset, continue
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Panic recovered during message processing, discarding poison pill",
				slog.String("topic", msg.Topic),
				slog.Int("partition", msg.Partition),
				slog.Int64("offset", msg.Offset),
				slog.String("panicType", fmt.Sprintf("%T", r)),
				slog.Any("panic", r),
			)
			c.metrics.RecordEvent("unknown", metrics.OutcomeInvalid)
			if claimedEventID != "" {
				c.releaseClaimFreshContext(claimedEventID, metrics.ReleaseReasonPanic)
			}
			_ = c.commitWithRetry(ctx, msg)
			err = nil // Never terminate process on panic
		}
	}()

	env, err := events.ParseEnvelope(msg.Value)
	if err != nil {
		c.logger.Warn("Invalid record envelope, discarding and committing offset",
			slog.String("topic", msg.Topic),
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.String("error", err.Error()),
		)
		c.metrics.RecordEvent("unknown", metrics.OutcomeInvalid)
		return c.commitWithRetry(ctx, msg)
	}

	if !events.IsHandledEventType(env.EventType) {
		c.metrics.RecordEvent(env.EventType, metrics.OutcomeIgnored)
		return c.commitWithRetry(ctx, msg)
	}

	notif, handled, err := notify.MapEventToNotification(env)
	if err != nil {
		c.logger.Warn("Invalid record payload for handled event, discarding and committing offset",
			slog.String("topic", msg.Topic),
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.String("eventId", env.EventID),
			slog.String("eventType", env.EventType),
			slog.String("error", err.Error()),
		)
		c.metrics.RecordEvent(env.EventType, metrics.OutcomeInvalid)
		return c.commitWithRetry(ctx, msg)
	}

	if !handled || notif == nil {
		c.metrics.RecordEvent(env.EventType, metrics.OutcomeIgnored)
		return c.commitWithRetry(ctx, msg)
	}

	// Extract correlationId from header if missing from envelope
	if notif.CorrelationID == "" {
		for _, h := range msg.Headers {
			if h.Key == "X-Correlation-Id" {
				notif.CorrelationID = string(h.Value)
				break
			}
		}
	}

	// Two-phase deduplication claim: SET <workerId>:IN_PROGRESS NX EX 60
	claimed, err := c.dedup.Claim(ctx, env.EventID)
	if err != nil {
		c.metrics.RecordRedisError()
		c.logger.Warn("Redis dedup claim failed on initial attempt, retrying with backoff",
			slog.String("eventId", env.EventID),
			slog.String("error", err.Error()),
		)

		claimed, err = c.retryDedupClaim(ctx, env.EventID)
		if err != nil {
			c.logger.Error("Redis unavailable for deduplication claim after retrying up to max poll interval, halting consumer to prevent offset loss",
				slog.String("eventId", env.EventID),
				slog.String("eventType", env.EventType),
				slog.String("error", err.Error()),
			)
			// Return error without committing offset
			return err
		}
	}

	if !claimed {
		// Duplicate event (value is "SENT")
		c.metrics.RecordEvent(env.EventType, metrics.OutcomeDuplicate)
		return c.commitWithRetry(ctx, msg)
	}

	claimedEventID = env.EventID

	// Dispatch notification with retry and backoff
	start := time.Now()
	var sendErr error
	backoff := c.initialBackoff

	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		sendErr = c.sender.Send(ctx, *notif)
		if sendErr == nil {
			break
		}

		if attempt < c.maxAttempts {
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, c.maxBackoff)
			case <-ctx.Done():
				c.releaseClaimFreshContext(claimedEventID, metrics.ReleaseReasonCancelled)
				return ctx.Err()
			}
		}
	}

	if sendErr == nil {
		c.metrics.ObserveSendDuration(time.Since(start).Seconds())
		c.metrics.RecordEvent(env.EventType, metrics.OutcomeSent)

		// Promote from <workerId>:IN_PROGRESS to SENT with full 7-day TTL
		if promErr := c.dedup.Promote(ctx, env.EventID); promErr != nil {
			c.logger.Warn("Failed to promote claim to SENT",
				slog.String("eventId", env.EventID),
				slog.String("error", promErr.Error()),
			)
			c.releaseClaimFreshContext(claimedEventID, metrics.ReleaseReasonCommitError)
			return promErr
		}
		// Claim promoted successfully; no release needed on exit
		claimedEventID = ""

		return c.commitWithRetry(ctx, msg)
	}

	// Sender failed after all attempts: release claim with compare-and-delete
	c.releaseClaimFreshContext(claimedEventID, metrics.ReleaseReasonFailure)
	claimedEventID = ""

	c.metrics.RecordEvent(env.EventType, metrics.OutcomeFailed)
	c.logger.Error("Notification dispatch failed after all retry attempts, committing offset (at-most-once delivery by design)",
		slog.String("eventId", env.EventID),
		slog.String("eventType", env.EventType),
		slog.String("orderId", notif.OrderID),
		slog.Int("attempts", c.maxAttempts),
		slog.String("error", sendErr.Error()),
	)

	return c.commitWithRetry(ctx, msg)
}

func (c *Consumer) releaseClaimFreshContext(eventID, reason string) {
	if eventID == "" {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.dedup.Release(releaseCtx, eventID, reason); err != nil {
		c.logger.Warn("Failed to release deduplication claim on exit path",
			slog.String("eventId", eventID),
			slog.String("reason", reason),
			slog.String("error", err.Error()),
		)
	}
}

func (c *Consumer) commitWithRetry(ctx context.Context, msg kafka.Message) error {
	backoff := 500 * time.Millisecond
	maxBackoff := 10 * time.Second
	var lastErr error

	for i := 1; i <= 5; i++ {
		err := c.reader.CommitMessages(ctx, msg)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return err
		}
		lastErr = err
		c.logger.Warn("Transient Kafka commit error, retrying",
			slog.Int("attempt", i),
			slog.String("error", err.Error()),
			slog.Duration("backoff", backoff),
		)
		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, maxBackoff)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	c.logger.Error("Kafka commit failed after 5 retries (safe because SENT key prevents duplicate on redelivery)",
		slog.Int64("offset", msg.Offset),
		slog.String("error", lastErr.Error()),
	)
	return lastErr
}

func (c *Consumer) retryDedupClaim(ctx context.Context, eventID string) (bool, error) {
	deadline := time.Now().Add(c.maxPollInterval)
	backoff := 200 * time.Millisecond
	maxBackoff := 5 * time.Second

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(backoff):
		}

		claimed, err := c.dedup.Claim(ctx, eventID)
		if err == nil {
			return claimed, nil
		}

		c.metrics.RecordRedisError()
		backoff = min(backoff*2, maxBackoff)
	}

	return false, fmt.Errorf("dedup claim retry timeout reached (%v)", c.maxPollInterval)
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
