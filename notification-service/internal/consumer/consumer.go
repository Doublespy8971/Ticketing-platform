package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka fetch message error: %w", err)
		}

		if err := c.ProcessMessage(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("critical error processing message, halting consumer: %w", err)
		}
	}
}

func (c *Consumer) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	env, err := events.ParseEnvelope(msg.Value)
	if err != nil {
		c.logger.Warn("Invalid record envelope, discarding and committing offset",
			slog.String("topic", msg.Topic),
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.String("error", err.Error()),
		)
		c.metrics.RecordEvent("unknown", metrics.OutcomeInvalid)
		return c.reader.CommitMessages(ctx, msg)
	}

	if !events.IsHandledEventType(env.EventType) {
		c.metrics.RecordEvent(env.EventType, metrics.OutcomeIgnored)
		return c.reader.CommitMessages(ctx, msg)
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
		return c.reader.CommitMessages(ctx, msg)
	}

	if !handled || notif == nil {
		c.metrics.RecordEvent(env.EventType, metrics.OutcomeIgnored)
		return c.reader.CommitMessages(ctx, msg)
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

	// Claim deduplication
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
		// Duplicate event
		c.metrics.RecordEvent(env.EventType, metrics.OutcomeDuplicate)
		return c.reader.CommitMessages(ctx, msg)
	}

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
				return ctx.Err()
			}
		}
	}

	if sendErr == nil {
		c.metrics.ObserveSendDuration(time.Since(start).Seconds())
		c.metrics.RecordEvent(env.EventType, metrics.OutcomeSent)
		return c.reader.CommitMessages(ctx, msg)
	}

	// Sender failed after all attempts
	if delErr := c.dedup.Release(ctx, env.EventID); delErr != nil {
		c.logger.Warn("Failed to delete deduplication key after sender failure",
			slog.String("eventId", env.EventID),
			slog.String("error", delErr.Error()),
		)
	}

	c.metrics.RecordEvent(env.EventType, metrics.OutcomeFailed)
	c.logger.Error("Notification dispatch failed after all retry attempts, committing offset (at-most-once delivery by design)",
		slog.String("eventId", env.EventID),
		slog.String("eventType", env.EventType),
		slog.String("orderId", notif.OrderID),
		slog.Int("attempts", c.maxAttempts),
		slog.String("error", sendErr.Error()),
	)

	return c.reader.CommitMessages(ctx, msg)
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
