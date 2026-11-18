package app

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ticketing-platform/notification-service/internal/consumer"
	"github.com/ticketing-platform/notification-service/internal/dedup"
	"github.com/ticketing-platform/notification-service/internal/metrics"
	"github.com/ticketing-platform/notification-service/internal/notify"
)

type dummyCloser struct {
	closed bool
}

func (d *dummyCloser) Close() error {
	d.closed = true
	return nil
}

func TestDrainAndShutdown_CleanCompletionExitsZero(t *testing.T) {
	reader := consumer.NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()
	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{})

	consumerDone := make(chan error, 1)
	consumerDone <- nil // Consumer completed cleanly

	closer := &dummyCloser{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	code := DrainAndShutdown(
		c,
		consumerDone,
		func() {},
		nil,
		1*time.Second,
		logger,
		closer,
	)

	if code != 0 {
		t.Fatalf("expected exit code 0 on clean drain, got %d", code)
	}
	if !closer.closed {
		t.Fatalf("expected resources to be closed")
	}
}

func TestDrainAndShutdown_TimeoutExceedingExitsOne(t *testing.T) {
	reader := consumer.NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()
	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{})

	// consumerDone never receives, simulating stuck consumer
	consumerDone := make(chan error)
	var cancelled bool
	consumerCancel := func() {
		cancelled = true
	}

	closer := &dummyCloser{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	start := time.Now()
	code := DrainAndShutdown(
		c,
		consumerDone,
		consumerCancel,
		nil,
		100*time.Millisecond,
		logger,
		closer,
	)

	elapsed := time.Since(start)
	if code != 1 {
		t.Fatalf("expected exit code 1 when drain timeout exceeded, got %d", code)
	}
	if !cancelled {
		t.Fatalf("expected consumerCancel to be called when drain timeout exceeded")
	}
	if !closer.closed {
		t.Fatalf("expected resources to be closed even after drain timeout")
	}
	if elapsed < 100*time.Millisecond || elapsed > 1*time.Second {
		t.Fatalf("expected drain timeout around 100ms, took %v", elapsed)
	}
}

func TestDrainAndShutdown_ConsumerExitedWithErrorExitsZeroIfCleanDrain(t *testing.T) {
	reader := consumer.NewMockMessageReader(nil)
	d := dedup.NewMemoryDeduplicator(1 * time.Hour)
	sender := notify.NewFailingSender(0, nil)
	m := metrics.NewMetrics()
	c := consumer.NewConsumer(reader, d, sender, m, consumer.Options{})

	consumerDone := make(chan error, 1)
	consumerDone <- context.Canceled

	closer := &dummyCloser{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	code := DrainAndShutdown(
		c,
		consumerDone,
		func() {},
		nil,
		1*time.Second,
		logger,
		closer,
	)

	if code != 0 {
		t.Fatalf("expected exit code 0 when consumer finishes before drain timeout, got %d", code)
	}
}
