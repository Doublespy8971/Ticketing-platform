package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/ticketing-platform/notification-service/internal/config"
	"github.com/ticketing-platform/notification-service/internal/consumer"
	"github.com/ticketing-platform/notification-service/internal/dedup"
	"github.com/ticketing-platform/notification-service/internal/health"
	"github.com/ticketing-platform/notification-service/internal/metrics"
	"github.com/ticketing-platform/notification-service/internal/notify"
)

func main() {
	// Configure structured JSON logging to stdout
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("Configuration validation failed at startup", slog.String("error", err.Error()))
		fmt.Fprintf(os.Stderr, "FATAL: configuration validation failed: %v\n", err)
		os.Exit(1)
	}

	logger.Info("Starting notification-service",
		slog.Int("port", cfg.ServerPort),
		slog.String("redisAddr", cfg.RedisAddr()),
		slog.Any("kafkaBrokers", cfg.KafkaBootstrapServers),
		slog.String("kafkaTopic", cfg.KafkaTopic),
		slog.String("kafkaGroup", cfg.KafkaGroupID),
	)

	// Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr(),
	})

	// Dedup
	deduplicator := dedup.NewRedisDeduplicator(rdb, cfg.KafkaGroupID, cfg.NotificationDedupTTL)

	// Metrics
	m := metrics.NewMetrics()

	// Sender (structured JSON logger)
	sender := notify.NewLogSender(os.Stdout)

	// Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBootstrapServers,
		GroupID:        cfg.KafkaGroupID,
		Topic:          cfg.KafkaTopic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        500 * time.Millisecond,
		StartOffset:    kafka.FirstOffset,
		IsolationLevel: kafka.ReadCommitted,
	})

	// Consumer
	c := consumer.NewConsumer(reader, deduplicator, sender, m, consumer.Options{
		MaxAttempts:     cfg.SenderMaxAttempts,
		InitialBackoff:  cfg.SenderInitialBackoff,
		MaxBackoff:      cfg.SenderMaxBackoff,
		MaxPollInterval: cfg.RedisMaxPollInterval,
		Logger:          logger,
	})

	// Health check pingers
	checkKafka := func(ctx context.Context) error {
		for _, broker := range cfg.KafkaBootstrapServers {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", broker)
			if err == nil {
				_ = conn.Close()
				return nil
			}
		}
		return errors.New("cannot connect to any Kafka broker")
	}

	checkRedis := func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	}

	healthHandler := health.NewHealthHandler(checkKafka, checkRedis, 2*time.Second)

	// HTTP Mux
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler.LivenessHandler)
	mux.HandleFunc("/health/liveness", healthHandler.LivenessHandler)
	mux.HandleFunc("/ready", healthHandler.ReadinessHandler)
	mux.HandleFunc("/health/readiness", healthHandler.ReadinessHandler)
	mux.Handle("/metrics", m.Handler())

	server := &http.Server{
		Addr:    cfg.ServerAddr(),
		Handler: mux,
	}

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumer in background
	consumerDone := make(chan error, 1)
	go func() {
		if err := c.Run(ctx); err != nil {
			logger.Error("Consumer terminated with error", slog.String("error", err.Error()))
			consumerDone <- err
			return
		}
		consumerDone <- nil
	}()

	// Start HTTP server in background
	serverDone := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", slog.String("addr", cfg.ServerAddr()))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", slog.String("error", err.Error()))
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	// Wait for termination signal or fatal error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigChan:
		logger.Info("Shutdown signal received, initiating graceful drain", slog.String("signal", sig.String()))
	case err := <-consumerDone:
		if err != nil {
			logger.Error("Fatal consumer error, initiating shutdown", slog.String("error", err.Error()))
		}
	case err := <-serverDone:
		if err != nil {
			logger.Error("Fatal server error, initiating shutdown", slog.String("error", err.Error()))
		}
	}

	// Graceful shutdown with 10 second drain
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer drainCancel()

	// 1. Stop consumer
	cancel()
	_ = reader.Close()

	// 2. Drain and shut down HTTP server
	if err := server.Shutdown(drainCtx); err != nil {
		logger.Warn("HTTP server shutdown encountered error", slog.String("error", err.Error()))
	}

	// 3. Close Redis
	if err := rdb.Close(); err != nil {
		logger.Warn("Redis client close encountered error", slog.String("error", err.Error()))
	}

	logger.Info("Graceful shutdown completed")
}
