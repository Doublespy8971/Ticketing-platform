package main

import (
	"context"
	"crypto/tls"
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
		slog.Int("redisDB", cfg.RedisDB),
		slog.Bool("redisTLS", cfg.RedisTLSEnabled),
		slog.Any("kafkaBrokers", cfg.KafkaBootstrapServers),
		slog.String("kafkaTopic", cfg.KafkaTopic),
		slog.String("kafkaGroup", cfg.KafkaGroupID),
		slog.String("workerId", cfg.WorkerID),
	)

	// Redis client with password, DB and optional TLS (never logging password)
	var tlsConfig *tls.Config
	if cfg.RedisTLSEnabled {
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:      cfg.RedisAddr(),
		Password:  cfg.RedisPassword,
		DB:        cfg.RedisDB,
		TLSConfig: tlsConfig,
	})

	// Metrics
	m := metrics.NewMetrics()

	// Dedup with two-phase claim and ownership
	deduplicator := dedup.NewRedisDeduplicator(
		rdb,
		cfg.KafkaGroupID,
		cfg.NotificationDedupTTL,
		dedup.WithRedisWorkerID(cfg.WorkerID),
		dedup.WithRedisClaimTTL(cfg.NotificationClaimTTL),
		dedup.WithRedisMetrics(m),
	)

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

	// Context for graceful drain
	drainPeriod := 10 * time.Second
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	// Start consumer in background
	consumerDone := make(chan error, 1)
	go func() {
		if err := c.Run(consumerCtx); err != nil {
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
			logger.Error("Consumer loop ended unexpectedly, initiating shutdown", slog.String("error", err.Error()))
		}
	case err := <-serverDone:
		if err != nil {
			logger.Error("Fatal server error, initiating shutdown", slog.String("error", err.Error()))
		}
	}

	// 1. Mark /ready as not ready immediately
	healthHandler.SetShuttingDown()

	// 2. Stop fetching new messages
	c.StopFetching()

	// 3. Graceful drain: let in-flight message finish using drain context of 10s
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainPeriod)
	defer drainCancel()

	consumerExited := make(chan struct{})
	go func() {
		<-consumerDone
		close(consumerExited)
	}()

	select {
	case <-consumerExited:
		logger.Info("In-flight consumer processing completed cleanly")
	case <-drainCtx.Done():
		consumerCancel()
		logger.Error("Drain period of 10s exceeded during shutdown; in-flight claim released, exiting non-zero")
		_ = reader.Close()
		_ = rdb.Close()
		_ = server.Close()
		os.Exit(1)
	}

	// 4. Close Kafka reader, Redis client, and HTTP server, in that order
	if err := reader.Close(); err != nil {
		logger.Warn("Kafka reader close encountered error", slog.String("error", err.Error()))
	}

	if err := rdb.Close(); err != nil {
		logger.Warn("Redis client close encountered error", slog.String("error", err.Error()))
	}

	httpDrainCtx, httpDrainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer httpDrainCancel()
	if err := server.Shutdown(httpDrainCtx); err != nil {
		logger.Warn("HTTP server shutdown encountered error", slog.String("error", err.Error()))
	}

	logger.Info("Graceful shutdown completed successfully")
}
