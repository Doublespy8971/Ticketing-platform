package app

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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

// Options allows overriding dependencies for tests.
type Options struct {
	Config          *config.Config
	Reader          consumer.MessageReader
	RedisClient     *redis.Client
	Sender          notify.Sender
	SigChan         chan os.Signal
	DrainPeriod     time.Duration
	HealthCheckFunc func() int
}

// DrainAndShutdown executes the process-level graceful drain and resource shutdown.
// It stops fetching, waits for consumer to finish up to drainPeriod, closes the closers in order,
// and returns 0 on clean completion or 1 if drain times out.
func DrainAndShutdown(
	c *consumer.Consumer,
	consumerDone <-chan error,
	consumerCancel context.CancelFunc,
	healthHandler *health.HealthHandler,
	drainPeriod time.Duration,
	logger *slog.Logger,
	closers ...io.Closer,
) int {
	if logger == nil {
		logger = slog.Default()
	}
	if drainPeriod <= 0 {
		drainPeriod = 10 * time.Second
	}

	// 1. Mark /ready as not ready immediately
	if healthHandler != nil {
		healthHandler.SetShuttingDown()
	}

	// 2. Stop fetching new messages
	if c != nil {
		c.StopFetching()
	}

	// 3. Graceful drain: let in-flight message finish using drain context
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainPeriod)
	defer drainCancel()

	exitCode := 0
	if consumerDone != nil {
		select {
		case err := <-consumerDone:
			if err != nil {
				logger.Warn("Consumer finished with error", slog.String("error", err.Error()))
			} else {
				logger.Info("In-flight consumer processing completed cleanly")
			}
		case <-drainCtx.Done():
			if consumerCancel != nil {
				consumerCancel()
			}
			logger.Error("Drain period of 10s exceeded during shutdown; in-flight claim released, exiting non-zero")
			exitCode = 1
		}
	}

	// 4. Close resources in the order passed (e.g. reader, rdb, server)
	for _, cl := range closers {
		if cl != nil {
			if err := cl.Close(); err != nil {
				logger.Warn("Resource close encountered error", slog.String("error", err.Error()))
			}
		}
	}

	if exitCode == 0 {
		logger.Info("Graceful shutdown completed successfully")
	}
	return exitCode
}

// Run executes the application with CLI args and standard IO.
func Run(args []string, stdout, stderr io.Writer) int {
	return RunWithOptions(args, stdout, stderr, Options{})
}

// RunWithOptions executes the application with customizable options.
func RunWithOptions(args []string, stdout, stderr io.Writer, opts Options) int {
	fs := flag.NewFlagSet("notification-service", flag.ContinueOnError)
	fs.SetOutput(stderr)
	healthCheckFlag := fs.Bool("healthcheck", false, "run HTTP health check against /health and exit 0 if UP, 1 otherwise")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	if *healthCheckFlag {
		if opts.HealthCheckFunc != nil {
			return opts.HealthCheckFunc()
		}
		return RunHealthCheck()
	}

	if stdout == nil {
		stdout = os.Stdout
	}
	if stderr == nil {
		stderr = os.Stderr
	}

	logger := slog.New(slog.NewJSONHandler(stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := opts.Config
	if cfg == nil {
		var err error
		cfg, err = config.LoadConfig()
		if err != nil {
			logger.Error("Configuration validation failed at startup", slog.String("error", err.Error()))
			fmt.Fprintf(stderr, "FATAL: configuration validation failed: %v\n", err)
			return 1
		}
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

	// Redis client
	rdb := opts.RedisClient
	if rdb == nil {
		var tlsConfig *tls.Config
		if cfg.RedisTLSEnabled {
			tlsConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		}
		rdb = redis.NewClient(&redis.Options{
			Addr:      cfg.RedisAddr(),
			Password:  cfg.RedisPassword,
			DB:        cfg.RedisDB,
			TLSConfig: tlsConfig,
		})
	}

	// Metrics
	m := metrics.NewMetrics()

	// Deduplicator with stop channel support
	stopChan := make(chan struct{})
	deduplicator := dedup.NewRedisDeduplicator(
		rdb,
		cfg.KafkaGroupID,
		cfg.NotificationDedupTTL,
		dedup.WithRedisWorkerID(cfg.WorkerID),
		dedup.WithRedisClaimTTL(cfg.NotificationClaimTTL),
		dedup.WithRedisMetrics(m),
		dedup.WithRedisStopChan(stopChan),
	)

	// Sender
	sender := opts.Sender
	if sender == nil {
		sender = notify.NewLogSender(stdout)
	}

	// Kafka Reader
	reader := opts.Reader
	if reader == nil {
		reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:           cfg.KafkaBootstrapServers,
			GroupID:           cfg.KafkaGroupID,
			Topic:             cfg.KafkaTopic,
			MinBytes:          10e3,
			MaxBytes:          10e6,
			MaxWait:           500 * time.Millisecond,
			StartOffset:       kafka.FirstOffset,
			IsolationLevel:    kafka.ReadCommitted,
			SessionTimeout:    cfg.KafkaSessionTimeout,
			HeartbeatInterval: cfg.KafkaHeartbeatInterval,
		})
	}

	// Consumer
	c := consumer.NewConsumer(reader, deduplicator, sender, m, consumer.Options{
		MaxAttempts:     cfg.SenderMaxAttempts,
		InitialBackoff:  cfg.SenderInitialBackoff,
		MaxBackoff:      cfg.SenderMaxBackoff,
		MaxPollInterval: cfg.RedisMaxPollInterval,
		Logger:          logger,
	})

	// Health handler
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

	drainPeriod := opts.DrainPeriod
	if drainPeriod <= 0 {
		drainPeriod = 10 * time.Second
	}

	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- c.Run(consumerCtx)
	}()

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

	sigChan := opts.SigChan
	if sigChan == nil {
		sigChan = make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	}

	var consumerAlreadyExited bool
	var consumerErr error
	var fatalServerError error

	select {
	case sig := <-sigChan:
		logger.Info("Shutdown signal received, initiating graceful drain", slog.String("signal", sig.String()))
	case err := <-consumerDone:
		consumerAlreadyExited = true
		consumerErr = err
		if err != nil {
			logger.Error("Consumer loop ended unexpectedly, initiating shutdown", slog.String("error", err.Error()))
		}
	case err := <-serverDone:
		if err != nil {
			fatalServerError = err
			logger.Error("Fatal server error, initiating shutdown", slog.String("error", err.Error()))
		}
	}

	// Forward stop to deduplicator stop channel
	close(stopChan)

	// Process-level drain and shutdown
	var drainConsumerDone <-chan error
	if !consumerAlreadyExited {
		drainConsumerDone = consumerDone
	}

	exitCode := DrainAndShutdown(
		c,
		drainConsumerDone,
		consumerCancel,
		healthHandler,
		drainPeriod,
		logger,
		reader,
		rdb,
		server,
	)

	if fatalServerError != nil || (consumerAlreadyExited && consumerErr != nil) {
		return 1
	}

	return exitCode
}

// RunHealthCheck runs HTTP health check against /health and returns 0 if UP, 1 otherwise.
func RunHealthCheck() int {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8084"
	}
	client := &http.Client{
		Timeout: 2 * time.Second,
	}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%s/health", port))
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	var res struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 1
	}
	if res.Status != "UP" {
		return 1
	}
	return 0
}
