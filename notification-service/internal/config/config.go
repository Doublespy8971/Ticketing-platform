package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Config struct {
	WorkerID              string
	ServerPort            int
	RedisHost             string
	RedisPort             int
	RedisPassword         string
	RedisDB               int
	RedisTLSEnabled       bool
	KafkaBootstrapServers []string
	NotificationDedupTTL  time.Duration
	NotificationClaimTTL  time.Duration
	KafkaGroupID          string
	KafkaTopic            string
	SenderMaxAttempts     int
	SenderInitialBackoff  time.Duration
	SenderMaxBackoff      time.Duration
	SenderTimeout         time.Duration
	RedisMaxPollInterval  time.Duration
}

func (c *Config) ServerAddr() string {
	return fmt.Sprintf(":%d", c.ServerPort)
}

func (c *Config) RedisAddr() string {
	return fmt.Sprintf("%s:%d", c.RedisHost, c.RedisPort)
}

func LoadConfig() (*Config, error) {
	portStr := getEnv("SERVER_PORT", "8084")
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid SERVER_PORT %q: must be integer between 1 and 65535", portStr)
	}

	redisHost := getEnv("REDIS_HOST", "localhost")
	if strings.TrimSpace(redisHost) == "" {
		return nil, fmt.Errorf("REDIS_HOST cannot be empty")
	}

	redisPortStr := getEnv("REDIS_PORT", "6379")
	redisPort, err := strconv.Atoi(redisPortStr)
	if err != nil || redisPort < 1 || redisPort > 65535 {
		return nil, fmt.Errorf("invalid REDIS_PORT %q: must be integer between 1 and 65535", redisPortStr)
	}

	redisPassword := getEnv("REDIS_PASSWORD", "")

	redisDBStr := getEnv("REDIS_DB", "0")
	redisDB, err := strconv.Atoi(redisDBStr)
	if err != nil || redisDB < 0 {
		return nil, fmt.Errorf("invalid REDIS_DB %q: must be integer >= 0", redisDBStr)
	}

	redisTLSStr := strings.ToLower(strings.TrimSpace(getEnv("REDIS_TLS_ENABLED", "false")))
	var redisTLSEnabled bool
	switch redisTLSStr {
	case "true", "1", "yes":
		redisTLSEnabled = true
	case "false", "0", "no", "":
		redisTLSEnabled = false
	default:
		return nil, fmt.Errorf("invalid REDIS_TLS_ENABLED %q: must be boolean (true/false)", redisTLSStr)
	}

	bootstrapServersStr := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
	var bootstrapServers []string
	for _, s := range strings.Split(bootstrapServersStr, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			bootstrapServers = append(bootstrapServers, s)
		}
	}
	if len(bootstrapServers) == 0 {
		return nil, fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS cannot be empty")
	}

	dedupTTLStr := getEnv("NOTIFICATION_DEDUP_TTL_SECONDS", "604800")
	dedupTTLSec, err := strconv.Atoi(dedupTTLStr)
	if err != nil || dedupTTLSec <= 0 {
		return nil, fmt.Errorf("invalid NOTIFICATION_DEDUP_TTL_SECONDS %q: must be positive integer", dedupTTLStr)
	}

	claimTTLStr := getEnv("NOTIFICATION_CLAIM_TTL_SECONDS", "60")
	claimTTLSec, err := strconv.Atoi(claimTTLStr)
	if err != nil || claimTTLSec <= 0 {
		return nil, fmt.Errorf("invalid NOTIFICATION_CLAIM_TTL_SECONDS %q: must be positive integer", claimTTLStr)
	}

	groupID := getEnv("KAFKA_GROUP_ID", "notification-service-order-cg")
	if strings.TrimSpace(groupID) == "" {
		return nil, fmt.Errorf("KAFKA_GROUP_ID cannot be empty")
	}

	topic := getEnv("KAFKA_TOPIC", "order.events")
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("KAFKA_TOPIC cannot be empty")
	}

	maxAttemptsStr := getEnv("NOTIFICATION_SENDER_MAX_ATTEMPTS", "3")
	maxAttempts, err := strconv.Atoi(maxAttemptsStr)
	if err != nil || maxAttempts < 1 {
		return nil, fmt.Errorf("invalid NOTIFICATION_SENDER_MAX_ATTEMPTS %q: must be integer >= 1", maxAttemptsStr)
	}

	initialBackoffMsStr := getEnv("NOTIFICATION_SENDER_INITIAL_BACKOFF_MS", "200")
	initialBackoffMs, err := strconv.Atoi(initialBackoffMsStr)
	if err != nil || initialBackoffMs < 0 {
		return nil, fmt.Errorf("invalid NOTIFICATION_SENDER_INITIAL_BACKOFF_MS %q: must be integer >= 0", initialBackoffMsStr)
	}

	maxBackoffMsStr := getEnv("NOTIFICATION_SENDER_MAX_BACKOFF_MS", "800")
	maxBackoffMs, err := strconv.Atoi(maxBackoffMsStr)
	if err != nil || maxBackoffMs < initialBackoffMs {
		return nil, fmt.Errorf("invalid NOTIFICATION_SENDER_MAX_BACKOFF_MS %q: must be integer >= initial backoff (%d ms)", maxBackoffMsStr, initialBackoffMs)
	}

	senderTimeoutMsStr := getEnv("NOTIFICATION_SENDER_TIMEOUT_MS", "5000")
	senderTimeoutMs, err := strconv.Atoi(senderTimeoutMsStr)
	if err != nil || senderTimeoutMs <= 0 {
		return nil, fmt.Errorf("invalid NOTIFICATION_SENDER_TIMEOUT_MS %q: must be positive integer", senderTimeoutMsStr)
	}

	maxPollIntervalMsStr := getEnv("REDIS_MAX_POLL_INTERVAL_MS", "300000")
	maxPollIntervalMs, err := strconv.Atoi(maxPollIntervalMsStr)
	if err != nil || maxPollIntervalMs <= 0 {
		return nil, fmt.Errorf("invalid REDIS_MAX_POLL_INTERVAL_MS %q: must be positive integer", maxPollIntervalMsStr)
	}

	// Validate that claim TTL exceeds worst-case send time (attempts * timeout + backoff sum)
	var backoffSum time.Duration
	curBackoff := time.Duration(initialBackoffMs) * time.Millisecond
	for i := 1; i < maxAttempts; i++ {
		backoffSum += curBackoff
		curBackoff = curBackoff * 2
		if curBackoff > time.Duration(maxBackoffMs)*time.Millisecond {
			curBackoff = time.Duration(maxBackoffMs) * time.Millisecond
		}
	}
	worstCaseSendTime := time.Duration(maxAttempts)*time.Duration(senderTimeoutMs)*time.Millisecond + backoffSum
	if time.Duration(claimTTLSec)*time.Second <= worstCaseSendTime {
		return nil, fmt.Errorf("NOTIFICATION_CLAIM_TTL_SECONDS (%ds) must exceed worst case send time (%v = %d attempts * %v timeout + %v backoff sum)",
			claimTTLSec, worstCaseSendTime, maxAttempts, time.Duration(senderTimeoutMs)*time.Millisecond, backoffSum)
	}

	return &Config{
		WorkerID:              uuid.NewString(),
		ServerPort:            port,
		RedisHost:             redisHost,
		RedisPort:             redisPort,
		RedisPassword:         redisPassword,
		RedisDB:               redisDB,
		RedisTLSEnabled:       redisTLSEnabled,
		KafkaBootstrapServers: bootstrapServers,
		NotificationDedupTTL:  time.Duration(dedupTTLSec) * time.Second,
		NotificationClaimTTL:  time.Duration(claimTTLSec) * time.Second,
		KafkaGroupID:          groupID,
		KafkaTopic:            topic,
		SenderMaxAttempts:     maxAttempts,
		SenderInitialBackoff:  time.Duration(initialBackoffMs) * time.Millisecond,
		SenderMaxBackoff:      time.Duration(maxBackoffMs) * time.Millisecond,
		SenderTimeout:         time.Duration(senderTimeoutMs) * time.Millisecond,
		RedisMaxPollInterval:  time.Duration(maxPollIntervalMs) * time.Millisecond,
	}, nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
