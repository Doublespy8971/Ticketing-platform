package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServerPort                   int
	RedisHost                    string
	RedisPort                    int
	KafkaBootstrapServers        []string
	NotificationDedupTTL         time.Duration
	KafkaGroupID                 string
	KafkaTopic                   string
	SenderMaxAttempts            int
	SenderInitialBackoff         time.Duration
	SenderMaxBackoff             time.Duration
	RedisMaxPollInterval         time.Duration
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

	maxPollIntervalMsStr := getEnv("REDIS_MAX_POLL_INTERVAL_MS", "300000")
	maxPollIntervalMs, err := strconv.Atoi(maxPollIntervalMsStr)
	if err != nil || maxPollIntervalMs <= 0 {
		return nil, fmt.Errorf("invalid REDIS_MAX_POLL_INTERVAL_MS %q: must be positive integer", maxPollIntervalMsStr)
	}

	return &Config{
		ServerPort:            port,
		RedisHost:             redisHost,
		RedisPort:             redisPort,
		KafkaBootstrapServers: bootstrapServers,
		NotificationDedupTTL:  time.Duration(dedupTTLSec) * time.Second,
		KafkaGroupID:          groupID,
		KafkaTopic:            topic,
		SenderMaxAttempts:     maxAttempts,
		SenderInitialBackoff:  time.Duration(initialBackoffMs) * time.Millisecond,
		SenderMaxBackoff:      time.Duration(maxBackoffMs) * time.Millisecond,
		RedisMaxPollInterval:  time.Duration(maxPollIntervalMs) * time.Millisecond,
	}, nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
