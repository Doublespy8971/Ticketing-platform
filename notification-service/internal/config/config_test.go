package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	os.Clearenv()
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ServerPort != 8084 {
		t.Errorf("expected port 8084, got %d", cfg.ServerPort)
	}
	if cfg.RedisHost != "localhost" {
		t.Errorf("expected RedisHost localhost, got %s", cfg.RedisHost)
	}
	if cfg.RedisPort != 6379 {
		t.Errorf("expected RedisPort 6379, got %d", cfg.RedisPort)
	}
	if len(cfg.KafkaBootstrapServers) != 1 || cfg.KafkaBootstrapServers[0] != "localhost:29092" {
		t.Errorf("expected KafkaBootstrapServers [localhost:29092], got %v", cfg.KafkaBootstrapServers)
	}
	if cfg.NotificationDedupTTL != 7*24*time.Hour {
		t.Errorf("expected DedupTTL 7 days, got %v", cfg.NotificationDedupTTL)
	}
	if cfg.KafkaGroupID != "notification-service-order-cg" {
		t.Errorf("expected GroupID notification-service-order-cg, got %s", cfg.KafkaGroupID)
	}
	if cfg.KafkaTopic != "order.events" {
		t.Errorf("expected Topic order.events, got %s", cfg.KafkaTopic)
	}
	if cfg.SenderMaxAttempts != 3 {
		t.Errorf("expected SenderMaxAttempts 3, got %d", cfg.SenderMaxAttempts)
	}
	if cfg.SenderInitialBackoff != 200*time.Millisecond {
		t.Errorf("expected SenderInitialBackoff 200ms, got %v", cfg.SenderInitialBackoff)
	}
	if cfg.SenderMaxBackoff != 800*time.Millisecond {
		t.Errorf("expected SenderMaxBackoff 800ms, got %v", cfg.SenderMaxBackoff)
	}
	if cfg.RedisMaxPollInterval != 300*time.Second {
		t.Errorf("expected RedisMaxPollInterval 300s, got %v", cfg.RedisMaxPollInterval)
	}
	if cfg.WorkerID == "" {
		t.Errorf("expected non-empty WorkerID")
	}
	if cfg.RedisPassword != "" {
		t.Errorf("expected empty default RedisPassword, got %q", cfg.RedisPassword)
	}
	if cfg.RedisDB != 0 {
		t.Errorf("expected default RedisDB 0, got %d", cfg.RedisDB)
	}
	if cfg.RedisTLSEnabled != false {
		t.Errorf("expected default RedisTLSEnabled false, got %v", cfg.RedisTLSEnabled)
	}
	if cfg.NotificationClaimTTL != 60*time.Second {
		t.Errorf("expected default NotificationClaimTTL 60s, got %v", cfg.NotificationClaimTTL)
	}
	if cfg.SenderTimeout != 5*time.Second {
		t.Errorf("expected default SenderTimeout 5s, got %v", cfg.SenderTimeout)
	}
}

func TestLoadConfigInvalidEnv(t *testing.T) {
	testCases := []struct {
		name string
		env  map[string]string
	}{
		{"invalid port", map[string]string{"SERVER_PORT": "invalid"}},
		{"negative port", map[string]string{"SERVER_PORT": "-1"}},
		{"port too high", map[string]string{"SERVER_PORT": "70000"}},
		{"empty redis host", map[string]string{"REDIS_HOST": "   "}},
		{"invalid redis port", map[string]string{"REDIS_PORT": "abc"}},
		{"negative redis db", map[string]string{"REDIS_DB": "-1"}},
		{"invalid redis db string", map[string]string{"REDIS_DB": "bad"}},
		{"invalid redis tls string", map[string]string{"REDIS_TLS_ENABLED": "maybe"}},
		{"empty kafka bootstrap", map[string]string{"KAFKA_BOOTSTRAP_SERVERS": "   "}},
		{"invalid dedup ttl", map[string]string{"NOTIFICATION_DEDUP_TTL_SECONDS": "-10"}},
		{"invalid claim ttl", map[string]string{"NOTIFICATION_CLAIM_TTL_SECONDS": "0"}},
		{"claim ttl less than worst case send time", map[string]string{
			"NOTIFICATION_CLAIM_TTL_SECONDS":     "10",
			"NOTIFICATION_SENDER_MAX_ATTEMPTS":   "3",
			"NOTIFICATION_SENDER_TIMEOUT_MS":     "5000",
		}},
		{"empty group id", map[string]string{"KAFKA_GROUP_ID": " "}},
		{"empty topic", map[string]string{"KAFKA_TOPIC": " "}},
		{"invalid max attempts", map[string]string{"NOTIFICATION_SENDER_MAX_ATTEMPTS": "0"}},
		{"invalid initial backoff", map[string]string{"NOTIFICATION_SENDER_INITIAL_BACKOFF_MS": "-1"}},
		{"max backoff less than initial", map[string]string{
			"NOTIFICATION_SENDER_INITIAL_BACKOFF_MS": "500",
			"NOTIFICATION_SENDER_MAX_BACKOFF_MS":     "200",
		}},
		{"invalid redis max poll", map[string]string{"REDIS_MAX_POLL_INTERVAL_MS": "0"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			os.Clearenv()
			for k, v := range tc.env {
				_ = os.Setenv(k, v)
			}
			cfg, err := LoadConfig()
			if err == nil {
				t.Errorf("expected error for case %q, got nil, cfg=%+v", tc.name, cfg)
			}
		})
	}
}

func TestLoadConfigRedisAndClaimSettings(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("REDIS_PASSWORD", "super_secret_redis_pw")
	_ = os.Setenv("REDIS_DB", "3")
	_ = os.Setenv("REDIS_TLS_ENABLED", "true")
	_ = os.Setenv("NOTIFICATION_CLAIM_TTL_SECONDS", "120")
	_ = os.Setenv("NOTIFICATION_SENDER_TIMEOUT_MS", "3000")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.RedisPassword != "super_secret_redis_pw" {
		t.Errorf("expected RedisPassword super_secret_redis_pw, got %q", cfg.RedisPassword)
	}
	if cfg.RedisDB != 3 {
		t.Errorf("expected RedisDB 3, got %d", cfg.RedisDB)
	}
	if !cfg.RedisTLSEnabled {
		t.Errorf("expected RedisTLSEnabled true, got %v", cfg.RedisTLSEnabled)
	}
	if cfg.NotificationClaimTTL != 120*time.Second {
		t.Errorf("expected NotificationClaimTTL 120s, got %v", cfg.NotificationClaimTTL)
	}
	if cfg.SenderTimeout != 3*time.Second {
		t.Errorf("expected SenderTimeout 3s, got %v", cfg.SenderTimeout)
	}
}
