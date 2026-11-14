package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	ServiceName = "notification-service"

	OutcomeSent      = "sent"
	OutcomeDuplicate = "duplicate"
	OutcomeInvalid   = "invalid"
	OutcomeIgnored   = "ignored"
	OutcomeFailed    = "failed"
)

type Metrics struct {
	Registry                    *prometheus.Registry
	NotificationEventsTotal     *prometheus.CounterVec
	NotificationSendDuration    *prometheus.HistogramVec
	NotificationDedupRedisErrors *prometheus.CounterVec
	LegacyNotificationsSent     *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	eventsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notification_events_total",
			Help: "Total notification events processed by outcome and event type",
		},
		[]string{"service", "event_type", "outcome"},
	)

	sendDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "notification_send_duration_seconds",
			Help:    "Duration of notification dispatches in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"service"},
	)

	redisErrors := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notification_dedup_redis_errors_total",
			Help: "Total Redis errors encountered during deduplication claims",
		},
		[]string{"service"},
	)

	legacySent := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ticketing_notifications_sent_total",
			Help: "Total notifications dispatched or deduplicated via Redis",
		},
		[]string{"service", "event_type", "status"},
	)

	reg.MustRegister(eventsTotal, sendDuration, redisErrors, legacySent)

	return &Metrics{
		Registry:                     reg,
		NotificationEventsTotal:      eventsTotal,
		NotificationSendDuration:     sendDuration,
		NotificationDedupRedisErrors: redisErrors,
		LegacyNotificationsSent:      legacySent,
	}
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func (m *Metrics) RecordEvent(eventType, outcome string) {
	normEventType := normalizeEventType(eventType)
	m.NotificationEventsTotal.WithLabelValues(ServiceName, normEventType, outcome).Inc()

	// Update legacy metric if applicable
	switch outcome {
	case OutcomeSent:
		m.LegacyNotificationsSent.WithLabelValues(ServiceName, normEventType, "SENT").Inc()
	case OutcomeDuplicate:
		m.LegacyNotificationsSent.WithLabelValues(ServiceName, normEventType, "DEDUPLICATED").Inc()
	case OutcomeFailed:
		m.LegacyNotificationsSent.WithLabelValues(ServiceName, normEventType, "FAILED").Inc()
	}
}

func (m *Metrics) ObserveSendDuration(seconds float64) {
	m.NotificationSendDuration.WithLabelValues(ServiceName).Observe(seconds)
}

func (m *Metrics) RecordRedisError() {
	m.NotificationDedupRedisErrors.WithLabelValues(ServiceName).Inc()
}

func normalizeEventType(eventType string) string {
	switch eventType {
	case "order.confirmed", "order.cancelled":
		return eventType
	default:
		return "unknown"
	}
}
