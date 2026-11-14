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

	ReleaseReasonFailure     = "failure"
	ReleaseReasonCancelled   = "cancelled"
	ReleaseReasonPanic       = "panic"
	ReleaseReasonCommitError = "commit_error"
)

var CanonicalEventTypes = map[string]bool{
	"inventory.seat.reserved":              true,
	"inventory.seat.released":              true,
	"inventory.reservation.expired":        true,
	"inventory.seat.confirmed":             true,
	"inventory.seat.confirmation_rejected": true,
	"order.payment.requested":              true,
	"order.refund.requested":               true,
	"order.confirmed":                      true,
	"order.cancelled":                      true,
	"payment.succeeded":                    true,
	"payment.failed":                       true,
	"payment.refunded":                     true,
}

type Metrics struct {
	Registry                     *prometheus.Registry
	NotificationEventsTotal      *prometheus.CounterVec
	NotificationSendDuration     *prometheus.HistogramVec
	NotificationDedupRedisErrors *prometheus.CounterVec
	LegacyNotificationsSent      *prometheus.CounterVec
	NotificationClaimWaitTotal   *prometheus.CounterVec
	NotificationClaimReleaseTotal *prometheus.CounterVec
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

	claimWaitTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notification_claim_wait_total",
			Help: "Total times a worker waited on an in-progress deduplication claim held by another worker",
		},
		[]string{"service"},
	)

	claimReleaseTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notification_claim_release_total",
			Help: "Total deduplication claims released before promotion partitioned by reason",
		},
		[]string{"service", "reason"},
	)

	reg.MustRegister(eventsTotal, sendDuration, redisErrors, legacySent, claimWaitTotal, claimReleaseTotal)

	return &Metrics{
		Registry:                      reg,
		NotificationEventsTotal:       eventsTotal,
		NotificationSendDuration:      sendDuration,
		NotificationDedupRedisErrors:  redisErrors,
		LegacyNotificationsSent:       legacySent,
		NotificationClaimWaitTotal:    claimWaitTotal,
		NotificationClaimReleaseTotal: claimReleaseTotal,
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

func (m *Metrics) RecordClaimWait() {
	m.NotificationClaimWaitTotal.WithLabelValues(ServiceName).Inc()
}

func (m *Metrics) RecordClaimRelease(reason string) {
	switch reason {
	case ReleaseReasonFailure, ReleaseReasonCancelled, ReleaseReasonPanic, ReleaseReasonCommitError:
	default:
		reason = "failure"
	}
	m.NotificationClaimReleaseTotal.WithLabelValues(ServiceName, reason).Inc()
}

func normalizeEventType(eventType string) string {
	if CanonicalEventTypes[eventType] {
		return eventType
	}
	return "unknown"
}
