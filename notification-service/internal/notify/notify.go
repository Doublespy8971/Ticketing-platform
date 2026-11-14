package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ticketing-platform/notification-service/internal/events"
)

const (
	KindConfirmation = "CONFIRMATION"
	KindCancellation = "CANCELLATION"
	ServiceName      = "notification-service"
)

type Notification struct {
	EventID            string `json:"eventId"`
	EventType          string `json:"eventType"`
	OrderID            string `json:"orderId"`
	NotificationKind   string `json:"notificationKind"`
	FinalStatus        string `json:"finalStatus,omitempty"`
	CancellationReason string `json:"cancellationReason,omitempty"`
	CorrelationID      string `json:"correlationId,omitempty"`
}

type Sender interface {
	Send(ctx context.Context, notif Notification) error
}

type LogSender struct {
	writer io.Writer
	mu     sync.Mutex
}

func NewLogSender(w io.Writer) *LogSender {
	if w == nil {
		w = os.Stdout
	}
	return &LogSender{writer: w}
}

type structuredNotificationLog struct {
	Timestamp          string `json:"timestamp"`
	Level              string `json:"level"`
	Service            string `json:"service"`
	CorrelationID      string `json:"correlationId,omitempty"`
	Message            string `json:"message"`
	EventID            string `json:"eventId"`
	EventType          string `json:"eventType"`
	OrderID            string `json:"orderId"`
	NotificationKind   string `json:"notificationKind"`
	FinalStatus        string `json:"finalStatus,omitempty"`
	CancellationReason string `json:"cancellationReason,omitempty"`
}

func (s *LogSender) Send(ctx context.Context, notif Notification) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	entry := structuredNotificationLog{
		Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
		Level:              "INFO",
		Service:            ServiceName,
		CorrelationID:      notif.CorrelationID,
		Message:            "Notification dispatched",
		EventID:            notif.EventID,
		EventType:          notif.EventType,
		OrderID:            notif.OrderID,
		NotificationKind:   notif.NotificationKind,
		FinalStatus:        notif.FinalStatus,
		CancellationReason: notif.CancellationReason,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal notification log: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = fmt.Fprintln(s.writer, string(data))
	return err
}

type FailingSender struct {
	failuresRemaining int32
	totalAttempts     int32
	failForever       bool
	errToReturn       error
}

func NewFailingSender(failTimes int, err error) *FailingSender {
	if err == nil {
		err = errors.New("simulated sender failure")
	}
	return &FailingSender{
		failuresRemaining: int32(failTimes),
		errToReturn:       err,
	}
}

func NewAlwaysFailingSender(err error) *FailingSender {
	if err == nil {
		err = errors.New("simulated permanent sender failure")
	}
	return &FailingSender{
		failForever: true,
		errToReturn: err,
	}
}

func (f *FailingSender) Send(ctx context.Context, notif Notification) error {
	atomic.AddInt32(&f.totalAttempts, 1)
	if f.failForever {
		return f.errToReturn
	}

	for {
		rem := atomic.LoadInt32(&f.failuresRemaining)
		if rem <= 0 {
			return nil
		}
		if atomic.CompareAndSwapInt32(&f.failuresRemaining, rem, rem-1) {
			return f.errToReturn
		}
	}
}

func (f *FailingSender) TotalAttempts() int {
	return int(atomic.LoadInt32(&f.totalAttempts))
}

type SlowSender struct {
	delay         time.Duration
	totalAttempts int32
}

func NewSlowSender(delay time.Duration) *SlowSender {
	return &SlowSender{delay: delay}
}

func (s *SlowSender) Send(ctx context.Context, notif Notification) error {
	atomic.AddInt32(&s.totalAttempts, 1)
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *SlowSender) TotalAttempts() int {
	return int(atomic.LoadInt32(&s.totalAttempts))
}

// MapEventToNotification is the table-driven mapping function.
func MapEventToNotification(env *events.Envelope) (*Notification, bool, error) {
	if env == nil {
		return nil, false, errors.New("nil envelope")
	}

	switch env.EventType {
	case events.EventTypeOrderConfirmed:
		payload, err := events.ParseOrderConfirmedPayload(env.Payload)
		if err != nil {
			return nil, false, fmt.Errorf("invalid order.confirmed payload: %w", err)
		}
		return &Notification{
			EventID:          env.EventID,
			EventType:        env.EventType,
			OrderID:          payload.OrderID,
			NotificationKind: KindConfirmation,
			CorrelationID:    env.CorrelationID,
		}, true, nil

	case events.EventTypeOrderCancelled:
		payload, err := events.ParseOrderCancelledPayload(env.Payload)
		if err != nil {
			return nil, false, fmt.Errorf("invalid order.cancelled payload: %w", err)
		}
		return &Notification{
			EventID:            env.EventID,
			EventType:          env.EventType,
			OrderID:            payload.OrderID,
			NotificationKind:   KindCancellation,
			FinalStatus:        payload.FinalStatus,
			CancellationReason: payload.CancellationReason,
			CorrelationID:      env.CorrelationID,
		}, true, nil

	default:
		// Unknown or unhandled event type
		return nil, false, nil
	}
}
