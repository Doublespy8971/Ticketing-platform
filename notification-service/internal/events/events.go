package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	EventTypeOrderConfirmed = "order.confirmed"
	EventTypeOrderCancelled = "order.cancelled"
)

type Envelope struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	OccurredAt    string          `json:"occurredAt"`
	CorrelationID string          `json:"correlationId"`
	SchemaVersion int             `json:"schemaVersion"`
	Payload       json.RawMessage `json:"payload"`
}

type Seat struct {
	SeatID      string `json:"seatId"`
	SectionCode string `json:"sectionCode"`
	RowLabel    string `json:"rowLabel"`
	SeatNumber  string `json:"seatNumber"`
	PriceCents  int64  `json:"priceCents"`
}

type OrderConfirmedPayload struct {
	OrderID          string `json:"orderId"`
	ReservationID    string `json:"reservationId"`
	UserID           string `json:"userId"`
	EventID          string `json:"eventId"`
	PaymentID        string `json:"paymentId"`
	Seats            []Seat `json:"seats"`
	TotalAmountCents int64  `json:"totalAmountCents"`
	Currency         string `json:"currency"`
	ConfirmedAt      string `json:"confirmedAt"`
}

type OrderCancelledPayload struct {
	OrderID            string `json:"orderId"`
	ReservationID      string `json:"reservationId"`
	UserID             string `json:"userId"`
	EventID            string `json:"eventId"`
	FinalStatus        string `json:"finalStatus"`
	CancellationReason string `json:"cancellationReason"`
	CancelledAt        string `json:"cancelledAt"`
}

func IsHandledEventType(eventType string) bool {
	return eventType == EventTypeOrderConfirmed || eventType == EventTypeOrderCancelled
}

func ParseEnvelope(data []byte) (*Envelope, error) {
	if len(data) == 0 {
		return nil, errors.New("empty record data")
	}

	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("malformed envelope JSON: %w", err)
	}

	if err := validateUUID(env.EventID, "eventId"); err != nil {
		return nil, err
	}
	if env.EventType == "" {
		return nil, errors.New("missing required field eventType")
	}
	if err := validateTimestamp(env.OccurredAt, "occurredAt"); err != nil {
		return nil, err
	}
	if err := validateUUID(env.CorrelationID, "correlationId"); err != nil {
		return nil, err
	}
	if env.SchemaVersion != 1 {
		return nil, fmt.Errorf("unsupported schemaVersion: %d (expected 1)", env.SchemaVersion)
	}
	if len(env.Payload) == 0 || string(env.Payload) == "null" {
		return nil, errors.New("missing or null required field payload")
	}

	return &env, nil
}

func ParseOrderConfirmedPayload(raw json.RawMessage) (*OrderConfirmedPayload, error) {
	var p OrderConfirmedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("malformed order.confirmed payload JSON: %w", err)
	}

	if err := validateUUID(p.OrderID, "orderId"); err != nil {
		return nil, err
	}
	if err := validateUUID(p.ReservationID, "reservationId"); err != nil {
		return nil, err
	}
	if err := validateUUID(p.UserID, "userId"); err != nil {
		return nil, err
	}
	if err := validateUUID(p.EventID, "eventId"); err != nil {
		return nil, err
	}
	if err := validateUUID(p.PaymentID, "paymentId"); err != nil {
		return nil, err
	}
	if len(p.Seats) == 0 {
		return nil, errors.New("seats array must not be empty")
	}
	for i, s := range p.Seats {
		if err := validateUUID(s.SeatID, fmt.Sprintf("seats[%d].seatId", i)); err != nil {
			return nil, err
		}
		if s.SectionCode == "" {
			return nil, fmt.Errorf("seats[%d].sectionCode must not be empty", i)
		}
		if s.RowLabel == "" {
			return nil, fmt.Errorf("seats[%d].rowLabel must not be empty", i)
		}
		if s.SeatNumber == "" {
			return nil, fmt.Errorf("seats[%d].seatNumber must not be empty", i)
		}
		if s.PriceCents <= 0 {
			return nil, fmt.Errorf("seats[%d].priceCents must be positive", i)
		}
	}
	if p.TotalAmountCents <= 0 {
		return nil, errors.New("totalAmountCents must be positive")
	}
	if p.Currency == "" {
		return nil, errors.New("currency must not be empty")
	}
	if err := validateTimestamp(p.ConfirmedAt, "confirmedAt"); err != nil {
		return nil, err
	}

	return &p, nil
}

func ParseOrderCancelledPayload(raw json.RawMessage) (*OrderCancelledPayload, error) {
	var p OrderCancelledPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("malformed order.cancelled payload JSON: %w", err)
	}

	if err := validateUUID(p.OrderID, "orderId"); err != nil {
		return nil, err
	}
	if err := validateUUID(p.ReservationID, "reservationId"); err != nil {
		return nil, err
	}
	if err := validateUUID(p.UserID, "userId"); err != nil {
		return nil, err
	}
	if err := validateUUID(p.EventID, "eventId"); err != nil {
		return nil, err
	}

	validFinalStatus := map[string]bool{
		"CANCELLED_PAYMENT_FAILED": true,
		"CANCELLED_EXPIRED":        true,
		"CANCELLED_REFUNDED":       true,
	}
	if !validFinalStatus[p.FinalStatus] {
		return nil, fmt.Errorf("invalid finalStatus: %q", p.FinalStatus)
	}

	validReasons := map[string]bool{
		"PAYMENT_FAILED":             true,
		"RESERVATION_EXPIRED":        true,
		"LATE_PAYMENT_EXPIRED_HOLD": true,
	}
	if !validReasons[p.CancellationReason] {
		return nil, fmt.Errorf("invalid cancellationReason: %q", p.CancellationReason)
	}

	if err := validateTimestamp(p.CancelledAt, "cancelledAt"); err != nil {
		return nil, err
	}

	return &p, nil
}

func validateUUID(val, fieldName string) error {
	if val == "" {
		return fmt.Errorf("missing required field %s", fieldName)
	}
	u, err := uuid.Parse(val)
	if err != nil || u == uuid.Nil {
		return fmt.Errorf("invalid UUID for field %s: %q", fieldName, val)
	}
	return nil
}

func validateTimestamp(val, fieldName string) error {
	if val == "" {
		return fmt.Errorf("missing required field %s", fieldName)
	}
	_, err := time.Parse(time.RFC3339Nano, val)
	if err != nil {
		// Also try standard RFC3339
		_, err = time.Parse(time.RFC3339, val)
		if err != nil {
			return fmt.Errorf("invalid ISO-8601 timestamp for field %s: %q", fieldName, val)
		}
	}
	return nil
}
