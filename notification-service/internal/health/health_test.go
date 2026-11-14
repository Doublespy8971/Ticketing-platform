package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLivenessHandler(t *testing.T) {
	h := NewHealthHandler(nil, nil, 1*time.Second)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	h.LivenessHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse json response: %v", err)
	}
	if body["status"] != "UP" {
		t.Fatalf("expected status UP, got %s", body["status"])
	}

	// Method not allowed
	postReq := httptest.NewRequest(http.MethodPost, "/health", nil)
	postRec := httptest.NewRecorder()
	h.LivenessHandler(postRec, postReq)
	if postRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", postRec.Code)
	}
}

func TestReadinessHandlerBothUp(t *testing.T) {
	h := NewHealthHandler(
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
		1*time.Second,
	)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	h.ReadinessHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp ReadinessResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse json response: %v", err)
	}
	if resp.Status != "UP" || resp.Kafka != "UP" || resp.Redis != "UP" {
		t.Fatalf("expected all UP, got %+v", resp)
	}
}

func TestReadinessHandlerKafkaDown(t *testing.T) {
	h := NewHealthHandler(
		func(ctx context.Context) error { return errors.New("dial tcp: localhost:9092: connection refused") },
		func(ctx context.Context) error { return nil },
		1*time.Second,
	)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	h.ReadinessHandler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var resp ReadinessResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse json response: %v", err)
	}
	if resp.Status != "DOWN" || resp.Kafka != "DOWN" || resp.Redis != "UP" {
		t.Fatalf("expected Kafka DOWN, got %+v", resp)
	}

	// Verify no address leaked in response body
	bodyStr := rec.Body.String()
	if strings.Contains(bodyStr, "localhost") || strings.Contains(bodyStr, "9092") || strings.Contains(bodyStr, "refused") {
		t.Fatalf("address or error message leaked in body: %s", bodyStr)
	}
}

func TestReadinessHandlerRedisDown(t *testing.T) {
	h := NewHealthHandler(
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return errors.New("redis: connection timeout") },
		1*time.Second,
	)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	h.ReadinessHandler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var resp ReadinessResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse json response: %v", err)
	}
	if resp.Status != "DOWN" || resp.Kafka != "UP" || resp.Redis != "DOWN" {
		t.Fatalf("expected Redis DOWN, got %+v", resp)
	}
}
