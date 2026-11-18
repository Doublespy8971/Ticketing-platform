package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestMainRun_HealthCheckDown(t *testing.T) {
	// Point to unreachable port
	t.Setenv("SERVER_PORT", "65530")
	code := run([]string{"-healthcheck"})
	if code != 1 {
		t.Fatalf("expected exit code 1 when healthcheck endpoint is down, got %d", code)
	}
}

func TestMainRun_HealthCheckUp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	}))
	defer server.Close()

	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse url: %v", err)
	}
	t.Setenv("SERVER_PORT", u.Port())

	code := run([]string{"-healthcheck"})
	if code != 0 {
		t.Fatalf("expected exit code 0 when healthcheck endpoint is up, got %d", code)
	}
}

func TestMainRun_InvalidFlag(t *testing.T) {
	code := run([]string{"--invalid-flag"})
	if code != 1 {
		t.Fatalf("expected exit code 1 on invalid flag, got %d", code)
	}
}
