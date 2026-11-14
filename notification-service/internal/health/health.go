package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type Pinger func(ctx context.Context) error

type HealthHandler struct {
	checkKafka Pinger
	checkRedis Pinger
	timeout    time.Duration
}

func NewHealthHandler(checkKafka, checkRedis Pinger, timeout time.Duration) *HealthHandler {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &HealthHandler{
		checkKafka: checkKafka,
		checkRedis: checkRedis,
		timeout:    timeout,
	}
}

func (h *HealthHandler) LivenessHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "UP",
		})
	}
}

type ReadinessResponse struct {
	Status string `json:"status"`
	Kafka  string `json:"kafka"`
	Redis  string `json:"redis"`
}

func (h *HealthHandler) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	kafkaStatus := "UP"
	if h.checkKafka != nil {
		if err := h.checkKafka(ctx); err != nil {
			kafkaStatus = "DOWN"
		}
	}

	redisStatus := "UP"
	if h.checkRedis != nil {
		if err := h.checkRedis(ctx); err != nil {
			redisStatus = "DOWN"
		}
	}

	isUp := kafkaStatus == "UP" && redisStatus == "UP"
	resp := ReadinessResponse{
		Kafka: kafkaStatus,
		Redis: redisStatus,
	}

	w.Header().Set("Content-Type", "application/json")
	if isUp {
		resp.Status = "UP"
		w.WriteHeader(http.StatusOK)
	} else {
		resp.Status = "DOWN"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(resp)
	}
}
