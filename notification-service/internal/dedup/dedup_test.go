package dedup

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryDeduplicatorClaimAndDuplicate(t *testing.T) {
	ctx := context.Background()
	d := NewMemoryDeduplicator(1 * time.Hour)

	eventID := "01926b54-7c8a-7000-8000-1a2b3c4d5e6f"

	// First claim: should succeed
	claimed, err := d.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !claimed {
		t.Fatalf("expected first claim to succeed")
	}

	// Second claim: should fail (duplicate)
	claimed2, err := d.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if claimed2 {
		t.Fatalf("expected second claim to return false (duplicate)")
	}

	// Release
	if err := d.Release(ctx, eventID); err != nil {
		t.Fatalf("unexpected release error: %v", err)
	}

	// Third claim: should succeed again
	claimed3, err := d.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !claimed3 {
		t.Fatalf("expected claim after release to succeed")
	}
}

func TestMemoryDeduplicatorErrors(t *testing.T) {
	ctx := context.Background()
	d := NewMemoryDeduplicator(1 * time.Hour)

	simErr := errors.New("redis connection refused")
	d.SetFailClaim(simErr)

	_, err := d.Claim(ctx, "evt-1")
	if !errors.Is(err, simErr) {
		t.Fatalf("expected simErr on claim, got %v", err)
	}

	d.SetFailClaim(nil)
	d.SetFailRelease(simErr)

	if err := d.Release(ctx, "evt-1"); !errors.Is(err, simErr) {
		t.Fatalf("expected simErr on release, got %v", err)
	}
}

func TestRedisDeduplicatorKeyFormat(t *testing.T) {
	d := NewRedisDeduplicator(nil, "my-group", 10*time.Minute)
	key := d.Key("evt-123")
	if key != "notif:dedup:my-group:evt-123" {
		t.Fatalf("unexpected key format: %s", key)
	}
}

func TestMemoryDeduplicatorPromoteAndTwoPhase(t *testing.T) {
	ctx := context.Background()
	d := NewMemoryDeduplicator(1*time.Hour, WithMemoryWorkerID("worker-test-1"))

	eventID := "01926b54-7c8a-7000-8000-1a2b3c4d5e6f"

	// 1. Claim
	claimed, err := d.Claim(ctx, eventID)
	if err != nil || !claimed {
		t.Fatalf("expected claim to succeed: %v", err)
	}

	// 2. Promote to SENT
	if err := d.Promote(ctx, eventID); err != nil {
		t.Fatalf("expected promote to succeed: %v", err)
	}

	// 3. Next claim should see SENT and return false (duplicate)
	claimed2, err := d.Claim(ctx, eventID)
	if err != nil || claimed2 {
		t.Fatalf("expected duplicate claim to return false, got claimed=%v, err=%v", claimed2, err)
	}

	// 4. Release with reason
	if err := d.Release(ctx, eventID, "failure"); err != nil {
		t.Fatalf("release should succeed: %v", err)
	}
}
