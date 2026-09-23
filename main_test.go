package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestRunEmptyDSN proves the no-DB path returns cleanly (and its defers —
// router pools, Redis client — unwind without hanging).
func TestRunEmptyDSN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := Config{RedisAddr: "localhost:6379", HTTPAddr: ":0", Pools: 2}
	if err := run(ctx, cfg); err != nil {
		if strings.Contains(err.Error(), "redis ping") {
			t.Skipf("redis unreachable: %v", err)
		}
		t.Fatalf("empty DSN: got %v, want nil", err)
	}
}

// TestRunBadRedis proves startup failure propagates as a returned error
// (which main turns into exit 1) instead of hanging or panicking.
func TestRunBadRedis(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := Config{RedisAddr: "localhost:1", Pools: 2}
	err := run(ctx, cfg)
	if err == nil || !strings.Contains(err.Error(), "redis ping") {
		t.Fatalf("bad redis: got %v, want redis ping error", err)
	}
}
