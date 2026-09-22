package main

import (
	"context"
	"testing"
	"time"
)

func TestProductKey(t *testing.T) {
	cases := []struct {
		name      string
		table, id string
		want      string
	}{
		{"products", "products", "p1", "products:p1"},
		{"orders", "orders", "o9", "orders:o9"},
		{"empty id", "products", "", "products:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cacheKey(tc.table, tc.id); got != tc.want {
				t.Fatalf("cacheKey(%q, %q) = %q, want %q", tc.table, tc.id, got, tc.want)
			}
		})
	}
}

// TestInvalidateProductLive needs a real Redis/Valkey on REDIS_ADDR
// (default localhost:6379). Skips when unreachable.
func TestInvalidateProductLive(t *testing.T) {
	addr := "localhost:6379"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rdb := newRedisClient(addr)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis at %s unreachable: %v", addr, err)
	}

	const id = "live-test-p1"
	if err := rdb.Set(ctx, cacheKey("products", id), "stale", 0).Err(); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	if err := invalidateProduct(ctx, rdb, "products", id); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	if n, err := rdb.Exists(ctx, cacheKey("products", id)).Result(); err != nil || n != 0 {
		t.Fatalf("key still exists: n=%d err=%v", n, err)
	}
	// Second DEL on a missing key stays a no-op, not an error.
	if err := invalidateProduct(ctx, rdb, "products", id); err != nil {
		t.Fatalf("second invalidate: %v", err)
	}
}
