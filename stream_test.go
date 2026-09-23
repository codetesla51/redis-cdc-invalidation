package main

import (
	"context"
	"testing"
	"time"

	"github.com/codetesla51/phylax"
)

// TestStreamRoundTrip proves the whole stream topology with real Valkey:
// publish one WAL change, consume it, key gone, group backlog empty.
func TestStreamRoundTrip(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	const stream, group = "test-cdc", "test-inv"

	rdb.Del(ctx, stream)
	if err := ensureGroup(ctx, rdb, stream, group); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	const id = "stream-p1"
	if err := rdb.Set(ctx, cacheKey("products", id), "stale", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := publishChange(ctx, rdb, stream, &phylax.Change{
		Table:     "products",
		Operation: "update",
		NewRow:    map[string]any{"id": id, "name": "v2"},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	n, err := consumeOnce(ctx, rdb, stream, group, "test-consumer", 2*time.Second)
	if err != nil || n != 1 {
		t.Fatalf("consume: n=%d err=%v, want 1", n, err)
	}
	if n := rdb.Exists(ctx, cacheKey("products", id)).Val(); n != 0 {
		t.Fatal("key survived the stream round trip")
	}
	pending, err := rdb.XPending(ctx, stream, group).Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending=%+v err=%v, want empty (ACKed)", pending, err)
	}
}
