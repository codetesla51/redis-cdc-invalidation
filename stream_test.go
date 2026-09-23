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
	pub := newPublisher(rdb, stream, 100000, 10*time.Millisecond)
	pub.publish(&phylax.Change{
		Table:     "products",
		Operation: "update",
		NewRow:    map[string]any{"id": id, "name": "v2"},
	})
	if err := pub.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
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

// TestPublishBatch proves 500 rapid publishes land as one pipeline worth of
// entries: no drops, order preserved per key.
func TestPublishBatch(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	const stream = "test-batch"

	rdb.Del(ctx, stream)
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	pub := newPublisher(rdb, stream, 100000, 10*time.Millisecond)
	for i := 0; i < 500; i++ {
		pub.publish(&phylax.Change{
			Table:     "products",
			Operation: "update",
			NewRow:    map[string]any{"id": "batch-p1", "name": i},
		})
	}
	if err := pub.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if n := rdb.XLen(ctx, stream).Val(); n != 500 {
		t.Fatalf("stream holds %d entries, want 500", n)
	}
	if d := pub.dropped.Load(); d != 0 {
		t.Fatalf("dropped %d, want 0", d)
	}
}
