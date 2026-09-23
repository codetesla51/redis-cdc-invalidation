package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// cacheKey returns the cache key invalidated on every write to a row:
// "<table>:<id>" (e.g. "products:p1"). Table-agnostic by construction:
// a new watched table needs no code change, just adding it to TABLES.
func cacheKey(table, id string) string {
	return table + ":" + id
}

// newRedisClient dials Redis/Valkey at addr. Fail-fast on purpose: when the
// cache is down, every read degrades to Postgres, so the client must fail
// fast instead of burning time before the fallback runs. DialerRetries (not
// MaxRetries) governs pool dial attempts — go-redis retries dead-server
// dials 5 times by default, ~1.8s per op. One attempt is sufficient signal
// to take the degradation path.
func newRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:          addr,
		DialTimeout:   500 * time.Millisecond,
		ReadTimeout:   1 * time.Second,
		WriteTimeout:  1 * time.Second,
		MaxRetries:    0,
		DialerRetries: 1,
	})
}

// invalidateProduct deletes the cached row so the next read repopulates
// from Postgres. DEL is idempotent: missing keys are not an error.
func invalidateProduct(ctx context.Context, rdb *redis.Client, table, id string) error {
	return rdb.Del(ctx, cacheKey(table, id)).Err()
}
