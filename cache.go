package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// cacheKey returns the cache key invalidated on every write to a row:
// "<table>:<id>" (e.g. "products:p1"). Table-agnostic by construction:
// a new watched table needs no code change, just adding it to TABLES.
func cacheKey(table, id string) string {
	return table + ":" + id
}

// newRedisClient dials Redis/Valkey at addr. Callers Ping to fail fast.
func newRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}

// invalidateProduct deletes the cached row so the next read repopulates
// from Postgres. DEL is idempotent: missing keys are not an error.
func invalidateProduct(ctx context.Context, rdb *redis.Client, table, id string) error {
	return rdb.Del(ctx, cacheKey(table, id)).Err()
}
