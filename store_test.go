package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rdb := newRedisClient("localhost:6379")
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unreachable: %v", err)
	}
	return rdb
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := "postgres://postgres@localhost:5432/redis_cdc"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	return pool
}

// TestGetProductLive walks the whole cache-aside contract against real
// Postgres + Redis: miss populates, hit serves stale, invalidate refreshes,
// unknown id is not found.
func TestGetProductLive(t *testing.T) {
	rdb := testRedis(t)
	pool := testPool(t)
	ctx := context.Background()

	const id = "cache-aside-p1"
	if _, err := pool.Exec(ctx,
		`INSERT INTO products (id, name) VALUES ($1, 'v1')
		 ON CONFLICT (id) DO UPDATE SET name = 'v1', updated_at = now()`, id); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM products WHERE id = $1`, id)
		rdb.Del(context.Background(), cacheKey("products", id))
	})
	if err := rdb.Del(ctx, cacheKey("products", id)).Err(); err != nil {
		t.Fatalf("force miss: %v", err)
	}

	// Miss → Postgres → repopulate.
	p, err := getProduct(ctx, rdb, pool, id)
	if err != nil || p.Name != "v1" {
		t.Fatalf("miss: got %+v err %v, want name v1", p, err)
	}
	if n := rdb.Exists(ctx, cacheKey("products", id)).Val(); n != 1 {
		t.Fatal("miss did not populate Redis")
	}

	// Hit → serves cached v1 even though Postgres moved to v2.
	if _, err := pool.Exec(ctx, `UPDATE products SET name = 'v2' WHERE id = $1`, id); err != nil {
		t.Fatalf("move db: %v", err)
	}
	if p, err := getProduct(ctx, rdb, pool, id); err != nil || p.Name != "v1" {
		t.Fatalf("hit: got %+v err %v, want stale v1", p, err)
	}

	// Invalidate → next read refreshes to v2.
	if err := invalidateProduct(ctx, rdb, "products", id); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	if p, err := getProduct(ctx, rdb, pool, id); err != nil || p.Name != "v2" {
		t.Fatalf("refresh: got %+v err %v, want v2", p, err)
	}

	// Unknown id → not found, and misses are not cached.
	if _, err := getProduct(ctx, rdb, pool, "no-such-product"); !errors.Is(err, ErrProductNotFound) {
		t.Fatalf("unknown id: got %v, want ErrProductNotFound", err)
	}
	if n := rdb.Exists(ctx, cacheKey("products", "no-such-product")).Val(); n != 0 {
		t.Fatal("miss cached a negative entry")
	}
}
