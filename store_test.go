package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
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

func TestValidTableName(t *testing.T) {
	cases := []struct {
		name  string
		table string
		want  bool
	}{
		{"simple", "products", true},
		{"underscore", "order_items", true},
		{"mixed case", "OrderItems", true},
		{"with digits", "t2", true},
		{"empty", "", false},
		{"leading digit", "2fast", false},
		{"hyphen", "my-table", false},
		{"space", "my table", false},
		{"semicolon", "products; DROP TABLE products;--", false},
		{"quote", "products'", false},
		{"dot", "public.products", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := validTableName(tc.table); got != tc.want {
				t.Fatalf("validTableName(%q) = %v, want %v", tc.table, got, tc.want)
			}
		})
	}
}

// TestStoreGetLive walks the whole cache-aside contract against real
// Postgres + Redis: miss populates, hit serves stale, invalidate refreshes,
// unknown id is not found. It runs on a scratch table OUTSIDE the
// publication on purpose: the live watcher streams watched tables, so using
// products/orders here would race its DELs against the assertions.
func TestStoreGetLive(t *testing.T) {
	rdb := testRedis(t)
	pool := testPool(t)
	store := NewStore(rdb, pool)
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS cdc_test_kv (
		id TEXT PRIMARY KEY, val TEXT NOT NULL)`); err != nil {
		t.Fatalf("scratch table: %v", err)
	}

	const table, id = "cdc_test_kv", "k1"
	if _, err := pool.Exec(ctx,
		`INSERT INTO cdc_test_kv (id, val) VALUES ($1, 'v1')
		 ON CONFLICT (id) DO UPDATE SET val = 'v1'`, id); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM cdc_test_kv WHERE id = $1`, id)
		rdb.Del(context.Background(), cacheKey(table, id))
	})
	if err := rdb.Del(ctx, cacheKey(table, id)).Err(); err != nil {
		t.Fatalf("force miss: %v", err)
	}

	// Miss -> Postgres -> repopulate.
	data, err := store.Get(ctx, table, id)
	if err != nil || !strings.Contains(string(data), `"v1"`) {
		t.Fatalf("miss: got %s err %v, want v1", data, err)
	}
	if n := rdb.Exists(ctx, cacheKey(table, id)).Val(); n != 1 {
		t.Fatal("miss did not populate Redis")
	}

	// Hit -> serves cached v1 even though Postgres moved to v2.
	if _, err := pool.Exec(ctx, `UPDATE cdc_test_kv SET val = 'v2' WHERE id = $1`, id); err != nil {
		t.Fatalf("move db: %v", err)
	}
	if data, err := store.Get(ctx, table, id); err != nil || !strings.Contains(string(data), `"v1"`) {
		t.Fatalf("hit: got %s err %v, want stale v1", data, err)
	}

	// Invalidate -> next read refreshes to v2.
	if err := invalidateProduct(ctx, rdb, table, id); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	if data, err := store.Get(ctx, table, id); err != nil || !strings.Contains(string(data), `"v2"`) {
		t.Fatalf("refresh: got %s err %v, want v2", data, err)
	}

	// Unknown id -> not found, and misses are not cached.
	if _, err := store.Get(ctx, table, "no-such-key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown id: got %v, want ErrNotFound", err)
	}
	if n := rdb.Exists(ctx, cacheKey(table, "no-such-key")).Val(); n != 0 {
		t.Fatal("miss cached a negative entry")
	}

	// Hostile table name never reaches SQL.
	if _, err := store.Get(ctx, "products; DROP TABLE products;--", "x"); err == nil {
		t.Fatal("injection table accepted")
	}
}

// TestSingleflightDedup proves concurrent misses on one key share a single
// Postgres flight: 30 callers, fetch blocked on a gate, exactly 1 fetch,
// all callers share its result. No database needed.
func TestSingleflightDedup(t *testing.T) {
	rdb := testRedis(t)
	store := NewStore(rdb, nil)
	_ = rdb.Del(context.Background(), cacheKey("products", "herd-p1")).Err()
	t.Cleanup(func() { rdb.Del(context.Background(), cacheKey("products", "herd-p1")) })

	release := make(chan struct{})
	var calls atomic.Int64
	store.fetch = func(ctx context.Context, table, id string) ([]byte, error) {
		calls.Add(1)
		<-release // hold the flight open so all 30 callers pile onto it
		return []byte(`{"id":"herd-p1","shared":true}`), nil
	}

	const riders = 30
	var wg sync.WaitGroup
	errs := make([]error, riders)
	got := make([]string, riders)
	for i := 0; i < riders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data, err := store.Get(context.Background(), "products", "herd-p1")
			got[i], errs[i] = string(data), err
		}(i)
	}
	time.Sleep(200 * time.Millisecond) // let every rider join the flight
	close(release)
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Fatalf("fetch ran %d times, want 1", n)
	}
	for i := range got {
		if errs[i] != nil || !strings.Contains(got[i], `"shared":true`) {
			t.Fatalf("rider %d: got %s err %v, want shared", i, got[i], errs[i])
		}
	}
}

// TestCircuitBreaker is pure logic, no infrastructure: failures trip the
// breaker for the cooldown, success resets it, expiry re-allows.
func TestCircuitBreaker(t *testing.T) {
	b := newBreaker(3, 50*time.Millisecond)
	if !b.allow() {
		t.Fatal("fresh breaker must allow")
	}
	b.record(false)
	b.record(false)
	if !b.allow() {
		t.Fatal("below threshold must still allow")
	}
	b.record(false)
	if b.allow() {
		t.Fatal("at threshold must trip")
	}
	b.record(true) // success resets even mid-trip
	if !b.allow() {
		t.Fatal("success must reset the breaker")
	}
	b.record(false)
	b.record(false)
	b.record(false)
	if b.allow() {
		t.Fatal("must trip again")
	}
	time.Sleep(60 * time.Millisecond)
	if !b.allow() {
		t.Fatal("cooldown expiry must re-allow")
	}
}
