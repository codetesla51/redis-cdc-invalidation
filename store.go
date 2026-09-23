package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// productTTL bounds how long a cached row can go stale if an
// invalidation is ever missed. CDC DELs are the primary path; TTL is the backstop.
const productTTL = 5 * time.Minute

// ErrNotFound is returned when id exists in neither cache nor Postgres.
var ErrNotFound = errors.New("row not found")

// Store serves cache-aside reads for any table with an `id` column. Rows
// travel as their JSON encoding, so no per-table struct is needed. The
// singleflight group makes concurrent misses on one key share a single
// Postgres flight instead of stampeding it: one caller queries, the rest
// wait and share the result.
type Store struct {
	rdb    *redis.Client
	pool   *pgxpool.Pool
	flight singleflight.Group
	// fetch loads one row from Postgres; a field (not a method) so tests
	// can substitute a counting fake without a database.
	fetch func(ctx context.Context, table, id string) ([]byte, error)
}

// NewStore wires a Store over live Redis and Postgres connections.
func NewStore(rdb *redis.Client, pool *pgxpool.Pool) *Store {
	s := &Store{rdb: rdb, pool: pool}
	s.fetch = s.fetchRow
	return s
}

// validTableName rejects anything that is not a plain identifier. The table
// name is interpolated into SQL (placeholders can't name tables), so this
// check is the SQL-injection boundary: letters, digits, underscores only.
func validTableName(table string) bool {
	if table == "" {
		return false
	}
	for i, r := range table {
		ok := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9'
		if !ok {
			return false
		}
	}
	return true
}

// Get implements cache-aside for any table: a Redis hit returns the cached
// row JSON; on a miss the first caller loads Postgres and repopulates Redis
// while concurrent callers for the same table+id share its result. Redis
// trouble degrades to Postgres instead of failing the read, and a failed
// SET is left to the TTL rather than failing a good row.
func (s *Store) Get(ctx context.Context, table, id string) ([]byte, error) {
	if !validTableName(table) {
		return nil, fmt.Errorf("invalid table %q", table)
	}
	key := cacheKey(table, id)
	if hit, err := s.rdb.Get(ctx, key).Bytes(); err == nil && json.Valid(hit) {
		return hit, nil
	}
	// Miss, corrupt entry, or Redis down: Postgres is the source of truth.
	// The separator keeps ("ab","c") and ("a","bc") in separate flights.
	v, err, _ := s.flight.Do(table+"\x00"+id, func() (any, error) {
		data, err := s.fetch(ctx, table, id)
		if err != nil {
			return nil, err
		}
		// Best-effort repopulate; a failed SET just means the next read misses again.
		_ = s.rdb.Set(ctx, key, data, productTTL).Err()
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// fetchRow reads one row as JSON from Postgres. Unknown ids are not cached
// as negatives: every miss retries Postgres next time.
func (s *Store) fetchRow(ctx context.Context, table, id string) ([]byte, error) {
	var data []byte
	query := `SELECT to_jsonb(t) FROM ` + table + ` t WHERE id = $1`
	err := s.pool.QueryRow(ctx, query, id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s:%s", ErrNotFound, table, id)
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}
