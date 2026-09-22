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
)

// productTTL bounds how long a cached product can go stale if an
// invalidation is ever missed. CDC DELs are the primary path; TTL is the backstop.
const productTTL = 5 * time.Minute

// ErrProductNotFound is returned when id exists in neither cache nor Postgres.
var ErrProductNotFound = errors.New("product not found")

// Product mirrors the products table: the unit cached in Redis.
type Product struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	UpdatedAt time.Time `json:"updated_at"`
}

// getProduct implements cache-aside: a Redis hit returns immediately; on a
// miss it reads Postgres and repopulates Redis. Redis trouble degrades to
// Postgres instead of failing the read, and a failed SET is left to the TTL
// rather than failing a good row.
func getProduct(ctx context.Context, rdb *redis.Client, pool *pgxpool.Pool, id string) (Product, error) {
	if hit, err := rdb.Get(ctx, cacheKey("products", id)).Result(); err == nil {
		var p Product
		if json.Unmarshal([]byte(hit), &p) == nil {
			return p, nil
		}
		// Corrupt entry: fall through to the source of truth and overwrite it.
	}
	// Miss, corrupt entry, or Redis down: Postgres is the source of truth.

	var p Product
	err := pool.QueryRow(ctx, `SELECT id, name, updated_at FROM products WHERE id = $1`, id).
		Scan(&p.ID, &p.Name, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Product{}, fmt.Errorf("%w: %s", ErrProductNotFound, id)
	}
	if err != nil {
		return Product{}, err
	}

	// Best-effort repopulate; a failed SET just means the next read misses again.
	if data, err := json.Marshal(p); err == nil {
		_ = rdb.Set(ctx, cacheKey("products", id), data, productTTL).Err()
	}
	return p, nil
}
