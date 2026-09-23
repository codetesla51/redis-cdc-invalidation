package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/codetesla51/phylax"
	"github.com/redis/go-redis/v9"
)

// publishChange appends one WAL change to the stream. Consumers in a group
// split these entries; each entry carries everything a worker needs.
func publishChange(ctx context.Context, rdb *redis.Client, stream string, c *phylax.Change) error {
	id, ok := rowID(c)
	if !ok {
		return nil // TRUNCATE and id-less changes have no key to route
	}
	return rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"table": c.Table,
			"op":    c.Operation,
			"id":    id,
		},
	}).Err()
}

// ensureGroup creates the consumer group once; BUSYGROUP means it exists.
func ensureGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

// consumeOnce reads one batch, applies every DEL, and ACKs only what was
// applied. Unacked entries stay pending for redelivery — at-least-once.
func consumeOnce(ctx context.Context, rdb *redis.Client, stream, group, consumer string, timeout time.Duration) (int, error) {
	res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    100,
		Block:    timeout,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}
	applied := 0
	for _, s := range res {
		for _, m := range s.Messages {
			table, _ := m.Values["table"].(string)
			id, _ := m.Values["id"].(string)
			if table == "" || id == "" {
				continue // malformed entry: leave unacked for inspection
			}
			if err := invalidateProduct(ctx, rdb, table, id); err != nil {
				return applied, err
			}
			if err := rdb.XAck(ctx, stream, group, m.ID).Err(); err != nil {
				return applied, err
			}
			applied++
		}
	}
	return applied, nil
}

// runConsumer feeds DELs from the stream until ctx ends. Sequential by
// design: one consumer processes in stream order; the group spreads keys
// across consumers, and DELs are order-insensitive anyway.
func runConsumer(ctx context.Context, rdb *redis.Client, stream, group string) error {
	consumer, err := os.Hostname()
	if err != nil {
		consumer = "worker"
	}
	consumer = fmt.Sprintf("%s-%d", consumer, os.Getpid())
	if err := ensureGroup(ctx, rdb, stream, group); err != nil {
		return fmt.Errorf("consumer group: %w", err)
	}
	log.Printf("consuming %s as %s/%s", stream, group, consumer)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if _, err := consumeOnce(ctx, rdb, stream, group, consumer, 5*time.Second); err != nil {
			return err
		}
	}
}
