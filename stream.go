package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/codetesla51/phylax"
	"github.com/redis/go-redis/v9"
)

// streamEntry is one WAL change distilled to what a worker needs: DEL
// takes only table+id, so entries stay tiny (~100 bytes).
type streamEntry struct {
	table, op, id string
}

// publisher batches WAL changes and flushes them as one pipeline per tick,
// turning ~100µs per XADD into ~1µs amortized. The queue absorbs bursts;
// beyond its cap, entries drop loudly (logged) rather than stalling the
// broadcast loop — size the cap for the worst burst, not the average rate.
type publisher struct {
	rdb      *redis.Client
	stream   string
	interval time.Duration
	queue    chan streamEntry
	dropped  atomic.Int64
}

func newPublisher(rdb *redis.Client, stream string, queueCap int, interval time.Duration) *publisher {
	return &publisher{rdb: rdb, stream: stream, interval: interval, queue: make(chan streamEntry, queueCap)}
}

// publish enqueues one change; id-less changes (TRUNCATE) have no key.
func (p *publisher) publish(c *phylax.Change) {
	id, ok := rowID(c)
	if !ok {
		return
	}
	select {
	case p.queue <- streamEntry{table: c.Table, op: c.Operation, id: id}:
	default:
		n := p.dropped.Add(1)
		log.Printf("publish queue full, dropped %d total", n)
	}
}

// run flushes every tick until ctx ends, then drains once and returns.
func (p *publisher) run(ctx context.Context) error {
	tick := time.NewTicker(p.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return p.flush(context.Background())
		case <-tick.C:
			if err := p.flush(ctx); err != nil {
				return err
			}
		}
	}
}

// flush sends everything queued as a single pipeline. Order within the
// batch is preserved, so per-key order survives batching.
func (p *publisher) flush(ctx context.Context) error {
	n := len(p.queue)
	if n == 0 {
		return nil
	}
	if n > 5000 {
		n = 5000
	}
	_, err := p.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < n; i++ {
			select {
			case e := <-p.queue:
				// MaxLen caps the stream so a dead consumer group can't
				// grow it forever (~100k entries ≈ 4MB measured).
				pipe.XAdd(ctx, &redis.XAddArgs{
					Stream: p.stream,
					MaxLen: 100000,
					Approx: true,
					Values: map[string]any{"table": e.table, "op": e.op, "id": e.id},
				})
			default:
				return nil
			}
		}
		return nil
	})
	return err
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
	// Per-consumer applied counts make fan-out splits observable: one
	// HINCRBY per batch ≈ 1 extra op per ~100 DELs.
	if applied > 0 {
		_ = rdb.HIncrBy(ctx, "cdc:stats:"+group, consumer, int64(applied)).Err()
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
