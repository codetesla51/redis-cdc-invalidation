# redis-cdc-invalidation

Keep a Redis cache in sync with Postgres automatically. Instead of clearing the cache in every write path, this watches Postgres's write-ahead log (WAL) and deletes the matching Redis key on every insert, update, and delete — from any writer, with zero polling.

## Why it exists

Manual cache invalidation rots: every new write path (API handler, admin panel, script, import) must remember to clear the cache, and one forgotten path serves stale data forever. Reacting to committed data instead of code paths covers every writer by construction, at the cost of milliseconds of lag between commit and invalidation.

## Why not the alternatives

- Manual write-through invalidation is instant per call but only covers paths you instrumented.
- Debezium + Kafka gives replayability and multi-consumer fan-out, at the price of a JVM, a broker, and real ops. This is the same idea minus the broker: one binary, no topic ops. Graduate to Kafka when a second consumer of the change stream appears.

## How it works

```
Write → Postgres commits → phylax reads WAL → hash routes to pool
  → worker DELs Redis key → next read repopulates from Postgres
```

- **phylax** tails a logical replication slot and emits a `Change` per committed row. The slot retains WAL until acknowledged, so restarts resume where they left off instead of missing writes.
- **Router** assigns each row ID to one of N worker pools: `pool = fnv32a(id) % N`. Same ID always lands on the same pool (per-key order); different IDs spread across pools (parallel across keys).
- **Worker pools** (`pond`, concurrency 1 each) run the `DEL`. Concurrency must stay 1: two workers on one key could invalidate out of order and let an older state win.
- **Cache keys** are `<table>:<id>` (e.g. `products:p1`), derived from the change itself — new tables need no code change, just adding them to `TABLES`.
- **Reads** are cache-aside (`getProduct`): Redis hit returns, miss reads Postgres and repopulates with a 5-minute TTL. The TTL is a backstop; `DEL` is the primary path. Redis trouble degrades to Postgres instead of failing reads.

## Quick start

Requirements: Postgres with `wal_level = logical`, and Redis or Valkey on `localhost:6379`.

```sh
# from the repo root
DATABASE_URL='postgres://postgres@localhost:5432/redis_cdc' go run .
```

This starts the WAL watcher plus a console at `http://localhost:8080/dashboard` (KPIs, lag sparkline, live change feed).

Now write a row from anywhere — psql, your app, a script:

```sql
INSERT INTO products (id, name) VALUES ('demo1', 'apple');
```

The app logs `invalidated products:demo1 pool=5 (insert)`, the dashboard's `changes_processed` ticks up, and the next `getProduct("demo1")` repopulates Redis from Postgres.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | _(empty)_ | Postgres DSN. Empty means start nothing (useful for running tests without a DB). |
| `TABLES` | `products` | Comma-separated tables to watch, e.g. `products,orders`. Adding a table also needs `ALTER PUBLICATION my_publication ADD TABLE <table>;` once. |
| `REDIS_ADDR` | `localhost:6379` | Redis/Valkey address. Pinged at startup — fail fast if unreachable. |
| `HTTP_ADDR` | `:8080` | Console address (`/dashboard`, `/events`, `/metrics/stream`). |
| `WORKER_POOLS` | `8` | Router pool count. Must be a positive int. Pools are stateless, so changing this across a restart loses nothing. |

## Tests

```sh
go test -race ./...
```

Pure unit tests (hash stability, ordering, key format) always run. Tests touching Postgres/Redis use the local defaults above and skip when unreachable.

## Things to watch out for

- **Lag is inherent, not zero.** Commit → WAL → handler → Redis is milliseconds. If a path needs read-your-write, add a targeted inline invalidation there alongside CDC.
- **`REPLICA IDENTITY`:** with the default, Postgres only ships old-row data for primary-key changes. Deletes and key updates work; if you ever need old non-key values (e.g. "invalidate the old category listing"), set `REPLICA IDENTITY FULL` and accept the extra WAL.
- **Slot lag:** if this process stops, WAL accumulates in `my_slot` on Postgres. Monitor `pg_replication_slots` and alert on growth.
- **Hot keys:** after `DEL` on a very hot key, concurrent reads can stampede Postgres to repopulate. Mitigate with `singleflight` at the read path if you measure it.
- **At-least-once delivery:** restarts replay unacknowledged changes. `DEL` is idempotent so replays are harmless — keep future handlers idempotent too.
- **Shutdown:** Ctrl-C stops replication and the console gracefully. Only SIGINT is handled; `kill` (SIGTERM) terminates without the graceful path.
