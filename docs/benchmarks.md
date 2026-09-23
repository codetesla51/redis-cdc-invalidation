# Benchmarks

How the numbers below were produced, and what each run taught. Every finding
was verified by rerun, not assumed. The barrage configs live in
[`benchmarks/`](../benchmarks/).

## Method

[barrage](https://github.com/codetesla51/barrage) `db:` runners fire
`INSERT ... ON CONFLICT DO UPDATE` at Postgres — each write generates one WAL
change, so one write ≈ one `DEL` through the pipeline. barrage `concurrency`
doubles as its DB pool size; Postgres `max_connections = 100`. While a run
goes, watch the console counters (`changes_processed`, `changes_dropped`,
slot lag via `pg_replication_slots`).

## Write floods

| Run | Writes | Success | Rate | P99 | CDC processed | CDC dropped |
|---|---|---|---|---|---|---|
| 8 hot keys, 500/s | 13,749 | 76.9% | 458/s | 816ms | +10,570 | 0 |
| 500 keys, 2000/s | 55,002 | 100% | 1,833/s | 87ms | +55k | 0 |
| 500 keys, 2000/s | 55,005 | 93.7% | 1,833/s | 130ms | +51k | 330 |
| 500 keys, 2000/s, buffer 5000 | 54,997 | 93.3% | 1,833/s | 129ms | +51k | 0 |
| 500 keys, 2000/s | 54,984 | 47.1% | 1,833/s | 202ms | +26k | 0 |
| 500 keys, 2000/s, 10 min, buffer 5000 | 1,189,998 | 100% | 1,983/s | 103ms | +1.19M | 0 |
| 10k keys, CI ladder (barrage v0.6.5) | 55k / 115k / 1.19M | 100% | 1,833–1,983/s | 2ms | clean | 0 |

What the runs taught:

- **Row-lock queues, not CDC.** The 8-key run's 816ms P99 was 50 workers queuing on 8 rows — Postgres contention from the key choice, while CDC stayed clean. Wide keys measure the pipeline; hot keys measure locks.
- **Success-rate variance is the tool's pool.** Identical configs scored 100% / 93% / 47% with zero Postgres errors; Little's law (≈2000/s × 37ms ≈ 74 conns vs barrage's 80-conn pool) says the generator starved itself. The 76.9% run's failures were all end-of-run shutdown cancels in the PG log.
- **The dashboard tab drops.** 25k drops in one run traced to a Firefox tab on the console: its `/events` feed (10-deep buffer) can't drink a 1.8k/s firehose, so phylax dropped *its* copies. The invalidator never missed one — drop-on-full protecting the stream, exactly as designed. Close the tab for clean numbers.
- **The 100-deep buffer clips bursts.** 330 drops (0.6%) at 1.8k/s with one subscriber. Removing per-key logging changed nothing (156 → 330), disproving the first theory — burst depth, not consumer speed, was the cause. Fix: `CHANGE_BUFFER_SIZE=5000` (phylax `v0.3.3`, ~1KB per change) → drops 0 on reflood.
- **Sustained proof.** 10 minutes, 1.19M writes, 100% success, P99 103ms, zero drops, slot lag drained to idle — run with `CHANGE_BUFFER_SIZE=5000` (`WORKER_POOLS=8`, `TABLES=products`). The pipeline holds.

> [!NOTE] What P99 means here
> Every P99 in the table above is **Postgres write latency** (how long each
> `INSERT` took), measured by barrage. It says nothing about the stream
> fan-out: that health is read off separate dials — `changes_dropped`,
> group backlog (`XPENDING`), and the producer/consumer logs. A slow P99
> with zero drops means the database sweated and the pipeline didn't miss
> one. Conflating the two caused most of the confusion below.

## The 10k saga (a wrong-number detective story)

The 10k-key runs scored 43–58% "DB failing" three times running. Three
theories died with evidence before the fourth stuck:

1. **Pool contention** — widened barrage 80→150 and Postgres 100→200. Score: 43% → 45%. Falsified.
2. **Cold-insert cost** — plausible (10k fresh index entries vs 500 hot rewrites) but untestable post-mortem; the VM was gone with the evidence.
3. **Faster box, same failure** — 4 CPU/16GB EPYC, still 45%. Not hardware.
4. **The load generator** — barrage rebuilt its 10,000-entry cumulative weight table **per request** (40M element-ops/sec of self-inflicted overhead) and then linear-scanned it. Postgres's own log showed zero errors throughout: the database was innocent, the tool was timing out on its own paperwork.

Fixes, both in barrage: hoist the table build out of the hot path (v0.6.4), then binary search over it (v0.6.5, proven identical winners over 50k draws, 916ns/op on 10k). Rerun: 115,000 writes, 100%, P99 ~0ms. The keyspace question resolved the way theory predicted — wide keys are faster — once the measuring tool stopped tripping over itself.

Lesson kept: when the target's own logs are clean, interrogate the harness before theorizing about the target.

## Invalidation lag

Measured directly: 200 iterations of seed-stale-key → `UPDATE` → poll `EXISTS` until gone, client-side on localhost against the idle stack:

`min 6.7ms · p50 7.5ms · p95 12.1ms · p99 13.0ms · max 16.5ms`

That window covers commit + WAL + hash route + `DEL` (plus ~1ms of client round trip, so true commit→DEL is slightly under). Reads inside it can serve the pre-write value — if a path needs read-your-write, invalidate inline there alongside CDC.

## Read path: hits vs misses vs stampede

The invalidator only guarantees correctness — whether the cache actually *helps* is a read-side question, measured separately (200 samples each, localhost):

| Path | p50 | p99 |
|---|---|---|
| Redis hit | 0.06ms | 0.16ms |
| Miss → Postgres → repopulate | 0.28ms | 0.47ms |
| 30 concurrent reads, 1 just-deleted key (singleflight) | 6.8ms | 7.3ms (7.6ms wall) |

A miss costs ~5× a hit — the delete-tradeoff, quantified. The burst row is the `singleflight` payoff: before it, all 30 readers stormed Postgres (50ms wall); now one flight repopulates while 29 share, wall 7.6ms. If a hot key ever outgrows even this, the next lever is a shorter TTL or a warmer (recompute) for that key alone.

With the cache down entirely: reads degrade to Postgres correctly, and a circuit breaker (3 failures → skip Redis 5s) keeps them at ~0.2ms instead of ~200ms of dial retries. See `Store` in `../store.go`.

## Update side: the cost of deleting unread keys

Every change costs a Redis write even for keys nobody reads. Measured: after the write floods, `DBSIZE` showed essentially zero flood keys cached — in a write-heavy workload ~100% of `DEL`s hit absent keys. Cost per `DEL` (100k samples, sequential, localhost): **~165µs no-op, ~367µs deleting** (the latter includes re-`SET` setup per iteration; both dominated by round trip, server time is sub-microsecond). Across 8 pools at 2k writes/sec, that's ~4% pool utilization — noise. Recompute (`SELECT` + serialize + `SET` per change) would cost strictly more per unread key, so `DEL` stays. Revisit only if a specific hot key's cold-miss cost is measured.

## Chaos tests

- **Mixed load** (1k writes/s + 500 redis ops/s): DB 100%, coexistence clean.
- **SIGKILL mid-flood, 17s down**: slot grew to 4.2MB (~250KB/s — size disk alarms from that); restart replayed everything, 0 drops, lag drained.
- **Redis down**: correct reads via Postgres, breaker-kept at ~0.2ms; Valkey restarted, app never needed one.

## Load configs

[`benchmarks/`](../benchmarks/) holds the barrage configs: `flood-8key.yaml` (lock-contention demo), `flood-wide.yaml` (500 keys, 2000/s), `flood-1m.yaml` (same, 10 minutes). DSNs point at the local dev stack.
