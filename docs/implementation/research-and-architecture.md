# Backfill job system: research and recommended architecture

## Executive summary

We can meet the requirements with off‑the‑shelf crates and a small amount of glue. The lowest‑risk, fastest path is Postgres‑first using a mature Rust job queue built on `FOR UPDATE SKIP LOCKED` and `LISTEN/NOTIFY`. Our top recommendation is to adopt Graphile Worker RS (Rust port of Graphile Worker) for the Postgres backend, with Apalis as a viable alternative if we need both Postgres and Redis support under a single abstraction. For teams already standardized on `sqlx` and Dockerized Postgres tests, these fit well and require minimal custom code.

Recommended baseline:
- Primary: graphile_worker_rs (aka graphile_worker crate) on Postgres for durability, priority, scheduling, retries/backoff, and low‑latency dispatch [8][9][10].
- Optional alternative/multi‑backend: Apalis (Postgres/Redis), which recently added SQL‑backend priority and provides cron, retries, middleware via Tower [1][3][4][6].
- If we later need “Postgres‑only, macro‑based jobs with transactions/2PC and checkpoints,” sqlxmq is an excellent fit [13][17].

This approach yields: durable priority queues; insert‑at‑front via priority/run_at; optional deadlines; at‑least‑once execution with retries and exponential backoff; dead‑letter handling via “max attempts reached” policies; multiple queues with per‑queue concurrency and backpressure; strong testability against live Postgres.

Time to first working system: days, not months.

---

## Problem statement distilled

From docs/problem-statement.md, we need:
- Durable priority queue; ability to insert at front/high priority.
- Async I/O‑heavy workers (many Rust processes; `tokio`).
- Optional batching; sometimes per‑item deadlines.
- Retries with backoff; dead letter mailbox for unrecoverables.
- Can handle very large backfills and steady state; good perf and acceptable memory.
- Multiple queues, each with its own allowed rate/backpressure/retry rules.
- Prefer well‑tested, maintained crates; `sqlx` for RDBMS; Redis or Postgres as backends; no giant external job system; strong integration tests with live Postgres in Docker; no mocks.

---

## Correct implementation patterns (durable async jobs)

Key patterns that map directly to our requirements:

- Durable queue on Postgres using SKIP LOCKED
  - Use `SELECT … FOR UPDATE SKIP LOCKED` for concurrent claim without head‑of‑line blocking; order by priority then due time (run_at) to honor urgent work first [18][20][21].
  - Use `LISTEN/NOTIFY` to minimize polling latency for new jobs (Graphile Worker uses this) [8][9].
  - Represent scheduling/deadlines via `run_at` timestamps; “insert at front” by lowering `priority` and/or setting earlier `run_at`.
  - Retry with exponential backoff by updating `run_at` to now + backoff after a failure; use `attempts` and `max_attempts` to trigger DLQ flow [18].
  - Dead‑letter via: mark “failed (max attempts)” and move/archive or re‑queue to a dedicated DLQ table/queue for operator triage [11][12].

- Multi‑queue and backpressure
  - Use named queues; per‑queue concurrency via worker pools or per‑queue worker processes (Graphile Worker named queues, or separate worker pools per queue) [10].
  - Rate limiting/backpressure by limiting concurrent tasks and outbound calls; use semaphores/tokens per queue; Tower middleware if using Apalis [3][6].

- Redis‑based queuing (if chosen)
  - Use Redis Streams + consumer groups with `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM/AUTOCLAIM` for durable at‑least‑once processing and crash recovery [14][15][16].
  - Model priority with multiple streams per priority class and read high→low first; or model with sorted sets if we build custom. RSMQ is a simpler FIFO option [7].

- Batching
  - Prefer application‑level batching tasks (e.g., a “BatchProcess” job) when upstream allows. For Postgres queues, fairness and priority are preserved even when tasks batch internally.

- Observability
  - Use `tracing` and metrics (Prometheus) exposed by frameworks (Apalis has optional metrics; Graphile Worker has event hooks) [3][6][10].

---

## Candidate crates and fit

1) Graphile Worker RS (graphile_worker crate)
- What it is: Rust port of Graphile Worker, a proven Postgres‑backed job queue. Uses `SKIP LOCKED` and `LISTEN/NOTIFY` for low‑latency dispatch; supports priority, scheduled/cron jobs, retries/backoff, dedup via job keys, named queues, and per‑queue concurrency techniques [8][9][10].
- Why it fits: Meets durability, priority, run_at (deadlines), retries/backoff, multi‑queue needs with minimal glue; integrates with `sqlx` in examples [8][9].
- Notes: Priority is a first‑class concept in Graphile Worker (lower numeric = higher priority) [10].

2) Apalis (multi‑backend job processing)
- What it is: Simple, extensible background jobs for Rust; supports Postgres/SQL via apalis‑sql and Redis via apalis‑redis; Tower middleware; cron; retries and backoff; 0.7 adds SQL‑backend priority; runtime‑agnostic (Tokio supported) [1][3][4][5][6].
- Why it fits: If we want a uniform abstraction across Postgres and Redis, Apalis provides it. Good ergonomics and examples; widely used.
- Notes: Recent releases standardized backend behavior and added priority for SQL backends (aligns with our priority requirement) [1].

3) sqlxmq
- What it is: Job queue built on `sqlx` + Postgres; jobs defined with a `#[job]` macro; supports transactions, two‑phase commit, checkpoints, channels; emphasizes backup/consistency benefits of DB‑resident jobs [13][17].
- Why it fits: Postgres‑only, `sqlx`‑first design; great if we want macro‑based job definitions and transactional integration.

4) Underway
- What it is: Postgres + `sqlx` background jobs with multi‑step workflows, retries, cron‑like scheduling; queues/workers/scheduler abstractions; MIT/Apache‑2.0 [22].
- Why it fits: Postgres‑only option with explicit job‑step workflows; newer but aligns with our stack.

5) background_jobs
- What it is: Background jobs framework with adapters (Postgres, Sled), retries/backoff, Actix/Tokio runners [19].
- Considerations: License is AGPL‑3.0, which may be incompatible with our needs; otherwise featureful.

6) PGMQ (+ pgmq Rust client)
- What it is: Postgres extension‑based message queue; SQS‑like; provides Rust SDK; supports visibility timeouts, archiving [11][12].
- Considerations: Requires installing a PG extension; more ops surface area; probably heavier than we want for “not a giant system.”

7) Redis options
- rsmq_async (RSMQ for Redis): simple FIFO with visibility timeout; exactly‑one consumer within VT window [7].
- Redis Streams via `redis`/`fred` crates: durable stream + consumer groups; reclaim stuck messages; works well at scale; priority needs multi‑stream or ZSET design [14][15][16].

8) Other Postgres‑based crates
- graphile_worker_rs (library crate docs) [9], postgres_queue (simpler PG queue) [18], plus articles showing SKIP LOCKED implementations [18][20][21].

Summary of fit vs requirements
- Priority + insert at front: Graphile Worker RS (native) [10]; Apalis (SQL backends) [1]; sqlxmq/Underway can model via `priority` + `run_at` in schema.
- Deadlines/scheduling: `run_at` and cron in Graphile Worker RS and Apalis [6][9].
- Retries/backoff + DLQ: Built‑in/backoff across candidates; DLQ can be modeled by “max attempts → failed table/queue” [8][9][11][12][19].
- Multi‑queue, per‑queue limits: Named queues and per‑worker concurrency (Graphile Worker techniques), Tower middleware in Apalis [6][10].
- Postgres/Redis backends: Graphile Worker is PG‑only; Apalis supports both; sqlxmq/Underway are PG‑only.
- Testing: All PG options are compatible with live Docker Postgres and `sqlx` migrations.

---

## Recommended architecture

Postgres‑first design with Graphile Worker RS

- Producers (any service) enqueue jobs with fields: task identifier, payload, `priority` (smaller = higher), `run_at` (schedule/deadline), `max_attempts`, optional `job_key` for dedup.
- Storage: Postgres schema managed by Graphile Worker migrations; uses `SKIP LOCKED` and `LISTEN/NOTIFY` for efficient dispatch [8][9].
- Workers: N Rust processes using Tokio; concurrency tuned per queue; per‑queue backpressure via semaphores; retries with exponential backoff; on `max_attempts` exceeded, mark and route to DLQ table/queue for operator actions.
- Multiple queues: Use named queues for isolation and per‑queue concurrency; dedicate workers where needed for strict limits [10].
- “Insert at front”: set a very high priority (lowest numeric) and/or set `run_at = now()` to jump the line [10].
- Deadlines: map to `run_at` for scheduling; optionally store a hard `deadline` and add monitoring to alert when overdue.
- Batching: model as explicit batch jobs or let a handler drain a finite number of ready items per invocation.
- Observability: tracing spans around claim/exec/ack; metrics (through Graphile Worker events or custom counters). 
- Testing: integration tests spin up Postgres in Docker; migrations applied; each test wraps in a transaction and rolls back.

Why this choice
- Minimal glue, strong feature coverage (priority, run_at, retries/backoff, DLQ strategy, multi‑queue, low‑latency) [8][9][10].
- Aligns with `sqlx`, `tokio`, and our Docker Postgres test posture.
- Avoids introducing/kicking off a “giant system,” while leveraging mature, maintained OSS.

Alternative: Apalis abstraction (PG and/or Redis)
- If we want a single API across PG and Redis, or want Tower middleware across the job pipeline, Apalis is attractive (cron, retry layers, filtering, priority for SQL backends) [1][3][6].
- We would still implement DLQ routing and per‑queue policies with application code and Apalis storage APIs.

Fallbacks or specialized needs
- If we need transactional coupling/2PC and checkpoints: sqlxmq [13][17].
- If we need Redis for very high throughput or operational reasons: Redis Streams with consumer groups via `redis`/`fred` [14][16] or Apalis‑Redis [4].

---

## Implementation outline (no code)

1) Choose backend and crate
- Default: Graphile Worker RS (Postgres) [8][9].
- Keep Apalis or sqlxmq as secondary paths should requirements evolve.

2) Define job types and queues
- Identify task identifiers and per‑queue policies (concurrency, rate limits, backoff, max_attempts, priority bands).
- Standardize payload schema and idempotency keys (job_key) for dedup on enqueue.

3) Enqueue API (producer side)
- Library function to enqueue with: payload, queue name, priority, run_at, max_attempts, job_key.
- “Insert at front” implemented as priority override and `run_at = now()`.

4) Worker process
- Concurrency per queue via semaphore; bounded parallelism.
- Retry with exponential backoff; classify errors as retryable/fatal; on `max_attempts`, send to DLQ.
- Optional batching: certain handlers aggregate multiple items.

5) DLQ and operations
- Model DLQ as a separate queue/table; include last_error, attempts, timestamps.
- Provide admin methods to re‑enqueue or purge after fixes.

6) Observability & SLOs
- tracing spans around claim/execute/ack; Prometheus counters/histograms for latency, attempts, failures.

7) Testing
- Integration tests against live Docker Postgres; migrations applied; each suite uses transactions for cleanup.

---

## Testing strategy

- Start Postgres in Docker in CI; run migrations for the chosen crate (Graphile Worker RS) [8][9].
- For each test case:
  - Begin a transaction.
  - Enqueue jobs with varying priorities and run_at; start a worker with limited concurrency.
  - Assert ordering (priority/run_at), retry behavior (exponential backoff), DLQ routing on `max_attempts`.
  - Verify per‑queue concurrency limits and backpressure.
  - Roll back the transaction to clean up.

---

## What good solutions look like

- Jobs are idempotent and carry idempotency keys for deduplication when possible.
- Priority and scheduling are explicit (`priority`, `run_at`), and workers consistently order by these.
- Retries use exponential backoff; operators can see last_error/attempts and re‑enqueue from DLQ.
- Per‑queue concurrency is tuned to protect downstreams; rate limiting/backpressure enforced centrally.
- Observability provides visibility into lag (now − run_at), success/failure rates, attempt distributions, and worker health.
- Integration tests exercise real Postgres, covering backfill volume scenarios and steady‑state.

---

## Recommendation

- Adopt Graphile Worker RS on Postgres for the first iteration. It satisfies our requirements out of the box, aligns with `sqlx`/Tokio, and is proven in production designs [8][9][10].
- Keep Apalis as a Plan B if we foresee value in a single abstraction across PG and Redis, or we want Tower middleware end‑to‑end [1][3][6].
- If deep transaction coupling or job checkpoints become primary needs, consider migrating specific job types to sqlxmq [13][17].

---

## Sources

[1] “Introducing apalis v0.7!” (priority for SQL backends) – Reddit announcement
https://www.reddit.com/r/rust/comments/1jjebum/introducing_apalis_v07/

[2] Apalis GitHub repository
https://github.com/geofmureithi/apalis

[3] Apalis crate docs (features, Tower, retries)
https://docs.rs/apalis/latest/apalis/

[4] apalis-redis crate docs
https://docs.rs/apalis-redis

[5] Blog: Background job processing with Rust using Actix + Redis (Apalis)
https://mureithi.me/blog/background-job-processing-with-rust-actix-redis

[6] apalis-cron crate docs
https://docs.rs/crate/apalis-cron/latest

[7] rsmq_async (Redis Simple Message Queue for Rust) docs
https://docs.rs/rsmq_async

[8] graphile_worker (Rust) crate docs summary (SKIP LOCKED, LISTEN/NOTIFY, retries)
https://docs.rs/graphile_worker

[9] Graphile Worker RS GitHub (features, sqlx examples)
https://github.com/leo91000/graphile_worker_rs

[10] Graphile Worker docs – jobs view (priority field; lower = higher priority) and techniques
https://worker.graphile.org/docs/jobs-view
https://worker.graphile.org/docs/techniques

[11] PGMQ GitHub (Postgres message queue extension)
https://github.com/pgmq/pgmq

[12] pgmq crate on crates.io
https://crates.io/crates/pgmq

[13] sqlxmq crate docs
https://docs.rs/sqlxmq

[14] redis crate streams API
https://docs.rs/redis/latest/redis/streams

[15] redis-streams crate
https://docs.rs/redis-streams

[16] fred (advanced Redis client) docs (streams interface feature)
https://docs.rs/fred/latest/fred/

[17] sqlxmq docs – jobs, checkpoints, transactions
https://docs.rs/sqlxmq/latest/sqlxmq/

[18] Example SKIP LOCKED queue patterns (postgres_queue crate; blog with schema/queries)
https://docs.rs/postgres_queue
https://artur.bearblog.dev/background-jobs-without-redis-postgres-skip-locked-in-production/

[19] background_jobs crate docs (Postgres/Sled adapters, retries/backoff)
https://docs.rs/background-jobs

[20] Neon guide – Queue System using SKIP LOCKED
https://neon.com/guides/queue-system

[21] Medium: Master concurrent queues with SKIP LOCKED
https://medium.com/@kanishksinghpujari/master-concurrent-queues-with-skip-locked-boost-your-systems-performance-46de4bfb0511

[22] Underway crate docs (Postgres + sqlx job steps, cron)
https://docs.rs/underway/latest/underway/
