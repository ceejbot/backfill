 # Backfill on Graphile Worker RS — design and implementation plan (no code)

 ## Decision

 - Use Graphile Worker RS (graphile_worker crate) on Postgres as the durable job queue backend.
 - Postgres‑only for v1; no AGPL dependencies.
 - Delivery semantics: at‑least‑once with job idempotency.
 - Deadlines are soft; preferred to run before time but late is acceptable.

 References: priority semantics and architecture from Graphile Worker docs and crate material [1][2][3].

 ---

 ## System components

 - Producers: services enqueue jobs with payload, queue name, priority, run_at, max_attempts, and optional job_key (for deduplication).
 - Storage: Postgres schema managed by Graphile Worker; uses `FOR UPDATE SKIP LOCKED` and `LISTEN/NOTIFY` for low‑latency dispatch [1][2].
 - Workers: multiple Tokio processes consuming named queues with per‑queue concurrency.
 - DLQ: dedicated queue/table for jobs that exceed `max_attempts`, with tooling to inspect and re‑enqueue.

 ---

 ## Queues, priority, and scheduling

 - Queues
   - fast: latency‑sensitive tasks.
   - bulk: backfill and less urgent tasks.
   - dead_letter: failed jobs for operator triage.

 - Priority bands (Graphile Worker: lower number = higher priority) [3]
   - fast: −10 (high), may use −20 for emergency hotfixes.
   - bulk: 0 (default) to +10 for very low urgency.

 - Insert‑at‑front
   - Set priority lower than typical values (e.g., −20) and `run_at = now()`.

 - Scheduling and soft deadlines
   - Use `run_at` to schedule earliest execution time.
   - Add `deadline_at` to payload metadata (soft). Monitor “lag” = now − deadline_at; alert when positive.

 ---

 ## Idempotency and deduplication

 - Use `job_key` to deduplicate enqueues for the same logical work unit (e.g., `entity:{id}:op`).
 - Handlers must be idempotent:
   - Upsert or compare‑and‑set patterns for DB side effects.
   - External calls should carry idempotency keys when supported; otherwise record outgoing request keys to avoid duplicates.

 ---

 ## Retries, backoff, and classification

 - Default `max_attempts`: start with 8 (configurable per task). Exponential backoff (e.g., 1s, 5s, 25s, 2m, 10m, 30m, 2h, 6h).
 - Classify errors:
   - Retryable: transient network/5xx; use backoff.
   - Non‑retryable: validation, 4xx; send directly to DLQ.
 - On `max_attempts` exceeded: mark failed and enqueue metadata to `dead_letter` queue (or move/flag in a DLQ table) with `last_error`, `attempts`, timestamps, and an operator hint.

 ---

 ## Concurrency, rate limiting, and backpressure

 - Per‑queue concurrency limits (e.g., fast: 32, bulk: 8 to protect shared resources).
 - Per‑destination rate limits inside handlers using semaphores or token buckets (e.g., 50 rps to a given API).
 - Optional “tenant fairness”: if needed, use multiple named queues or a per‑tenant shaper (round‑robin enqueue keys) [3].

 ---

 ## Batching patterns

 - Aggregator job: periodic job drains up to N ready items of a type and processes in batch where upstream supports it.
 - Micro‑batching in handlers: process payloads in chunks to balance throughput vs. fairness.

 ---

 ## Observability and operations

 - Metrics
   - queue depth per queue.
   - execution latency (enqueue→start, start→finish), attempts, failure rate.
   - lag vs. run_at and lag vs. deadline_at.
   - DLQ counts by task type and error class.
 - Tracing
   - Span per job with job_key, queue, priority, attempts, and downstream call spans.
 - Runbooks
   - Re‑enqueue from DLQ after remediation.
   - Temporarily increase fast queue concurrency during incidents; decrease bulk to preserve SLOs.

 ---

 ## Testing plan (integration; no mocks)

 - Spin up Postgres in Docker; apply Graphile Worker migrations in schema `graphile_worker` [1][2].
 - Wrap each test group in a transaction and roll back.
 - Test cases
   - Priority ordering and insert‑front behavior (fast job precedes bulk).
   - Retry/backoff progression and DLQ routing upon exceeding `max_attempts`.
   - Soft deadlines respected: earlier `run_at` executes first; verify lag metrics for late jobs.
   - Multi‑queue isolation and per‑queue concurrency limits.
   - Idempotency: duplicate enqueues with same job_key result in single execution.

 ---

 ## Migration & schema

 - Use the Graphile Worker RS migrations and keep them version‑controlled alongside app migrations.
 - Schema name: `graphile_worker`.
 - For DLQ, either:
   - model as a separate named queue (preferred for uniform handling), or
   - maintain an app‑owned `dead_jobs` table for audit/search and tooling.

 ---

 ## Deployment & configuration

 - Environment
   - DATABASE_URL (Postgres).
   - WORKER_CONCURRENCY per queue.
   - MAX_ATTEMPTS default; overridable per task.
 - Packaging
   - Provide a worker binary that initializes the registry, connects to Postgres, and runs the worker pool.
   - One process can handle multiple queues with distinct concurrency settings, or run separate processes per queue for isolation.

 ---

 ## Rollout plan

 1) Stand up Postgres + run Graphile Worker migrations in non‑prod.
 2) Implement minimal producer for one task type and a single worker pool.
 3) Validate ordering, retries, and DLQ in load tests (include backfill burst).
 4) Roll to staging; tune concurrency and backpressure limits.
 5) Gradual production rollout; add dashboards and alerts.

 ---

 ## Open questions

 - Do we need tenant‑level fairness now, or is queue‑level priority sufficient?
 - Any per‑queue SLOs that should gate concurrency changes automatically?

 ---

 ## Sources

 [1] Graphile Worker RS GitHub (features, sqlx examples)
 https://github.com/leo91000/graphile_worker_rs

 [2] graphile_worker crate docs (low‑latency dispatch with SKIP LOCKED/LISTEN‑NOTIFY)
 https://docs.rs/graphile_worker

 [3] Graphile Worker docs — jobs view / techniques (priority semantics; concurrency techniques)
 https://worker.graphile.org/docs/jobs-view
 https://worker.graphile.org/docs/techniques
