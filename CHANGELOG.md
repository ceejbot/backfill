# Changelog

All notable changes to the Backfill project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project will adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) after reaching version 1.0.0.

## [2.0.0] — Deprecation cleanup

Removes the API surface that was marked `#[deprecated(since = "1.2.0")]` in
the previous release. No behavioural changes — every removed item was
already non-functional or duplicative; the deprecation warnings in 1.2.0
were the upgrade signal.

### Removed (BREAKING)

**`RetryPolicy` math methods** — graphile_worker schedules retries via a
fixed `exp(min(attempts, 10))`-second SQL formula, so these never reached
the worker:

- `RetryPolicy::new(max, initial, max_delay, multiplier)` — multi-arg
  constructor. Replace with `RetryPolicy { max_attempts: n,
  ..Default::default() }` or one of the presets.
- `RetryPolicy::with_jitter(f64)` — replace with: drop the call (had no
  runtime effect).
- `RetryPolicy::calculate_delay(attempt)` — replace with: drop the call.
- `RetryPolicy::calculate_retry_time(attempt, base)` — replace with: drop.
- `JobSpec::calculate_retry_time(attempt, failed_at)` — replace with: drop.

The `RetryPolicy` struct itself is preserved with all five fields. Only
`max_attempts` is honored; the other fields document themselves as
not-honored. Keeping the shape leaves room for upstream graphile_worker to
expose per-job timing config without another API break. Presets (`fast`,
`aggressive`, `conservative`) and the JobSpec retry builders
(`with_fast_retries` / `with_aggressive_retries` / `with_conservative_retries`)
are unchanged — they're cheap convenience for setting `max_attempts`.

**`QueueConfig` multi-queue API** — graphile_worker doesn't expose
per-worker queue filtering, so the multi-config shape was always a lie:

- `pub struct QueueConfig` — removed entirely.
- `QueueConfig::default_queue` / `named_queue` / `priority_queue` — gone
  with the struct.
- `WorkerConfig::with_queues(Vec<QueueConfig>)` — replace with
  `WorkerConfig::with_concurrency(n)`.
- `WorkerConfig.queue_configs: Vec<QueueConfig>` field — replaced with
  `WorkerConfig.concurrency: usize`. Set it via `with_concurrency` or
  struct-literal init. To run multiple specialized workers, spawn multiple
  `WorkerRunner` instances yourself.
- `WorkerOptionsBuilder.queue_name` field and `with_queue_name` method —
  removed (the value was never propagated anywhere it would have effect).
- The internal "Queue name configuration is not supported" WARN log —
  removed (now unreachable).

**Migration for the `named_queue` use case**: per-job queue routing
remains available at enqueue time via `Queue::serial(name)` and
`Queue::serial_for(entity, id)`. That was always the right tool; the
`QueueConfig::named_queue` constructor was a misleading second path that
silently delivered serial-by-default behaviour to users who just wanted
a label.

### Tests

- 108 (was 113 in 1.2.0). Five removed: three for the now-deleted
  `QueueConfig` constructors, one for the "multi-queue first-config
  wins" behaviour that no longer exists, one in-source unit test for
  the deleted `RetryPolicy` math.

## [1.2.0] — Production-readiness audit

A focused bug-fix release driven by a top-to-bottom production-readiness
review of the priority queue, retry, and DLQ subsystems. Several silent
failure modes are corrected, several misleading APIs are deprecated with
migration paths, and two long-tail correctness bugs in the DLQ subsystem
are fixed. Every fix is covered by a regression test that fails on the
prior code.

### Critical fixes

- **DLQ no longer loses jobs on worker restart.** `WorkerRunner` startup
  used to call `cleanup_permanently_failed_jobs()` (which `DELETE`s rows
  with `attempts >= max_attempts`) before the DLQ processor's first tick
  — any job that hit max_attempts while the worker was down was silently
  deleted instead of captured. The startup sequence now runs
  `process_failed_jobs()` synchronously before cleanup whenever the DLQ
  is enabled.
- **`WorkerError::is_retryable()` is now wired to actual retry behaviour.**
  The classification was previously dead code; a `WorkerError::ValidationFailed`
  would retry up to `max_attempts` instead of going straight to DLQ.
  A new `PermanentFailurePlugin` (auto-registered when DLQ is enabled)
  hooks `JobFail`, classifies the error, and short-circuits retries for
  non-retryable variants by setting `attempts = max_attempts`.
- **`process_failed_jobs` now atomically moves rows from `_private_jobs` to
  the DLQ.** Previously the UPSERT and DELETE were separate statements; a
  crash between them could duplicate the job. Combined into a single
  writable-CTE statement.

### Breaking changes

- `WorkerRunner::process_available_jobs` returns `Result<(), BackfillError>`
  (was `Result<usize, BackfillError>` always returning `Ok(0)`). For
  job-count instrumentation, register a `JobComplete` / `JobFail` plugin
  before building the worker.
- `WorkerRunner::worker_count()` returns `1` truthfully (was
  `queue_configs.len()`, which lied any time multi-queue config was passed
  — only one worker has ever actually been spawned).

### Deprecations

All marked `#[deprecated(since = "1.2.0")]` with migration notes.
Scheduled for removal in `2.0.0`.

- `RetryPolicy::new(4-arg)`, `with_jitter`, `calculate_delay`,
  `calculate_retry_time`, `JobSpec::calculate_retry_time` — graphile_worker
  uses a hard-coded `exp(min(attempts, 10))`-second SQL formula for retry
  scheduling, so the timing fields on `RetryPolicy` and the math helpers
  that operated on them never reached the worker. Only `max_attempts` was
  ever honored. Use `RetryPolicy { max_attempts: n, ..Default::default() }`
  or the `fast` / `aggressive` / `conservative` presets, which now
  differ only in attempt count.
- `WorkerConfig::with_queues`, `QueueConfig::named_queue`,
  `QueueConfig::priority_queue` — graphile_worker doesn't expose
  per-worker queue filtering; only the first config's `concurrency`
  was ever used at runtime, and `priority_range` was never read. Use
  `WorkerConfig::with_concurrency(n)` and route jobs to named queues at
  enqueue time via `Queue::serial(name)`.

### Other fixes and improvements

- `list_dlq_jobs.total` now respects filters (was returning unfiltered
  count regardless of filter, breaking paginated admin UIs).
- `Queue::metric_label()` added — bounded `"parallel"`/`"serial"` label
  for built-in metrics. Built-in metric emission no longer uses
  unbounded queue names (e.g., `Queue::serial_for("user", id)`) as
  Prometheus labels.
- DLQ-side metrics emit `"parallel"` instead of empty string for
  parallel-origin jobs.
- `requeue_dlq_job` no longer fails the operation when the post-enqueue
  bookkeeping `UPDATE` blips; the enqueue (the real intent) succeeded,
  so a stale `requeued_count` is logged at `WARN` rather than propagated
  as an error.
- DLQ processor uses exponential backoff on consecutive errors
  (interval, 2x, 4x, … capped at 32x). Resets on success.
- `process_failed_jobs` switched from `id NOT IN (subquery)` to
  `NOT EXISTS` for the DLQ-membership check — better planner behaviour
  and no NULL-poisoning workaround needed.
- `failure_count` on DLQ rows is now a clean touch counter (1 on first
  DLQ landing, +1 each subsequent UPSERT) instead of cumulative
  handler-attempts.
- `cleanup_permanently_failed_jobs` now logs at `WARN` when DLQ is
  disabled (those jobs cannot be recovered) and `INFO` when DLQ exists.
- `delete_dlq_job` halved its DB round-trips (single `DELETE … RETURNING`).
- `enqueue` records duration on every outcome, not just success.
- Stale-lock SQL parameterized instead of format-interpolating timeout.
- `enqueue_emergency` no longer redundantly sets `run_at = NOW()`.

### Documentation

- Truthful `RetryPolicy` rustdoc — explicit about which fields graphile_worker
  honors and which are stored-but-ignored.
- `EnqueueOutcome::AlreadyInProgress` rustdoc rewritten to flag the
  footgun of unconditionally `.unwrap()`ing.
- `docs/01-database-setup.md` clarifies that backfill itself uses runtime
  SQLx queries; the offline-mode setup is for *user* queries.
- `docs/02-dlq.md` "queue_name shows as default" warning marked resolved
  (was fixed in 1.1.1 / PR #9, docs were stale).
- `Cargo.toml` warns about the `_private_*` schema coupling so
  `cargo update` doesn't silently break across graphile_worker patch
  releases.
- `lib.rs` documents which `graphile_worker` types are re-exported and
  the SemVer implications of upstream changes.

### Internals

- Removed orphan `src/client/queries.rs` (was never `mod`-declared).
- Removed empty `.sqlx/sqlx-data.json` (compile-time query macros aren't
  used).

### Tests

- Test count: 107 → 115 (+8). New regression tests for the startup race,
  the retryable / non-retryable plugin behaviour, the filtered-pagination
  fix, the failure_count touch counter, the bounded metric label, an
  end-to-end retry-to-DLQ path through the actual worker, and a
  concurrent-enqueue stress test.
- All P0 and P1 fixes were verified to *fail* on the prior code via
  `git stash` / re-run before being committed.

## [Unreleased]

### Breaking Changes
- **Queue API redesigned for parallel-by-default execution**
  - Removed `Queue::Fast`, `Queue::Bulk`, `Queue::DeadLetter`, `Queue::Custom` variants
  - Added `Queue::Parallel` (now the default) and `Queue::Serial(String)`
  - Jobs now execute **in parallel** by default across all workers
  - Use `Queue::serial("name")` or `Queue::serial_for("entity", id)` when you need serialization

  **Migration guide:**
  ```rust
  // Before (v1.x): Named queues caused unintended serialization
  JobSpec { queue: Queue::Fast, .. }      // ALL "fast" jobs ran one at a time!
  JobSpec { queue: Queue::Bulk, .. }      // ALL "bulk" jobs ran one at a time!
  JobSpec { queue: Queue::Custom("x".into()), .. }

  // After (v2.x): Parallel by default, explicit serialization
  JobSpec { queue: Queue::Parallel, .. }  // Jobs run concurrently (default)
  JobSpec { queue: Queue::serial("rate-limit-api"), .. }  // Explicit serialization
  JobSpec { queue: Queue::serial_for("user", user_id), .. }  // Per-entity serialization
  ```

### Added
- `Queue::serial(name)` - create a named serial queue
- `Queue::serial_for(entity, id)` - create per-entity serial queues (e.g., "user:123")
- `Queue::is_parallel()` and `Queue::is_serial()` helper methods
- `enqueue_serial()` convenience function for explicit serial execution
- Admin API endpoints for lock diagnostics: `GET /locks/status`, `POST /locks/cleanup`
- Docker Compose configuration for test database
- Comprehensive queue behavior tests (parallel vs serial execution)
- Implemented `WorkerRunner::process_available_jobs()` for batch processing and testing scenarios
- Comprehensive Dead Letter Queue (DLQ) system with full CRUD operations
- DLQ processor for automatic migration of failed jobs to the DLQ table
- Admin HTTP API with DLQ management endpoints (experimental, feature-gated behind `axum`)
- Exponential backoff retry system with jitter to prevent thundering herds
- Three preset retry policies: `fast()`, `aggressive()`, `conservative()`
- Priority-based job scheduling with six priority levels
- Comprehensive metrics using the `metrics` facade crate
- Structured logging via the `log` crate
- Complete documentation suite (8 major guides + examples)
- Five working examples demonstrating key usage patterns
- Integration test suite with isolated schema testing pattern (55 tests, 74.62% coverage)
- Code coverage reporting in CI with badges
- SQLx compile-time query verification

### Changed
- Switched from `tracing` to `log` crate for logging (tracing still used for instrumentation)
- Metrics now use Prometheus-compatible naming with underscores

### Fixed
- Enforced `#![deny(clippy::unwrap_used)]` to eliminate `.unwrap()` calls
- Fixed admin_server example compilation with feature gates

### Documentation
- Added API status table to Admin API docs documenting experimental endpoints
- Added Known Limitations section to DLQ guide
- Documented queue name and payload visibility limitations
- Added comprehensive testing guide with isolated schema pattern
- Created CLAUDE.md for AI pair programming guidance
- Added DLQ migrations guide
- Added metrics integration guide

### Security
- Added `cargo-audit` for vulnerability checking in CI
- No `unsafe` code allowed (`#![deny(unsafe_code)]`)

## [0.1.0] - UNRELEASED

Initial development version. Not yet recommended for production use.

### Core Features
- PostgreSQL-backed async job queue using GraphileWorker
- Job enqueueing with priorities, queues, and scheduling
- Worker runner with graceful shutdown and background task patterns
- Dead Letter Queue for failed job management
- Retry policies with exponential backoff
- Metrics and observability support

---

## Release Checklist for 1.0.0

Before releasing 1.0.0, the following items should be completed:

- [ ] Finalize Admin API (complete stub endpoints or mark as experimental)
- [ ] Resolve DLQ queue_name tracking limitation
- [ ] Resolve DLQ payload visibility limitation (or document workaround)
- [ ] Complete documentation restructuring (quick start → tutorials → reference)
- [ ] Production testing in real workloads
- [ ] Performance benchmarking and tuning
- [ ] Security audit
- [ ] Publish to crates.io

---

For the complete project history and detailed commit messages, see the Git log:
```bash
git log --oneline --graph
```
