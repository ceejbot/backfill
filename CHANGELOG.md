# Changelog

All notable changes to the Backfill project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project will adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) after reaching version 1.0.0.

## [3.0.0] — Public surface cleanup, doc overhaul, exemplary examples

A polish-and-tighten release. No behavioural changes; all the breaks
are simplifications of the public API surface and removals of
example-side concerns that had leaked into library types. Most callers
will need no migration.

### Removed (BREAKING)

**`RetryPolicy` reduced to one field.** The four backoff-timing fields
(`initial_delay`, `max_delay`, `backoff_multiplier`, `jitter_factor`)
were stored but never honored — graphile_worker uses a fixed
`exp(min(attempts, 10))`-second SQL schedule that ignores them.
`max_attempts` is the only field that ever reached the worker. The
struct is now a one-field newtype around `max_attempts`. The
`fast()` / `aggressive()` / `conservative()` presets are unchanged
in behaviour and continue to be the recommended call sites.

Migration:

```rust
// Before (2.x):
RetryPolicy { max_attempts: 6, ..Default::default() }
// After (3.x):
RetryPolicy { max_attempts: 6 }
```

**`BackfillError` shrunk from 17 → 9 variants.** Removed:
`BindError`, `FastQueueParseInt`, `BulkQueueParseInt`,
`DeadLetterParseInt`, `PollIntervalParseInt`, `ShutdownTimeoutParseInt`,
`DlqProcessorIntervalParseInt`. None of these had constructors in `src/`
— they existed because `examples/basic_worker.rs` was using
`BackfillError` for its own env-var parsing failures. The example now
defines a local `ExampleConfigError` enum, which is the pattern users
should copy for their own configuration code. If you were pattern-matching
on any of these variants, define your own error type for env-var parsing
and route library errors via `BackfillError::ParseInt(_)` (which is
unchanged).

**`WorkerOptionsBuilder` is now `pub(crate)`.** It only existed as a
clone-able snapshot of `graphile_worker::WorkerOptions` (which is
`!Clone`); it was never a callable extension point. Configure the
worker via `WorkerRunnerBuilder` instead — its public surface is
unchanged. The comprehensive cron-schedule rustdoc has moved onto
`WorkerRunnerBuilder::add_cron_schedule`, where users will actually
encounter it.

**Wildcard re-exports replaced with explicit lists.** `src/lib.rs`
no longer does `pub use client::*; pub use priorities::*; pub use
retries::*; pub use worker::*;`. The exported names are now
enumerated:

- `client::{BackfillClient, DlqFilter, DlqJob, DlqJobList, DlqStats}`
- `priorities::Priority`
- `retries::RetryPolicy`
- `worker::{WorkerConfig, WorkerRunner, WorkerRunnerBuilder}`

`src/client/mod.rs` similarly replaces `pub use dlq::*` with an explicit
list. Any name that was previously reachable through `backfill::*` only
because of a wildcard, but isn't in the explicit list, is no longer
exported. The motivation is to stop new `pub` items in those modules
from auto-joining the public API by accident.

**Direct-dependency hygiene.** Three deps moved or removed:

- `graphile_worker_lifecycle_hooks` removed from `[dependencies]`. It
  was never directly imported in `src/`; the hook types reach us
  transitively via `graphile_worker`. If you depended on
  `graphile_worker_lifecycle_hooks` types being available in your
  build's dep graph just because backfill listed it, declare the dep
  yourself.
- `tracing` and `tracing-subscriber` moved from `[dependencies]` to
  `[dev-dependencies]`. They're only used by examples now (admin.rs
  switched to `log` per CLAUDE.md's logging policy). Same caveat as
  above: declare these in your own `[dependencies]` if you were
  relying on transitive availability.

### Added

- `rust-version = "1.85"` declared in `Cargo.toml`. This reflects the
  edition-2024 floor; not a behaviour change, but tooling that respects
  MSRV will now use it.
- `keywords = ["queue", "jobs", "postgresql", "graphile-worker", "async"]`
  for crates.io discoverability.
- `JobSpec → GraphileJobSpec` conversion now `log::warn!`s when
  `max_attempts` clamps into `i16` range. Previously silent.

### Changed (non-breaking)

- `categories` refined from `["concurrency", "data-structures"]` to
  `["concurrency", "database"]` — better fit for a Postgres-backed
  queue.
- `src/admin.rs` switched from `tracing::{error, info, warn}` to
  `log::{error, info, warn}`, matching the rest of the library and
  CLAUDE.md's stated policy.
- CI: `cargo-audit` bumped from `0.21.2` to `0.22.1` in the security
  workflow. 0.21.2 was choking on CVSS 4.0 advisories
  (e.g. `RUSTSEC-2026-0073`); 0.22 added CVSS 4.0 parsing.

### Documentation

A comprehensive sweep — every user-facing markdown doc was audited
against the actual API and rewritten where it had drifted.

- **`docs/03-metrics.md`** — full rewrite. Documents only the metrics
  the library actually emits (with correct `"parallel"`/`"serial"`
  bounded queue labels). The plugin example uses the real
  `Plugin` + `HookRegistry::on(Event, handler)` API matching
  `examples/metrics_plugin.rs` (was based on a non-existent
  `LifecycleHooks` trait).
- **`docs/07-plugins.md`** — full rewrite for the same reason. Now
  covers observer events (`JobStart`, `JobComplete`, `JobFail`, …)
  and interceptor events (`BeforeJobRun`, `AfterJobRun`,
  `BeforeJobSchedule`) with their real `HookResult` /
  `JobScheduleResult` return types. Documents the auto-registered
  `DlqCleanupPlugin` and `PermanentFailurePlugin`.
- **`docs/04-admin-api.md`** — endpoint table rebuilt from
  `src/admin.rs`'s actual route list. Now correctly lists
  `/dlq/batch-delete`, `/locks/status`, `/locks/cleanup`; corrects
  `/dlq/{id}` paths; updates implementation status (most endpoints
  are stable, two `/jobs/{id}` endpoints remain stubs). Removed a
  stale `/Users/ceej/...` absolute path.
- **`docs/02-dlq.md`** — fixed two `WorkerConfig` examples that used
  the long-removed `QueueConfig::named_queue`; fixed broken cross-references.
- **`docs/01-database-setup.md`, `docs/05-testing.md`,
  `docs/06-dlq-migrations.md`** — `BackfillClient::new` signature
  fixes, stale test counts removed, schema column lists matched to
  the actual DDL.
- **`README.md`** — Status callout sharpened (Admin API is "stable
  except for two stubs", not "experimental"); env-var section
  reframed as example-side; logging description corrected.
- **`docs/implementation/incomplete-features.md`** — banner added
  flagging the whole `implementation/` folder as historical archive.
- Two broken intra-doc links in `src/lib.rs` fixed; `cargo doc
  --no-deps -F axum` is now warning-free.

### Examples

`examples/basic_worker.rs` rewritten to demonstrate recommended patterns:

- Local `ExampleConfigError` (thiserror) for env-var parse failures,
  with a structured `InvalidInteger { var, value, source }` variant.
- Single `parse_env<T: FromStr<Err = ParseIntError>>` helper
  replacing six near-identical `.map_err()` closures.
- Single honest `CONCURRENCY` env var replacing the misleading
  `FAST_QUEUE_CONCURRENCY` / `BULK_QUEUE_CONCURRENCY` /
  `DLQ_CONCURRENCY` trio that all collapsed into one number anyway.
- Pinned-future graceful shutdown that actually waits for the worker
  to drain (the previous code awaited `CancellationToken::cancelled()`
  *after* calling `cancel()`, which returned immediately).
- All `tracing::span!` removed; `log` is the only logging facade.

`examples/enqueue_jobs.rs` updated for the slimmer `RetryPolicy`.

### Internals

- Dropped speculative `record_oldest_stale_lock_age` metric helper
  (was `#[allow(dead_code)]` "for future use").
- Dropped unused `JobHandlerConfig.identifier` field.
- Dropped unused `WorkerOptionsBuilder::with_concurrency` (no callers
  after the visibility change).
- DLQ row deserialization de-duplicated: `DlqJob::from_row(&PgRow)`
  helper replaces three copies of a 17-field manual mapping in
  `list_dlq_jobs` / `get_dlq_job` / `add_to_dlq`. Adding a column to
  `backfill_dlq` is now a one-edit change.

### Tests

Test count unchanged at 111 integration + 24 unit + 10 doctests.
Verified green against a live PostgreSQL run before tagging.

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

---

For the complete project history and detailed commit messages, see the Git log:
```bash
git log --oneline --graph
```
