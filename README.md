# backfill

[![CI](https://github.com/ceejbot/backfill/workflows/CI/badge.svg)](https://github.com/ceejbot/backfill/actions)
[![Security](https://github.com/ceejbot/backfill/actions/workflows/security.yml/badge.svg)](https://github.com/ceejbot/backfill/actions/workflows/security.yml)

A boringly-named priority queue system for doing async work. This library and work process wrap the the [graphile_worker crate](https://lib.rs/crates/graphile_worker) to do things the way I want to do them. It's unlikely you'll want to do things exactly this way, but perhaps you can learn by reading the code, or get a jumpstart by borrowing open-source code, or heck, maybe this will do what you need.

## What it does

This is a postgres-backed async work queue library that is a set of conveniences and features on top of the rust port of Graphile Worker. It gives you a library you can integrate with your own project to handle background tasks.

> **Status**: Core features (job enqueueing, worker processing, DLQ
> management, lifecycle plugins, stale-lock cleanup) are complete and
> covered by an integration test suite. The Admin API (feature-gated
> behind `axum`) is also stable, except for two stub endpoints —
> `GET /jobs/{id}` and `DELETE /jobs/{id}/cancel` — that return 501. See
> [CHANGELOG.md](CHANGELOG.md) for details and [Known
> Limitations](docs/02-dlq.md#known-limitations).

### What's New Over graphile_worker

Built on top of `graphile_worker` (v0.11.x), backfill adds these production-ready features:

- 🎯 **Priority System** — Six-level priority enum (EMERGENCY=-20 down through
  BULK_LOWEST=10), mapped through to graphile_worker's `priority asc` fetch
  ordering.
- 📦 **Parallel + Serial queues** — `Queue::Parallel` (default, jobs run
  concurrently across workers) or `Queue::Serial(name)` (one-job-at-a-time
  per named queue, for rate limiting or per-entity ordering).
- 🔄 **Retry policy presets** — `fast` / `aggressive` / `conservative`
  presets that differ in `max_attempts`. Note: backoff *timing* is fixed by
  graphile_worker (see [`docs/02-dlq.md`](docs/02-dlq.md)) — only the
  attempt count is configurable.
- 💀 **Dead Letter Queue (DLQ)** — Automatic failed-job handling with
  query/requeue/deletion APIs. Includes a permanent-failure short-circuit
  plugin: handlers that return non-retryable `WorkerError` variants land in
  the DLQ on the first failure rather than waiting for `max_attempts` to
  exhaust.
- 📊 **Comprehensive Metrics** — Prometheus-compatible metrics for jobs,
  DLQ, and database operations.
- 🛠️ **High-Level Client API** — `BackfillClient` with ergonomic enqueueing
  helpers.
- 🏃 **Flexible Worker Patterns** — `WorkerRunner` supporting
  `tokio::select!`, background tasks, and one-shot processing.
- 🔧 **Admin API** — Optional Axum router for HTTP-based job management
  (experimental).
- 📝 **Convenience Functions** — `enqueue_fast()`, `enqueue_bulk()`,
  `enqueue_critical()`, etc.
- 🧹 **Stale Lock Cleanup** — Automatic cleanup of orphaned locks from
  crashed workers (startup + periodic). Ordered correctly with the DLQ
  scanner so failed jobs aren't lost across restarts.

All built on graphile_worker's rock-solid foundation of PostgreSQL SKIP LOCKED and LISTEN/NOTIFY.

### Features

- **Priority queues**: EMERGENCY (-20), FAST_HIGH (-10), FAST_DEFAULT (-5),
  BULK_DEFAULT (0), BULK_LOW (5), BULK_LOWEST (10) — lower number = higher
  priority.
- **Queue types**: `Queue::Parallel` (default), `Queue::Serial(name)` — plus
  `Queue::serial_for(entity, id)` for per-entity ordering.
- **Scheduling**: Immediate or delayed execution with `run_at`.
- **Idempotency**: Use `job_key` for deduplication.
- **Retries**: Configurable `max_attempts` per job; graphile_worker handles
  the exponential-backoff schedule (`exp(min(attempts, 10))` seconds, capped
  at ~6h per retry).
- **Dead letter queue**: Automatic capture of jobs that exceed their retry
  limits or return non-retryable errors. Includes a synchronous startup
  pre-move so DLQ doesn't lose jobs across worker restarts.
- **Error classification**: `WorkerError` variants split into retryable and
  non-retryable; non-retryable errors short-circuit retries to DLQ via an
  auto-registered lifecycle plugin.
- **Metrics**: Comprehensive metrics via the `metrics` crate — bring your
  own exporter (Prometheus, StatsD, etc.).
- **Logging**: Structured logging via the `log` crate facade — bring your
  own sink (`env_logger`, `tracing-log`, etc.).
- **Building blocks for an axum admin api**: via a router you can mount on
  your own axum api server.

Look at the `examples/` directory and the readme there for practical usage examples.

## Documentation

Read these in order for the best learning experience:

1. **[Database Setup](docs/01-database-setup.md)** - PostgreSQL configuration and automatic schema management
2. **[Dead Letter Queue (DLQ)](docs/02-dlq.md)** - Comprehensive guide to handling failed jobs:
   - How the DLQ works and why it's essential
   - Client API and HTTP admin API usage
   - Operational best practices for production
   - Monitoring, alerting, and troubleshooting
   - Common workflows for handling failures
3. **[Metrics Guide](docs/03-metrics.md)** - Comprehensive metrics for Prometheus, StatsD, and other backends
4. **[Admin API Reference](docs/04-admin-api.md)** - HTTP API for job management and monitoring (experimental)
5. **[Testing Guide](docs/05-testing.md)** - Testing strategies for workers and jobs with isolated schemas
6. **[DLQ Migrations](docs/06-dlq-migrations.md)** - Migration strategies for the DLQ schema in production

## Configuration and setup

The library itself takes configuration through [`WorkerConfig`](#workerconfig-options)
— there are no implicit environment variables. The bundled examples
(`examples/basic_worker.rs` etc.) read a few env vars on top of that for
convenience:

- `DATABASE_URL`: PostgreSQL connection string
- `RUST_LOG`: log level for `env_logger` (e.g., `info`, `backfill=debug`)

### WorkerConfig Options

When building a `WorkerRunner`, you can configure additional options:

```rust
use std::time::Duration;
use backfill::{WorkerConfig, WorkerRunner};

let config = WorkerConfig::new(&database_url)
    .with_schema("graphile_worker")           // PostgreSQL schema (default)
    .with_poll_interval(Duration::from_millis(200))  // Job polling interval
    .with_dlq_processor_interval(Some(Duration::from_secs(60)))  // DLQ processing
    // Stale lock cleanup configuration
    .with_stale_lock_cleanup_interval(Some(Duration::from_secs(60)))  // Periodic cleanup
    .with_stale_queue_lock_timeout(Duration::from_secs(300))   // 5 min (queue locks)
    .with_stale_job_lock_timeout(Duration::from_secs(1800));   // 30 min (job locks)

let worker = WorkerRunner::builder(config).await?
    .define_job::<MyJob>()
    .build().await?;
```

#### Stale Lock Cleanup

When workers crash without graceful shutdown, they can leave locks behind that prevent jobs from being processed. Backfill automatically cleans these up:

- **Startup cleanup**: Runs when the worker starts
- **Periodic cleanup**: Runs every 60 seconds by default (configurable)

**Configuration options:**

| Option | Default | Description |
|--------|---------|-------------|
| `stale_lock_cleanup_interval` | 60s | How often to check for stale locks. Set to `None` to disable periodic cleanup. |
| `stale_queue_lock_timeout` | 5 min | Queue locks older than this are considered stale. Queue locks are normally held for milliseconds. |
| `stale_job_lock_timeout` | 30 min | Job locks older than this are considered stale. **Set this longer than your longest-running job!** |

**⚠️ Warning:** Setting `stale_job_lock_timeout` too short can cause duplicate job execution if jobs legitimately run longer than the timeout. This can lead to data corruption.

### SQLx usage

Backfill currently uses runtime SQLx queries (`sqlx::query()` /
`sqlx::query_scalar()`) rather than the compile-time-checked
`sqlx::query!()` / `query_as!()` macros. No `DATABASE_URL` is required at
compile time, and there's no `.sqlx/` metadata cache to maintain. Schema
errors surface at runtime (caught by the integration test suite).

If you write your own SQLx queries against backfill's tables in *your*
application, the compile-time macros are a great fit — see
[Database Setup](docs/01-database-setup.md) for `cargo sqlx prepare`
guidance.

### Automatic Setup

The `graphile_worker` crate sets up all its database tables with no action needed if the database user has create table permissions. The library can also automatically create the DLQ schema:

```rust
use backfill::BackfillClient;

// Default schema ("graphile_worker"):
let client = BackfillClient::new("postgresql://localhost/mydb").await?;

// Custom schema:
let client = BackfillClient::new_with_schema(
    "postgresql://localhost/mydb",
    "my_schema",
).await?;

client.init_dlq().await?;  // Creates DLQ table if needed
```

For production environments with controlled migrations, use the provided SQL files:

```bash
# Using the default graphile_worker schema
psql -d your_database -f docs/dlq_schema.sql

# Using a custom schema name
sed 's/graphile_worker/your_schema/g' docs/dlq_schema.sql | psql -d your_database
```

See [DLQ Migrations](docs/06-dlq-migrations.md) for detailed migration instructions and integration with popular migration tools.

## LICENSE

This code is licensed via [the Parity Public License.](https://paritylicense.com) This license requires people who fork and change this source code to share their work with the community, too. Either contribute your work back as a PR or make your forked repo public. Fair's fair! See the license text for details.
