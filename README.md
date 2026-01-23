# backfill

[![CI](https://github.com/ceejbot/backfill/workflows/CI/badge.svg)](https://github.com/ceejbot/backfill/actions)
[![Coverage](https://img.shields.io/badge/coverage-64.67%25-yellow)](https://github.com/ceejbot/backfill/actions)
[![Security](https://github.com/ceejbot/backfill/actions/workflows/security.yml/badge.svg)](https://github.com/ceejbot/backfill/actions/workflows/security.yml)

A boringly-named priority queue system for doing async work. This library and work process wrap the the [graphile_worker crate](https://lib.rs/crates/graphile_worker) to do things the way I want to do them. It's unlikely you'll want to do things exactly this way, but perhaps you can learn by reading the code, or get a jumpstart by borrowing open-source code, or heck, maybe this will do what you need.

## What it does

This is a postgres-backed async work queue library that is a set of conveniences and features on top of the rust port of Graphile Worker. It gives you a library you can integrate with your own project to handle background tasks.

> **Status**: Core features are complete and tested (64.67%% test coverage, 55 tests). The library is suitable for production use for job enqueueing, worker processing, and DLQ management. The Admin API (feature-gated) is experimental. See [CHANGELOG.md](CHANGELOG.md) for details and [Known Limitations](docs/02-dlq.md#known-limitations).

### What's New Over graphile_worker

Built on top of `graphile_worker` (v0.8.6), backfill adds these production-ready features:

- 🎯 **Priority System** - Six-level priority queue (EMERGENCY to BULK_LOWEST) with numeric priority values
- 📦 **Named Queues** - Pre-configured Fast/Bulk queues plus custom queue support
- 🔄 **Smart Retry Policies** - Exponential backoff with jitter (fast/aggressive/conservative presets)
- 💀 **Dead Letter Queue (DLQ)** - Automatic failed job handling with query/requeue/deletion APIs
- 📊 **Comprehensive Metrics** - Prometheus-compatible metrics for jobs, DLQ, and database operations
- 🛠️ **High-Level Client API** - `BackfillClient` with ergonomic enqueueing helpers
- 🏃 **Flexible Worker Patterns** - `WorkerRunner` supporting tokio::select!, background tasks, and one-shot processing
- 🔧 **Admin API** - Optional Axum router for HTTP-based job management (experimental)
- 📝 **Convenience Functions** - `enqueue_fast()`, `enqueue_bulk()`, `enqueue_critical()`, etc.
- 🧹 **Stale Lock Cleanup** - Automatic cleanup of orphaned locks from crashed workers (startup + periodic)

All built on graphile_worker's rock-solid foundation of PostgreSQL SKIP LOCKED and LISTEN/NOTIFY.

### Features

- **Priority queues**: EMERGENCY, FAST_HIGH, FAST_DEFAULT, BULK_DEFAULT, BULK_LOW, BULK_LOWEST
- **Named queues**: Fast, Bulk, DeadLetter, Custom(name)
- **Scheduling**: Immediate or delayed execution with `run_at`
- **Idempotency**: Use `job_key` for deduplication
- **Exponential backoff**: Built-in retry policies with jitter to prevent thundering herds
- **Dead letter queue**: Handling jobs that experience un-retryable failures or exceed their retry limits
- **Error handling**: Automatic retry classification
- **Metrics**: Comprehensive metrics via the `metrics` crate - bring your own exporter (Prometheus, StatsD, etc.)
- **Monitoring**: Structured logging and tracing throughout
- **Building blocks for an axum admin api**: via a router you can mount on your own axum api server

Look at the `examples/` directory and the readme there for practical usage examples.

## Documentation

Read these in order for the best learning experience:

1. **[Database Setup](docs/01-database-setup.md)** - PostgreSQL configuration, automatic schema management, and SQLx compile-time verification
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

All configuration is passed in via environment variables:

- `DATABASE_URL`: PostgreSQL connection string
- `FAST_QUEUE_CONCURRENCY`: Workers for high-priority jobs (default: 10)
- `BULK_QUEUE_CONCURRENCY`: Workers for bulk processing (default: 5)
- `POLL_INTERVAL_MS`: Job polling interval (default: 200ms)
- `RUST_LOG`: Logging configuration

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

### SQLx Compile-Time Query Verification

This library uses SQLx's compile-time query verification for production safety. Set `DATABASE_URL` during compilation to enable type-safe, compile-time checked SQL queries:

```bash
export DATABASE_URL="postgresql://localhost:5432/backfill"
cargo build  # Queries verified against actual database schema
```

Alternatively, use offline mode with pre-generated query metadata:
```bash
cargo sqlx prepare  # Generates .sqlx/sqlx-data.json
cargo build         # Uses cached metadata, no database required
```

See [Database Setup](docs/01-database-setup.md#sqlx-compile-time-query-verification) for detailed setup instructions and best practices.

### Automatic Setup

The `graphile_worker` crate sets up all its database tables with no action needed if the database user has create table permissions. The library can also automatically create the DLQ schema:

```rust
use backfill::BackfillClient;

let client = BackfillClient::new("postgresql://localhost/mydb", "my_schema").await?;
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
