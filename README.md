# backfill

[![CI](https://github.com/ceejbot/backfill/workflows/CI/badge.svg)](https://github.com/ceejbot/backfill/actions)
[![Coverage](https://img.shields.io/badge/coverage-74.62%25-yellow)](https://github.com/ceejbot/backfill/actions)

A boringly-named priority queue system for doing async work. This library and work process wrap the the [graphile_worker crate](https://lib.rs/crates/graphile_worker) to do things the way I want to do them. It's unlikely you'll want to do things exactly this way, but perhaps you can learn by reading the code, or get a jumpstart by borrowing open-source code, or heck, maybe this will do what you need.

## What it does

This is a postgres-backed async work queue library that is a set of conveniences and features on top of the rust port of Graphile Worker. It gives you a library you can integrate with your own project to handle background tasks.

> **Status**: Core features are complete and tested (74.62% test coverage, 55 tests). The library is suitable for production use for job enqueueing, worker processing, and DLQ management. The Admin API (feature-gated) is experimental. See [CHANGELOG.md](CHANGELOG.md) for details and [Known Limitations](docs/DLQ.md#known-limitations).

Features:

- **Priority queues**: EMERGENCY, FAST_HIGH, FAST_DEFAULT, BULK_DEFAULT, BULK_LOW, BULK_LOWEST
- **Named queues**: Fast, Bulk, DeadLetter, Custom(name)
- **Scheduling**: Immediate or delayed execution with `run_at`
- **Idempotency**: Use `job_key` for deduplication
- **Exponential backoff**: Built-in retry policies with jitter to prevent thundering herds
- **Dead letter queue**: Handling jobs that experience un-retryable failures or exceed their retry limits
- **Error handling**: Automatic retry classification
- **Metrics**: Comprehensive metrics via the `metrics` crate - bring your own exporter (Prometheus, StatsD, etc.)
- **Monitoring**: Structured logging and tracing throughout
- **Building blocks for an axum admin api**: via a router you can mount on your own axum api server.

Look at the `examples/` directory and the readme there for practical usage examples.

## Documentation

### Core Guides

- **[Dead Letter Queue (DLQ) Guide](docs/DLQ.md)** - Comprehensive guide to handling failed jobs, including:
  - How the DLQ works and why it's essential
  - Client API and HTTP admin API usage
  - Operational best practices for production
  - Monitoring, alerting, and troubleshooting
  - Common workflows for handling failures
- **[Admin API Reference](docs/ADMIN_API.md)** - HTTP API for job management and monitoring
- **[Metrics Guide](docs/METRICS.md)** - Comprehensive metrics for Prometheus, StatsD, and other backends
- **[Database Setup](docs/DATABASE_SETUP.md)** - PostgreSQL configuration and schema management
- **[DLQ Migrations](docs/DLQ_MIGRATIONS.md)** - Migration strategies for the DLQ schema
- **[SQLx Setup](docs/SQLX_SETUP.md)** - Compile-time query verification setup
- **[Testing Guide](docs/TESTING.md)** - Testing strategies for workers and jobs

## Configuration and setup

All configuration is passed in via environment variables:

- `DATABASE_URL`: PostgreSQL connection string
- `FAST_QUEUE_CONCURRENCY`: Workers for high-priority jobs (default: 10)
- `BULK_QUEUE_CONCURRENCY`: Workers for bulk processing (default: 5)
- `POLL_INTERVAL_MS`: Job polling interval (default: 200ms)
- `RUST_LOG`: Logging configuration

### SQLx Compile-Time Query Verification

This library is designed to take advantage of SQLx's compile-time query verification for production safety. Set `DATABASE_URL` during compilation to enable type-safe, compile-time checked SQL queries:

```bash
export DATABASE_URL="postgresql://localhost:5432/backfill"
cargo build  # Queries verified against actual database schema
```

Alternatively, use offline mode with pre-generated query metadata:
```bash
cargo sqlx prepare  # Generates .sqlx/sqlx-data.json
cargo build         # Uses cached metadata, no database required
```

See [`docs/SQLX_SETUP.md`](docs/SQLX_SETUP.md) for detailed setup instructions and best practices.

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

See [`docs/DLQ_MIGRATIONS.md`](docs/DLQ_MIGRATIONS.md) for detailed migration instructions and integration with popular migration tools.

## LICENSE

This code is licensed via [the Parity Public License.](https://paritylicense.com) This license requires people who fork and change this source code to share their work with the community, too. Either contribute your work back as a PR or make your forked repo public. Fair's fair! See the license text for details.
