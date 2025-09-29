# backfill

[![CI](https://github.com/ceejbot/backfill/workflows/CI/badge.svg)](https://github.com/ceejbot/backfill/actions)
[![Coverage](https://img.shields.io/badge/coverage-49.57%25-orange)](https://github.com/ceejbot/backfill/actions)

A boringly-named priority queue system for doing async work. This library and work process wrap the the [graphile_worker crate](https://lib.rs/crates/graphile_worker) to do things the way I want to do them. It's unlikely you'll want to do things exactly this way, but perhaps you can learn by reading the code, or get a jumpstart by borrowing open-source code, or heck, maybe this will do what you need.

## What it does

This is a postgres-backed

[ TODO ]

Features:

- **Priority queues**: EMERGENCY, FAST_HIGH, FAST_DEFAULT, BULK_DEFAULT, BULK_LOW, BULK_LOWEST
- **Named queues**: Fast, Bulk, DeadLetter, Custom(name)
- **Scheduling**: Immediate or delayed execution with `run_at`
- **Idempotency**: Use `job_key` for deduplication
- **Exponential backoff**: Built-in retry policies with jitter to prevent thundering herds
- **Dead letter queue**: Handling jobs that experience un-retryable failures or exceed their retry limits
- **Error handling**: Automatic retry classification
- **Monitoring**: Comprehensive logging and tracing


Look at the `examples/` directory and the readme there for a practical usage example.

## Configuration and setup


All configuration is passed in via environment variables:

- `DATABASE_URL`: PostgreSQL connection string
- `FAST_QUEUE_CONCURRENCY`: Workers for high-priority jobs (default: 10)
- `BULK_QUEUE_CONCURRENCY`: Workers for bulk processing (default: 5)
- `POLL_INTERVAL_MS`: Job polling interval (default: 200ms)
- `RUST_LOG`: Logging configuration

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

This code is licensed via [the Parity Public License.](https://paritylicense.com) This license requires people who build on top of this source code to share their work with the community, too. This means if you hack on it for work, you have to make your work repo public somehow. Fair's fair. See the license text for details.
