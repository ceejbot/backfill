# Backfill Worker Examples

This directory contains examples of how to use the Backfill worker system.

## Available Examples

- **`basic_worker.rs`** - Complete worker setup with job handlers and graceful shutdown
- **`enqueue_jobs.rs`** - How to enqueue jobs with different priorities and configurations
- **`admin_server.rs`** - Axum HTTP API for job management and monitoring (requires `axum` feature)
- **`dlq_management.rs`** - Dead Letter Queue management workflows (monitoring, requeue, cleanup)
- **`metrics_plugin.rs`** - Prometheus metrics via a lifecycle hook plugin

## Quick Start

1. Set up PostgreSQL and create a database: `createdb backfill`
2. Set environment variables: `export DATABASE_URL=postgresql://localhost:5432/backfill`
3. Run the example: `cargo run --example enqueue_jobs`
4. Start the worker: `cargo run --example basic_worker`

## Example Jobs

The enqueueing example demonstrates four job types:

- **ExampleJob**: Basic demonstration with configurable delay/failure
- **SendEmailJob**: Email notifications with validation  
- **ProcessUserDataJob**: User data processing for analytics
- **GenerateReportJob**: Business report generation

## Configuration

Key environment variables:

- `DATABASE_URL`: PostgreSQL connection string
- `FAST_QUEUE_CONCURRENCY`: Workers for high-priority jobs (default: 10)
- `BULK_QUEUE_CONCURRENCY`: Workers for bulk processing (default: 5)
- `POLL_INTERVAL_MS`: Job polling interval (default: 200ms)
- `RUST_LOG`: Logging configuration

## Dead Letter Queue (DLQ) Management

The `dlq_management.rs` example demonstrates operational workflows for handling failed jobs:

```bash
# Run the DLQ management example
cargo run --example dlq_management
```

**Features demonstrated:**
- View DLQ statistics and health metrics
- List and filter failed jobs by task type, queue, or time range
- Inspect individual job failures with full error details
- Batch requeue jobs after deploying fixes
- Clean up old DLQ entries

For comprehensive DLQ documentation, see [`docs/02-dlq.md`](../docs/02-dlq.md).

## Features

- **Priority queues**: EMERGENCY, FAST_HIGH, FAST_DEFAULT, BULK_DEFAULT, BULK_LOW, BULK_LOWEST
- **Queue types**: `Queue::Parallel` (default — concurrent execution) or
  `Queue::Serial(name)` for ordering / rate limiting
- **Scheduling**: Immediate or delayed execution with `run_at`
- **Idempotency**: Use `job_key` for deduplication
- **Retries**: Configurable `max_attempts` per job; graphile_worker handles
  backoff timing on a fixed `exp(min(attempts, 10))` second schedule
- **Dead letter queue**: Automatic handling of permanently failed jobs
- **Error handling**: Automatic retry classification
- **Monitoring**: Comprehensive logging and tracing

## Retry Policies

The example demonstrates three built-in retry presets:

- **Fast** (`RetryPolicy::fast`): 3 attempts, for jobs where rapid
  failure-to-DLQ is preferred over many retries
- **Aggressive** (`RetryPolicy::aggressive`): 12 attempts, for critical jobs
  that must eventually succeed if at all possible
- **Conservative** (`RetryPolicy::conservative`): 5 attempts, for bulk
  processing where consistency matters more than latency

Backoff timing between retries is fixed by graphile_worker (~1s, ~3s, ~7s,
~20s, ~55s, ~2.5min, … capped at ~6h per retry).
