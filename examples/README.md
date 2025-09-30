# Backfill Worker Examples

This directory contains examples of how to use the Backfill worker system.

## Available Examples

- **`basic_worker.rs`** - Complete worker setup with job handlers and graceful shutdown
- **`enqueue_jobs.rs`** - How to enqueue jobs with different priorities and configurations
- **`admin_server.rs`** - Axum HTTP API for job management and monitoring (requires `axum` feature)
- **`axum_integration.rs`** - Full-stack integration example with Axum
- **`dlq_management.rs`** - Dead Letter Queue management workflows (monitoring, requeue, cleanup)

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

For comprehensive DLQ documentation, see [`docs/DLQ.md`](../docs/DLQ.md).

## Features

- **Priority queues**: EMERGENCY, FAST_HIGH, FAST_DEFAULT, BULK_DEFAULT, BULK_LOW, BULK_LOWEST
- **Named queues**: Fast, Bulk, DeadLetter, Custom(name)
- **Scheduling**: Immediate or delayed execution with `run_at`
- **Idempotency**: Use `job_key` for deduplication
- **Exponential backoff**: Built-in retry policies with jitter to prevent thundering herds
- **Dead letter queue**: Automatic handling of permanently failed jobs
- **Error handling**: Automatic retry classification
- **Monitoring**: Comprehensive logging and tracing

## Retry Policies

The example demonstrates three built-in retry policies plus custom configuration:

- **Fast retries**: 3 attempts, 100ms-30s delays, for high-priority jobs
- **Aggressive retries**: 12 attempts, 500ms-600s delays, for critical jobs  
- **Conservative retries**: 5 attempts, 5s-1800s delays, for bulk processing
- **Custom policies**: Configure attempts, delays, backoff multiplier, and jitter
