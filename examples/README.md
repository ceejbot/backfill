# Backfill Worker Examples

This directory contains examples of how to use the Backfill worker system.

## Quick Start

1. Set up PostgreSQL and create a database: `createdb backfill`
2. Set environment variables: `export DATABASE_URL=postgresql://localhost:5432/backfill`
3. Run the example: `cargo run --example enqueue_jobs`
4. Start the worker: `cargo run --bin backfill-worker`

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

## Features

- **Priority queues**: EMERGENCY, FAST_HIGH, FAST_DEFAULT, BULK_DEFAULT, BULK_LOW, BULK_LOWEST
- **Named queues**: Fast, Bulk, DeadLetter, Custom(name)
- **Scheduling**: Immediate or delayed execution with `run_at`
- **Idempotency**: Use `job_key` for deduplication
- **Error handling**: Automatic retry classification
- **Monitoring**: Comprehensive logging and tracing
