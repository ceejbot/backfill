# Metrics

The backfill library emits comprehensive metrics using the [`metrics`](https://docs.rs/metrics) crate. This allows you to collect and export metrics to any backend (Prometheus, StatsD, etc.) by configuring a metrics recorder in your application.

## Metrics Philosophy

As a library, backfill uses the `metrics` crate's facade pattern:
- **We emit metrics** using `metrics` macros throughout the library
- **You choose the backend** by installing a recorder (e.g., `metrics-exporter-prometheus`)
- **You control the export** mechanism (HTTP endpoint, push gateway, etc.)

This design keeps the library backend-agnostic while providing comprehensive observability.

## Quick Start

```rust
use metrics_exporter_prometheus::PrometheusBuilder;
use backfill::WorkerRunner;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install Prometheus recorder
    PrometheusBuilder::new()
        .install()
        .expect("failed to install Prometheus recorder");
    
    // Your backfill usage - metrics are automatically emitted
    let worker = WorkerRunner::builder(config).await?
        .define_job::<MyJob>()
        .build()
        .await?;
    
    // Metrics are now available at the recorder's endpoint
    Ok(())
}
```

## Metric Categories

### Job Lifecycle Metrics

Track the complete lifecycle of jobs through the system.

#### `backfill.jobs.enqueued`
- **Type**: Counter
- **Description**: Total number of jobs enqueued
- **Labels**:
  - `queue`: Queue name (fast, bulk, dead_letter, or custom)
  - `task`: Task identifier
  - `priority_band`: Priority band (emergency, fast_high, fast_default, bulk_default, bulk_low, bulk_lowest)
- **Use**: Monitor job ingestion rate, identify high-volume tasks

#### `backfill.jobs.started`
- **Type**: Counter
- **Description**: Total number of jobs that began execution
- **Labels**:
  - `queue`: Queue name
  - `task`: Task identifier
- **Use**: Track job processing throughput, compare with enqueued to see queue depth trends

#### `backfill.jobs.completed`
- **Type**: Counter
- **Description**: Total number of jobs that completed successfully
- **Labels**:
  - `queue`: Queue name
  - `task`: Task identifier
  - `attempt`: Which attempt succeeded (1-N)
- **Use**: Monitor success rate, identify tasks that often succeed on retry

#### `backfill.jobs.failed`
- **Type**: Counter
- **Description**: Total number of job execution failures
- **Labels**:
  - `queue`: Queue name
  - `task`: Task identifier
  - `error_type`: Error classification (retryable, permanent, timeout, etc.)
  - `attempt`: Which attempt failed (1-N)
- **Use**: Identify problematic tasks, error patterns, retry effectiveness

#### `backfill.jobs.duration_seconds`
- **Type**: Histogram
- **Description**: Job execution duration in seconds
- **Labels**:
  - `queue`: Queue name
  - `task`: Task identifier
  - `status`: Result status (success, failed)
- **Buckets**: [0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0]
- **Use**: Identify slow tasks, detect performance regressions, capacity planning

#### `backfill.jobs.wait_time_seconds`
- **Type**: Histogram
- **Description**: Time from enqueue to execution start (queue latency)
- **Labels**:
  - `queue`: Queue name
  - `task`: Task identifier
  - `priority_band`: Priority band
- **Buckets**: [0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0]
- **Use**: Monitor queue backlog, detect capacity issues, validate SLAs

### Queue Depth Metrics

Real-time visibility into queue state.

#### `backfill.queue.depth`
- **Type**: Gauge
- **Description**: Current number of pending jobs in queue
- **Labels**:
  - `queue`: Queue name
  - `priority_band`: Priority band (optional, for detailed monitoring)
- **Use**: Monitor queue backlog, autoscaling triggers, capacity planning

#### `backfill.queue.active_jobs`
- **Type**: Gauge
- **Description**: Current number of jobs actively being processed
- **Labels**:
  - `queue`: Queue name
- **Use**: Monitor worker utilization, identify bottlenecks

### Dead Letter Queue (DLQ) Metrics

Track failed jobs that require manual intervention.

#### `backfill.dlq.jobs_added`
- **Type**: Counter
- **Description**: Total number of jobs moved to DLQ
- **Labels**:
  - `queue`: Original queue name
  - `task`: Task identifier
  - `reason`: Failure reason category (max_attempts_exceeded, non_retryable, etc.)
- **Use**: Alert on DLQ growth, identify problematic tasks

#### `backfill.dlq.size`
- **Type**: Gauge
- **Description**: Current number of jobs in DLQ
- **Labels**:
  - `task`: Task identifier (optional, for detailed breakdown)
- **Use**: Monitor DLQ health, alert on accumulation

#### `backfill.dlq.jobs_requeued`
- **Type**: Counter
- **Description**: Total number of jobs requeued from DLQ
- **Labels**:
  - `task`: Task identifier
  - `queue`: Target queue for requeue
- **Use**: Track remediation efforts, validate fixes

#### `backfill.dlq.jobs_deleted`
- **Type**: Counter
- **Description**: Total number of jobs permanently deleted from DLQ
- **Labels**:
  - `task`: Task identifier
- **Use**: Track DLQ cleanup operations

#### `backfill.dlq.age_seconds`
- **Type**: Histogram
- **Description**: Age of jobs in DLQ (time since moved to DLQ)
- **Labels**:
  - `task`: Task identifier
- **Buckets**: [300.0, 1800.0, 3600.0, 21600.0, 86400.0, 604800.0]
- **Use**: Identify stale DLQ entries, alert on old failures

### Worker Metrics

Monitor worker pool health and utilization.

#### `backfill.worker.active`
- **Type**: Gauge
- **Description**: Number of worker instances currently running
- **Labels**:
  - `queue`: Queue name
- **Use**: Monitor worker fleet health, autoscaling validation

#### `backfill.worker.utilization`
- **Type**: Gauge (0.0 to 1.0)
- **Description**: Worker utilization (active_jobs / concurrency_limit)
- **Labels**:
  - `queue`: Queue name
- **Use**: Identify under/over-provisioning, capacity planning

#### `backfill.worker.polls`
- **Type**: Counter
- **Description**: Total number of polling operations
- **Labels**:
  - `queue`: Queue name
  - `result`: Poll result (jobs_found, empty, error)
- **Use**: Monitor polling efficiency, detect issues

### Retry Metrics

Understand retry patterns and effectiveness.

#### `backfill.retries.attempted`
- **Type**: Counter
- **Description**: Total number of retry attempts
- **Labels**:
  - `task`: Task identifier
  - `attempt`: Attempt number (2, 3, 4, ...)
  - `queue`: Queue name
- **Use**: Identify retry-heavy tasks, validate retry policies

#### `backfill.retries.exhausted`
- **Type**: Counter
- **Description**: Jobs that exhausted all retry attempts
- **Labels**:
  - `task`: Task identifier
  - `max_attempts`: Configured max attempts
- **Use**: Identify tasks that need policy adjustment or fixes

### Database Metrics

Track database interaction health.

#### `backfill.db.operations`
- **Type**: Counter
- **Description**: Database operations performed
- **Labels**:
  - `operation`: Operation type (enqueue, claim, complete, fail, dlq_add, etc.)
  - `status`: Result (success, error)
- **Use**: Monitor database health, identify bottlenecks

#### `backfill.db.operation_duration_seconds`
- **Type**: Histogram
- **Description**: Database operation duration
- **Labels**:
  - `operation`: Operation type
- **Buckets**: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
- **Use**: Identify slow operations, database performance issues

## Integration Examples

### Prometheus Export

```rust
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create Prometheus recorder with HTTP endpoint
    let builder = PrometheusBuilder::new();
    let handle = builder.install_recorder()?;
    
    // Spawn metrics HTTP server
    let addr = SocketAddr::from(([0, 0, 0, 0], 9000));
    tokio::spawn(async move {
        use hyper::{Body, Request, Response, Server};
        use hyper::service::{make_service_fn, service_fn};
        
        let make_svc = make_service_fn(move |_| {
            let handle = handle.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |_: Request<Body>| {
                    let metrics = handle.render();
                    async move { Ok::<_, hyper::Error>(Response::new(Body::from(metrics))) }
                }))
            }
        });
        
        Server::bind(&addr).serve(make_svc).await
    });
    
    // Your backfill worker
    let worker = WorkerRunner::builder(config).await?
        .define_job::<MyJob>()
        .build()
        .await?;
    
    worker.run_until_cancelled(token).await?;
    Ok(())
}
```

### Axum Integration

```rust
use axum::{Router, routing::get};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

async fn metrics_handler(handle: PrometheusHandle) -> String {
    handle.render()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let recorder = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Prefix("backfill.jobs.duration".to_string()),
            &[0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0],
        )?
        .install_recorder()?;
    
    let app = Router::new()
        .route("/metrics", get(|| metrics_handler(recorder.clone())));
    
    // Start your server and workers...
    
    Ok(())
}
```

### StatsD Export

```rust
use metrics_exporter_statsd::StatsdBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Export to StatsD
    StatsdBuilder::from("statsd.example.com", 8125)
        .with_queue_size(5000)
        .with_buffer_size(1024)
        .install()?;
    
    // Metrics automatically exported to StatsD
    let worker = WorkerRunner::builder(config).await?
        .define_job::<MyJob>()
        .build()
        .await?;
    
    Ok(())
}
```

## Alerting Recommendations

### Critical Alerts

```yaml
# DLQ growing rapidly
- alert: DLQGrowthRate
  expr: rate(backfill_dlq_jobs_added[5m]) > 10
  for: 10m
  severity: critical
  description: DLQ receiving >10 jobs/sec for 10+ minutes

# Queue depth growing
- alert: QueueBacklog
  expr: backfill_queue_depth > 10000
  for: 15m
  severity: critical
  description: Queue has >10k pending jobs for 15+ minutes

# High failure rate
- alert: JobFailureRate
  expr: rate(backfill_jobs_failed[5m]) / rate(backfill_jobs_started[5m]) > 0.1
  for: 5m
  severity: critical
  description: Job failure rate >10% for 5+ minutes
```

### Warning Alerts

```yaml
# Slow jobs
- alert: SlowJobExecution
  expr: histogram_quantile(0.95, backfill_jobs_duration_seconds) > 300
  for: 10m
  severity: warning
  description: P95 job duration >5 minutes

# Queue latency
- alert: HighQueueLatency
  expr: histogram_quantile(0.95, backfill_jobs_wait_time_seconds) > 60
  for: 5m
  severity: warning
  description: P95 queue wait time >1 minute

# Worker utilization
- alert: WorkerOverutilized
  expr: backfill_worker_utilization > 0.9
  for: 30m
  severity: warning
  description: Workers >90% utilized for 30+ minutes
```

## Grafana Dashboard Queries

### Job Throughput

```promql
# Jobs enqueued per second
rate(backfill_jobs_enqueued[5m])

# Jobs completed per second
rate(backfill_jobs_completed[5m])

# Job success rate
rate(backfill_jobs_completed[5m]) / rate(backfill_jobs_started[5m])
```

### Queue Health

```promql
# Current queue depth
backfill_queue_depth

# Queue wait time (P95)
histogram_quantile(0.95, backfill_jobs_wait_time_seconds)

# Active jobs by queue
sum(backfill_queue_active_jobs) by (queue)
```

### DLQ Monitoring

```promql
# DLQ size
backfill_dlq_size

# DLQ growth rate
rate(backfill_dlq_jobs_added[1h])

# Jobs by failure reason
sum(increase(backfill_dlq_jobs_added[1h])) by (reason, task)
```

### Performance

```promql
# Job duration by task (P95)
histogram_quantile(0.95, 
  sum(rate(backfill_jobs_duration_seconds_bucket[5m])) by (task, le))

# Database operation latency
histogram_quantile(0.99,
  sum(rate(backfill_db_operation_duration_seconds_bucket[5m])) by (operation, le))
```

## Best Practices

### Label Cardinality

Be mindful of label cardinality to avoid overwhelming your metrics backend:

✅ **Good**: Fixed set of labels
- `queue`: Small set (fast, bulk, dead_letter)
- `error_type`: Classification categories (retryable, permanent, timeout)
- `priority_band`: Fixed priority bands

⚠️ **Caution**: Potentially high cardinality
- `task`: Number of different job types in your system
- `attempt`: Usually 1-12, manageable

❌ **Avoid**: Unbounded cardinality
- ~~`job_id`~~: Every job has unique ID
- ~~`job_key`~~: User-provided, unbounded
- ~~`error_message`~~: Free-form text

### Sampling for High-Volume Systems

For very high-volume systems (>1000 jobs/sec), consider:

```rust
// Sample metrics for high-volume tasks
if should_sample() {
    metrics::histogram!("backfill.jobs.duration_seconds", duration);
}

fn should_sample() -> bool {
    // Sample 1% of metrics
    rand::random::<f64>() < 0.01
}
```

### Aggregation

Aggregate metrics at query time rather than collection time:

```promql
# Average duration across all tasks
avg(rate(backfill_jobs_duration_seconds_sum[5m]) / 
    rate(backfill_jobs_duration_seconds_count[5m]))

# Per-task breakdown
avg by (task)(rate(backfill_jobs_duration_seconds_sum[5m]) / 
              rate(backfill_jobs_duration_seconds_count[5m]))
```

## Metric Lifecycle

Metrics are emitted at key points:

1. **Enqueue**: When `client.enqueue()` is called
   - `backfill.jobs.enqueued` incremented
   - `backfill.queue.depth` updated

2. **Job Start**: When worker claims and begins job
   - `backfill.jobs.started` incremented
   - `backfill.jobs.wait_time_seconds` recorded
   - `backfill.queue.depth` decremented
   - `backfill.queue.active_jobs` incremented

3. **Job Complete**: When job finishes (success or failure)
   - `backfill.jobs.completed` or `backfill.jobs.failed` incremented
   - `backfill.jobs.duration_seconds` recorded
   - `backfill.queue.active_jobs` decremented

4. **DLQ Movement**: When job moves to DLQ
   - `backfill.dlq.jobs_added` incremented
   - `backfill.dlq.size` incremented

5. **Retry**: When job is retried
   - `backfill.retries.attempted` incremented

## Disabling Metrics

If you don't want metrics overhead, simply don't install a recorder:

```rust
// No recorder installed = no metrics overhead
// Metrics macros become no-ops
let worker = WorkerRunner::builder(config).await?
    .define_job::<MyJob>()
    .build()
    .await?;
```

The `metrics` crate uses static dispatch, so the overhead when no recorder is installed is minimal (single branch check).

## Troubleshooting

### Metrics Not Appearing

1. **Verify recorder is installed** before creating workers
2. **Check label names** match exactly (case-sensitive)
3. **Verify metrics endpoint** is accessible
4. **Check for errors** during recorder installation

### High Memory Usage

1. **Audit label cardinality** - too many unique label combinations
2. **Consider sampling** for high-volume metrics
3. **Increase recorder buffer sizes** or export frequency

### Missing Labels

The library only emits metrics when data is available:
- `attempt` label: Only on retries (attempt > 1)
- `error_type` label: Only on failures
- Some metrics require feature flags (`axum` for admin API metrics)

## See Also

- [`metrics` crate documentation](https://docs.rs/metrics)
- [`metrics-exporter-prometheus`](https://docs.rs/metrics-exporter-prometheus)
- [Prometheus best practices](https://prometheus.io/docs/practices/naming/)
- [Example integration](../examples/metrics_integration.rs)
