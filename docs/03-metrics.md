# Metrics

Backfill emits metrics through the [`metrics`](https://docs.rs/metrics)
crate facade. Names use Prometheus-compatible underscores. The library
itself never installs a recorder — bring your own
(`metrics-exporter-prometheus`, StatsD, OpenTelemetry, …) and it'll pick
up everything below. With no recorder installed the emission is
effectively free (the `metrics` macros become no-ops).

## What backfill emits automatically

These are the metrics that fall out of normal client and worker
operation. You don't need to do anything beyond installing a recorder.

### Enqueue

| Metric | Type | Labels | When |
|---|---|---|---|
| `backfill_jobs_enqueued` | counter | `queue` (`"parallel"` \| `"serial"`), `task`, `priority_band` | Every successful `client.enqueue()` |
| `backfill_jobs_already_in_progress` | counter | `queue`, `task` | `enqueue` skipped because a job with the same `job_key` is currently locked by a worker |

`priority_band` collapses the i16 priority into a fixed set of strings
(`emergency`, `fast_high`, `fast_default`, `bulk_default`, `bulk_low`,
`bulk_lowest`, `other`) so cardinality stays bounded. The `queue` label
is also bounded to `"parallel"` or `"serial"` — this means `Queue::serial_for("user", id)` collapses to `"serial"` rather than emitting one
time-series per user.

### Dead letter queue

| Metric | Type | Labels | When |
|---|---|---|---|
| `backfill_dlq_jobs_added` | counter | `queue` (`"parallel"` \| `"serial"`), `task`, `reason` | Job moved into `backfill_dlq` |
| `backfill_dlq_jobs_requeued` | counter | `task`, `queue` | Job requeued from DLQ |
| `backfill_dlq_jobs_deleted` | counter | `task` | Job deleted from DLQ (or `"(by_job_key)"` when cleared by `delete_dlq_by_job_key`) |
| `backfill_dlq_size` | gauge | (none) and `task` | Updated on every `dlq_stats()` call |
| `backfill_dlq_age_seconds` | histogram | `task` | Recorded for every DLQ row returned by `list_dlq_jobs` / `get_dlq_job` |

### Database operations

| Metric | Type | Labels | When |
|---|---|---|---|
| `backfill_db_operations` | counter | `operation` (`enqueue`, `dlq_add`, …), `status` (`success`, `error`, `already_in_progress`) | Every internal DB call |
| `backfill_db_operation_duration_seconds` | histogram | `operation` | Same call, recorded regardless of outcome |

### Worker lifecycle

| Metric | Type | Labels | When |
|---|---|---|---|
| `backfill_worker_active` | gauge | `queue` (always `"worker"` from the library; plugins may override) | Set to 1 on `WorkerRunner::run_until_cancelled` start, 0 on stop |

### Stale-lock cleanup

These tell you whether the periodic cleanup loop is running and what
it's finding.

| Metric | Type | Labels | When |
|---|---|---|---|
| `backfill_cleanup_queue_locks_released` | counter | (none) | Stale queue locks released |
| `backfill_cleanup_queue_locks_last_released` | gauge | (none) | Count from the most recent cleanup that released > 0 |
| `backfill_cleanup_job_locks_released` | counter | (none) | Stale job locks released |
| `backfill_cleanup_job_locks_last_released` | gauge | (none) | Count from the most recent cleanup that released > 0 |
| `backfill_cleanup_failed_jobs_deleted` | counter | (none) | Permanently-failed rows deleted from `_private_jobs` |
| `backfill_cleanup_failures` | counter | `operation`, `error_type` | A cleanup operation hit an error |
| `backfill_cleanup_last_success_timestamp` | gauge | (none) | Unix timestamp of the most recent fully-successful cleanup tick — **alert if this stops advancing** |

## Job lifecycle metrics: register a plugin

Backfill does **not** automatically emit per-job lifecycle metrics
(start, complete, fail, duration, wait time). Those come from
graphile_worker's lifecycle hooks, which means you opt in by registering
a `Plugin` on your `WorkerRunner`. This keeps the library's metric
schema small and stable, and lets you choose names and labels that fit
your environment.

A complete working plugin lives at
[`examples/metrics_plugin.rs`](../examples/metrics_plugin.rs). The
shape is:

```rust
use backfill::{
    HookRegistry, JobComplete, JobCompleteContext, JobFail, JobFailContext,
    JobStart, JobStartContext, Plugin, WorkerRunner, WorkerConfig,
};

#[derive(Clone)]
struct MetricsPlugin;

impl Plugin for MetricsPlugin {
    fn register(self, hooks: &mut HookRegistry) {
        hooks.on(JobStart, |ctx: JobStartContext| async move {
            let task = ctx.job.task_identifier();
            metrics::counter!("jobs_started", "task" => task.to_string()).increment(1);
        });

        hooks.on(JobComplete, |ctx: JobCompleteContext| async move {
            let task = ctx.job.task_identifier().to_string();
            metrics::counter!("jobs_completed", "task" => task.clone()).increment(1);
            metrics::histogram!(
                "job_duration_seconds",
                "task" => task,
                "status" => "success",
            ).record(ctx.duration.as_secs_f64());
        });

        hooks.on(JobFail, |ctx: JobFailContext| async move {
            let status = if ctx.will_retry { "retrying" } else { "failed" };
            metrics::counter!(
                "jobs_failed",
                "task" => ctx.job.task_identifier().to_string(),
                "status" => status,
            ).increment(1);
        });
    }
}

# async fn build(config: WorkerConfig) -> Result<(), backfill::BackfillError> {
let _worker = WorkerRunner::builder(config).await?
    // .define_job::<MyJob>()  // your jobs go here
    .add_plugin(MetricsPlugin)
    .build().await?;
# Ok(())
# }
```

A few things to know:

- **Pick your own names.** The example uses `jobs_started`,
  `jobs_completed`, `jobs_failed`, `job_duration_seconds`, etc. — there
  is no required prefix. If you want them to live alongside the
  library's automatic metrics, prefix them with `backfill_` yourself.
- **`ctx.job` accessors are methods**, not fields:
  `ctx.job.task_identifier()`, `ctx.job.attempts()`,
  `ctx.job.created_at()`, `ctx.job.id()`. They borrow from the job, so
  call `.to_string()` / `.clone()` when you need an owned `String` for
  a metric label.
- **Cardinality.** `task` is bounded by your handler set. Don't put
  `job_key`, error messages, or job IDs in labels — `metrics-exporter-prometheus` will gladly accept them and your storage will not.

The `examples/metrics_plugin.rs` file shows the complete set:
`WorkerStart`, `WorkerShutdown`, `JobStart`, `JobComplete`, `JobFail`,
`JobPermanentlyFail`, including wait-time tracking from `created_at` and
error-type classification.

## Installing a recorder

### Prometheus (in-process scrape endpoint)

```rust
use metrics_exporter_prometheus::PrometheusBuilder;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let handle = PrometheusBuilder::new()
    .install_recorder()?;

// Mount on whatever HTTP framework you're already using:
// `handle.render()` returns the Prometheus text format.
# Ok(())
# }
```

### Axum

```rust
use axum::{Router, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::Arc;

async fn metrics_endpoint(handle: Arc<PrometheusHandle>) -> String {
    handle.render()
}

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let handle = Arc::new(PrometheusBuilder::new().install_recorder()?);
let _app: Router = Router::new()
    .route("/metrics", get({
        let handle = handle.clone();
        move || metrics_endpoint(handle.clone())
    }));
# Ok(())
# }
```

### StatsD

```rust
use metrics_exporter_statsd::StatsdBuilder;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
StatsdBuilder::from("statsd.example.com", 8125)
    .with_queue_size(5000)
    .with_buffer_size(1024)
    .install()?;
# Ok(())
# }
```

## Alerting recommendations

These reference only metrics that backfill actually emits. Job-lifecycle
alerts (failure rate, duration P95) assume you've registered the plugin
above and are using its names — adapt accordingly.

```yaml
# Cleanup loop has stalled — stale locks will accumulate.
- alert: BackfillCleanupNotRunning
  expr: time() - backfill_cleanup_last_success_timestamp > 300
  for: 5m
  severity: critical

# DLQ growing fast.
- alert: BackfillDLQGrowthRate
  expr: rate(backfill_dlq_jobs_added[5m]) > 1
  for: 10m
  severity: warning

# Total DLQ size hitting an actionable threshold (tune to your volume).
- alert: BackfillDLQSize
  expr: backfill_dlq_size > 500
  for: 5m
  severity: warning

# DB operations failing — covers connection issues, schema mismatches.
- alert: BackfillDBErrors
  expr: rate(backfill_db_operations{status="error"}[5m]) > 0.1
  for: 5m
  severity: warning

# Stale job locks released — indicates one or more workers crashed.
- alert: BackfillStaleJobLocks
  expr: increase(backfill_cleanup_job_locks_released[10m]) > 0
  for: 0m
  severity: warning
```

## See also

- [Plugins guide](07-plugins.md) — full lifecycle hook API, including
  the interceptor hooks (`BeforeJobRun`, `AfterJobRun`,
  `BeforeJobSchedule`) that don't appear in the metrics example
- [`examples/metrics_plugin.rs`](../examples/metrics_plugin.rs) —
  complete working plugin
- [`metrics` crate documentation](https://docs.rs/metrics)
- [Prometheus naming conventions](https://prometheus.io/docs/practices/naming/)
