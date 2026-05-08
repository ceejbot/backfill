# Lifecycle Hook Plugins

Backfill exposes graphile_worker's lifecycle hook system, so you can run
your own code at well-defined points in a worker's life: when it starts,
when a job is fetched, when a job completes or fails, when the worker
shuts down. This is the supported extension point for cross-cutting
concerns — metrics, logging, validation, payload enrichment, error
tracking — that you don't want to wedge into individual job handlers.

The library uses this same mechanism internally: when DLQ is enabled,
`DlqCleanupPlugin` and `PermanentFailurePlugin` are auto-registered.

## Quick start

A plugin is any `Clone`-able struct that implements `Plugin`. The
`register` method receives a `HookRegistry`; you call `hooks.on(Event,
handler)` for each event you care about.

```rust
use backfill::{HookRegistry, JobComplete, JobCompleteContext, Plugin};

#[derive(Clone)]
struct MyPlugin;

impl Plugin for MyPlugin {
    fn register(self, hooks: &mut HookRegistry) {
        hooks.on(JobComplete, |ctx: JobCompleteContext| async move {
            println!(
                "Job {} completed in {:?}",
                ctx.job.task_identifier(),
                ctx.duration,
            );
        });
    }
}
```

Add it to a worker via the builder:

```rust
# use backfill::{WorkerRunner, WorkerConfig};
# #[derive(Clone)] struct MyPlugin;
# impl backfill::Plugin for MyPlugin { fn register(self, _: &mut backfill::HookRegistry) {} }
# async fn build(config: WorkerConfig) -> Result<(), backfill::BackfillError> {
let _worker = WorkerRunner::builder(config).await?
    // .define_job::<MyJob>()
    .add_plugin(MyPlugin)
    .build().await?;
# Ok(())
# }
```

`add_plugin` requires `Clone` because the worker may be rebuilt during
its lifetime. If your plugin holds shared state, wrap it in `Arc` —
cloning the plugin clones the handle, not the state.

## Available events

The events listed below are re-exported from
`graphile_worker_lifecycle_hooks` through the `backfill` crate root. Use
the event type as the first argument to `hooks.on(...)`.

### Observer events

These run *alongside* the worker — the value your async block returns
is `()` (or, if you don't return anything, the unit type by default).
They cannot affect job execution.

| Event | Context type | Fires when |
|---|---|---|
| `WorkerInit` | `WorkerInitContext` | Worker is initializing (one-time) |
| `WorkerStart` | `WorkerStartContext` | Worker has started processing |
| `WorkerShutdown` | `WorkerShutdownContext` | Worker is shutting down (with `reason`) |
| `JobFetch` | `JobFetchContext` | Job has been fetched from the queue |
| `JobStart` | `JobStartContext` | Job execution begins |
| `JobComplete` | `JobCompleteContext` | Job completed successfully (with `duration`) |
| `JobFail` | `JobFailContext` | Job failed (with `error` and `will_retry`) |
| `JobPermanentlyFail` | `JobPermanentlyFailContext` | Job exhausted retries or hit a non-retryable error |
| `CronTick` | `CronTickContext` | Cron scheduler tick |
| `CronJobScheduled` | `CronJobScheduledContext` | Cron job was scheduled into the queue |

### Interceptor events

These run *in line with* the worker and can influence what happens
next. Your handler returns a result type that the worker acts on.

| Event | Context type | Return type | Effect |
|---|---|---|---|
| `BeforeJobRun` | `BeforeJobRunContext` | `HookResult` | Skip / fail / retry / continue, before the handler runs |
| `AfterJobRun` | `AfterJobRunContext` | `HookResult` | Override the result *after* the handler returns |
| `BeforeJobSchedule` | `BeforeJobScheduleContext` | `JobScheduleResult` | Modify the payload, skip, or fail at enqueue time |

`HookResult` variants:

```rust
pub enum HookResult {
    Continue,           // Default — proceed normally
    Skip,               // Mark the job complete without running the handler
    Fail(String),       // Fail with an error message
    Retry { delay: Duration }, // Retry after delay
}
```

`JobScheduleResult` variants:

```rust
pub enum JobScheduleResult {
    Continue(serde_json::Value), // Proceed with this (possibly modified) payload
    Skip,                        // Don't enqueue (returns an error to the caller)
    Fail(String),                // Fail enqueue with a message
}
```

## Context shapes

Every event hands you a context struct. The `job` field is an
`Arc<Job>`; `Job` accessors are *methods*, not fields:

```rust,ignore
ctx.job.id()                  // i64
ctx.job.task_identifier()     // &str
ctx.job.attempts()            // i16
ctx.job.max_attempts()        // i16
ctx.job.priority()            // &i16
ctx.job.created_at()          // &DateTime<Utc>
ctx.job.run_at()              // &DateTime<Utc>
ctx.job.payload()             // &serde_json::Value
ctx.job.key()                 // Option<&str>  (the job_key)
```

A few of the higher-traffic contexts:

```rust,ignore
pub struct JobCompleteContext {
    pub job: Arc<Job>,
    pub worker_id: String,
    pub duration: Duration,
}

pub struct JobFailContext {
    pub job: Arc<Job>,
    pub worker_id: String,
    pub error: String,
    pub will_retry: bool,
}

pub struct WorkerShutdownContext {
    pub pool: PgPool,
    pub worker_id: String,
    pub reason: ShutdownReason, // Signal | Error | Graceful
}
```

## Common patterns

### Logging

```rust
use backfill::{
    HookRegistry, JobComplete, JobCompleteContext, JobFail, JobFailContext,
    JobStart, JobStartContext, Plugin,
};

#[derive(Clone)]
struct LoggingPlugin;

impl Plugin for LoggingPlugin {
    fn register(self, hooks: &mut HookRegistry) {
        hooks.on(JobStart, |ctx: JobStartContext| async move {
            log::info!(
                "Job started: {} (attempt {})",
                ctx.job.task_identifier(),
                ctx.job.attempts(),
            );
        });

        hooks.on(JobComplete, |ctx: JobCompleteContext| async move {
            log::info!(
                "Job completed: {} in {:.2}s",
                ctx.job.task_identifier(),
                ctx.duration.as_secs_f64(),
            );
        });

        hooks.on(JobFail, |ctx: JobFailContext| async move {
            if ctx.will_retry {
                log::warn!(
                    "Job failed (will retry): {} - {}",
                    ctx.job.task_identifier(),
                    ctx.error,
                );
            } else {
                log::error!(
                    "Job failed (no retry): {} - {}",
                    ctx.job.task_identifier(),
                    ctx.error,
                );
            }
        });
    }
}
```

### Metrics

See [`examples/metrics_plugin.rs`](../examples/metrics_plugin.rs) for a
complete plugin covering `WorkerStart`, `WorkerShutdown`, `JobStart`,
`JobComplete`, `JobFail`, `JobPermanentlyFail` — including duration,
wait-time, and error-type histograms.

### Error tracking (Sentry/Rollbar/etc.)

```rust,ignore
use backfill::{HookRegistry, JobFail, JobFailContext, Plugin};
use std::sync::Arc;

#[derive(Clone)]
struct SentryPlugin {
    client: Arc<sentry::Client>,
}

impl Plugin for SentryPlugin {
    fn register(self, hooks: &mut HookRegistry) {
        let _client = self.client;
        hooks.on(JobFail, move |ctx: JobFailContext| async move {
            // Only capture final failures — not transient retries.
            if ctx.will_retry {
                return;
            }
            sentry::capture_message(
                &format!("Job {} failed: {}", ctx.job.task_identifier(), ctx.error),
                sentry::Level::Error,
            );
        });
    }
}
```

### Validation (interceptor)

`BeforeJobRun` lets you skip or fail a job before its handler runs.
Useful for kill switches, feature flags, or payload validation that
should bypass the handler entirely.

```rust,ignore
use backfill::{BeforeJobRun, BeforeJobRunContext, HookRegistry, HookResult, Plugin};

#[derive(Clone)]
struct ValidationPlugin;

impl Plugin for ValidationPlugin {
    fn register(self, hooks: &mut HookRegistry) {
        hooks.on(BeforeJobRun, |ctx: BeforeJobRunContext| async move {
            if ctx.payload.get("user_id").is_none() {
                return HookResult::Fail("missing user_id".to_string());
            }
            if is_feature_disabled(ctx.job.task_identifier()) {
                return HookResult::Skip;
            }
            HookResult::Continue
        });
    }
}

fn is_feature_disabled(_task: &str) -> bool { false }
```

### Payload enrichment (interceptor)

`BeforeJobSchedule` runs before the row is inserted into
`_private_jobs`, so it can add metadata or rewrite payloads.

```rust,ignore
use backfill::{BeforeJobSchedule, BeforeJobScheduleContext, HookRegistry, JobScheduleResult, Plugin};
use serde_json::json;

#[derive(Clone)]
struct EnrichmentPlugin;

impl Plugin for EnrichmentPlugin {
    fn register(self, hooks: &mut HookRegistry) {
        hooks.on(BeforeJobSchedule, |ctx: BeforeJobScheduleContext| async move {
            let mut payload = ctx.payload.clone();
            if let Some(obj) = payload.as_object_mut() {
                obj.insert(
                    "scheduled_at".to_string(),
                    json!(chrono::Utc::now().to_rfc3339()),
                );
            }
            JobScheduleResult::Continue(payload)
        });
    }
}
```

## Multiple plugins

You can register as many plugins as you like. Observer events are
delivered to every registered handler. Interceptor events run in
registration order; once a handler returns `Skip` / `Fail`, later
handlers don't see the event for that job.

```rust
# use backfill::{WorkerRunner, WorkerConfig};
# #[derive(Clone)] struct MetricsPlugin;
# impl backfill::Plugin for MetricsPlugin { fn register(self, _: &mut backfill::HookRegistry) {} }
# #[derive(Clone)] struct LoggingPlugin;
# impl backfill::Plugin for LoggingPlugin { fn register(self, _: &mut backfill::HookRegistry) {} }
# #[derive(Clone)] struct ValidationPlugin;
# impl backfill::Plugin for ValidationPlugin { fn register(self, _: &mut backfill::HookRegistry) {} }
# async fn build(config: WorkerConfig) -> Result<(), backfill::BackfillError> {
let _worker = WorkerRunner::builder(config).await?
    // .define_job::<MyJob>()
    .add_plugin(ValidationPlugin)  // interceptors first
    .add_plugin(MetricsPlugin)
    .add_plugin(LoggingPlugin)
    .build().await?;
# Ok(())
# }
```

A practical ordering rule: put validation / interceptor plugins
*before* observation / metrics plugins. Otherwise your metrics will
show jobs as starting that the validator turned around.

## Auto-registered plugins

When you set `dlq_processor_interval = Some(_)` on `WorkerConfig`,
backfill automatically registers two plugins:

- **`DlqCleanupPlugin`** (`JobComplete`): when a requeued job succeeds,
  removes the matching DLQ row by `job_key`. This stops a successful
  job from being requeued again later.
- **`PermanentFailurePlugin`** (`JobFail`): classifies the error via
  `WorkerError::classify_from_message`. If the error is non-retryable
  (`ValidationFailed`, `Unauthorized`, …), sets
  `attempts = max_attempts` so the next DLQ-processor tick captures it
  rather than retrying for hours.

You don't need to register either of these yourself; both are part of
the standard DLQ machinery.

## See also

- [`examples/metrics_plugin.rs`](../examples/metrics_plugin.rs) — full
  metrics plugin
- [`tests/plugin_tests.rs`](../tests/plugin_tests.rs) — integration
  tests exercising the observer hooks
- [`src/dlq_cleanup_plugin.rs`](../src/dlq_cleanup_plugin.rs),
  [`src/permanent_failure_plugin.rs`](../src/permanent_failure_plugin.rs)
  — the in-tree plugins, both ~100 lines each
- [graphile_worker source](https://lib.rs/crates/graphile_worker) — the
  upstream lifecycle hook implementation
