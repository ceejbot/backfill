# Lifecycle Hook Plugins

Backfill exposes GraphileWorker's lifecycle hooks system, allowing you to receive callbacks for worker and job lifecycle events. This enables powerful observability, validation, and control patterns without modifying job handlers.

## Table of Contents

- [Quick Start](#quick-start)
- [Available Hooks](#available-hooks)
- [Hook Context Types](#hook-context-types)
- [Common Patterns](#common-patterns)
  - [Metrics Plugin](#metrics-plugin)
  - [Logging Plugin](#logging-plugin)
  - [Job Validation](#job-validation)
  - [Error Tracking](#error-tracking)
- [Advanced Features](#advanced-features)
- [Multiple Plugins](#multiple-plugins)

## Quick Start

1. **Implement the `LifecycleHooks` trait:**

```rust
use backfill::{LifecycleHooks, JobCompleteContext};

#[derive(Clone)]
struct MyPlugin;

impl LifecycleHooks for MyPlugin {
    async fn on_job_complete(&self, ctx: JobCompleteContext) {
        println!("Job {} completed in {:?}",
            ctx.job.task_identifier,
            ctx.duration
        );
    }
}
```

2. **Add the plugin to your worker:**

```rust
let worker = WorkerRunner::builder(config).await?
    .define_job::<MyJob>()
    .add_plugin(MyPlugin)
    .build().await?;
```

That's it! Your plugin will now receive callbacks for all job completions.

## Available Hooks

The `LifecycleHooks` trait provides 13 hooks (all optional - only implement what you need):

### Observer Hooks (Fire-and-forget)

These hooks observe events without affecting execution:

| Hook | When It Fires | Context |
|------|--------------|---------|
| `on_worker_init` | Worker initializes | `WorkerInitContext` |
| `on_worker_start` | Worker starts processing | `WorkerStartContext` |
| `on_worker_shutdown` | Worker shuts down | `WorkerShutdownContext` |
| `on_job_fetch` | Job fetched from queue | `JobFetchContext` |
| `on_job_start` | Job execution begins | `JobStartContext` |
| `on_job_complete` | Job completes successfully | `JobCompleteContext` |
| `on_job_fail` | Job fails | `JobFailContext` |
| `on_job_permanently_fail` | Job exhausts all retries | `JobPermanentlyFailContext` |
| `on_cron_tick` | Cron scheduler ticks | `CronTickContext` |
| `on_cron_job_scheduled` | Cron job scheduled | `CronJobScheduledContext` |

### Interceptor Hooks (Can modify execution)

These hooks can intercept and modify job execution:

| Hook | When It Fires | Return Type | Can Do |
|------|--------------|-------------|--------|
| `before_job_run` | Before job handler executes | `HookResult` | Skip, fail, retry, or continue |
| `after_job_run` | After job handler executes | `HookResult` | Override result (skip, fail, retry) |
| `before_job_schedule` | Before job inserted to DB | `JobScheduleResult` | Modify payload, skip, or fail |

## Hook Context Types

Each hook receives a context struct with relevant data:

### JobStartContext

```rust
pub struct JobStartContext {
    pub job: Arc<Job>,        // Full job details
    pub worker_id: String,    // Worker processing this job
}
```

Available job data:
- `job.task_identifier` - Task name
- `job.attempts` - Current attempt number
- `job.created_at` - When job was enqueued
- `job.priority` - Job priority
- `job.payload` - Job payload (JSON)
- ... and more (see GraphileWorker docs)

### JobCompleteContext

```rust
pub struct JobCompleteContext {
    pub job: Arc<Job>,
    pub worker_id: String,
    pub duration: Duration,   // How long the job took!
}
```

**Key feature:** Duration is automatically tracked for you.

### JobFailContext

```rust
pub struct JobFailContext {
    pub job: Arc<Job>,
    pub worker_id: String,
    pub error: String,        // Error message
    pub will_retry: bool,     // True if job will be retried
}
```

**Key feature:** `will_retry` tells you if this is a transient failure (will retry) or final failure (exhausted retries or permanent error).

### JobPermanentlyFailContext

```rust
pub struct JobPermanentlyFailContext {
    pub job: Arc<Job>,
    pub worker_id: String,
    pub error: String,        // Final error message
}
```

This fires when a job exhausts all retry attempts or encounters a permanent failure.

### WorkerStartContext / WorkerShutdownContext

```rust
pub struct WorkerStartContext {
    pub pool: PgPool,
    pub worker_id: String,
    pub extensions: ReadOnlyExtensions,
}

pub struct WorkerShutdownContext {
    pub pool: PgPool,
    pub worker_id: String,
    pub reason: ShutdownReason,  // Signal, Error, or Graceful
}
```

## Common Patterns

### Metrics Plugin

Track job execution metrics automatically:

```rust
use backfill::{LifecycleHooks, JobStartContext, JobCompleteContext, JobFailContext};

#[derive(Clone)]
struct MetricsPlugin;

impl LifecycleHooks for MetricsPlugin {
    async fn on_job_start(&self, ctx: JobStartContext) {
        metrics::counter!("jobs_started",
            "task" => ctx.job.task_identifier.clone()
        ).increment(1);
    }

    async fn on_job_complete(&self, ctx: JobCompleteContext) {
        let task = &ctx.job.task_identifier;

        metrics::counter!("jobs_completed",
            "task" => task.clone()
        ).increment(1);

        metrics::histogram!("job_duration_seconds",
            "task" => task.clone(),
            "status" => "success"
        ).record(ctx.duration.as_secs_f64());
    }

    async fn on_job_fail(&self, ctx: JobFailContext) {
        let status = if ctx.will_retry { "retrying" } else { "failed" };

        metrics::counter!("jobs_failed",
            "task" => ctx.job.task_identifier.clone(),
            "status" => status
        ).increment(1);
    }
}
```

See `examples/metrics_plugin.rs` for a complete working example.

### Logging Plugin

Structured logging for all job events:

```rust
#[derive(Clone)]
struct LoggingPlugin;

impl LifecycleHooks for LoggingPlugin {
    async fn on_job_start(&self, ctx: JobStartContext) {
        log::info!(
            "Job started: {} (attempt {}, priority {})",
            ctx.job.task_identifier,
            ctx.job.attempts,
            ctx.job.priority
        );
    }

    async fn on_job_complete(&self, ctx: JobCompleteContext) {
        log::info!(
            "Job completed: {} in {:.2}s",
            ctx.job.task_identifier,
            ctx.duration.as_secs_f64()
        );
    }

    async fn on_job_fail(&self, ctx: JobFailContext) {
        if ctx.will_retry {
            log::warn!(
                "Job failed (will retry): {} - {}",
                ctx.job.task_identifier,
                ctx.error
            );
        } else {
            log::error!(
                "Job failed permanently: {} - {}",
                ctx.job.task_identifier,
                ctx.error
            );
        }
    }

    async fn on_job_permanently_fail(&self, ctx: JobPermanentlyFailContext) {
        log::error!(
            "Job exhausted retries: {} (attempts: {}) - {}",
            ctx.job.task_identifier,
            ctx.job.attempts,
            ctx.error
        );
    }
}
```

### Job Validation

Prevent invalid jobs from running using interceptor hooks:

```rust
use backfill::{LifecycleHooks, BeforeJobRunContext, HookResult};

#[derive(Clone)]
struct ValidationPlugin;

impl LifecycleHooks for ValidationPlugin {
    async fn before_job_run(&self, ctx: BeforeJobRunContext) -> HookResult {
        // Check if job payload has required fields
        if let Some(user_id) = ctx.payload.get("user_id") {
            if user_id.as_str().map_or(false, |s| s.is_empty()) {
                return HookResult::Fail("Invalid user_id".to_string());
            }
        } else {
            return HookResult::Fail("Missing user_id".to_string());
        }

        // Check feature flags, rate limits, etc.
        if is_feature_disabled(&ctx.job.task_identifier) {
            return HookResult::Skip; // Skip execution, mark as complete
        }

        HookResult::Continue
    }
}

fn is_feature_disabled(task: &str) -> bool {
    // Check your feature flag system
    false
}
```

**HookResult options:**
- `Continue` - Proceed with normal execution
- `Skip` - Mark job as complete without running it
- `Fail(msg)` - Fail the job with error message
- `Retry { delay }` - Retry job after delay

### Error Tracking

Send errors to Sentry, Rollbar, etc.:

```rust
use backfill::{LifecycleHooks, JobFailContext, JobPermanentlyFailContext};

#[derive(Clone)]
struct ErrorTrackingPlugin {
    sentry_client: Arc<sentry::Client>,
}

impl LifecycleHooks for ErrorTrackingPlugin {
    async fn on_job_fail(&self, ctx: JobFailContext) {
        // Only send to Sentry if NOT retrying (reduce noise)
        if !ctx.will_retry {
            sentry::capture_message(
                &format!("Job {} failed: {}", ctx.job.task_identifier, ctx.error),
                sentry::Level::Error,
            );
        }
    }

    async fn on_job_permanently_fail(&self, ctx: JobPermanentlyFailContext) {
        // Send with full context
        sentry::with_scope(
            |scope| {
                scope.set_tag("task", &ctx.job.task_identifier);
                scope.set_extra("attempts", ctx.job.attempts.into());
                scope.set_extra("job_id", ctx.job.id.into());
            },
            || {
                sentry::capture_message(
                    &format!("Job permanently failed: {}", ctx.error),
                    sentry::Level::Error,
                );
            },
        );
    }
}
```

## Advanced Features

### Payload Transformation

Modify job payloads before they're inserted into the database:

```rust
use backfill::{LifecycleHooks, BeforeJobScheduleContext, JobScheduleResult};

#[derive(Clone)]
struct PayloadEnrichmentPlugin;

impl LifecycleHooks for PayloadEnrichmentPlugin {
    async fn before_job_schedule(&self, ctx: BeforeJobScheduleContext) -> JobScheduleResult {
        let mut payload = ctx.payload.clone();

        // Add metadata
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("scheduled_at".to_string(),
                json!(chrono::Utc::now().to_rfc3339()));
            obj.insert("version".to_string(), json!("1.0"));
        }

        JobScheduleResult::Continue(payload)
    }
}
```

**JobScheduleResult options:**
- `Continue(payload)` - Continue with (possibly modified) payload
- `Skip` - Don't schedule the job (returns error to caller)
- `Fail(msg)` - Fail scheduling with error message

### Wait Time Tracking

Calculate how long jobs wait in the queue:

```rust
impl LifecycleHooks for MetricsPlugin {
    async fn on_job_start(&self, ctx: JobStartContext) {
        if let Some(created_at) = ctx.job.created_at.as_ref() {
            let wait_time = chrono::Utc::now()
                .signed_duration_since(*created_at)
                .num_milliseconds() as f64 / 1000.0;

            metrics::histogram!("job_wait_time_seconds",
                "task" => ctx.job.task_identifier.clone()
            ).record(wait_time);
        }
    }
}
```

## Multiple Plugins

You can register multiple plugins - they're called in registration order:

```rust
let worker = WorkerRunner::builder(config).await?
    .define_job::<MyJob>()
    .add_plugin(MetricsPlugin)
    .add_plugin(LoggingPlugin)
    .add_plugin(ValidationPlugin)
    .build().await?;
```

**Execution order:**
1. Plugins called in registration order for observer hooks
2. For interceptor hooks (`before_job_run`, etc.):
   - If one plugin returns `Skip` or `Fail`, subsequent plugins are NOT called
   - For `before_job_schedule` with `Continue(payload)`, the transformed payload is passed to the next plugin

**Best practices:**
- Put validation plugins first (they might skip/fail jobs)
- Put metrics/logging plugins last (they should see the final decision)

## Migration from JobMetrics

If you were using the old `JobMetrics` helper, here's how to migrate:

**Before (manual instrumentation):**
```rust
impl TaskHandler for MyJob {
    async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        JobMetrics::new("my_queue", Self::IDENTIFIER, &ctx)
            .instrument(|| async {
                // Job logic
            })
            .await
    }
}
```

**After (lifecycle hooks):**
```rust
// 1. Remove JobMetrics from job handlers
impl TaskHandler for MyJob {
    async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        // Just your job logic, no metrics!
        Ok(())
    }
}

// 2. Add a metrics plugin when building the worker
let worker = WorkerRunner::builder(config).await?
    .define_job::<MyJob>()
    .add_plugin(MetricsPlugin)  // Now handles ALL jobs automatically
    .build().await?;
```

**Benefits:**
- No manual instrumentation needed in every job handler
- Metrics are consistent across all jobs
- Richer data (duration, will_retry, etc.)
- Can change metrics implementation without touching job code

## See Also

- `examples/metrics_plugin.rs` - Complete working metrics plugin example
- `tests/plugin_tests.rs` - Plugin integration tests
- [GraphileWorker Lifecycle Hooks Documentation](https://github.com/graphile/worker) - Upstream docs
