//! Example: Metrics Plugin using Lifecycle Hooks
//!
//! This example demonstrates how to implement a metrics plugin that records job
//! lifecycle events using the `metrics` crate facade. This replaces the need
//! for manual instrumentation with `JobMetrics`.
//!
//! The lifecycle hooks provide rich data about jobs including duration, retry
//! status, error information, and more.
//!
//! # Running this example
//!
//! ```bash
//! # Set up your database
//! export DATABASE_URL="postgresql://localhost:5432/backfill"
//!
//! # Run the example
//! cargo run --example metrics_plugin
//! ```

use std::time::Duration;

use backfill::{
    BackfillClient, BackfillError, HookRegistry, IntoTaskHandlerResult, JobComplete, JobCompleteContext, JobFail,
    JobFailContext, JobPermanentlyFail, JobPermanentlyFailContext, JobStart, JobStartContext, Plugin, TaskHandler,
    WorkerConfig, WorkerContext, WorkerRunner, WorkerShutdown, WorkerShutdownContext, WorkerStart, WorkerStartContext,
    enqueue_fast,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

/// A metrics plugin that records job lifecycle events
///
/// This plugin demonstrates how to use lifecycle hooks to emit metrics for:
/// - Worker lifecycle (start/shutdown)
/// - Job execution (start/complete/fail)
/// - Retry tracking (via will_retry flag)
/// - Duration tracking (from JobCompleteContext)
#[derive(Clone)]
struct MetricsPlugin;

impl Plugin for MetricsPlugin {
    fn register(self, hooks: &mut HookRegistry) {
        hooks.on(WorkerStart, |ctx: WorkerStartContext| async move {
            metrics::gauge!("backfill_worker_active").set(1.0);
            log::info!("Worker {} started - metrics recording enabled", ctx.worker_id);
        });

        hooks.on(WorkerShutdown, |ctx: WorkerShutdownContext| async move {
            metrics::gauge!("backfill_worker_active").set(0.0);
            log::info!("Worker {} shutdown (reason: {:?})", ctx.worker_id, ctx.reason);
        });

        hooks.on(JobStart, |ctx: JobStartContext| async move {
            let task = ctx.job.task_identifier();
            let attempt = ctx.job.attempts();

            metrics::counter!("jobs_started", "task" => task.clone()).increment(1);

            log::debug!("Job started: {} (attempt {})", task, attempt);

            // Track wait time (time from creation to start)
            let created_at = ctx.job.created_at();
            let wait_time = chrono::Utc::now().signed_duration_since(*created_at).num_milliseconds() as f64 / 1000.0;

            metrics::histogram!("job_wait_time_seconds", "task" => task.clone()).record(wait_time);
        });

        hooks.on(JobComplete, |ctx: JobCompleteContext| async move {
            let task = ctx.job.task_identifier();
            let attempt = ctx.job.attempts();
            let duration = ctx.duration.as_secs_f64();

            metrics::counter!("jobs_completed", "task" => task.clone(), "attempt" => attempt.to_string()).increment(1);

            metrics::histogram!("job_duration_seconds", "task" => task.clone(), "status" => "success").record(duration);

            log::info!(
                "Job completed: {} (attempt {}, duration: {:.2}s)",
                task,
                attempt,
                duration
            );
        });

        hooks.on(JobFail, |ctx: JobFailContext| async move {
            let task = ctx.job.task_identifier();
            let attempt = ctx.job.attempts();
            let will_retry = ctx.will_retry;

            // Use will_retry to distinguish between transient failures and final failures
            let status = if will_retry { "retrying" } else { "failed" };

            metrics::counter!(
                "jobs_failed",
                "task" => task.clone(),
                "attempt" => attempt.to_string(),
                "will_retry" => status
            )
            .increment(1);

            // Classify the error type for more detailed metrics
            let error_type = classify_error(&ctx.error);
            metrics::counter!(
                "job_errors_by_type",
                "task" => task.clone(),
                "error_type" => error_type
            )
            .increment(1);

            log::warn!(
                "Job failed: {} (attempt {}, will_retry: {}, error: {})",
                task,
                attempt,
                will_retry,
                ctx.error
            );
        });

        hooks.on(JobPermanentlyFail, |ctx: JobPermanentlyFailContext| async move {
            let task = ctx.job.task_identifier();
            let final_attempt = ctx.job.attempts();

            metrics::counter!("jobs_permanently_failed", "task" => task.clone()).increment(1);

            log::error!(
                "Job permanently failed: {} (final attempt: {}, error: {})",
                task,
                final_attempt,
                ctx.error
            );
        });
    }
}

/// Classify errors for metrics labels
fn classify_error(error: &str) -> &'static str {
    let msg = error.to_lowercase();

    if msg.contains("timeout") || msg.contains("timed out") {
        "timeout"
    } else if msg.contains("network") || msg.contains("connection") {
        "network"
    } else if msg.contains("not found") || msg.contains("404") {
        "not_found"
    } else if msg.contains("unauthorized") || msg.contains("401") {
        "unauthorized"
    } else if msg.contains("forbidden") || msg.contains("403") {
        "forbidden"
    } else if msg.contains("validation") || msg.contains("invalid") {
        "validation"
    } else if msg.contains("rate limit") || msg.contains("429") {
        "rate_limit"
    } else if msg.contains("unavailable") || msg.contains("503") {
        "unavailable"
    } else {
        "unknown"
    }
}

// Example job that succeeds
#[derive(Clone, Serialize, Deserialize)]
struct SuccessJob {
    message: String,
}

impl TaskHandler for SuccessJob {
    const IDENTIFIER: &'static str = "success_job";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        log::info!("Processing success job: {}", self.message);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok::<(), BackfillError>(())
    }
}

// Example job that fails and retries
#[derive(Clone, Serialize, Deserialize)]
struct RetryJob {
    attempt_to_succeed: i16,
}

impl TaskHandler for RetryJob {
    const IDENTIFIER: &'static str = "retry_job";

    async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        let current_attempt = *ctx.job().attempts();

        log::info!(
            "Processing retry job (attempt {}/{})",
            current_attempt,
            self.attempt_to_succeed
        );

        if current_attempt < self.attempt_to_succeed {
            Err(BackfillError::WorkerRuntime(format!(
                "Temporary failure on attempt {}",
                current_attempt
            )))
        } else {
            log::info!("Retry job succeeded on attempt {}", current_attempt);
            Ok(())
        }
    }
}

// Example job that always fails
#[derive(Clone, Serialize, Deserialize)]
struct FailJob {
    error_type: String,
}

impl TaskHandler for FailJob {
    const IDENTIFIER: &'static str = "fail_job";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        log::info!("Processing fail job (will fail with: {})", self.error_type);
        Err::<(), BackfillError>(BackfillError::WorkerRuntime(self.error_type))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Initialize metrics (using metrics-exporter-prometheus for this example)
    // In production, you'd typically set up a Prometheus endpoint or StatsD
    // exporter
    let _recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus recorder");

    log::info!("Metrics available at http://localhost:9000/metrics");

    // Get database URL from environment
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost:5432/backfill".to_string());

    let client = BackfillClient::new(&database_url).await?;

    // Enqueue some sample jobs
    log::info!("Enqueueing sample jobs...");

    // Success job
    enqueue_fast(
        &client,
        "success_job",
        &SuccessJob {
            message: "This will succeed".to_string(),
        },
        None,
    )
    .await?;

    // Retry job (will fail twice, then succeed)
    enqueue_fast(&client, "retry_job", &RetryJob { attempt_to_succeed: 3 }, None).await?;

    // Fail job (will fail permanently after max attempts)
    enqueue_fast(
        &client,
        "fail_job",
        &FailJob {
            error_type: "timeout error - simulated".to_string(),
        },
        None,
    )
    .await?;

    log::info!("Jobs enqueued, starting worker with metrics plugin...");

    // Create worker with metrics plugin
    let config = WorkerConfig::new(&database_url).with_poll_interval(Duration::from_millis(100));

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SuccessJob>()
        .define_job::<RetryJob>()
        .define_job::<FailJob>()
        .add_plugin(MetricsPlugin) // Add the metrics plugin!
        .build()
        .await?;

    let shutdown_token = CancellationToken::new();

    // Spawn worker in background
    let worker_handle = worker.spawn_background(shutdown_token.clone());

    // Spawn metrics server
    let metrics_handle = tokio::spawn(async move {
        log::info!("Starting Prometheus metrics server on :9000");
        let addr: std::net::SocketAddr = ([127, 0, 0, 1], 9000).into();
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()
            .expect("Failed to start metrics server");

        // Keep server running
        tokio::signal::ctrl_c().await.ok();
    });

    // Let worker run for a bit to process jobs
    log::info!("Worker running. Press Ctrl+C to stop or wait 30 seconds...");
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            log::info!("Time limit reached, shutting down...");
        }
        _ = tokio::signal::ctrl_c() => {
            log::info!("Ctrl+C received, shutting down...");
        }
    }

    // Shutdown worker
    shutdown_token.cancel();
    worker_handle.await??;
    metrics_handle.abort();

    log::info!("Worker stopped. Check the metrics that were recorded!");

    // Print a sample of metrics
    log::info!("\nExample metrics that were recorded:");
    log::info!("- backfill_worker_active: Worker start/stop");
    log::info!("- jobs_started: Job execution begins");
    log::info!("- jobs_completed: Successful job completion with duration");
    log::info!("- jobs_failed: Failed jobs with retry status");
    log::info!("- jobs_permanently_failed: Jobs that exhausted retries");
    log::info!("- job_wait_time_seconds: Time from enqueue to start");
    log::info!("- job_duration_seconds: How long jobs took to execute");

    Ok(())
}
