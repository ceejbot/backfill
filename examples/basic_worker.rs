//! Basic worker example: register a few job handlers and run a worker
//! with graceful shutdown.
//!
//! Demonstrates:
//! - Implementing `TaskHandler` for several job types
//! - Loading worker config from environment variables
//! - The `tokio::select!` shutdown pattern, suitable for Kubernetes-style pod
//!   restarts
//! - DLQ processing integration (enabled by default; toggle with
//!   `DLQ_PROCESSOR_INTERVAL_SECS=0` to disable)
//!
//! Run with:
//! ```bash
//! DATABASE_URL=postgresql://localhost/backfill cargo run --example basic_worker
//! ```
//!
//! Available env vars:
//! - `DATABASE_URL` (required-ish; defaults to
//!   `postgresql://localhost:5432/backfill`)
//! - `GRAPHILE_WORKER_SCHEMA` (default: `graphile_worker`)
//! - `CONCURRENCY` (default: 10) — max jobs the worker runs in parallel
//! - `POLL_INTERVAL_MS` (default: 200)
//! - `DLQ_PROCESSOR_INTERVAL_SECS` (default: 60)
//! - `RUST_LOG` (default: `info`)

use std::num::ParseIntError;
use std::str::FromStr;
use std::time::Duration;

use backfill::{IntoTaskHandlerResult, TaskHandler, WorkerConfig, WorkerContext, WorkerError, WorkerRunner};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::signal;
use tokio_util::sync::CancellationToken;

// === Configuration ==========================================================

/// Errors that can occur while loading this example's configuration from
/// the environment. These are example-side concerns — the library itself
/// has nothing to do with how *your* application reads env vars.
#[derive(Debug, Error)]
enum ExampleConfigError {
    #[error("invalid {var}={value:?}: {source}")]
    InvalidInteger {
        var: &'static str,
        value: String,
        #[source]
        source: ParseIntError,
    },
}

#[derive(Clone, Debug)]
struct ExampleWorkerConfig {
    database_url: String,
    schema: String,
    concurrency: usize,
    poll_interval: Duration,
    dlq_processor_interval: Duration,
}

impl Default for ExampleWorkerConfig {
    fn default() -> Self {
        Self {
            database_url: "postgresql://localhost:5432/backfill".to_string(),
            schema: "graphile_worker".to_string(),
            concurrency: 10,
            poll_interval: Duration::from_millis(200),
            dlq_processor_interval: Duration::from_secs(60),
        }
    }
}

impl ExampleWorkerConfig {
    fn from_env() -> Result<Self, ExampleConfigError> {
        let mut config = Self::default();

        if let Ok(url) = std::env::var("DATABASE_URL") {
            config.database_url = url;
        }
        if let Ok(schema) = std::env::var("GRAPHILE_WORKER_SCHEMA") {
            config.schema = schema;
        }
        if let Ok(value) = std::env::var("CONCURRENCY") {
            config.concurrency = parse_env("CONCURRENCY", value)?;
        }
        if let Ok(value) = std::env::var("POLL_INTERVAL_MS") {
            let ms: u64 = parse_env("POLL_INTERVAL_MS", value)?;
            config.poll_interval = Duration::from_millis(ms);
        }
        if let Ok(value) = std::env::var("DLQ_PROCESSOR_INTERVAL_SECS") {
            let secs: u64 = parse_env("DLQ_PROCESSOR_INTERVAL_SECS", value)?;
            config.dlq_processor_interval = Duration::from_secs(secs);
        }

        Ok(config)
    }
}

/// Parse an env var as any integer type, attaching the variable name and
/// raw value to the error for actionable diagnostics.
fn parse_env<T>(var: &'static str, value: String) -> Result<T, ExampleConfigError>
where
    T: FromStr<Err = ParseIntError>,
{
    value
        .parse()
        .map_err(|source| ExampleConfigError::InvalidInteger { var, value, source })
}

impl From<ExampleWorkerConfig> for WorkerConfig {
    fn from(value: ExampleWorkerConfig) -> Self {
        WorkerConfig::new(value.database_url)
            .with_schema(value.schema)
            .with_concurrency(value.concurrency)
            .with_poll_interval(value.poll_interval)
            .with_dlq_processor_interval(Some(value.dlq_processor_interval))
    }
}

// === Job handlers ===========================================================

/// A toy job: optional delay, optional simulated failure. Useful for
/// experimenting with retries and DLQ behaviour.
#[derive(Debug, Serialize, Deserialize)]
struct ExampleJob {
    message: String,
    delay_ms: Option<u64>,
    should_fail: Option<bool>,
}

impl TaskHandler for ExampleJob {
    const IDENTIFIER: &'static str = "example_job";

    async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        info!(
            "ExampleJob start (job_id={}, attempt={}/{}, message={:?})",
            ctx.job().id(),
            ctx.job().attempts(),
            ctx.job().max_attempts(),
            self.message,
        );

        if let Some(delay) = self.delay_ms {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }

        if self.should_fail.unwrap_or(false) {
            warn!("ExampleJob configured to fail; will retry per max_attempts");
            // `JobFailed` is the generic *retryable* failure. The
            // permanent-failure short-circuit plugin (auto-registered when
            // DLQ is enabled) only fires on the non-retryable WorkerError
            // variants — InvalidInput, ValidationFailed, Unauthorized, etc.
            return Err(WorkerError::JobFailed {
                message: "simulated failure".to_string(),
            });
        }

        info!("ExampleJob done (job_id={})", ctx.job().id());
        Ok::<(), WorkerError>(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SendEmailJob {
    to: String,
    subject: String,
    body: String,
}

impl TaskHandler for SendEmailJob {
    const IDENTIFIER: &'static str = "send_email";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        info!("SendEmail to={} subject={:?}", self.to, self.subject);

        // Validate at the edge so the rest of the function can assume a
        // well-formed address. Returning `InvalidInput` (a non-retryable
        // WorkerError variant) tells the permanent-failure plugin to
        // route this straight to the DLQ instead of retrying ~12 times
        // over six hours.
        if !self.to.contains('@') {
            return Err(WorkerError::InvalidInput {
                message: format!("not an email address: {:?}", self.to),
            });
        }

        // Pretend to send.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = self.body; // shut up the unused-field lint

        Ok::<(), WorkerError>(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ProcessUserDataJob {
    user_id: String,
    data_type: String,
    batch_size: Option<usize>,
}

impl TaskHandler for ProcessUserDataJob {
    const IDENTIFIER: &'static str = "process_user_data";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        info!(
            "ProcessUserData user_id={} data_type={} batch_size={}",
            self.user_id,
            self.data_type,
            self.batch_size.unwrap_or(100),
        );
        let batch_size = self.batch_size.unwrap_or(100);
        tokio::time::sleep(Duration::from_millis(batch_size as u64 * 10)).await;
        Ok::<(), WorkerError>(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct GenerateReportJob {
    report_type: String,
    date_range: String,
    format: String,
}

impl TaskHandler for GenerateReportJob {
    const IDENTIFIER: &'static str = "generate_report";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        info!(
            "GenerateReport type={} range={} format={}",
            self.report_type, self.date_range, self.format,
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok::<(), WorkerError>(())
    }
}

// === Wiring =================================================================

fn setup_logging() {
    let level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&level))
        .format_timestamp_millis()
        .init();
}

/// Block until SIGINT or (on Unix) SIGTERM arrives. Kubernetes sends
/// SIGTERM on pod stop; Ctrl-C sends SIGINT.
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received SIGINT"),
        _ = terminate => info!("received SIGTERM"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logging();

    info!("starting {} v{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"),);

    let config = ExampleWorkerConfig::from_env()?;
    info!("config: {config:#?}");

    let worker = WorkerRunner::builder(WorkerConfig::from(config))
        .await?
        .define_job::<ExampleJob>()
        .define_job::<SendEmailJob>()
        .define_job::<ProcessUserDataJob>()
        .define_job::<GenerateReportJob>()
        .build()
        .await?;

    info!("worker ready (dlq_enabled={})", worker.dlq_processor_enabled());

    let shutdown = CancellationToken::new();

    // Kubernetes-style shutdown pattern: race the worker against the
    // signal handler. Whichever resolves first ends the program.
    //
    // We pin the worker future so we can await it twice — once inside the
    // select! arm, then again after a signal arrives so we can give the
    // worker a bounded drain window. Without the second await the program
    // would exit the moment the signal fires, killing in-flight jobs.
    let worker_fut = worker.run_until_cancelled(shutdown.clone());
    tokio::pin!(worker_fut);

    tokio::select! {
        result = &mut worker_fut => {
            // Worker exited on its own. Propagate so the orchestrator
            // restarts us.
            match &result {
                Ok(()) => warn!("worker exited unexpectedly without an error"),
                Err(e) => error!("worker exited with error: {e}"),
            }
            result?;
        }
        _ = wait_for_shutdown_signal() => {
            info!("shutdown requested; cancelling worker");
            shutdown.cancel();
            // Give the worker a bounded window to drain. Anything still
            // running after this gets killed when the process exits —
            // your jobs should be idempotent.
            match tokio::time::timeout(Duration::from_secs(25), &mut worker_fut).await {
                Ok(Ok(())) => info!("worker drained cleanly"),
                Ok(Err(e)) => error!("worker error during drain: {e}"),
                Err(_) => warn!("graceful-shutdown deadline reached; forcing exit"),
            }
        }
    }

    info!("stopped");
    Ok(())
}
