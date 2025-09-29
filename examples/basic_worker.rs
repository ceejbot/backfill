//! Basic worker example showing how to use WorkerRunner with custom job types
//!
//! This example demonstrates:
//! - Defining custom job handlers
//! - Configuring multiple queues with different concurrency
//! - Environment-based configuration
//! - Graceful shutdown handling for Kubernetes deployment
//! - DLQ processing integration
//!
//! To run this example:
//! ```bash
//! DATABASE_URL=postgresql://localhost/backfill cargo run --example basic_worker
//! ```

use std::num::ParseIntError;
use std::time::Duration;

use backfill::{
    BackfillError, IntoTaskHandlerResult, QueueConfig, TaskHandler, WorkerConfig, WorkerContext, WorkerError,
    WorkerRunner,
};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{Level, span};

/// Configuration for the example worker loaded from environment variables
#[derive(Clone, Debug)]
pub struct ExampleWorkerConfig {
    pub database_url: String,
    pub schema: String,
    pub fast_concurrency: usize,
    pub bulk_concurrency: usize,
    pub dlq_concurrency: usize,
    pub poll_interval: Duration,
    pub dlq_processor_interval: Duration,
}

impl Default for ExampleWorkerConfig {
    fn default() -> Self {
        Self {
            database_url: "postgresql://localhost:5432/backfill".to_string(),
            schema: "graphile_worker".to_string(),
            fast_concurrency: 10,
            bulk_concurrency: 5,
            dlq_concurrency: 2,
            poll_interval: Duration::from_millis(200),
            dlq_processor_interval: Duration::from_secs(60),
        }
    }
}

impl ExampleWorkerConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self, BackfillError> {
        let mut config = Self::default();

        if let Ok(url) = std::env::var("DATABASE_URL") {
            config.database_url = url;
        }

        if let Ok(schema) = std::env::var("GRAPHILE_WORKER_SCHEMA") {
            config.schema = schema;
        }

        if let Ok(concurrency) = std::env::var("FAST_QUEUE_CONCURRENCY") {
            config.fast_concurrency = concurrency
                .parse()
                .map_err(|e: ParseIntError| BackfillError::FastQueueParseInt(e.to_string()))?;
        }

        if let Ok(concurrency) = std::env::var("BULK_QUEUE_CONCURRENCY") {
            config.bulk_concurrency = concurrency
                .parse()
                .map_err(|e: ParseIntError| BackfillError::BulkQueueParseInt(e.to_string()))?;
        }

        if let Ok(concurrency) = std::env::var("DLQ_CONCURRENCY") {
            config.dlq_concurrency = concurrency
                .parse()
                .map_err(|e: ParseIntError| BackfillError::DeadLetterParseInt(e.to_string()))?;
        }

        if let Ok(interval) = std::env::var("POLL_INTERVAL_MS") {
            let ms: u64 = interval
                .parse()
                .map_err(|e: ParseIntError| BackfillError::PollIntervalParseInt(e.to_string()))?;
            config.poll_interval = Duration::from_millis(ms);
        }

        if let Ok(interval) = std::env::var("DLQ_PROCESSOR_INTERVAL_SECS") {
            let secs: u64 = interval
                .parse()
                .map_err(|e: ParseIntError| BackfillError::DlqProcessorIntervalParseInt(e.to_string()))?;
            config.dlq_processor_interval = Duration::from_secs(secs);
        }

        Ok(config)
    }

    /// Convert to graphile's WorkerConfig for the library
    pub fn into_worker_config(self) -> WorkerConfig {
        self.into()
    }
}

impl From<ExampleWorkerConfig> for WorkerConfig {
    fn from(value: ExampleWorkerConfig) -> Self {
        WorkerConfig {
            database_url: value.database_url,
            schema: value.schema,
            queue_configs: vec![
                QueueConfig::named_queue("fast", value.fast_concurrency),
                QueueConfig::named_queue("bulk", value.bulk_concurrency),
                QueueConfig::named_queue("dead_letter", value.dlq_concurrency),
                QueueConfig::default_queue(5), // Default queue with moderate concurrency
            ],
            poll_interval: value.poll_interval,
            dlq_processor_interval: Some(value.dlq_processor_interval),
        }
    }
}

/// Example job handler for demonstration
#[derive(Debug, Serialize, Deserialize)]
pub struct ExampleJob {
    pub message: String,
    pub delay_ms: Option<u64>,
    pub should_fail: Option<bool>,
}

impl TaskHandler for ExampleJob {
    const IDENTIFIER: &'static str = "example_job";

    async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        let span = span!(Level::INFO, "example_job",
            message = %self.message,
            job_id = ctx.job().id(),
            attempt = ctx.job().attempts(),
            max_attempts = ?ctx.job().max_attempts()
        );
        let _enter = span.enter();

        info!(
            "Processing example job: message='{}'; job_id='{}'; attempt_num='{}';",
            self.message,
            ctx.job().id(),
            ctx.job().attempts()
        );

        // Simulate work delay if specified
        if let Some(delay) = self.delay_ms {
            info!("Simulating work delay; delay_ms='{delay}';");
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }

        // Simulate failure if requested (for testing)
        if self.should_fail.unwrap_or(false) {
            warn!("Job configured to fail for testing purposes");
            return Err(WorkerError::JobFailed {
                message: "Job was configured to fail".to_string(),
            });
        }

        info!(
            "Example job completed successfully; job_id='{}'; processing_time_ms='{}';",
            ctx.job().id(),
            self.delay_ms.unwrap_or(0),
        );
        Ok::<(), WorkerError>(())
    }
}

/// Send email job for notifications and user communication
#[derive(Debug, Serialize, Deserialize)]
pub struct SendEmailJob {
    pub to: String,
    pub subject: String,
    pub body: String,
    pub template: Option<String>,
}

impl TaskHandler for SendEmailJob {
    const IDENTIFIER: &'static str = "send_email";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        let span = span!(Level::INFO, "send_email", to = %self.to, subject = %self.subject);
        let _enter = span.enter();

        info!("Sending email to: {}", self.to);

        // Simulate email sending (in real implementation, integrate with email service)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Validate email address format
        if !self.to.contains('@') {
            return Err(WorkerError::InvalidInput {
                message: format!("Invalid email address: {}", self.to),
            });
        }

        info!("Email sent successfully to {}", self.to);
        Ok::<(), WorkerError>(())
    }
}

/// Process user data for analytics and reporting
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessUserDataJob {
    pub user_id: String,
    pub data_type: String,
    pub batch_size: Option<usize>,
}

impl TaskHandler for ProcessUserDataJob {
    const IDENTIFIER: &'static str = "process_user_data";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        let span = span!(Level::INFO, "process_user_data", user_id = %self.user_id, data_type = %self.data_type);
        let _enter = span.enter();

        info!("Processing {} data for user: {}", self.data_type, self.user_id);

        // Simulate data processing
        let batch_size = self.batch_size.unwrap_or(100);
        tokio::time::sleep(Duration::from_millis(batch_size as u64 * 10)).await;

        info!("User data processing completed for {}", self.user_id);
        Ok::<(), WorkerError>(())
    }
}

/// Generate reports for business intelligence
#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateReportJob {
    pub report_type: String,
    pub date_range: String,
    pub format: String,
}

impl TaskHandler for GenerateReportJob {
    const IDENTIFIER: &'static str = "generate_report";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        let span = span!(Level::INFO, "generate_report",
            report_type = %self.report_type,
            date_range = %self.date_range,
            format = %self.format
        );
        let _enter = span.enter();

        info!("Generating {} report for {}", self.report_type, self.date_range);

        // Simulate report generation
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("Report generation completed: {} format", self.format);
        Ok::<(), WorkerError>(())
    }
}

/// Setup logging using the standard log crate
fn setup_logging() -> Result<(), BackfillError> {
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&log_level))
        .format_timestamp_millis()
        .init();

    info!("Logging initialized at level: {}", log_level);
    Ok(())
}

/// Handle graceful shutdown signals (SIGTERM, SIGINT)
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
        _ = ctrl_c => info!("Received SIGINT"),
        _ = terminate => info!("Received SIGTERM"),
    }
}

/// Main application showing tokio::select! pattern for Kubernetes deployment
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging first
    setup_logging()?;

    info!(
        "Starting basic worker example; name='{}'; version='{}';",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    // Load configuration from environment
    let config = ExampleWorkerConfig::from_env()?;
    info!("Worker configuration: {config:#?}");

    // Create worker runner and register job types
    let worker_config = config.into_worker_config();

    let worker = WorkerRunner::builder(worker_config)
        .await?
        .define_job::<ExampleJob>()
        .define_job::<SendEmailJob>()
        .define_job::<ProcessUserDataJob>()
        .define_job::<GenerateReportJob>()
        .build()
        .await?;

    info!(
        "Worker runner created; workers='{}'; dlq_enabled='{}';",
        worker.worker_count(),
        worker.dlq_processor_enabled()
    );

    // Create shutdown coordination
    let shutdown_token = CancellationToken::new();

    // Use tokio::select! pattern for Kubernetes-style deployment
    // This ensures any service failure causes pod restart
    tokio::select! {
        // Run the worker until cancellation
        result = worker.run_until_cancelled(shutdown_token.clone()) => {
            error!("Worker failed: {:?}", result);
            result?; // Propagate error to cause process exit
        }

        // Wait for shutdown signal
        _ = wait_for_shutdown_signal() => {
            info!("Shutdown signal received, stopping worker gracefully");
            shutdown_token.cancel();

            // Give worker time to stop gracefully
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(25)) => {
                    warn!("Graceful shutdown timeout reached");
                }
                _ = shutdown_token.cancelled() => {
                    info!("Worker stopped gracefully");
                }
            }
        }
    }

    info!("Basic worker example stopped");
    Ok(())
}
