use std::time::Duration;

use backfill::{
    BackfillClient, BackfillError, IntoTaskHandlerResult, TaskHandler, WorkerContext, WorkerError, WorkerOptions,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{Level, error, info, span, warn};

// Worker binary uses tracing for structured logging

/// Configuration for the worker
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub database_url: String,
    pub schema: String,
    pub fast_queue_concurrency: usize,
    pub bulk_queue_concurrency: usize,
    pub dead_letter_queue_concurrency: usize,
    pub poll_interval: Duration,
    pub shutdown_timeout: Duration,
    pub dlq_processor_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            database_url: "postgresql://localhost:5432/backfill".to_string(),
            schema: "graphile_worker".to_string(),
            fast_queue_concurrency: 10,
            bulk_queue_concurrency: 5,
            dead_letter_queue_concurrency: 2,
            poll_interval: Duration::from_millis(200),
            shutdown_timeout: Duration::from_secs(30),
            dlq_processor_interval: Duration::from_secs(60), // Check for failed jobs every minute
        }
    }
}

impl WorkerConfig {
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
            config.fast_queue_concurrency = concurrency
                .parse()
                .map_err(|e: std::num::ParseIntError| BackfillError::FastQueueParseInt(e.to_string()))?;
        }

        if let Ok(concurrency) = std::env::var("BULK_QUEUE_CONCURRENCY") {
            config.bulk_queue_concurrency = concurrency
                .parse()
                .map_err(|e: std::num::ParseIntError| BackfillError::BulkQueueParseInt(e.to_string()))?;
        }

        if let Ok(concurrency) = std::env::var("DEAD_LETTER_QUEUE_CONCURRENCY") {
            config.dead_letter_queue_concurrency = concurrency
                .parse()
                .map_err(|e: std::num::ParseIntError| BackfillError::DeadLetterParseInt(e.to_string()))?;
        }

        if let Ok(interval_ms) = std::env::var("POLL_INTERVAL_MS") {
            let ms: u64 = interval_ms
                .parse()
                .map_err(|e: std::num::ParseIntError| BackfillError::PollIntervalParseInt(e.to_string()))?;
            config.poll_interval = Duration::from_millis(ms);
        }

        if let Ok(timeout_secs) = std::env::var("SHUTDOWN_TIMEOUT_SECS") {
            let secs: u64 = timeout_secs
                .parse()
                .map_err(|e: std::num::ParseIntError| BackfillError::ShutdownTimeoutParseInt(e.to_string()))?;
            config.shutdown_timeout = Duration::from_secs(secs);
        }

        if let Ok(interval_secs) = std::env::var("DLQ_PROCESSOR_INTERVAL_SECS") {
            let secs: u64 = interval_secs
                .parse()
                .map_err(|e: std::num::ParseIntError| BackfillError::DlqProcessorIntervalParseInt(e.to_string()))?;
            config.dlq_processor_interval = Duration::from_secs(secs);
        }

        Ok(config)
    }
}

// Note: Error classification is now handled by the WorkerError type

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
            job_id = ctx.job().id(),
            attempt = ctx.job().attempts(),
            "Processing example job: {}",
            self.message
        );

        // Simulate work delay if specified
        if let Some(delay) = self.delay_ms {
            info!(delay_ms = delay, "Simulating work delay");
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
            job_id = ctx.job().id(),
            processing_time_ms = ?self.delay_ms.unwrap_or(0),
            "Example job completed successfully"
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

        // Simulate data processing work
        let processing_time = self.batch_size.unwrap_or(10) * 50; // 50ms per item
        tokio::time::sleep(Duration::from_millis(processing_time as u64)).await;

        info!("User data processing completed for user: {}", self.user_id);
        Ok::<(), WorkerError>(())
    }
}

/// Generate reports job for business intelligence
#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateReportJob {
    pub report_type: String,
    pub date_range: String,
    pub output_format: String,
    pub recipients: Vec<String>,
}

impl TaskHandler for GenerateReportJob {
    const IDENTIFIER: &'static str = "generate_report";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        let span = span!(Level::INFO, "generate_report", report_type = %self.report_type, format = %self.output_format);
        let _enter = span.enter();

        info!(
            "Generating {} report for date range: {}",
            self.report_type, self.date_range
        );

        // Simulate report generation (could be CPU intensive)
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Validate output format
        if !["pdf", "csv", "json"].contains(&self.output_format.as_str()) {
            return Err(WorkerError::InvalidInput {
                message: format!("Unsupported output format: {}", self.output_format),
            });
        }

        info!("Report generated successfully: {} format", self.output_format);

        // Send to recipients (simulate)
        for recipient in &self.recipients {
            info!("Sending report to: {}", recipient);
        }

        Ok::<(), WorkerError>(())
    }
}

/// Simple job processor function for demonstration
#[allow(dead_code)]
async fn process_example_job(payload: serde_json::Value) -> Result<(), WorkerError> {
    let span = span!(Level::INFO, "process_example_job");
    let _enter = span.enter();

    // Deserialize the payload
    let job: ExampleJob = serde_json::from_value(payload)?;

    info!("Processing example job: {}", job.message);

    // This function is now deprecated since we use TaskHandler directly
    // Left for compatibility/example purposes
    warn!("process_example_job called - use TaskHandler directly instead");
    Err(WorkerError::JobFailed {
        message: "Use TaskHandler implementation instead".to_string(),
    })
}

/// Real worker implementation using GraphileWorker
async fn run_worker(config: &WorkerConfig) -> Result<(), BackfillError> {
    info!("Setting up database connection pool");

    // Create PostgreSQL connection pool
    let total_connections =
        (config.fast_queue_concurrency + config.bulk_queue_concurrency + config.dead_letter_queue_concurrency) as u32;
    let pg_pool = PgPoolOptions::new()
        .max_connections(total_connections.max(5)) // Ensure at least 5 connections
        .connect(&config.database_url)
        .await?;

    info!("Database connection pool established");

    // Ensure the BackfillClient schema is set up (for job enqueuing compatibility)
    let client = BackfillClient::new(&config.database_url).await?;
    info!("Backfill schema initialized");

    // Initialize dead letter queue schema
    if let Err(e) = client.init_dlq().await {
        error!(error = %e, "Failed to initialize DLQ schema");
        return Err(e);
    }
    info!("Dead letter queue schema initialized");

    info!("Starting GraphileWorker with configuration:");
    info!("  Schema: {}", config.schema);
    info!("  Fast queue concurrency: {}", config.fast_queue_concurrency);
    info!("  Bulk queue concurrency: {}", config.bulk_queue_concurrency);
    info!("  Dead letter concurrency: {}", config.dead_letter_queue_concurrency);
    info!("  Poll interval: {:?}", config.poll_interval);

    // Configure and start the GraphileWorker
    let worker = WorkerOptions::default()
        .concurrency(config.fast_queue_concurrency)
        .schema(&config.schema)
        .poll_interval(config.poll_interval)
        .define_job::<ExampleJob>()
        .define_job::<SendEmailJob>()
        .define_job::<ProcessUserDataJob>()
        .define_job::<GenerateReportJob>()
        .pg_pool(pg_pool)
        .init()
        .await?;

    info!("GraphileWorker initialized successfully");

    // Set up cancellation token for graceful shutdown
    let cancellation_token = CancellationToken::new();
    let dlq_cancellation_token = cancellation_token.clone();

    // Start the DLQ processor background task
    info!(
        dlq_interval_secs = config.dlq_processor_interval.as_secs(),
        "Starting DLQ processor background task"
    );

    let dlq_processor_handle = client.start_dlq_processor(config.dlq_processor_interval, dlq_cancellation_token);

    info!("Worker is now ready to process jobs");

    // Run the worker - this will block and process jobs until shutdown
    let worker_result = worker
        .run()
        .await
        .map_err(|e| BackfillError::WorkerRuntime(e.to_string()));

    // Signal DLQ processor to shutdown
    cancellation_token.cancel();

    // Wait for DLQ processor to finish (with timeout)
    match tokio::time::timeout(Duration::from_secs(5), dlq_processor_handle).await {
        Ok(_) => info!("DLQ processor stopped gracefully"),
        Err(_) => warn!("DLQ processor shutdown timeout - may still be running"),
    }

    info!("Worker stopped");
    worker_result?;
    Ok(())
}

/// Setup logging for the worker with feature-based configuration
fn setup_logging() -> Result<(), BackfillError> {
    {
        use tracing_subscriber::EnvFilter;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("backfill=info,backfill_worker=info"));

        // Check if JSON output is requested via environment variable
        let use_json = std::env::var("LOG_FORMAT")
            .map(|format| format.to_lowercase() == "json")
            .unwrap_or(false);

        let subscriber = tracing_subscriber::registry().with(filter);

        if use_json {
            // JSON structured logging for production
            subscriber
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_current_span(false)
                        .with_span_list(true)
                        .with_target(true)
                        .with_thread_ids(true)
                        .with_level(true)
                        .with_file(false)
                        .with_line_number(false),
                )
                .init();
        } else {
            // Human-readable logging for development
            subscriber
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(false)
                        .with_thread_ids(true)
                        .with_level(true)
                        .with_ansi(atty::is(atty::Stream::Stdout))
                        .with_file(false)
                        .with_line_number(false),
                )
                .init();
        }
    }

    Ok(())
}

/// Handle graceful shutdown
async fn wait_for_shutdown() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}

#[tokio::main]
async fn main() -> Result<(), BackfillError> {
    // Setup logging first
    setup_logging()?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        name = env!("CARGO_PKG_NAME"),
        "Starting backfill worker"
    );

    // Load configuration
    let config = WorkerConfig::from_env()?;
    info!(
        database_url = %config.database_url.split('@').last().unwrap_or("redacted"),
        schema = %config.schema,
        fast_concurrency = config.fast_queue_concurrency,
        bulk_concurrency = config.bulk_queue_concurrency,
        dlq_concurrency = config.dead_letter_queue_concurrency,
        poll_interval_ms = config.poll_interval.as_millis(),
        shutdown_timeout_secs = config.shutdown_timeout.as_secs(),
        dlq_processor_interval_secs = config.dlq_processor_interval.as_secs(),
        "Worker configuration loaded"
    );

    // Start the worker with graceful shutdown handling
    info!("Starting GraphileWorker...");

    // Set up graceful shutdown handling with tokio::select!
    let shutdown_signal = async {
        wait_for_shutdown().await;
        info!("Shutdown signal received, stopping worker gracefully");
    };

    let worker_task = async {
        match run_worker(&config).await {
            Ok(()) => info!("Worker stopped normally"),
            Err(e) => {
                error!("Worker stopped with error: {}", e);
                return Err(e);
            }
        }
        Ok(())
    };

    // Run worker and wait for either completion or shutdown signal
    tokio::select! {
        result = worker_task => {
            match result {
                Ok(()) => info!("Worker completed successfully"),
                Err(e) => error!("Worker failed: {}", e),
            }
        }
        _ = shutdown_signal => {
            info!("Graceful shutdown initiated");
            // GraphileWorker should handle graceful shutdown internally
            // Give it some time to finish current jobs
            tokio::time::sleep(config.shutdown_timeout).await;
        }
    }

    info!("Backfill worker stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let config = WorkerConfig::default();
        assert_eq!(config.database_url, "postgresql://localhost:5432/backfill");
        assert_eq!(config.schema, "graphile_worker");
        assert_eq!(config.fast_queue_concurrency, 10);
        assert_eq!(config.bulk_queue_concurrency, 5);
        assert_eq!(config.dead_letter_queue_concurrency, 2);
        assert_eq!(config.poll_interval, Duration::from_millis(200));
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
        assert_eq!(config.dlq_processor_interval, Duration::from_secs(60));
    }
}
