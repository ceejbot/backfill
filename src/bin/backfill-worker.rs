use std::time::Duration;

use backfill::{BackfillClient, BackfillError, WorkerError};
use serde::{Deserialize, Serialize};
use tokio::signal;
use tracing::{Level, error, info, span, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

/// Handler for example jobs
pub async fn handle_example_job(job: ExampleJob) -> Result<(), WorkerError> {
    let span = span!(Level::INFO, "handle_example_job", message = %job.message);
    let _enter = span.enter();

    info!("Processing example job: {}", job.message);

    // Simulate work delay if specified
    if let Some(delay) = job.delay_ms {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }

    // Simulate failure if requested (for testing)
    if job.should_fail.unwrap_or(false) {
        return Err(WorkerError::JobFailed {
            message: "Job was configured to fail".to_string(),
        });
    }

    info!("Example job completed successfully");
    Ok(())
}

/// Simple job processor function for demonstration
async fn process_example_job(payload: serde_json::Value) -> Result<(), WorkerError> {
    let span = span!(Level::INFO, "process_example_job");
    let _enter = span.enter();

    // Deserialize the payload
    let job: ExampleJob = serde_json::from_value(payload)?;

    info!("Processing example job: {}", job.message);

    // Execute the actual job handler
    match handle_example_job(job).await {
        Ok(()) => {
            info!("Job completed successfully");
            Ok(())
        }
        Err(worker_error) => {
            if worker_error.is_retryable() {
                warn!("Job failed with retryable error: {}", worker_error);
            } else {
                error!("Job failed with non-retryable error: {}", worker_error);
                // For non-retryable errors, we still return the error
                // The worker framework will handle DLQ routing
            }
            Err(worker_error)
        }
    }
}

/// Simple worker implementation that connects to the database and processes
/// jobs
async fn run_worker(config: &WorkerConfig) -> Result<(), BackfillError> {
    // Create the backfill client for database setup (ensures DB is set up)
    let _client = BackfillClient::new(&config.database_url).await?;
    info!("Database connection and schema setup completed");

    // For now, implement a simple polling loop
    // In a full implementation, we would use GraphileWorker properly
    info!("Worker started - polling for jobs (mock implementation)");

    loop {
        // This is a placeholder implementation
        // A real worker would:
        // 1. Poll the database for jobs
        // 2. Process jobs according to priority and queue
        // 3. Handle retries and error classification
        // 4. Update job status in the database

        tokio::time::sleep(config.poll_interval).await;

        // Check for shutdown signal (non-blocking)
        // In a real implementation, this would be handled differently
        // using select! to wait for either jobs or shutdown
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Continue processing
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received, stopping worker");
                break;
            }
        }
    }

    Ok(())
}

/// Setup tracing for the worker
fn setup_tracing() -> Result<(), BackfillError> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("backfill=info,backfill_worker=info"));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(true)
                .with_level(true)
                .with_ansi(atty::is(atty::Stream::Stdout)),
        )
        .with(filter)
        .init();

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
    // Setup tracing first
    setup_tracing()?;

    info!("Starting backfill worker");

    // Load configuration
    let config = WorkerConfig::from_env()?;
    info!("Worker configuration loaded: {:#?}", config);

    // Start the worker
    info!("Starting worker...");

    let shutdown_timeout = config.shutdown_timeout;

    // Run the worker with graceful shutdown
    let worker_handle = tokio::spawn(async move {
        match run_worker(&config).await {
            Ok(()) => info!("Worker stopped normally"),
            Err(e) => error!("Worker stopped with error: {}", e),
        }
    });

    // Wait for shutdown signal
    wait_for_shutdown().await;

    // Give the worker time to finish current jobs
    info!(
        "Waiting for worker to shutdown gracefully (timeout: {:?})",
        shutdown_timeout
    );

    match tokio::time::timeout(shutdown_timeout, worker_handle).await {
        Ok(_) => info!("Worker shutdown complete"),
        Err(_) => {
            warn!("Worker shutdown timed out, forcing exit");
        }
    }

    info!("Backfill worker stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_classification() {
        // Non-retryable errors
        let non_retryable_cases = vec![
            "Invalid input provided", "Malformed JSON data", "Unauthorized access", "Forbidden operation",
            "Resource not found", "Bad request format", "Validation failed", "Parse error in payload",
        ];

        for case in non_retryable_cases {
            let error = WorkerError::classify_from_message(case.to_string());
            assert!(!error.is_retryable(), "Expected '{}' to be non-retryable", case);
        }

        // Retryable errors
        let retryable_cases = vec![
            "Connection timeout",
            "Network unreachable",
            "Service unavailable",
            "Rate limit exceeded",
            "Too many requests",
            "Database connection failed",
        ];

        for case in retryable_cases {
            let error = WorkerError::classify_from_message(case.to_string());
            assert!(error.is_retryable(), "Expected '{}' to be retryable", case);
        }

        // Unknown errors default to retryable
        let error = WorkerError::classify_from_message("Some unknown error".to_string());
        assert!(error.is_retryable(), "Unknown errors should be retryable");
    }

    #[test]
    fn test_config_defaults() {
        let config = WorkerConfig::default();
        assert_eq!(config.database_url, "postgresql://localhost:5432/backfill");
        assert_eq!(config.schema, "graphile_worker");
        assert_eq!(config.fast_queue_concurrency, 10);
        assert_eq!(config.bulk_queue_concurrency, 5);
        assert_eq!(config.dead_letter_queue_concurrency, 2);
        assert_eq!(config.poll_interval, Duration::from_millis(200));
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
    }
}
