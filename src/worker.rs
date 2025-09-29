//! Worker runner for flexible integration with existing applications
//!
//! The `WorkerRunner` provides a reusable component for processing jobs that
//! can be integrated into existing async applications in multiple ways:
//!
//! 1. **`select!` Integration** - Run alongside other services with unified
//!    shutdown
//! 2. **Background Tasks** - Spawn as independent tokio tasks with cancellation
//!    tokens
//! 3. **One-shot Processing** - Process available jobs and return
//!
//! # Examples
//!
//! ## Kubernetes-style `select!` Integration
//! ```rust,no_run
//! use backfill::{WorkerRunner, WorkerConfig, QueueConfig};
//! use tokio_util::sync::CancellationToken;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = WorkerConfig {
//!         database_url: "postgresql://localhost/myapp".to_string(),
//!         schema: "jobs".to_string(),
//!         queue_configs: vec![
//!             QueueConfig::default_queue(10),
//!             QueueConfig::named_queue("bulk", 50),
//!         ],
//!         poll_interval: Duration::from_millis(100),
//!         dlq_processor_interval: Some(Duration::from_secs(30)),
//!     };
//!
//!     let worker = WorkerRunner::new(config).await?;
//!     let shutdown_token = CancellationToken::new();
//!
//!     tokio::select! {
//!         result = run_axum_server() => {
//!             eprintln!("Axum server failed: {:?}", result);
//!             result?;
//!         }
//!         result = worker.run_until_cancelled(shutdown_token.clone()) => {
//!             eprintln!("Worker failed: {:?}", result);
//!             result?;
//!         }
//!         _ = wait_for_shutdown_signal() => {
//!             println!("Shutting down gracefully");
//!             shutdown_token.cancel();
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Background Task Integration
//! ```rust,no_run
//! use backfill::{WorkerRunner, WorkerConfig};
//! use tokio_util::sync::CancellationToken;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let worker = WorkerRunner::new(WorkerConfig::default()).await?;
//!     let shutdown_token = CancellationToken::new();
//!
//!     // Start worker in background
//!     let worker_handle = worker.spawn_background(shutdown_token.clone());
//!
//!     // Run your application
//!     run_my_application().await?;
//!
//!     // Shutdown worker
//!     shutdown_token.cancel();
//!     worker_handle.await??;
//!
//!     Ok(())
//! }
//! ```

use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{BackfillClient, BackfillError, TaskHandler, WorkerOptions};

/// Configuration for a worker queue
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Queue name (None for default queue)
    pub name: Option<String>,
    /// Number of concurrent jobs to process in this queue
    pub concurrency: usize,
    /// Priority range for jobs in this queue (inclusive)
    pub priority_range: Option<(i32, i32)>,
}

impl QueueConfig {
    /// Create configuration for the default queue
    pub fn default_queue(concurrency: usize) -> Self {
        Self {
            name: None,
            concurrency,
            priority_range: None,
        }
    }

    /// Create configuration for a named queue
    pub fn named_queue(name: impl Into<String>, concurrency: usize) -> Self {
        Self {
            name: Some(name.into()),
            concurrency,
            priority_range: None,
        }
    }

    /// Create configuration for a priority-based queue
    pub fn priority_queue(name: impl Into<String>, concurrency: usize, min_priority: i32, max_priority: i32) -> Self {
        Self {
            name: Some(name.into()),
            concurrency,
            priority_range: Some((min_priority, max_priority)),
        }
    }
}

/// Flexible worker configuration for integration with existing applications
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// PostgreSQL database connection URL
    pub database_url: String,
    /// GraphileWorker schema name (default: "graphile_worker")
    pub schema: String,
    /// Queue configurations - each queue will run with its own concurrency
    pub queue_configs: Vec<QueueConfig>,
    /// How often to poll for new jobs
    pub poll_interval: Duration,
    /// How often to process failed jobs into DLQ (None to disable)
    pub dlq_processor_interval: Option<Duration>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            database_url: "postgresql://localhost:5432/backfill".to_string(),
            schema: "graphile_worker".to_string(),
            queue_configs: vec![QueueConfig::default_queue(10)],
            poll_interval: Duration::from_millis(200),
            dlq_processor_interval: Some(Duration::from_secs(60)),
        }
    }
}

impl WorkerConfig {
    /// Create a new configuration with database URL
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            ..Default::default()
        }
    }

    /// Set the GraphileWorker schema
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Set queue configurations
    pub fn with_queues(mut self, queues: Vec<QueueConfig>) -> Self {
        self.queue_configs = queues;
        self
    }

    /// Set poll interval
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set DLQ processor interval (or disable with None)
    pub fn with_dlq_processor_interval(mut self, interval: Option<Duration>) -> Self {
        self.dlq_processor_interval = interval;
        self
    }
}

/// Cloneable wrapper around WorkerOptions configuration
///
/// This allows us to store the configuration needed to create WorkerOptions
/// and recreate them as needed, since the underlying WorkerOptions doesn't
/// implement Clone.
#[derive(Clone)]
pub struct WorkerOptionsBuilder {
    pub(crate) schema: String,
    pub(crate) poll_interval: Duration,
    pub(crate) pool: sqlx::PgPool,
    pub(crate) concurrency: usize,
    pub(crate) queue_name: Option<String>,
    pub(crate) job_handlers: Vec<JobHandlerConfig>,
}

/// Configuration for a job handler that can be recreated
#[derive(Clone)]
pub(crate) struct JobHandlerConfig {
    #[allow(dead_code)]
    pub identifier: String,
    pub builder_fn: fn(WorkerOptions) -> WorkerOptions,
}

impl WorkerOptionsBuilder {
    /// Create a new WorkerOptionsBuilder from config
    pub async fn new(config: &WorkerConfig) -> Result<Self, BackfillError> {
        // Create client to get the pool
        let client = if config.schema == "graphile_worker" {
            BackfillClient::new(&config.database_url).await?
        } else {
            BackfillClient::new_with_schema(&config.database_url, &config.schema).await?
        };

        // Initialize DLQ if processor is enabled
        if config.dlq_processor_interval.is_some() {
            client.init_dlq().await?;
        }

        // Use the first queue config for concurrency, or default
        let concurrency = config.queue_configs.first().map(|q| q.concurrency).unwrap_or(10);

        // Use the first queue's name if specified
        let queue_name = config.queue_configs.first().and_then(|q| q.name.clone());

        Ok(Self {
            schema: config.schema.clone(),
            poll_interval: config.poll_interval,
            pool: client.pool().clone(),
            concurrency,
            queue_name,
            job_handlers: Vec::new(),
        })
    }

    /// Register a job type with this builder
    pub fn define_job<T: TaskHandler + 'static>(mut self) -> Self {
        // Store a function that can add this job type to WorkerOptions
        let builder_fn = |worker_options: WorkerOptions| worker_options.define_job::<T>();

        self.job_handlers.push(JobHandlerConfig {
            identifier: T::IDENTIFIER.to_string(),
            builder_fn,
        });

        self
    }

    /// Set the concurrency for this worker
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the queue name for this worker
    pub fn with_queue_name(mut self, queue_name: Option<String>) -> Self {
        self.queue_name = queue_name;
        self
    }
}

impl From<WorkerOptionsBuilder> for WorkerOptions {
    fn from(builder: WorkerOptionsBuilder) -> Self {
        let mut worker_options = WorkerOptions::default()
            .schema(&builder.schema)
            .poll_interval(builder.poll_interval)
            .pg_pool(builder.pool)
            .concurrency(builder.concurrency);

        // Note: GraphileWorker doesn't expose queue_name configuration in WorkerOptions
        // Multiple queues are handled at the job enqueueing level
        if builder.queue_name.is_some() {
            tracing::warn!("Queue name configuration is not supported by GraphileWorker's WorkerOptions - ignoring");
        }

        // Register all job handlers
        for handler_config in builder.job_handlers {
            worker_options = (handler_config.builder_fn)(worker_options);
        }

        worker_options
    }
}

/// Reusable worker runner for flexible integration patterns
///
/// The `WorkerRunner` can be integrated into existing applications in several
/// ways:
/// - Run until cancelled (for `tokio::select!` patterns)
/// - Spawn as background task
/// - Process available jobs once and return
pub struct WorkerRunner {
    config: WorkerConfig,
    client: BackfillClient,
    worker_options_builder: WorkerOptionsBuilder,
}

impl WorkerRunner {
    /// Create a new worker runner builder
    ///
    /// This creates a builder that you can use to configure job types before
    /// finalizing the worker runner.
    pub async fn builder(config: WorkerConfig) -> Result<WorkerRunnerBuilder, BackfillError> {
        WorkerRunnerBuilder::new(config).await
    }

    /// Create a worker runner from a pre-configured builder
    async fn from_builder(
        config: WorkerConfig,
        worker_options_builder: WorkerOptionsBuilder,
    ) -> Result<Self, BackfillError> {
        // Create the client
        let client = if config.schema == "graphile_worker" {
            BackfillClient::new(&config.database_url).await?
        } else {
            BackfillClient::new_with_schema(&config.database_url, &config.schema).await?
        };

        // Initialize DLQ if processor is enabled
        if config.dlq_processor_interval.is_some() {
            client.init_dlq().await?;
        }

        Ok(Self {
            config,
            client,
            worker_options_builder,
        })
    }

    /// Create a worker instance from the stored configuration
    async fn create_worker(&self) -> Result<graphile_worker::Worker, BackfillError> {
        let worker_options: WorkerOptions = self.worker_options_builder.clone().into();
        worker_options
            .init()
            .await
            .map_err(|e| BackfillError::WorkerRuntime(e.to_string()))
    }
}

/// Builder for WorkerRunner that allows configuring job types
pub struct WorkerRunnerBuilder {
    config: WorkerConfig,
    worker_options_builder: WorkerOptionsBuilder,
}

impl WorkerRunnerBuilder {
    async fn new(config: WorkerConfig) -> Result<Self, BackfillError> {
        let worker_options_builder = WorkerOptionsBuilder::new(&config).await?;

        Ok(Self {
            config,
            worker_options_builder,
        })
    }

    /// Register a job type with this worker runner
    pub fn define_job<T: TaskHandler + 'static>(mut self) -> Self {
        self.worker_options_builder = self.worker_options_builder.define_job::<T>();
        self
    }

    /// Build the final WorkerRunner
    pub async fn build(self) -> Result<WorkerRunner, BackfillError> {
        WorkerRunner::from_builder(self.config, self.worker_options_builder).await
    }
}

impl WorkerRunner {
    /// Run the worker until the cancellation token is triggered
    ///
    /// This method is designed for `tokio::select!` integration patterns where
    /// the worker runs alongside other services with unified shutdown handling.
    ///
    /// The method will return when:
    /// - The cancellation token is cancelled
    /// - An unrecoverable error occurs
    /// - The worker completes (unusual)
    pub async fn run_until_cancelled(&self, cancellation_token: CancellationToken) -> Result<(), BackfillError> {
        tracing::info!(
            dlq_enabled = self.config.dlq_processor_interval.is_some(),
            "Starting worker runner"
        );

        // Start DLQ processor if configured
        let dlq_handle = if let Some(interval) = self.config.dlq_processor_interval {
            tracing::info!(interval_secs = interval.as_secs(), "Starting DLQ processor");
            Some(self.client.start_dlq_processor(interval, cancellation_token.clone()))
        } else {
            None
        };

        // Create and start the worker
        tracing::info!("Starting worker instance");
        let worker = self.create_worker().await?;

        let handle = tokio::spawn({
            async move {
                worker
                    .run()
                    .await
                    .map_err(|e| BackfillError::WorkerRuntime(e.to_string()))
            }
        });

        // Wait for cancellation or worker completion
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Cancellation requested, stopping worker");
            }
            result = handle => {
                match result {
                    Ok(worker_result) => {
                        tracing::warn!("Worker completed unexpectedly");
                        worker_result?;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Worker task failed");
                        return Err(BackfillError::WorkerRuntime(e.to_string()));
                    }
                }
            }
        }

        // Wait for DLQ processor to stop
        if let Some(dlq_handle) = dlq_handle {
            tracing::info!("Waiting for DLQ processor to stop");
            match tokio::time::timeout(Duration::from_secs(5), dlq_handle).await {
                Ok(Ok(())) => tracing::info!("DLQ processor stopped gracefully"),
                Ok(Err(e)) => tracing::warn!(error = %e, "DLQ processor stopped with error"),
                Err(_) => tracing::warn!("DLQ processor shutdown timeout"),
            }
        }

        tracing::info!("Worker runner stopped");
        Ok(())
    }

    /// Spawn the worker as a background task
    ///
    /// This method is designed for applications that want to run the worker
    /// alongside other services but manage them independently.
    ///
    /// Returns a `JoinHandle` that can be awaited to get the final result.
    pub fn spawn_background(&self, cancellation_token: CancellationToken) -> JoinHandle<Result<(), BackfillError>> {
        let runner = WorkerRunner {
            config: self.config.clone(),
            client: self.client.clone(),
            worker_options_builder: self.worker_options_builder.clone(),
        };

        tokio::spawn(async move { runner.run_until_cancelled(cancellation_token).await })
    }

    /// Process all currently available jobs and return
    ///
    /// This method is designed for batch processing or testing scenarios where
    /// you want to process the current job queue without running a persistent
    /// worker.
    ///
    /// Returns the number of jobs processed across all workers.
    pub async fn process_available_jobs(&self) -> Result<usize, BackfillError> {
        tracing::info!("Processing available jobs (one-shot mode)");

        // For now, this is a simplified implementation
        // In a full implementation, we'd query the job count and process them
        // without starting the persistent polling loop

        tracing::warn!("process_available_jobs is not yet fully implemented");
        tracing::info!("Consider using run_until_cancelled with a short timeout instead");

        Ok(0)
    }

    /// Get access to the underlying BackfillClient for job enqueueing and
    /// management
    pub fn client(&self) -> &BackfillClient {
        &self.client
    }

    /// Get the number of worker instances configured
    pub fn worker_count(&self) -> usize {
        self.config.queue_configs.len()
    }

    /// Check if DLQ processor is enabled
    pub fn dlq_processor_enabled(&self) -> bool {
        self.config.dlq_processor_interval.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_config_builders() {
        let default = QueueConfig::default_queue(5);
        assert_eq!(default.name, None);
        assert_eq!(default.concurrency, 5);
        assert_eq!(default.priority_range, None);

        let named = QueueConfig::named_queue("bulk", 10);
        assert_eq!(named.name, Some("bulk".to_string()));
        assert_eq!(named.concurrency, 10);
        assert_eq!(named.priority_range, None);

        let priority = QueueConfig::priority_queue("urgent", 3, 100, 1000);
        assert_eq!(priority.name, Some("urgent".to_string()));
        assert_eq!(priority.concurrency, 3);
        assert_eq!(priority.priority_range, Some((100, 1000)));
    }

    #[test]
    fn test_worker_config_builders() {
        let config = WorkerConfig::new("postgresql://localhost/test")
            .with_schema("my_jobs")
            .with_poll_interval(Duration::from_millis(50))
            .with_dlq_processor_interval(None);

        assert_eq!(config.database_url, "postgresql://localhost/test");
        assert_eq!(config.schema, "my_jobs");
        assert_eq!(config.poll_interval, Duration::from_millis(50));
        assert_eq!(config.dlq_processor_interval, None);
    }
}
