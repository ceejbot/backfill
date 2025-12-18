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
//! use backfill::{WorkerRunner, WorkerConfig, QueueConfig, TaskHandler, WorkerContext, IntoTaskHandlerResult};
//! use tokio_util::sync::CancellationToken;
//! use std::time::Duration;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyJob { data: String }
//!
//! impl TaskHandler for MyJob {
//!     const IDENTIFIER: &'static str = "my_job";
//!     async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
//!         Ok::<(), String>(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = WorkerConfig::new("postgresql://localhost/myapp")
//!         .with_schema("jobs");
//!
//!     let worker = WorkerRunner::builder(config).await?
//!         .define_job::<MyJob>()
//!         .build().await?;
//!
//!     let shutdown_token = CancellationToken::new();
//!
//!     tokio::select! {
//!         result = worker.run_until_cancelled(shutdown_token.clone()) => {
//!             eprintln!("Worker stopped: {:?}", result);
//!             result?;
//!         }
//!         _ = tokio::signal::ctrl_c() => {
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
//! use backfill::{WorkerRunner, WorkerConfig, TaskHandler, WorkerContext, IntoTaskHandlerResult};
//! use tokio_util::sync::CancellationToken;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyJob { data: String }
//!
//! impl TaskHandler for MyJob {
//!     const IDENTIFIER: &'static str = "my_job";
//!     async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
//!         Ok::<(), String>(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let worker = WorkerRunner::builder(WorkerConfig::default()).await?
//!         .define_job::<MyJob>()
//!         .build().await?;
//!
//!     let shutdown_token = CancellationToken::new();
//!
//!     // Start worker in background
//!     let worker_handle = worker.spawn_background(shutdown_token.clone());
//!
//!     // Run your application (e.g., HTTP server)
//!     tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
//!
//!     // Shutdown worker
//!     shutdown_token.cancel();
//!     worker_handle.await??;
//!
//!     Ok(())
//! }
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{BackfillClient, BackfillError, LifecycleHooks, TaskHandler, WorkerOptions};

// Re-export for use in wrapper
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
    pub(crate) crontabs: Vec<String>,
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
            crontabs: Vec::new(),
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

    /// Add a cron schedule for periodic job execution
    ///
    /// # Syntax
    /// The crontab format is: `<timer> <task_identifier> ?<options>
    /// {<payload>}`
    ///
    /// - **Timer**: Standard 5-field cron syntax (minute hour day month
    ///   day-of-week)
    /// - **Task**: Must match a registered job handler's IDENTIFIER
    /// - **Options**: Query string format for job configuration
    ///   - `fill`: Backfill period (e.g., `?fill=10m` to execute missed runs
    ///     within 10 minutes)
    ///   - `job_key`: Unique identifier for deduplication
    ///   - `job_key_mode`: How to handle duplicates (`replace`,
    ///     `preserve_run_at`, etc.)
    ///   - `priority`: Job priority (default: 0)
    ///   - `max`: Maximum attempts (default: 25)
    ///   - `queue`: Queue name (default: task identifier)
    /// - **Payload**: Optional JSON object to pass to the job handler
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use backfill::{WorkerRunner, WorkerConfig};
    /// # async fn example() -> Result<(), backfill::BackfillError> {
    /// # let config = WorkerConfig::default();
    /// WorkerRunner::builder(config).await?
    ///     // Every 5 minutes
    ///     .add_cron_schedule("*/5 * * * * cleanup_task")?
    ///     // Daily at 2:00 AM with backfill
    ///     .add_cron_schedule("0 2 * * * backup_task ?fill=1h")?
    ///     // With payload
    ///     .add_cron_schedule(r#"0 * * * * report_task {\"format\":\"pdf\"}"#)?
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns `BackfillError::CrontabParse` if the cron syntax is invalid.
    pub fn add_cron_schedule(mut self, spec: &str) -> Result<Self, crate::BackfillError> {
        // Validate the crontab spec by attempting to parse it
        use graphile_worker_crontab_parser::parse_crontab;
        parse_crontab(spec)?;

        // Store the validated spec
        self.crontabs.push(spec.to_string());
        Ok(self)
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
            log::warn!("Queue name configuration is not supported by GraphileWorker's WorkerOptions - ignoring");
        }

        // Register all job handlers
        for handler_config in builder.job_handlers {
            worker_options = (handler_config.builder_fn)(worker_options);
        }

        // Add all cron schedules
        for crontab_spec in builder.crontabs {
            worker_options = worker_options
                .with_crontab(&crontab_spec)
                .expect("Crontab already validated in add_cron_schedule");
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
    plugin_appliers: Vec<PluginApplier>,
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
        plugin_appliers: Vec<PluginApplier>,
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
            plugin_appliers,
        })
    }

    /// Create a worker instance from the stored configuration
    async fn create_worker(&self) -> Result<graphile_worker::Worker, BackfillError> {
        let mut worker_options: WorkerOptions = self.worker_options_builder.clone().into();

        // Apply all plugins
        for applier in &self.plugin_appliers {
            worker_options = applier(worker_options);
        }

        // If DLQ is enabled, add the cleanup plugin that removes DLQ entries
        // when jobs with matching job_keys complete successfully
        if self.config.dlq_processor_interval.is_some() {
            let cleanup_plugin = crate::DlqCleanupPlugin::new(self.client.clone());
            worker_options = worker_options.add_plugin(cleanup_plugin);
        }

        worker_options
            .init()
            .await
            .map_err(|e| BackfillError::WorkerRuntime(e.to_string()))
    }
}

/// Type-erased plugin applier that can be cloned
type PluginApplier = Arc<dyn Fn(WorkerOptions) -> WorkerOptions + Send + Sync>;

/// Builder for WorkerRunner that allows configuring job types and plugins
pub struct WorkerRunnerBuilder {
    config: WorkerConfig,
    worker_options_builder: WorkerOptionsBuilder,
    plugin_appliers: Vec<PluginApplier>,
}

impl WorkerRunnerBuilder {
    async fn new(config: WorkerConfig) -> Result<Self, BackfillError> {
        let worker_options_builder = WorkerOptionsBuilder::new(&config).await?;

        Ok(Self {
            config,
            worker_options_builder,
            plugin_appliers: Vec::new(),
        })
    }

    /// Register a job type with this worker runner
    pub fn define_job<T: TaskHandler + 'static>(mut self) -> Self {
        self.worker_options_builder = self.worker_options_builder.define_job::<T>();
        self
    }

    /// Add a cron schedule for periodic job execution
    ///
    /// Schedules a task to run automatically at specified intervals using cron
    /// syntax. The task must be registered with `define_job()` before
    /// adding a cron schedule.
    ///
    /// # Syntax
    /// `<timer> <task_identifier> ?<options> {<payload>}`
    ///
    /// See [`WorkerOptionsBuilder::add_cron_schedule`] for detailed syntax
    /// documentation.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use serde::{Deserialize, Serialize};
    /// # use backfill::{WorkerRunner, WorkerConfig, TaskHandler, WorkerContext, IntoTaskHandlerResult};
    /// # #[derive(Clone, Deserialize, Serialize)]
    /// # struct CleanupTask;
    /// # impl TaskHandler for CleanupTask {
    /// #     const IDENTIFIER: &'static str = "cleanup";
    /// #     async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult { Ok::<(), backfill::BackfillError>(()) }
    /// # }
    /// # async fn example() -> Result<(), backfill::BackfillError> {
    /// # let config = WorkerConfig::default();
    /// let worker = WorkerRunner::builder(config).await?
    ///     .define_job::<CleanupTask>()
    ///     .add_cron_schedule("*/5 * * * * cleanup")?  // Every 5 minutes
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns `BackfillError::CrontabParse` if the cron syntax is invalid.
    pub fn add_cron_schedule(mut self, spec: &str) -> Result<Self, BackfillError> {
        self.worker_options_builder = self.worker_options_builder.add_cron_schedule(spec)?;
        Ok(self)
    }

    /// Add a lifecycle hook plugin
    ///
    /// Plugins receive callbacks for worker and job lifecycle events.
    /// Multiple plugins can be registered and will be called in registration
    /// order.
    ///
    /// # Requirements
    /// The plugin must implement `Clone` to support worker cloning for
    /// background tasks.
    ///
    /// # Example
    /// ```rust,no_run
    /// use backfill::{WorkerRunner, WorkerConfig, LifecycleHooks, JobCompleteContext};
    ///
    /// #[derive(Clone)]
    /// struct MyPlugin;
    ///
    /// impl LifecycleHooks for MyPlugin {
    ///     async fn on_job_complete(&self, ctx: JobCompleteContext) {
    ///         println!("Job {} completed in {:?}", ctx.job.task_identifier(), ctx.duration);
    ///     }
    /// }
    ///
    /// # async fn example() -> Result<(), backfill::BackfillError> {
    /// # let config = WorkerConfig::default();
    /// let worker = WorkerRunner::builder(config).await?
    ///     .add_plugin(MyPlugin)
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_plugin<H: LifecycleHooks + Clone + 'static>(mut self, plugin: H) -> Self {
        // Store a closure that captures the plugin and applies it to WorkerOptions
        let applier = Arc::new(move |opts: WorkerOptions| opts.add_plugin(plugin.clone()));
        self.plugin_appliers.push(applier);
        self
    }

    /// Build the final WorkerRunner
    pub async fn build(self) -> Result<WorkerRunner, BackfillError> {
        WorkerRunner::from_builder(self.config, self.worker_options_builder, self.plugin_appliers).await
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
        log::info!(
            "Starting worker runner (dlq_enabled: {})",
            self.config.dlq_processor_interval.is_some()
        );

        // Run startup cleanup to release stale locks and clean up failed jobs
        if let Err(e) = self.client.startup_cleanup().await {
            log::warn!("Startup cleanup failed (continuing anyway): {}", e);
        }

        // Record worker starting (increment active worker count)
        crate::metrics::update_worker_active("worker", 1);

        // Start DLQ processor if configured
        let dlq_handle = if let Some(interval) = self.config.dlq_processor_interval {
            log::info!("Starting DLQ processor (interval_secs: {})", interval.as_secs());
            Some(self.client.start_dlq_processor(interval, cancellation_token.clone()))
        } else {
            None
        };

        // Create and start the worker
        log::info!("Starting worker instance");
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
                log::info!("Cancellation requested, stopping worker");
            }
            result = handle => {
                match result {
                    Ok(worker_result) => {
                        log::warn!("Worker completed unexpectedly");
                        worker_result?;
                    }
                    Err(e) => {
                        log::error!("Worker task failed: {}", e);
                        // Decrement worker count before returning error
                        crate::metrics::update_worker_active("worker", 0);
                        return Err(BackfillError::WorkerRuntime(e.to_string()));
                    }
                }
            }
        }

        // Wait for DLQ processor to stop
        if let Some(dlq_handle) = dlq_handle {
            log::info!("Waiting for DLQ processor to stop");
            match tokio::time::timeout(Duration::from_secs(5), dlq_handle).await {
                Ok(Ok(())) => log::info!("DLQ processor stopped gracefully"),
                Ok(Err(e)) => log::warn!("DLQ processor stopped with error: {}", e),
                Err(_) => log::warn!("DLQ processor shutdown timeout"),
            }
        }

        // Record worker stopping (decrement active worker count)
        crate::metrics::update_worker_active("worker", 0);

        log::info!("Worker runner stopped");
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
            plugin_appliers: self.plugin_appliers.clone(),
        };

        tokio::spawn(async move { runner.run_until_cancelled(cancellation_token).await })
    }

    /// Process all currently available jobs and return
    ///
    /// This method is designed for batch processing or testing scenarios where
    /// you want to process the current job queue without running a persistent
    /// worker.
    ///
    /// This method processes all jobs that are currently available (where
    /// `run_at <= now()`), respecting the configured concurrency limit.
    /// Jobs are processed in priority order (lower priority number = higher
    /// priority), then by `run_at` timestamp.
    ///
    /// The method returns when:
    /// - All available jobs have been processed
    /// - No more jobs are available to process
    ///
    /// Note: This method currently returns 0 as an accurate job count would
    /// require additional instrumentation. The jobs are still processed
    /// correctly.
    ///
    /// # Returns
    ///
    /// Returns `Ok(0)` on success (job count tracking not yet implemented).
    ///
    /// # Errors
    ///
    /// Returns an error if worker initialization or job processing fails.
    pub async fn process_available_jobs(&self) -> Result<usize, BackfillError> {
        log::info!("Processing available jobs (one-shot mode)");

        // Create worker instance
        let worker = self.create_worker().await?;

        // Use GraphileWorker's run_once() method which processes all available jobs
        // and returns when the queue is empty
        worker
            .run_once()
            .await
            .map_err(|e| BackfillError::WorkerRuntime(e.to_string()))?;

        log::info!("Finished processing available jobs");

        // Note: Returning 0 for now as accurate counting would require additional
        // instrumentation. Consider using metrics or hooks to track job counts.
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
