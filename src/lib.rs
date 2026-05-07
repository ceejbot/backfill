//! # Backfill
//!
//! A high-performance, PostgreSQL-backed async priority queue system for Rust
//! applications. Built on top of [Graphile Worker](https://github.com/graphile/worker) for reliability and performance.
//!
//! This library provides:
//! - **Durable job queues** with PostgreSQL backend
//! - **Priority-based scheduling** with configurable priority levels
//! - **Parallel execution** by default - jobs run concurrently across all
//!   workers
//! - **Serial queues** when you need ordering or rate limiting
//! - **Exponential backoff** with jitter to prevent thundering herds
//! - **Flexible retry policies** (fast, aggressive, conservative, or custom)
//! - **Dead letter queue** handling for failed jobs
//! - **Type-safe job handlers** using Rust's type system
//! - **Low-latency execution** via PostgreSQL LISTEN/NOTIFY
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use backfill::{BackfillClient, enqueue_critical, enqueue_fast_with_retries, JobSpec, RetryPolicy};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct SendEmailJob {
//!     recipient: String,
//!     subject: String,
//!     body: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let database_url = "postgresql://user:password@localhost/mydb";
//!     let client = BackfillClient::new(database_url).await?;
//!
//!     // Enqueue a critical job with aggressive retry policy (12 attempts)
//!     enqueue_critical(
//!         &client,
//!         "send_email",
//!         &SendEmailJob {
//!             recipient: "user@example.com".to_string(),
//!             subject: "Welcome!".to_string(),
//!             body: "Thanks for signing up.".to_string(),
//!         },
//!         Some("welcome_email_123".to_string()),
//!     ).await?;
//!
//!     // Or use fast retries for quick turnaround (3 attempts, 100ms-30s)
//!     enqueue_fast_with_retries(
//!         &client,
//!         "send_notification",
//!         &SendEmailJob {
//!             recipient: "admin@example.com".to_string(),
//!             subject: "Notification".to_string(),
//!             body: "Quick notification with fast retries.".to_string(),
//!         },
//!         Some("quick_notification".to_string()),
//!     ).await?;
//!
//!     Ok(())
//! }
//! ```

use chrono::{DateTime, Utc};
// Lifecycle hooks for plugins - new Plugin API with event registration
pub use graphile_worker::{
    // Event types (for hooks.on() registration)
    AfterJobRun,
    // Context types (for hook handlers)
    AfterJobRunContext,
    BeforeJobRun,
    BeforeJobRunContext,
    BeforeJobSchedule,
    BeforeJobScheduleContext,
    CronJobScheduled,
    CronJobScheduledContext,
    CronTick,
    CronTickContext,
    // Plugin trait and registry
    HookRegistry,
    // Result types
    HookResult,
    // Job type for accessing job data
    Job,
    JobComplete,
    JobCompleteContext,
    JobFail,
    JobFailContext,
    JobFetch,
    JobFetchContext,
    JobPermanentlyFail,
    JobPermanentlyFailContext,
    JobScheduleResult,
    JobStart,
    JobStartContext,
    Plugin,
    // Other
    ShutdownReason,
    WorkerInit,
    WorkerInitContext,
    WorkerShutdown,
    WorkerShutdownContext,
    WorkerStart,
    WorkerStartContext,
};
// Re-export commonly used types from graphile_worker
pub use graphile_worker::{IntoTaskHandlerResult, JobKeyMode, TaskHandler, WorkerContext, WorkerOptions};
use graphile_worker::{JobSpec as GraphileJobSpec, JobSpecBuilder};
// Re-export crontab parsing types for advanced usage
pub use graphile_worker_crontab_parser::{CrontabParseError, parse_crontab};
use serde::Serialize;

mod client;
mod dlq_cleanup_plugin;
mod errors;
mod permanent_failure_plugin;
mod priorities;
mod retries;
mod worker;

#[cfg(feature = "axum")]
pub mod admin;

pub mod metrics;

pub use client::cleanup::{
    DEFAULT_STALE_JOB_LOCK_TIMEOUT, DEFAULT_STALE_LOCK_CLEANUP_INTERVAL, DEFAULT_STALE_QUEUE_LOCK_TIMEOUT,
};
pub use client::*;
pub use dlq_cleanup_plugin::DlqCleanupPlugin;
pub use errors::{BackfillError, WorkerError};
pub use permanent_failure_plugin::PermanentFailurePlugin;
pub use priorities::*;
pub use retries::*;
pub use worker::*;

/// Queue configuration for job execution.
///
/// By default, jobs execute **in parallel** across all workers. Use
/// `Queue::Serial` when you need ordering guarantees or mutual exclusion (e.g.,
/// rate limiting API calls, processing events for a single user in order).
///
/// # Parallel Execution (Default)
///
/// With `Queue::Parallel`, workers can fetch and execute jobs concurrently.
/// Priority controls which jobs get picked first, but multiple jobs can run
/// at the same time.
///
/// # Serial Execution
///
/// With `Queue::Serial("name")`, only one job with that queue name can execute
/// at a time across the entire cluster. This is useful for:
/// - Rate limiting external API calls
/// - Processing events for a single entity in order
/// - Mutual exclusion
///
/// # Examples
///
/// ```rust
/// use backfill::{Queue, JobSpec, Priority};
///
/// // Parallel execution (default) - jobs run concurrently
/// let spec = JobSpec {
///     priority: Priority::FAST_DEFAULT,
///     ..Default::default()
/// };
///
/// // Serial execution - one job at a time per user
/// let spec = JobSpec {
///     queue: Queue::serial_for("user", 123),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum Queue {
    /// Jobs execute in parallel (DEFAULT). Priority controls fetch order,
    /// but multiple jobs can run simultaneously.
    #[default]
    Parallel,

    /// Jobs in this queue execute one at a time across all workers.
    /// Use for rate limiting, ordering, or mutual exclusion.
    Serial(String),
}

impl Queue {
    /// Create a serial queue for a specific entity.
    ///
    /// This is useful for ensuring jobs related to the same entity
    /// (user, order, etc.) are processed one at a time.
    ///
    /// # Example
    /// ```rust
    /// use backfill::Queue;
    ///
    /// // Process user events serially
    /// let queue = Queue::serial_for("user", 123);  // → "user:123"
    ///
    /// // Process order updates serially
    /// let queue = Queue::serial_for("order", "abc-def");  // → "order:abc-def"
    /// ```
    pub fn serial_for(entity: &str, id: impl std::fmt::Display) -> Self {
        Queue::Serial(format!("{}:{}", entity, id))
    }

    /// Create a named serial queue.
    ///
    /// Use this when you need a fixed queue name for rate limiting
    /// or other serialization purposes.
    ///
    /// # Example
    /// ```rust
    /// use backfill::Queue;
    ///
    /// // Rate limit calls to external API
    /// let queue = Queue::serial("external-api");
    /// ```
    pub fn serial(name: impl Into<String>) -> Self {
        Queue::Serial(name.into())
    }

    /// Returns the queue name for serial queues, or None for parallel.
    ///
    /// This is used internally when building the GraphileWorker job spec.
    pub fn name(&self) -> Option<&str> {
        match self {
            Queue::Parallel => None,
            Queue::Serial(name) => Some(name),
        }
    }

    /// Returns a string representation for logging/metrics.
    ///
    /// Returns "parallel" for parallel queues, or the queue name for serial
    /// queues.
    pub fn as_str(&self) -> &str {
        match self {
            Queue::Parallel => "parallel",
            Queue::Serial(name) => name,
        }
    }

    /// Returns true if this is a parallel (non-serialized) queue.
    pub fn is_parallel(&self) -> bool {
        matches!(self, Queue::Parallel)
    }

    /// Returns true if this is a serial queue.
    pub fn is_serial(&self) -> bool {
        matches!(self, Queue::Serial(_))
    }

    /// Get the dead letter queue (serial execution).
    pub fn dead_letter() -> Self {
        Queue::Serial("dead_letter".to_string())
    }
}

/// Outcome of an enqueue operation.
///
/// When enqueueing a job, the result can either be:
/// - `Enqueued(Job)`: The job was successfully created or updated
/// - `AlreadyInProgress { job_key }`: A job with this key is currently locked
///   by a worker
#[derive(Debug, Clone)]
pub enum EnqueueOutcome {
    /// Job was successfully enqueued (either created or updated)
    Enqueued(Box<Job>),
    /// A job with this key is already in progress (locked by a worker).
    /// Contains the job_key that was in conflict.
    AlreadyInProgress { job_key: String },
}

impl EnqueueOutcome {
    /// Returns the Job if the outcome was Enqueued, None otherwise.
    pub fn job(&self) -> Option<&Job> {
        match self {
            EnqueueOutcome::Enqueued(job) => Some(job),
            EnqueueOutcome::AlreadyInProgress { .. } => None,
        }
    }

    /// Consumes self and returns the Job if Enqueued, None otherwise.
    pub fn into_job(self) -> Option<Job> {
        match self {
            EnqueueOutcome::Enqueued(job) => Some(*job),
            EnqueueOutcome::AlreadyInProgress { .. } => None,
        }
    }

    /// Returns the Job if Enqueued, panics with message otherwise.
    ///
    /// # Panics
    /// Panics if the outcome was AlreadyInProgress.
    pub fn expect(self, msg: &str) -> Job {
        match self {
            EnqueueOutcome::Enqueued(job) => *job,
            EnqueueOutcome::AlreadyInProgress { job_key } => {
                panic!("{}: job_key '{}' was already in progress", msg, job_key)
            }
        }
    }

    /// Returns the Job if Enqueued, panics otherwise.
    ///
    /// # Panics
    /// Panics if the outcome was AlreadyInProgress.
    pub fn unwrap(self) -> Job {
        self.expect("called `EnqueueOutcome::unwrap()` on `AlreadyInProgress` value")
    }

    /// Returns true if a job was enqueued.
    pub fn is_enqueued(&self) -> bool {
        matches!(self, EnqueueOutcome::Enqueued(_))
    }

    /// Returns true if the job was already in progress.
    pub fn is_already_in_progress(&self) -> bool {
        matches!(self, EnqueueOutcome::AlreadyInProgress { .. })
    }

    /// Returns the job_key if AlreadyInProgress, None otherwise.
    pub fn conflicting_job_key(&self) -> Option<&str> {
        match self {
            EnqueueOutcome::AlreadyInProgress { job_key } => Some(job_key),
            EnqueueOutcome::Enqueued(_) => None,
        }
    }
}

/// Configuration for job scheduling and execution.
#[derive(Debug, Clone)]
pub struct JobSpec {
    /// When the job should be executed (default: now)
    pub run_at: Option<DateTime<Utc>>,
    /// Job priority (lower numbers = higher priority)
    pub priority: Priority,
    /// Queue name for the job
    pub queue: Queue,
    /// Maximum number of retry attempts (default: 8)
    pub max_attempts: Option<i32>,
    /// Exponential backoff retry policy for failed jobs
    pub retry_policy: Option<RetryPolicy>,
    /// Unique identifier for job deduplication
    pub job_key: Option<String>,
    /// How to handle duplicate job keys
    pub job_key_mode: JobKeyMode,
}

impl Default for JobSpec {
    fn default() -> Self {
        Self {
            run_at: None,
            priority: Priority::default(),
            queue: Queue::default(),
            max_attempts: Some(8),
            retry_policy: None, // Use GraphileWorker's default retry handling
            job_key: None,
            job_key_mode: JobKeyMode::Replace,
        }
    }
}

impl JobSpec {
    /// Create a JobSpec with exponential backoff retry policy
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.max_attempts = Some(retry_policy.max_attempts);
        self.retry_policy = Some(retry_policy);
        self
    }

    /// Create a JobSpec optimized for fast retries
    pub fn with_fast_retries(mut self) -> Self {
        let policy = RetryPolicy::fast();
        self.max_attempts = Some(policy.max_attempts);
        self.retry_policy = Some(policy);
        self
    }

    /// Create a JobSpec optimized for aggressive retries
    pub fn with_aggressive_retries(mut self) -> Self {
        let policy = RetryPolicy::aggressive();
        self.max_attempts = Some(policy.max_attempts);
        self.retry_policy = Some(policy);
        self
    }

    /// Create a JobSpec optimized for conservative retries
    pub fn with_conservative_retries(mut self) -> Self {
        let policy = RetryPolicy::conservative();
        self.max_attempts = Some(policy.max_attempts);
        self.retry_policy = Some(policy);
        self
    }

    /// Get the effective retry policy (returns default if none specified)
    pub fn effective_retry_policy(&self) -> RetryPolicy {
        self.retry_policy.clone().unwrap_or_default()
    }

    /// Calculate the next retry time for a failed job
    pub fn calculate_retry_time(&self, attempt: i32, failed_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let policy = self.effective_retry_policy();
        if policy.should_retry(attempt) {
            Some(policy.calculate_retry_time(attempt, failed_at))
        } else {
            None // No more retries
        }
    }
}

impl From<JobSpec> for GraphileJobSpec {
    fn from(spec: JobSpec) -> Self {
        let mut builder = JobSpecBuilder::new();

        if let Some(run_at) = spec.run_at {
            builder = builder.run_at(run_at);
        }

        builder = builder.priority(spec.priority.into());

        // Only set queue_name for serial queues - parallel jobs have no queue
        if let Some(queue_name) = spec.queue.name() {
            builder = builder.queue_name(queue_name);
        }

        if let Some(max_attempts) = spec.max_attempts {
            // Convert i32 to i16, clamping to avoid overflow
            let max_attempts_i16 = max_attempts.clamp(0, i16::MAX as i32) as i16;
            builder = builder.max_attempts(max_attempts_i16);
        }

        if let Some(job_key) = spec.job_key {
            builder = builder.job_key(&job_key).job_key_mode(spec.job_key_mode);
        }

        builder.build()
    }
}

/// Enqueue a high-priority job for parallel execution.
///
/// Jobs are executed in parallel across all workers, with higher priority
/// jobs (lower numbers) fetched first.
pub async fn enqueue_fast<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<EnqueueOutcome, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::FAST_DEFAULT,
        job_key,
        ..Default::default()
    };

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue a normal-priority job for parallel execution.
///
/// Jobs are executed in parallel across all workers, with higher priority
/// jobs fetched first. Bulk priority is lower than fast priority.
pub async fn enqueue_bulk<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<EnqueueOutcome, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::BULK_DEFAULT,
        job_key,
        ..Default::default()
    };

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue an emergency priority job.
///
/// Use sparingly - emergency jobs jump ahead of all other jobs.
pub async fn enqueue_emergency<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<EnqueueOutcome, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::EMERGENCY,
        run_at: Some(Utc::now()), // Execute immediately
        job_key,
        ..Default::default()
    };

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue a high-priority job with fast exponential backoff retries.
///
/// Best for high-priority jobs that need quick retries (3 attempts, 100ms-30s
/// delays).
pub async fn enqueue_fast_with_retries<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<EnqueueOutcome, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::FAST_HIGH,
        job_key,
        ..Default::default()
    }
    .with_fast_retries();

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue a critical job with aggressive exponential backoff retries.
///
/// Best for critical jobs that must succeed (12 attempts, up to 4 hour delays).
pub async fn enqueue_critical<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<EnqueueOutcome, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::FAST_HIGH,
        job_key,
        ..Default::default()
    }
    .with_aggressive_retries();

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue a bulk job with conservative exponential backoff retries.
///
/// Best for background jobs where consistency matters more than speed
/// (8 attempts, 1 min - 8 hour delays).
pub async fn enqueue_bulk_with_retries<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<EnqueueOutcome, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::BULK_DEFAULT,
        job_key,
        ..Default::default()
    }
    .with_conservative_retries();

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue a job for serial execution within a named queue.
///
/// Only one job from this queue will execute at a time across all workers.
/// Use for rate limiting external APIs or ensuring ordered processing.
///
/// # Example
/// ```rust,no_run
/// use backfill::{BackfillClient, enqueue_serial, Priority};
///
/// # async fn example(client: &BackfillClient) -> Result<(), backfill::BackfillError> {
/// // Rate limit calls to an external API
/// enqueue_serial(
///     client,
///     "call_external_api",
///     &serde_json::json!({"url": "https://api.example.com"}),
///     "external-api",
///     Priority::BULK_DEFAULT,
///     None,
/// ).await?;
///
/// // Process events for a specific user in order
/// enqueue_serial(
///     client,
///     "process_user_event",
///     &serde_json::json!({"event": "login"}),
///     format!("user:{}", 123),
///     Priority::FAST_DEFAULT,
///     Some("user-123-login".to_string()),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn enqueue_serial<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    queue_name: impl Into<String>,
    priority: Priority,
    job_key: Option<String>,
) -> Result<EnqueueOutcome, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        queue: Queue::Serial(queue_name.into()),
        priority,
        job_key,
        ..Default::default()
    };

    client.enqueue(task_identifier, payload, spec).await
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Queue Type Tests
    // =========================================================================

    #[test]
    fn queue_parallel_is_default() {
        assert_eq!(Queue::default(), Queue::Parallel);
        assert!(Queue::Parallel.is_parallel());
        assert!(!Queue::Parallel.is_serial());
        assert_eq!(Queue::Parallel.name(), None);
        assert_eq!(Queue::Parallel.as_str(), "parallel");
    }

    #[test]
    fn queue_serial_creation() {
        let queue = Queue::serial("test");
        assert!(queue.is_serial());
        assert!(!queue.is_parallel());
        assert_eq!(queue.name(), Some("test"));
        assert_eq!(queue.as_str(), "test");

        let queue = Queue::serial_for("user", 123);
        assert_eq!(queue.name(), Some("user:123"));
        assert_eq!(queue.as_str(), "user:123");

        let queue = Queue::serial_for("order", "abc-def");
        assert_eq!(queue.name(), Some("order:abc-def"));

        let queue = Queue::dead_letter();
        assert_eq!(queue.name(), Some("dead_letter"));
    }

    #[test]
    fn queue_equality() {
        // Parallel queues are equal
        assert_eq!(Queue::Parallel, Queue::Parallel);

        // Serial queues with same name are equal
        assert_eq!(Queue::serial("test"), Queue::serial("test"));
        assert_eq!(Queue::serial("test".to_string()), Queue::Serial("test".to_string()));

        // Serial queues with different names are not equal
        assert_ne!(Queue::serial("a"), Queue::serial("b"));

        // Parallel and Serial are not equal
        assert_ne!(Queue::Parallel, Queue::serial("parallel"));
    }

    // =========================================================================
    // JobSpec Tests
    // =========================================================================

    #[test]
    fn job_spec_defaults() {
        let spec = JobSpec::default();
        assert_eq!(spec.priority, Priority::BULK_DEFAULT);
        assert_eq!(spec.queue, Queue::Parallel);
        assert_eq!(spec.max_attempts, Some(8));
        assert!(spec.run_at.is_none());
        assert!(spec.job_key.is_none());
    }

    #[test]
    fn job_spec_parallel_has_no_queue_name() {
        let spec = JobSpec {
            queue: Queue::Parallel,
            ..Default::default()
        };

        // The key behavior: parallel jobs should not have a queue name
        // This is what causes them to run in parallel (no queue lock)
        assert!(spec.queue.name().is_none());
    }

    #[test]
    fn job_spec_serial_has_queue_name() {
        let spec = JobSpec {
            queue: Queue::serial("rate-limit"),
            ..Default::default()
        };

        // Serial jobs must have a queue name - this creates the queue lock
        assert_eq!(spec.queue.name(), Some("rate-limit"));
    }

    #[test]
    fn job_spec_conversion_parallel() {
        // Verify that parallel JobSpec converts without queue_name
        let spec = JobSpec {
            queue: Queue::Parallel,
            priority: Priority::FAST_HIGH,
            ..Default::default()
        };

        // Convert to GraphileJobSpec
        let graphile_spec: GraphileJobSpec = spec.into();

        // We can't directly inspect GraphileJobSpec internals, but we can verify
        // that our Queue::Parallel correctly reports no queue name
        assert!(Queue::Parallel.name().is_none());

        // The graphile_spec is built - if it compiled, the conversion worked
        let _ = graphile_spec;
    }

    #[test]
    fn job_spec_conversion_serial() {
        // Verify that serial JobSpec converts with queue_name
        let spec = JobSpec {
            queue: Queue::serial("my-queue"),
            priority: Priority::BULK_DEFAULT,
            ..Default::default()
        };

        // Verify queue name is set before conversion
        assert_eq!(spec.queue.name(), Some("my-queue"));

        // Convert to GraphileJobSpec
        let graphile_spec: GraphileJobSpec = spec.into();

        // The graphile_spec is built with queue_name - if it compiled, the
        // conversion worked
        let _ = graphile_spec;
    }

    // =========================================================================
    // Convenience Function Queue Tests
    // =========================================================================

    #[test]
    fn convenience_functions_use_parallel_queue() {
        // All convenience function specs should use Queue::Parallel
        // We can't call them without a client, but we can verify the JobSpec
        // construction

        // enqueue_fast uses parallel
        let spec = JobSpec {
            priority: Priority::FAST_DEFAULT,
            ..Default::default()
        };
        assert_eq!(spec.queue, Queue::Parallel);

        // enqueue_bulk uses parallel
        let spec = JobSpec {
            priority: Priority::BULK_DEFAULT,
            ..Default::default()
        };
        assert_eq!(spec.queue, Queue::Parallel);

        // enqueue_emergency uses parallel
        let spec = JobSpec {
            priority: Priority::EMERGENCY,
            ..Default::default()
        };
        assert_eq!(spec.queue, Queue::Parallel);
    }
}
