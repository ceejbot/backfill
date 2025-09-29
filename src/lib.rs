//! # Backfill
//!
//! A high-performance, PostgreSQL-backed async priority queue system for Rust
//! applications. Built on top of [Graphile Worker](https://github.com/graphile/worker) for reliability and performance.
//!
//! This library provides:
//! - **Durable job queues** with PostgreSQL backend
//! - **Priority-based scheduling** with configurable priority levels
//! - **Named queues** for organizing different types of work
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

use std::time::Duration;

use chrono::{DateTime, Utc};
// Re-export commonly used types from graphile_worker
pub use graphile_worker::{IntoTaskHandlerResult, JobKeyMode, TaskHandler, WorkerContext, WorkerOptions};
use graphile_worker::{Job, JobSpec as GraphileJobSpec, JobSpecBuilder, WorkerUtils};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

mod errors;
pub use errors::{BackfillError, WorkerError};

/// Priority levels for jobs in the backfill system.
///
/// Lower numbers indicate higher priority (closer to front of queue).
/// Fast queue uses negative priorities, bulk queue uses positive priorities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Priority(pub i16);

impl Priority {
    /// Emergency priority for the fast queue (-20)
    pub const EMERGENCY: Priority = Priority(-20);
    /// High priority for the fast queue (-10)
    pub const FAST_HIGH: Priority = Priority(-10);
    /// Default priority for the fast queue (-5)
    pub const FAST_DEFAULT: Priority = Priority(-5);
    /// Low priority for bulk processing (0)
    pub const BULK_DEFAULT: Priority = Priority(0);
    /// Lower priority for bulk processing (5)
    pub const BULK_LOW: Priority = Priority(5);
    /// Lowest priority for bulk processing (10)
    pub const BULK_LOWEST: Priority = Priority(10);
}

impl Default for Priority {
    fn default() -> Self {
        Self::BULK_DEFAULT
    }
}

impl From<Priority> for i32 {
    fn from(priority: Priority) -> Self {
        priority.0 as i32
    }
}

impl From<Priority> for i16 {
    fn from(priority: Priority) -> Self {
        priority.0
    }
}

/// Named queues for organizing different types of work.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Queue {
    /// Fast queue for high-priority, low-latency jobs
    Fast,
    /// Bulk queue for background processing
    Bulk,
    /// Dead letter queue for failed jobs
    DeadLetter,
    /// Custom named queue
    Custom(String),
}

impl Queue {
    pub fn as_str(&self) -> &str {
        match self {
            Queue::Fast => "fast",
            Queue::Bulk => "bulk",
            Queue::DeadLetter => "dead_letter",
            Queue::Custom(name) => name,
        }
    }
}

impl Default for Queue {
    fn default() -> Self {
        Self::Bulk
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

        builder = builder.priority(spec.priority.into()).queue_name(spec.queue.as_str());

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

/// High-level client for the backfill job queue system.
pub struct BackfillClient {
    pool: PgPool,
    schema: String,
}

impl BackfillClient {
    /// Create a new BackfillClient with the given database URL.
    ///
    /// This will create a connection pool and initialize the GraphileWorker
    /// schema.
    pub async fn new(database_url: &str) -> Result<Self, BackfillError> {
        let pool = PgPoolOptions::new().max_connections(10).connect(database_url).await?;

        Self::with_pool_and_schema(pool, "graphile_worker".to_string()).await
    }

    /// Create a BackfillClient with the given database URL and custom schema.
    pub async fn new_with_schema(database_url: &str, schema: &str) -> Result<Self, BackfillError> {
        let pool = PgPoolOptions::new().max_connections(10).connect(database_url).await?;

        Self::with_pool_and_schema(pool, schema.to_string()).await
    }

    /// Create a BackfillClient with an existing connection pool.
    pub async fn with_pool(pool: PgPool) -> Result<Self, BackfillError> {
        Self::with_pool_and_schema(pool, "graphile_worker".to_string()).await
    }

    /// Create a BackfillClient with an existing connection pool and custom
    /// schema.
    pub async fn with_pool_and_schema(pool: PgPool, schema: String) -> Result<Self, BackfillError> {
        // Run migrations to ensure schema is set up
        graphile_worker::WorkerOptions::default()
            .schema(&schema)
            .pg_pool(pool.clone())
            .init()
            .await?;

        Ok(Self { pool, schema })
    }

    /// Get the underlying PostgreSQL connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the schema name being used.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Create a WorkerUtils instance for job management.
    pub fn utils(&self) -> WorkerUtils {
        WorkerUtils::new(self.pool.clone(), self.schema.clone())
    }

    /// Enqueue a job with the specified task identifier and payload.
    ///
    /// # Arguments
    /// * `task_identifier` - The string identifier for the task type
    /// * `payload` - The job payload (must be JSON-serializable)
    /// * `spec` - Job specification including priority, scheduling, etc.
    ///
    /// # Returns
    /// The Job struct containing the job ID and metadata.
    pub async fn enqueue<T>(&self, task_identifier: &str, payload: &T, spec: JobSpec) -> Result<Job, BackfillError>
    where
        T: Serialize,
    {
        let utils = self.utils();
        let job = utils
            .add_raw_job(task_identifier, serde_json::to_value(payload)?, spec.into())
            .await?;

        Ok(job)
    }

    /// Enqueue a job with a type-safe task handler.
    ///
    /// This method uses the task's IDENTIFIER constant and ensures the payload
    /// type matches the expected task type.
    pub async fn enqueue_task<T>(&self, task: T, spec: JobSpec) -> Result<Job, BackfillError>
    where
        T: TaskHandler + Serialize,
    {
        let utils = self.utils();
        let job = utils.add_job(task, spec.into()).await?;
        Ok(job)
    }

    /// Remove a job by its unique key.
    pub async fn remove_job(&self, job_key: &str) -> Result<(), BackfillError> {
        let utils = self.utils();
        utils.remove_job(job_key).await?;
        Ok(())
    }

    /// Mark jobs as completed.
    pub async fn complete_jobs(&self, job_ids: &[i64]) -> Result<(), BackfillError> {
        let utils = self.utils();
        utils.complete_jobs(job_ids).await?;
        Ok(())
    }

    /// Permanently fail jobs with a reason.
    pub async fn fail_jobs(&self, job_ids: &[i64], reason: &str) -> Result<(), BackfillError> {
        let utils = self.utils();
        utils.permanently_fail_jobs(job_ids, reason).await?;
        Ok(())
    }
}

/// Convenience function to enqueue a high-priority job in the fast queue.
///
/// This is equivalent to calling `enqueue` with `Priority::FAST_DEFAULT` and
/// `Queue::Fast`.
pub async fn enqueue_fast<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<Job, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::FAST_DEFAULT,
        queue: Queue::Fast,
        job_key,
        ..Default::default()
    };

    client.enqueue(task_identifier, payload, spec).await
}

/// Convenience function to enqueue a job in the bulk queue.
///
/// This is equivalent to calling `enqueue` with `Priority::BULK_DEFAULT` and
/// `Queue::Bulk`.
pub async fn enqueue_bulk<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<Job, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::BULK_DEFAULT,
        queue: Queue::Bulk,
        job_key,
        ..Default::default()
    };

    client.enqueue(task_identifier, payload, spec).await
}

/// Convenience function to enqueue an emergency priority job.
///
/// Use sparingly - this will jump ahead of all other jobs in the fast queue.
pub async fn enqueue_emergency<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<Job, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::EMERGENCY,
        queue: Queue::Fast,
        run_at: Some(Utc::now()), // Execute immediately
        job_key,
        ..Default::default()
    };

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue a job with fast exponential backoff retry policy.
/// Best for high-priority, low-latency jobs that need quick retries.
pub async fn enqueue_fast_with_retries<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<Job, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::FAST_HIGH,
        queue: Queue::Fast,
        job_key,
        ..Default::default()
    }.with_fast_retries();

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue a job with aggressive exponential backoff retry policy.
/// Best for critical jobs that must succeed and can tolerate longer delays.
pub async fn enqueue_critical<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<Job, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::FAST_HIGH,
        queue: Queue::Fast,
        job_key,
        ..Default::default()
    }.with_aggressive_retries();

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue a bulk job with conservative exponential backoff retry policy.
/// Best for background processing jobs where consistency is more important than speed.
pub async fn enqueue_bulk_with_retries<T>(
    client: &BackfillClient,
    task_identifier: &str,
    payload: &T,
    job_key: Option<String>,
) -> Result<Job, BackfillError>
where
    T: Serialize,
{
    let spec = JobSpec {
        priority: Priority::BULK_DEFAULT,
        queue: Queue::Bulk,
        job_key,
        ..Default::default()
    }.with_conservative_retries();

    client.enqueue(task_identifier, payload, spec).await
}

/// Configuration for exponential backoff retry policy.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_attempts: i32,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Backoff multiplier (typically 2.0 for exponential backoff)
    pub backoff_multiplier: f64,
    /// Add jitter to prevent thundering herd (0.0 to 1.0, default 0.1)
    pub jitter_factor: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(300), // 5 minutes
            backoff_multiplier: 2.0,
            jitter_factor: 0.1, // 10% jitter
        }
    }
}

impl RetryPolicy {
    /// Create a new RetryPolicy with custom settings
    pub fn new(
        max_attempts: i32,
        initial_delay: Duration,
        max_delay: Duration,
        backoff_multiplier: f64,
    ) -> Self {
        Self {
            max_attempts,
            initial_delay,
            max_delay,
            backoff_multiplier,
            jitter_factor: 0.1,
        }
    }

    /// Create a RetryPolicy with jitter configuration
    pub fn with_jitter(mut self, jitter_factor: f64) -> Self {
        self.jitter_factor = jitter_factor.clamp(0.0, 1.0);
        self
    }

    /// Calculate the delay for a specific attempt number (0-based)
    pub fn calculate_delay(&self, attempt: i32) -> Duration {
        if attempt >= self.max_attempts {
            return Duration::ZERO;
        }

        // Calculate base delay: initial_delay * multiplier^attempt
        let base_delay_ms = (self.initial_delay.as_millis() as f64)
            * self.backoff_multiplier.powi(attempt);

        // Cap at max_delay
        let capped_delay_ms = base_delay_ms.min(self.max_delay.as_millis() as f64);

        // Add jitter to prevent thundering herd
        let jitter = if self.jitter_factor > 0.0 {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            
            // Use a simple hash-based jitter for deterministic randomness
            let mut hasher = DefaultHasher::new();
            attempt.hash(&mut hasher);
            std::thread::current().id().hash(&mut hasher);
            
            let hash = hasher.finish();
            let jitter_multiplier = (hash % 1000) as f64 / 1000.0; // 0.0 to 1.0
            let jitter_range = capped_delay_ms * self.jitter_factor;
            jitter_range * (jitter_multiplier * 2.0 - 1.0) // -jitter_range to +jitter_range
        } else {
            0.0
        };

        let final_delay_ms = (capped_delay_ms + jitter).max(0.0);
        Duration::from_millis(final_delay_ms as u64)
    }

    /// Calculate the run_at time for a retry
    pub fn calculate_retry_time(&self, attempt: i32, base_time: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        let delay = self.calculate_delay(attempt);
        base_time + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::MAX)
    }

    /// Check if we should retry for the given attempt number
    pub fn should_retry(&self, attempt: i32) -> bool {
        attempt < self.max_attempts
    }

    /// Get the total number of attempts (including initial attempt)
    pub fn total_attempts(&self) -> i32 {
        self.max_attempts + 1 // +1 for the initial attempt
    }

    /// Create a fast retry policy for high-priority jobs
    pub fn fast() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }

    /// Create an aggressive retry policy for critical jobs
    pub fn aggressive() -> Self {
        Self {
            max_attempts: 12,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(600), // 10 minutes
            backoff_multiplier: 1.5,
            jitter_factor: 0.15,
        }
    }

    /// Create a conservative retry policy for bulk jobs
    pub fn conservative() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(1800), // 30 minutes
            backoff_multiplier: 2.5,
            jitter_factor: 0.2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::EMERGENCY < Priority::FAST_HIGH);
        assert!(Priority::FAST_HIGH < Priority::FAST_DEFAULT);
        assert!(Priority::FAST_DEFAULT < Priority::BULK_DEFAULT);
        assert!(Priority::BULK_DEFAULT < Priority::BULK_LOW);
        assert!(Priority::BULK_LOW < Priority::BULK_LOWEST);
    }

    #[test]
    fn test_priority_conversion() {
        assert_eq!(i32::from(Priority::EMERGENCY), -20);
        assert_eq!(i32::from(Priority::FAST_HIGH), -10);
        assert_eq!(i32::from(Priority::BULK_DEFAULT), 0);
        assert_eq!(i32::from(Priority::BULK_LOWEST), 10);
    }

    #[test]
    fn test_queue_names() {
        assert_eq!(Queue::Fast.as_str(), "fast");
        assert_eq!(Queue::Bulk.as_str(), "bulk");
        assert_eq!(Queue::DeadLetter.as_str(), "dead_letter");
        assert_eq!(Queue::Custom("test".to_string()).as_str(), "test");
    }

    #[test]
    fn test_job_spec_defaults() {
        let spec = JobSpec::default();
        assert_eq!(spec.priority, Priority::BULK_DEFAULT);
        assert_eq!(spec.queue, Queue::Bulk);
        assert_eq!(spec.max_attempts, Some(8));
        assert!(spec.run_at.is_none());
        assert!(spec.job_key.is_none());
    }

    #[test]
    fn test_retry_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 8);
        assert_eq!(policy.initial_delay, Duration::from_millis(1000));
        assert_eq!(policy.max_delay, Duration::from_secs(300));
        assert_eq!(policy.backoff_multiplier, 2.0);
        assert_eq!(policy.jitter_factor, 0.1);
    }

    #[test]
    fn test_retry_policy_calculate_delay() {
        let policy = RetryPolicy::new(
            3,
            Duration::from_millis(100),
            Duration::from_secs(10),
            2.0,
        );

        // First retry (attempt 0): 100ms
        let delay0 = policy.calculate_delay(0);
        assert!(delay0.as_millis() >= 90 && delay0.as_millis() <= 110); // Allow for jitter

        // Second retry (attempt 1): 200ms
        let delay1 = policy.calculate_delay(1);
        assert!(delay1.as_millis() >= 180 && delay1.as_millis() <= 220); // Allow for jitter

        // Third retry (attempt 2): 400ms
        let delay2 = policy.calculate_delay(2);
        assert!(delay2.as_millis() >= 360 && delay2.as_millis() <= 440); // Allow for jitter

        // No more retries (attempt 3): 0ms
        let delay3 = policy.calculate_delay(3);
        assert_eq!(delay3, Duration::ZERO);
    }

    #[test]
    fn test_retry_policy_should_retry() {
        let policy = RetryPolicy::new(
            3,
            Duration::from_millis(100),
            Duration::from_secs(10),
            2.0,
        );

        assert!(policy.should_retry(0)); // First retry
        assert!(policy.should_retry(1)); // Second retry
        assert!(policy.should_retry(2)); // Third retry
        assert!(!policy.should_retry(3)); // No more retries
        assert!(!policy.should_retry(4)); // Definitely no more retries
    }

    #[test]
    fn test_retry_policy_max_delay_cap() {
        let policy = RetryPolicy::new(
            10,
            Duration::from_millis(100),
            Duration::from_secs(1), // Cap at 1 second
            2.0,
        ).with_jitter(0.0); // No jitter for predictable testing

        // Attempt 0: 100ms
        assert_eq!(policy.calculate_delay(0), Duration::from_millis(100));
        
        // Attempt 3: 100 * 2^3 = 800ms
        assert_eq!(policy.calculate_delay(3), Duration::from_millis(800));
        
        // Attempt 4: 100 * 2^4 = 1600ms, but capped at 1000ms
        assert_eq!(policy.calculate_delay(4), Duration::from_millis(1000));
        
        // Attempt 10: Still capped at 1000ms
        assert_eq!(policy.calculate_delay(9), Duration::from_millis(1000));
    }

    #[test]
    fn test_retry_policy_presets() {
        let fast = RetryPolicy::fast();
        assert_eq!(fast.max_attempts, 3);
        assert_eq!(fast.initial_delay, Duration::from_millis(100));
        assert_eq!(fast.max_delay, Duration::from_secs(30));

        let aggressive = RetryPolicy::aggressive();
        assert_eq!(aggressive.max_attempts, 12);
        assert_eq!(aggressive.initial_delay, Duration::from_millis(500));
        assert_eq!(aggressive.max_delay, Duration::from_secs(600));

        let conservative = RetryPolicy::conservative();
        assert_eq!(conservative.max_attempts, 5);
        assert_eq!(conservative.initial_delay, Duration::from_secs(5));
        assert_eq!(conservative.max_delay, Duration::from_secs(1800));
    }

    #[test]
    fn test_job_spec_with_retry_policies() {
        let spec = JobSpec::default().with_fast_retries();
        assert_eq!(spec.max_attempts, Some(3));
        assert!(spec.retry_policy.is_some());
        
        let policy = spec.effective_retry_policy();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_delay, Duration::from_millis(100));

        // Test retry time calculation
        let base_time = chrono::Utc::now();
        let retry_time = spec.calculate_retry_time(0, base_time);
        assert!(retry_time.is_some());
        assert!(retry_time.unwrap() > base_time);

        // Test when no more retries
        let no_retry = spec.calculate_retry_time(10, base_time);
        assert!(no_retry.is_none());
    }
}
