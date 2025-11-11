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

use chrono::{DateTime, Utc};
// Re-export commonly used types from graphile_worker
pub use graphile_worker::{IntoTaskHandlerResult, JobKeyMode, TaskHandler, WorkerContext, WorkerOptions};
use graphile_worker::{Job, JobSpec as GraphileJobSpec, JobSpecBuilder};
// Re-export crontab parsing types for advanced usage
pub use graphile_worker_crontab_parser::{CrontabParseError, parse_crontab};
use serde::Serialize;

mod client;
mod errors;
mod priorities;
mod retries;
mod worker;

#[cfg(feature = "axum")]
pub mod admin;

pub mod metrics;

pub use client::*;
pub use errors::{BackfillError, WorkerError};
pub use priorities::*;
pub use retries::*;
pub use worker::*;

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
    }
    .with_fast_retries();

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
    }
    .with_aggressive_retries();

    client.enqueue(task_identifier, payload, spec).await
}

/// Enqueue a bulk job with conservative exponential backoff retry policy.
/// Best for background processing jobs where consistency is more important than
/// speed.
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
    }
    .with_conservative_retries();

    client.enqueue(task_identifier, payload, spec).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_names() {
        assert_eq!(Queue::Fast.as_str(), "fast");
        assert_eq!(Queue::Bulk.as_str(), "bulk");
        assert_eq!(Queue::DeadLetter.as_str(), "dead_letter");
        assert_eq!(Queue::Custom("test".to_string()).as_str(), "test");
    }

    #[test]
    fn job_spec_defaults() {
        let spec = JobSpec::default();
        assert_eq!(spec.priority, Priority::BULK_DEFAULT);
        assert_eq!(spec.queue, Queue::Bulk);
        assert_eq!(spec.max_attempts, Some(8));
        assert!(spec.run_at.is_none());
        assert!(spec.job_key.is_none());
    }
}
