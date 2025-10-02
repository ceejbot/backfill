//! Metrics emission for observability
//!
//! This module provides helper functions for emitting metrics throughout the
//! library using the `metrics` crate. Consumers can install any metrics
//! recorder (Prometheus, StatsD, etc.) to collect these metrics.
//!
//! All metrics follow the naming convention: `backfill_<category>_<metric>`
//! (Prometheus-compatible naming with underscores)
//!
//! ## Automatic vs Manual Metrics
//!
//! The library automatically emits metrics for:
//! - Job enqueuing
//! - DLQ operations (add, requeue, delete)
//! - Database operations
//!
//! Job lifecycle metrics (start, completion, failure) require manual
//! instrumentation in your task handlers due to GraphileWorker limitations. Use
//! the public helper functions in this module to add metrics to your handlers.

use chrono::{DateTime, Utc};

/// Priority bands for metrics labels
#[derive(Debug, Clone, Copy)]
pub(crate) enum PriorityBand {
    Emergency,
    FastHigh,
    FastDefault,
    BulkDefault,
    BulkLow,
    BulkLowest,
    Other,
}

impl PriorityBand {
    pub(crate) fn from_priority(priority: i16) -> Self {
        match priority {
            ..=-20 => Self::Emergency,
            -19..=-10 => Self::FastHigh,
            -9..=-1 => Self::FastDefault,
            0 => Self::BulkDefault,
            1..=5 => Self::BulkLow,
            6..=10 => Self::BulkLowest,
            _ => Self::Other,
        }
    }

    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Emergency => "emergency",
            Self::FastHigh => "fast_high",
            Self::FastDefault => "fast_default",
            Self::BulkDefault => "bulk_default",
            Self::BulkLow => "bulk_low",
            Self::BulkLowest => "bulk_lowest",
            Self::Other => "other",
        }
    }
}

/// Record a job being enqueued (automatically called by the library)
pub(crate) fn record_job_enqueued(queue: &str, task: &str, priority: i16) {
    let priority_band = PriorityBand::from_priority(priority);
    metrics::counter!(
        "backfill_jobs_enqueued",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "priority_band" => priority_band.as_str().to_string(),
    )
    .increment(1);
}

/// Record a job starting execution
///
/// Call this at the beginning of your TaskHandler::run() method to track when
/// jobs start.
///
/// # Example
/// ```rust,ignore
/// use backfill::metrics;
///
/// async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult {
///     metrics::record_job_started("my_queue", "my_task");
///     // ... job logic
/// }
/// ```
pub fn record_job_started(queue: &str, task: &str) {
    metrics::counter!(
        "backfill_jobs_started",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
    )
    .increment(1);
}

/// Record a job completing successfully
///
/// Call this when your job completes successfully to track completion metrics.
pub fn record_job_completed(queue: &str, task: &str, attempt: i16) {
    metrics::counter!(
        "backfill_jobs_completed",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "attempt" => attempt.to_string(),
    )
    .increment(1);
}

/// Record a job failure
///
/// Call this when your job fails to track failure metrics and error types.
pub fn record_job_failed(queue: &str, task: &str, error_type: &str, attempt: i16) {
    metrics::counter!(
        "backfill_jobs_failed",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "error_type" => error_type.to_string(),
        "attempt" => attempt.to_string(),
    )
    .increment(1);
}

/// Record job execution duration
///
/// Call this after job completion to track how long jobs take.
pub fn record_job_duration(queue: &str, task: &str, status: &str, duration_secs: f64) {
    metrics::histogram!(
        "backfill_jobs_duration_seconds",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "status" => status.to_string(),
    )
    .record(duration_secs);
}

/// Record job wait time (enqueue to start)
///
/// Note: Currently unused - requires GraphileWorker lifecycle hooks to provide
/// job creation time. Will be enabled when hooks are available.
#[allow(dead_code)]
pub(crate) fn record_job_wait_time(
    queue: &str,
    task: &str,
    priority: i16,
    enqueued_at: DateTime<Utc>,
    started_at: DateTime<Utc>,
) {
    let wait_time = (started_at - enqueued_at).num_seconds() as f64;
    let priority_band = PriorityBand::from_priority(priority);

    metrics::histogram!(
        "backfill_jobs_wait_time_seconds",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "priority_band" => priority_band.as_str().to_string(),
    )
    .record(wait_time.max(0.0));
}

/// Update queue depth gauge
///
/// Note: Currently unused - requires periodic polling or GraphileWorker hooks.
/// Can be implemented with background task if needed.
#[allow(dead_code)]
pub(crate) fn update_queue_depth(queue: &str, depth: i64) {
    metrics::gauge!(
        "backfill_queue_depth",
        "queue" => queue.to_string(),
    )
    .set(depth as f64);
}

/// Update active jobs gauge
///
/// Note: Currently unused - requires GraphileWorker to expose active job count.
#[allow(dead_code)]
pub(crate) fn update_active_jobs(queue: &str, count: i64) {
    metrics::gauge!(
        "backfill_queue_active_jobs",
        "queue" => queue.to_string(),
    )
    .set(count as f64);
}

/// Record a job being added to DLQ
pub(crate) fn record_dlq_job_added(queue: &str, task: &str, reason: &str) {
    metrics::counter!(
        "backfill_dlq_jobs_added",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "reason" => reason.to_string(),
    )
    .increment(1);
}

/// Update DLQ size gauge
pub(crate) fn update_dlq_size(size: u32) {
    metrics::gauge!("backfill_dlq_size").set(size as f64);
}

/// Update DLQ size with task breakdown
pub(crate) fn update_dlq_size_by_task(task: &str, count: u32) {
    metrics::gauge!(
        "backfill_dlq_size",
        "task" => task.to_string(),
    )
    .set(count as f64);
}

/// Record a job being requeued from DLQ
pub(crate) fn record_dlq_job_requeued(task: &str, target_queue: &str) {
    metrics::counter!(
        "backfill_dlq_jobs_requeued",
        "task" => task.to_string(),
        "queue" => target_queue.to_string(),
    )
    .increment(1);
}

/// Record a job being deleted from DLQ
pub(crate) fn record_dlq_job_deleted(task: &str) {
    metrics::counter!(
        "backfill_dlq_jobs_deleted",
        "task" => task.to_string(),
    )
    .increment(1);
}

/// Record DLQ job age
pub(crate) fn record_dlq_age(task: &str, age_seconds: f64) {
    metrics::histogram!(
        "backfill_dlq_age_seconds",
        "task" => task.to_string(),
    )
    .record(age_seconds);
}

/// Update worker active count
pub(crate) fn update_worker_active(queue: &str, count: i32) {
    metrics::gauge!(
        "backfill_worker_active",
        "queue" => queue.to_string(),
    )
    .set(count as f64);
}

/// Update worker utilization
///
/// Note: Currently unused - requires GraphileWorker to expose concurrency
/// state.
#[allow(dead_code)]
pub(crate) fn update_worker_utilization(queue: &str, utilization: f64) {
    metrics::gauge!(
        "backfill_worker_utilization",
        "queue" => queue.to_string(),
    )
    .set(utilization.clamp(0.0, 1.0));
}

/// Record a worker poll operation
///
/// Note: Currently unused - requires GraphileWorker to expose poll events.
#[allow(dead_code)]
pub(crate) fn record_worker_poll(queue: &str, result: &str) {
    metrics::counter!(
        "backfill_worker_polls",
        "queue" => queue.to_string(),
        "result" => result.to_string(),
    )
    .increment(1);
}

/// Record a retry attempt
pub(crate) fn record_retry_attempted(task: &str, queue: &str, attempt: i16) {
    metrics::counter!(
        "backfill_retries_attempted",
        "task" => task.to_string(),
        "queue" => queue.to_string(),
        "attempt" => attempt.to_string(),
    )
    .increment(1);
}

/// Record retries exhausted
///
/// Note: Currently unused - requires GraphileWorker lifecycle hooks to detect
/// when a job has exhausted all retry attempts.
#[allow(dead_code)]
pub(crate) fn record_retries_exhausted(task: &str, max_attempts: i16) {
    metrics::counter!(
        "backfill_retries_exhausted",
        "task" => task.to_string(),
        "max_attempts" => max_attempts.to_string(),
    )
    .increment(1);
}

/// Record a database operation
pub(crate) fn record_db_operation(operation: &str, status: &str) {
    metrics::counter!(
        "backfill_db_operations",
        "operation" => operation.to_string(),
        "status" => status.to_string(),
    )
    .increment(1);
}

/// Record database operation duration
pub(crate) fn record_db_operation_duration(operation: &str, duration_secs: f64) {
    metrics::histogram!(
        "backfill_db_operation_duration_seconds",
        "operation" => operation.to_string(),
    )
    .record(duration_secs);
}

/// Helper for instrumenting job execution with metrics
///
/// This helper automatically records job start, duration, and
/// completion/failure metrics. Use it to wrap your job logic for automatic
/// metric collection.
///
/// # Example
/// ```rust,ignore
/// use backfill::{TaskHandler, WorkerContext, IntoTaskHandlerResult, metrics::JobMetrics};
///
/// struct MyJob { data: String }
///
/// impl TaskHandler for MyJob {
///     const IDENTIFIER: &'static str = "my_job";
///
///     async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult {
///         JobMetrics::new("my_queue", Self::IDENTIFIER, &ctx)
///             .instrument(|| async {
///                 // Your job logic here
///                 println!("Processing: {}", self.data);
///                 Ok::<(), Box<dyn std::error::Error>>(())
///             })
///             .await
///     }
/// }
/// ```
pub struct JobMetrics {
    queue: String,
    task: String,
    attempt: i16,
    start: std::time::Instant,
}

impl JobMetrics {
    /// Create a new job metrics helper
    ///
    /// # Arguments
    /// - `queue`: Queue name (e.g., "fast", "bulk")
    /// - `task`: Task identifier
    /// - `ctx`: WorkerContext from your task handler
    pub fn new(queue: impl Into<String>, task: impl Into<String>, ctx: &crate::WorkerContext) -> Self {
        let queue = queue.into();
        let task = task.into();
        let attempt = *ctx.job().attempts();

        // Record job start
        record_job_started(&queue, &task);

        Self {
            queue,
            task,
            attempt,
            start: std::time::Instant::now(),
        }
    }

    /// Instrument a job function with automatic metrics
    ///
    /// This will record:
    /// - Job started (already recorded in `new()`)
    /// - Job completion or failure
    /// - Job duration
    /// - Retry metrics if applicable
    pub async fn instrument<F, Fut, T, E>(self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::error::Error,
    {
        let result = f().await;
        let duration = self.start.elapsed().as_secs_f64();

        match &result {
            Ok(_) => {
                record_job_completed(&self.queue, &self.task, self.attempt);
                record_job_duration(&self.queue, &self.task, "success", duration);
            }
            Err(e) => {
                let error_type = classify_error_for_metrics(e);
                record_job_failed(&self.queue, &self.task, error_type, self.attempt);
                record_job_duration(&self.queue, &self.task, "failed", duration);

                if self.attempt > 1 {
                    record_retry_attempted(&self.task, &self.queue, self.attempt);
                }
            }
        }

        result
    }
}

/// Classify an error for metrics labels
///
/// Attempts to categorize errors into standard types for metrics.
/// Returns a string label suitable for the `error_type` metric label.
pub fn classify_error_for_metrics<E: std::error::Error + ?Sized>(error: &E) -> &'static str {
    let msg = error.to_string().to_lowercase();

    // Try to classify based on error message content
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_band_classification() {
        assert_eq!(PriorityBand::from_priority(-30).as_str(), "emergency");
        assert_eq!(PriorityBand::from_priority(-20).as_str(), "emergency");
        assert_eq!(PriorityBand::from_priority(-10).as_str(), "fast_high");
        assert_eq!(PriorityBand::from_priority(-5).as_str(), "fast_default");
        assert_eq!(PriorityBand::from_priority(0).as_str(), "bulk_default");
        assert_eq!(PriorityBand::from_priority(5).as_str(), "bulk_low");
        assert_eq!(PriorityBand::from_priority(10).as_str(), "bulk_lowest");
        assert_eq!(PriorityBand::from_priority(20).as_str(), "other");
    }

    #[test]
    fn test_metrics_emission() {
        // These tests just verify the functions can be called
        // Actual metric recording depends on the installed recorder
        record_job_enqueued("test_queue", "test_task", 0);
        record_job_started("test_queue", "test_task");
        record_job_completed("test_queue", "test_task", 1);
        record_job_failed("test_queue", "test_task", "retryable", 1);
        record_job_duration("test_queue", "test_task", "success", 1.5);

        update_queue_depth("test_queue", 10);
        update_active_jobs("test_queue", 5);

        record_dlq_job_added("test_queue", "test_task", "max_attempts");
        update_dlq_size(100);
        record_dlq_job_requeued("test_task", "test_queue");
        record_dlq_job_deleted("test_task");

        update_worker_active("test_queue", 5);
        update_worker_utilization("test_queue", 0.75);
        record_worker_poll("test_queue", "jobs_found");

        record_retry_attempted("test_task", "test_queue", 2);
        record_retries_exhausted("test_task", 5);

        record_db_operation("enqueue", "success");
        record_db_operation_duration("enqueue", 0.05);
    }
}
