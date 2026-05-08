//! Metrics emission for observability
//!
//! This module provides automatic metrics for client-side operations using the
//! `metrics` crate. Consumers can install any metrics recorder (Prometheus,
//! StatsD, etc.) to collect these metrics.
//!
//! All metrics follow the naming convention: `backfill_<category>_<metric>`
//! (Prometheus-compatible naming with underscores)
//!
//! ## Automatic Metrics
//!
//! The library automatically emits metrics for:
//! - Job enqueuing
//! - DLQ operations (add, requeue, delete)
//! - Database operations
//! - Worker lifecycle (start/stop)
//!
//! ## Job Lifecycle Metrics
//!
//! For job lifecycle metrics (start, completion, failure, duration), use
//! lifecycle hook plugins instead of manual instrumentation. See
//! `docs/07-plugins.md` and `examples/metrics_plugin.rs` for how to implement a
//! metrics plugin.
//!
//! Lifecycle hooks provide richer data than manual instrumentation including:
//! - Automatic duration tracking
//! - Retry status (will_retry flag)
//! - Job metadata (attempts, priority, created_at)
//! - Error information

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

/// Record when a job enqueue was skipped because a job with the same key is in
/// progress
pub(crate) fn record_job_already_in_progress(queue: &str, task: &str) {
    metrics::counter!(
        "backfill_jobs_already_in_progress",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
    )
    .increment(1);
}

/// Convert a stored DLQ `queue_name` (which may be "" for parallel-origin
/// jobs) into a bounded metric label of either `"parallel"` or `"serial"`.
///
/// Mirrors [`crate::Queue::metric_label`] but operates on the post-storage
/// string form used by `DlqJob.queue_name`.
pub(crate) fn queue_metric_label_from_name(queue_name: &str) -> &'static str {
    if queue_name.is_empty() { "parallel" } else { "serial" }
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

// === Cleanup Metrics ===

/// Record stale queue locks released during cleanup
pub(crate) fn record_cleanup_queue_locks_released(count: u64) {
    metrics::counter!("backfill_cleanup_queue_locks_released").increment(count);
    if count > 0 {
        metrics::gauge!("backfill_cleanup_queue_locks_last_released").set(count as f64);
    }
}

/// Record stale job locks released during cleanup
pub(crate) fn record_cleanup_job_locks_released(count: u64) {
    metrics::counter!("backfill_cleanup_job_locks_released").increment(count);
    if count > 0 {
        metrics::gauge!("backfill_cleanup_job_locks_last_released").set(count as f64);
    }
}

/// Record permanently failed jobs cleaned up
pub(crate) fn record_cleanup_failed_jobs_deleted(count: u64) {
    metrics::counter!("backfill_cleanup_failed_jobs_deleted").increment(count);
}

/// Record a cleanup operation failure
pub(crate) fn record_cleanup_failure(operation: &str, error_type: &str) {
    metrics::counter!(
        "backfill_cleanup_failures",
        "operation" => operation.to_string(),
        "error_type" => error_type.to_string(),
    )
    .increment(1);
}

/// Update cleanup health timestamp (unix timestamp of last successful cleanup)
pub(crate) fn update_cleanup_health_timestamp() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as f64;
    metrics::gauge!("backfill_cleanup_last_success_timestamp").set(now);
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
        record_job_already_in_progress("test_queue", "test_task");

        record_dlq_job_added("test_queue", "test_task", "max_attempts");
        update_dlq_size(100);
        update_dlq_size_by_task("test_task", 10);
        record_dlq_job_requeued("test_task", "test_queue");
        record_dlq_job_deleted("test_task");
        record_dlq_age("test_task", 120.5);

        update_worker_active("test_queue", 1);

        record_db_operation("enqueue", "success");
        record_db_operation_duration("enqueue", 0.05);
    }

    #[test]
    fn test_cleanup_metrics_emission() {
        // Verify cleanup metrics functions can be called
        record_cleanup_queue_locks_released(5);
        record_cleanup_job_locks_released(3);
        record_cleanup_failed_jobs_deleted(1);
        record_cleanup_failure("queue_locks", "timeout");
        update_cleanup_health_timestamp();
    }
}
