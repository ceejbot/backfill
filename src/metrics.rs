//! Metrics emission for observability
//!
//! This module provides helper functions for emitting metrics throughout the
//! library using the `metrics` crate. Consumers can install any metrics
//! recorder (Prometheus, StatsD, etc.) to collect these metrics.
//!
//! All metrics follow the naming convention: `backfill.<category>.<metric>`

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

/// Record a job being enqueued
pub(crate) fn record_job_enqueued(queue: &str, task: &str, priority: i16) {
    let priority_band = PriorityBand::from_priority(priority);
    metrics::counter!(
        "backfill.jobs.enqueued",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "priority_band" => priority_band.as_str().to_string(),
    )
    .increment(1);
}

/// Record a job starting execution
pub(crate) fn record_job_started(queue: &str, task: &str) {
    metrics::counter!(
        "backfill.jobs.started",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
    )
    .increment(1);
}

/// Record a job completing successfully
pub(crate) fn record_job_completed(queue: &str, task: &str, attempt: i16) {
    metrics::counter!(
        "backfill.jobs.completed",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "attempt" => attempt.to_string(),
    )
    .increment(1);
}

/// Record a job failure
pub(crate) fn record_job_failed(queue: &str, task: &str, error_type: &str, attempt: i16) {
    metrics::counter!(
        "backfill.jobs.failed",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "error_type" => error_type.to_string(),
        "attempt" => attempt.to_string(),
    )
    .increment(1);
}

/// Record job execution duration
pub(crate) fn record_job_duration(queue: &str, task: &str, status: &str, duration_secs: f64) {
    metrics::histogram!(
        "backfill.jobs.duration_seconds",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "status" => status.to_string(),
    )
    .record(duration_secs);
}

/// Record job wait time (enqueue to start)
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
        "backfill.jobs.wait_time_seconds",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "priority_band" => priority_band.as_str().to_string(),
    )
    .record(wait_time.max(0.0));
}

/// Update queue depth gauge
pub(crate) fn update_queue_depth(queue: &str, depth: i64) {
    metrics::gauge!(
        "backfill.queue.depth",
        "queue" => queue.to_string(),
    )
    .set(depth as f64);
}

/// Update active jobs gauge
pub(crate) fn update_active_jobs(queue: &str, count: i64) {
    metrics::gauge!(
        "backfill.queue.active_jobs",
        "queue" => queue.to_string(),
    )
    .set(count as f64);
}

/// Record a job being added to DLQ
pub(crate) fn record_dlq_job_added(queue: &str, task: &str, reason: &str) {
    metrics::counter!(
        "backfill.dlq.jobs_added",
        "queue" => queue.to_string(),
        "task" => task.to_string(),
        "reason" => reason.to_string(),
    )
    .increment(1);
}

/// Update DLQ size gauge
pub(crate) fn update_dlq_size(size: u32) {
    metrics::gauge!("backfill.dlq.size").set(size as f64);
}

/// Update DLQ size with task breakdown
pub(crate) fn update_dlq_size_by_task(task: &str, count: u32) {
    metrics::gauge!(
        "backfill.dlq.size",
        "task" => task.to_string(),
    )
    .set(count as f64);
}

/// Record a job being requeued from DLQ
pub(crate) fn record_dlq_job_requeued(task: &str, target_queue: &str) {
    metrics::counter!(
        "backfill.dlq.jobs_requeued",
        "task" => task.to_string(),
        "queue" => target_queue.to_string(),
    )
    .increment(1);
}

/// Record a job being deleted from DLQ
pub(crate) fn record_dlq_job_deleted(task: &str) {
    metrics::counter!(
        "backfill.dlq.jobs_deleted",
        "task" => task.to_string(),
    )
    .increment(1);
}

/// Record DLQ job age
pub(crate) fn record_dlq_age(task: &str, age_seconds: f64) {
    metrics::histogram!(
        "backfill.dlq.age_seconds",
        "task" => task.to_string(),
    )
    .record(age_seconds);
}

/// Update worker active count
pub(crate) fn update_worker_active(queue: &str, count: i32) {
    metrics::gauge!(
        "backfill.worker.active",
        "queue" => queue.to_string(),
    )
    .set(count as f64);
}

/// Update worker utilization
pub(crate) fn update_worker_utilization(queue: &str, utilization: f64) {
    metrics::gauge!(
        "backfill.worker.utilization",
        "queue" => queue.to_string(),
    )
    .set(utilization.clamp(0.0, 1.0));
}

/// Record a worker poll operation
pub(crate) fn record_worker_poll(queue: &str, result: &str) {
    metrics::counter!(
        "backfill.worker.polls",
        "queue" => queue.to_string(),
        "result" => result.to_string(),
    )
    .increment(1);
}

/// Record a retry attempt
pub(crate) fn record_retry_attempted(task: &str, queue: &str, attempt: i16) {
    metrics::counter!(
        "backfill.retries.attempted",
        "task" => task.to_string(),
        "queue" => queue.to_string(),
        "attempt" => attempt.to_string(),
    )
    .increment(1);
}

/// Record retries exhausted
pub(crate) fn record_retries_exhausted(task: &str, max_attempts: i16) {
    metrics::counter!(
        "backfill.retries.exhausted",
        "task" => task.to_string(),
        "max_attempts" => max_attempts.to_string(),
    )
    .increment(1);
}

/// Record a database operation
pub(crate) fn record_db_operation(operation: &str, status: &str) {
    metrics::counter!(
        "backfill.db.operations",
        "operation" => operation.to_string(),
        "status" => status.to_string(),
    )
    .increment(1);
}

/// Record database operation duration
pub(crate) fn record_db_operation_duration(operation: &str, duration_secs: f64) {
    metrics::histogram!(
        "backfill.db.operation_duration_seconds",
        "operation" => operation.to_string(),
    )
    .record(duration_secs);
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
