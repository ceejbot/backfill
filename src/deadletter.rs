//! Dead letter queue types

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// === Dead Letter Queue Types ===

/// A job that has been moved to the dead letter queue after failing
/// permanently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqJob {
    /// Unique DLQ entry ID
    pub id: i64,
    /// Original job ID from the main queue
    pub original_job_id: Option<i64>,
    /// Task identifier for the failed job
    pub task_identifier: String,
    /// Job payload as JSON
    pub payload: serde_json::Value,
    /// Queue name where the job originally ran
    pub queue_name: String,
    /// Job priority when originally enqueued
    pub priority: i32,
    /// Job key for deduplication (if any)
    pub job_key: Option<String>,
    /// Maximum retry attempts allowed
    pub max_attempts: Option<i32>,
    /// Human-readable failure reason
    pub failure_reason: String,
    /// Number of times the job failed
    pub failure_count: i32,
    /// Last error details as JSON
    pub last_error: Option<serde_json::Value>,
    /// When the original job was created
    pub original_created_at: Option<DateTime<Utc>>,
    /// When the original job was scheduled to run
    pub original_run_at: Option<DateTime<Utc>>,
    /// When the job was moved to DLQ
    pub failed_at: DateTime<Utc>,
    /// How many times this job has been requeued from DLQ
    pub requeued_count: i32,
    /// When this job was last requeued from DLQ
    pub last_requeued_at: Option<DateTime<Utc>>,
    /// Admin notes about this failure
    pub notes: Option<String>,
}

/// Filter parameters for querying DLQ jobs.
#[derive(Debug, Clone, Default)]
pub struct DlqFilter {
    /// Filter by task identifier
    pub task_identifier: Option<String>,
    /// Filter by queue name
    pub queue_name: Option<String>,
    /// Only jobs that failed after this time
    pub failed_after: Option<DateTime<Utc>>,
    /// Only jobs that failed before this time
    pub failed_before: Option<DateTime<Utc>>,
    /// Maximum number of results to return
    pub limit: Option<i32>,
    /// Offset for pagination
    pub offset: Option<i32>,
}

/// Paginated list of DLQ jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqJobList {
    /// The jobs in this page
    pub jobs: Vec<DlqJob>,
    /// Total number of jobs matching the filter
    pub total: u32,
    /// Offset of this page
    pub offset: i32,
    /// Limit used for this page
    pub limit: i32,
}

/// Statistics about the dead letter queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqStats {
    /// Total number of jobs in the DLQ
    pub total_jobs: u32,
    /// Number of unique task types in the DLQ
    pub unique_tasks: u32,
    /// Number of unique queues represented in the DLQ
    pub unique_queues: u32,
    /// Average number of failures per job
    pub avg_failure_count: f64,
    /// Total number of requeue operations performed
    pub total_requeued: u32,
    /// Timestamp of the oldest failure
    pub oldest_failure: Option<DateTime<Utc>>,
    /// Timestamp of the newest failure
    pub newest_failure: Option<DateTime<Utc>>,
    /// Breakdown of jobs by task type (top 10)
    pub task_breakdown: Vec<(String, u32)>,
}
