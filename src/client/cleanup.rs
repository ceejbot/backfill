//! Cleanup utilities for maintaining worker queue health
//!
//! Provides functions to clean up stale state that can accumulate when workers
//! crash or are forcibly terminated without graceful shutdown.

use std::time::Duration;

use super::BackfillClient;
use crate::BackfillError;

/// Default timeout for considering a queue lock stale.
///
/// Queue locks are held briefly during job selection (milliseconds), so any
/// lock older than this is almost certainly from a crashed worker.
pub const DEFAULT_STALE_LOCK_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

impl BackfillClient {
    /// Release stale queue locks that were left behind by crashed workers.
    ///
    /// When a worker crashes or is killed without graceful shutdown, it may
    /// leave queue locks behind. These stale locks prevent other workers
    /// from processing jobs in the affected queues.
    ///
    /// This function releases any queue locks older than the specified timeout.
    ///
    /// # Arguments
    /// * `timeout` - Locks older than this duration are considered stale
    ///
    /// # Returns
    /// Number of queue locks that were released
    pub async fn release_stale_queue_locks(&self, timeout: Duration) -> Result<u64, BackfillError> {
        let timeout_secs = timeout.as_secs();

        let query = format!(
            r#"
            UPDATE {schema}._private_job_queues
            SET locked_at = NULL, locked_by = NULL
            WHERE locked_at IS NOT NULL
              AND locked_at < NOW() - INTERVAL '{timeout_secs} seconds'
            "#,
            schema = self.schema,
            timeout_secs = timeout_secs
        );

        let result = sqlx::query(&query).execute(&self.pool).await?;
        let released = result.rows_affected();

        if released > 0 {
            log::info!(
                "Released stale queue locks (count: {}, timeout_secs: {})",
                released,
                timeout_secs
            );
        }

        Ok(released)
    }

    /// Delete permanently failed jobs from the main queue.
    ///
    /// Jobs that have exhausted all retry attempts (attempts >= max_attempts)
    /// remain in the main queue with `is_available = false`. These jobs
    /// will never be processed again and should be cleaned up.
    ///
    /// Note: These jobs should already be captured to the DLQ by the task
    /// handler or DLQ processor before reaching this state. This function
    /// removes the leftover rows from the main queue.
    ///
    /// # Returns
    /// Number of permanently failed jobs that were deleted
    pub async fn cleanup_permanently_failed_jobs(&self) -> Result<u64, BackfillError> {
        let query = format!(
            r#"
            DELETE FROM {schema}._private_jobs
            WHERE attempts >= max_attempts
              AND locked_at IS NULL
            "#,
            schema = self.schema
        );

        let result = sqlx::query(&query).execute(&self.pool).await?;
        let deleted = result.rows_affected();

        if deleted > 0 {
            log::info!(
                "Cleaned up permanently failed jobs from main queue (count: {})",
                deleted
            );
        }

        Ok(deleted)
    }

    /// Run all startup cleanup tasks.
    ///
    /// This should be called when a worker starts to clean up any stale state
    /// left behind by previous workers. It performs:
    /// 1. Release stale queue locks (using default timeout)
    /// 2. Delete permanently failed jobs from main queue
    ///
    /// # Returns
    /// Tuple of (stale_locks_released, failed_jobs_deleted)
    pub async fn startup_cleanup(&self) -> Result<(u64, u64), BackfillError> {
        log::info!("Running startup cleanup tasks");

        let locks_released = self.release_stale_queue_locks(DEFAULT_STALE_LOCK_TIMEOUT).await?;
        let jobs_deleted = self.cleanup_permanently_failed_jobs().await?;

        log::info!(
            "Startup cleanup completed (stale_locks_released: {}, failed_jobs_deleted: {})",
            locks_released,
            jobs_deleted
        );

        Ok((locks_released, jobs_deleted))
    }
}
