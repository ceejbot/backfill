//! Cleanup utilities for maintaining worker queue health
//!
//! Provides functions to clean up stale state that can accumulate when workers
//! crash or are forcibly terminated without graceful shutdown.
//!
//! ## Lock Types
//!
//! Graphile Worker uses two types of locks:
//!
//! 1. **Queue locks** (`_private_job_queues.locked_at`) - Brief locks held
//!    during job selection. These are typically held for milliseconds.
//!
//! 2. **Job locks** (`_private_jobs.locked_at`) - Locks held while jobs are
//!    being processed. These can be held for the duration of job execution
//!    (minutes).
//!
//! When a worker crashes, both types of locks can become orphaned. This module
//! provides functions to clean up both.

use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::BackfillClient;
use crate::BackfillError;

/// Default timeout for considering a queue lock stale.
///
/// Queue locks are held briefly during job selection (milliseconds), so any
/// lock older than this is almost certainly from a crashed worker.
pub const DEFAULT_STALE_QUEUE_LOCK_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

/// Default timeout for considering a job lock stale.
///
/// Job locks are held while jobs execute, which can take minutes. This timeout
/// should be longer than your longest-running job. Default: 30 minutes.
pub const DEFAULT_STALE_JOB_LOCK_TIMEOUT: Duration = Duration::from_secs(1800); // 30 minutes

/// Default interval for periodic stale lock cleanup.
pub const DEFAULT_STALE_LOCK_CLEANUP_INTERVAL: Duration = Duration::from_secs(60); // 1 minute

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

        // Bind timeout via parameter rather than format-interpolating it.
        // u64 → i64 cast is safe: i64::MAX seconds is ~292 billion years.
        let query = format!(
            r#"
            UPDATE {schema}._private_job_queues
            SET locked_at = NULL, locked_by = NULL
            WHERE locked_at IS NOT NULL
              AND locked_at < NOW() - ($1::bigint * interval '1 second')
            "#,
            schema = self.schema,
        );

        let result = sqlx::query(crate::audited_sql(query))
            .bind(timeout_secs as i64)
            .execute(&self.pool)
            .await?;
        let released = result.rows_affected();

        // Always emit metrics (counter increments even for 0)
        crate::metrics::record_cleanup_queue_locks_released(released);

        if released > 0 {
            log::info!(
                "Released stale queue locks (count: {}, timeout_secs: {})",
                released,
                timeout_secs
            );
        }

        Ok(released)
    }

    /// Release stale job locks that were left behind by crashed workers.
    ///
    /// When a worker crashes while processing a job, the job remains locked
    /// in `_private_jobs` and will never be retried. This function releases
    /// any job locks older than the specified timeout, allowing the jobs to
    /// be picked up again by other workers.
    ///
    /// # Arguments
    /// * `timeout` - Locks older than this duration are considered stale
    ///
    /// # Returns
    /// Number of job locks that were released
    pub async fn release_stale_job_locks(&self, timeout: Duration) -> Result<u64, BackfillError> {
        let timeout_secs = timeout.as_secs();

        let query = format!(
            r#"
            UPDATE {schema}._private_jobs
            SET locked_at = NULL, locked_by = NULL
            WHERE locked_at IS NOT NULL
              AND locked_at < NOW() - ($1::bigint * interval '1 second')
            "#,
            schema = self.schema,
        );

        let result = sqlx::query(crate::audited_sql(query))
            .bind(timeout_secs as i64)
            .execute(&self.pool)
            .await?;
        let released = result.rows_affected();

        // Always emit metrics (counter increments even for 0)
        crate::metrics::record_cleanup_job_locks_released(released);

        if released > 0 {
            log::info!(
                "Released stale job locks (count: {}, timeout_secs: {})",
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
    /// # Important: ordering with the DLQ
    ///
    /// **This function deletes the same rows the DLQ processor uses as input.**
    /// If you call it before `process_failed_jobs()` has captured those rows
    /// into the DLQ, those jobs are lost forever — they leave the main queue
    /// without ever reaching the DLQ.
    ///
    /// Safe usage when DLQ is enabled:
    /// 1. Call `process_failed_jobs()` first (moves rows into DLQ).
    /// 2. Then call this function (cleans up anything the DLQ processor chose
    ///    not to move — typically jobs with `max_attempts = 0`, which the DLQ
    ///    processor explicitly skips).
    ///
    /// `WorkerRunner::run_until_cancelled` already enforces this ordering at
    /// startup. Direct callers (ad-hoc maintenance scripts, etc.) must enforce
    /// it themselves.
    ///
    /// If DLQ is disabled, this function is the only cleanup mechanism and will
    /// silently delete failed jobs — that is by design.
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

        let result = sqlx::query(crate::audited_sql(query)).execute(&self.pool).await?;
        let deleted = result.rows_affected();

        // Emit metric
        crate::metrics::record_cleanup_failed_jobs_deleted(deleted);

        if deleted > 0 {
            // When the DLQ is enabled, these rows have already been captured
            // (either by an earlier `process_failed_jobs()` tick or by the
            // synchronous pre-cleanup move in `WorkerRunner` startup) so this
            // delete is just garbage collection — INFO is fine. When the DLQ
            // is *not* enabled, this delete is the only mechanism removing
            // failed jobs from the main queue and they are gone forever:
            // surface that loudly so an operator who didn't realize that's
            // the consequence can see it in their logs.
            //
            // Detection uses `to_regclass` which returns NULL if the table
            // doesn't exist. Failure of the existence check itself doesn't
            // matter — we default to the louder log on uncertainty.
            let dlq_oid: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
                .bind(format!("{}.backfill_dlq", self.schema))
                .fetch_one(&self.pool)
                .await
                .ok()
                .flatten();

            if dlq_oid.is_some() {
                log::info!(
                    "Cleaned up permanently failed jobs from main queue (count: {})",
                    deleted
                );
            } else {
                log::warn!(
                    "Deleted permanently failed jobs from main queue WITHOUT DLQ capture \
                     (count: {}); they cannot be recovered. Enable dlq_processor_interval \
                     to retain failed jobs for inspection.",
                    deleted
                );
            }
        }

        Ok(deleted)
    }

    /// Run all startup cleanup tasks with default timeouts.
    ///
    /// This should be called when a worker starts to clean up any stale state
    /// left behind by previous workers. It performs:
    /// 1. Release stale queue locks (5 minute timeout)
    /// 2. Release stale job locks (30 minute timeout)
    /// 3. Delete permanently failed jobs from main queue
    ///
    /// # Returns
    /// Tuple of (queue_locks_released, job_locks_released, failed_jobs_deleted)
    pub async fn startup_cleanup(&self) -> Result<(u64, u64, u64), BackfillError> {
        self.startup_cleanup_with_timeouts(DEFAULT_STALE_QUEUE_LOCK_TIMEOUT, DEFAULT_STALE_JOB_LOCK_TIMEOUT)
            .await
    }

    /// Run all startup cleanup tasks with custom timeouts.
    ///
    /// This allows configuring the stale lock thresholds for environments
    /// where the defaults aren't appropriate.
    ///
    /// # DLQ ordering note
    ///
    /// This calls `cleanup_permanently_failed_jobs()`, which DELETEs rows from
    /// `_private_jobs` where `attempts >= max_attempts`. If you run a DLQ,
    /// **call `process_failed_jobs()` first** so those rows reach the DLQ
    /// before they're deleted. `WorkerRunner::run_until_cancelled` does this
    /// automatically; direct callers must do it themselves.
    ///
    /// # Arguments
    /// * `queue_lock_timeout` - Timeout for queue locks (normally held for ms)
    /// * `job_lock_timeout` - Timeout for job locks (held during job execution)
    ///
    /// # Returns
    /// Tuple of (queue_locks_released, job_locks_released, failed_jobs_deleted)
    pub async fn startup_cleanup_with_timeouts(
        &self,
        queue_lock_timeout: Duration,
        job_lock_timeout: Duration,
    ) -> Result<(u64, u64, u64), BackfillError> {
        log::info!(
            "Running startup cleanup (queue_lock_timeout: {}s, job_lock_timeout: {}s)",
            queue_lock_timeout.as_secs(),
            job_lock_timeout.as_secs()
        );

        let queue_locks_released = self.release_stale_queue_locks(queue_lock_timeout).await?;
        let job_locks_released = self.release_stale_job_locks(job_lock_timeout).await?;
        let jobs_deleted = self.cleanup_permanently_failed_jobs().await?;

        log::info!(
            "Startup cleanup completed (queue_locks: {}, job_locks: {}, failed_jobs: {})",
            queue_locks_released,
            job_locks_released,
            jobs_deleted
        );

        Ok((queue_locks_released, job_locks_released, jobs_deleted))
    }

    /// Start a background task that periodically cleans up stale locks.
    ///
    /// This spawns a task that runs at the specified interval, cleaning up
    /// both queue-level and job-level locks using separate timeout thresholds.
    ///
    /// # Arguments
    /// * `interval` - How often to check for stale locks
    /// * `queue_lock_timeout` - Timeout for queue locks (normally held for ms)
    /// * `job_lock_timeout` - Timeout for job locks (held during job execution)
    /// * `cancellation_token` - Token to signal when to stop the background
    ///   task
    ///
    /// # Returns
    /// A JoinHandle for the background task
    pub fn start_stale_lock_cleanup(
        &self,
        interval: Duration,
        queue_lock_timeout: Duration,
        job_lock_timeout: Duration,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        let client = self.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            log::info!(
                "Starting stale lock cleanup task (interval: {}s, queue_timeout: {}s, job_timeout: {}s)",
                interval.as_secs(),
                queue_lock_timeout.as_secs(),
                job_lock_timeout.as_secs()
            );

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        log::info!("Stale lock cleanup task shutting down");
                        break;
                    }
                    _ = interval_timer.tick() => {
                        let mut all_succeeded = true;

                        // Clean queue locks (short timeout - these are normally held briefly)
                        match client.release_stale_queue_locks(queue_lock_timeout).await {
                            Ok(_) => {}
                            Err(e) => {
                                log::warn!("Failed to release stale queue locks: {}", e);
                                crate::metrics::record_cleanup_failure(
                                    "queue_locks",
                                    crate::metrics::classify_error_for_metrics(&e),
                                );
                                all_succeeded = false;
                            }
                        }

                        // Clean job locks (longer timeout - jobs can run for a while)
                        match client.release_stale_job_locks(job_lock_timeout).await {
                            Ok(_) => {}
                            Err(e) => {
                                log::warn!("Failed to release stale job locks: {}", e);
                                crate::metrics::record_cleanup_failure(
                                    "job_locks",
                                    crate::metrics::classify_error_for_metrics(&e),
                                );
                                all_succeeded = false;
                            }
                        }

                        // Update health timestamp if both operations succeeded
                        if all_succeeded {
                            crate::metrics::update_cleanup_health_timestamp();
                        }
                    }
                }
            }
        })
    }
}
