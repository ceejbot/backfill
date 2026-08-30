//! Built-in plugin that short-circuits retries for non-retryable errors.
//!
//! `WorkerError` distinguishes retryable failures (`ConnectionTimeout`,
//! `RateLimitExceeded`, …) from non-retryable failures (`ValidationFailed`,
//! `Unauthorized`, …). Without this plugin, graphile_worker treats every
//! `Err` identically and keeps retrying until `max_attempts` is exhausted —
//! which can mean six hours of pointless retries for a malformed payload.
//!
//! With this plugin registered, when a handler returns an error that
//! `WorkerError::classify_from_message` flags as non-retryable, the job's
//! `attempts` is set to `max_attempts` so it lands in the DLQ on the next
//! processor tick instead of retrying.
//!
//! The plugin is auto-registered when the DLQ processor is enabled
//! (`WorkerConfig::dlq_processor_interval = Some(_)`). Without DLQ enabled
//! it would set `attempts = max_attempts` and the row would just sit in
//! `_private_jobs` forever — pointless and confusing — so the gating is
//! intentional.
//!
//! # Implementation note: hook order in graphile_worker 0.13
//!
//! `fail_job` runs *before* `JobFail` is emitted, and it already clears
//! `locked_at` / `locked_by`. The UPDATE therefore matches unlocked rows
//! (`locked_at IS NULL`) rather than the in-flight lock. `attempts <
//! max_attempts` keeps us from touching a row another worker already
//! claimed.

use graphile_worker::{HookRegistry, JobFail, JobFailContext, Plugin};

use crate::{BackfillClient, WorkerError};

/// Plugin that converts non-retryable handler errors into immediate DLQ
/// candidates by short-circuiting retry attempts.
///
/// Auto-registered by `WorkerRunner` when DLQ is enabled.
#[derive(Clone)]
pub struct PermanentFailurePlugin {
    client: BackfillClient,
}

impl PermanentFailurePlugin {
    /// Create a new permanent-failure plugin.
    pub fn new(client: BackfillClient) -> Self {
        Self { client }
    }
}

impl Plugin for PermanentFailurePlugin {
    fn register(self, hooks: &mut HookRegistry) {
        let client = self.client;
        hooks.on(JobFail, move |ctx: JobFailContext| {
            let client = client.clone();
            async move {
                let classified = WorkerError::classify_from_message(ctx.error.clone());
                if classified.is_retryable() {
                    // Retryable error — let graphile_worker reschedule normally.
                    return;
                }

                let job_id = *ctx.job.id();
                let task = ctx.job.task_identifier().to_string();

                // Set attempts = max_attempts so the next `get_job()` doesn't
                // pick this up (`is_available` is `locked_at IS NULL AND
                // attempts < max_attempts`). The next DLQ tick then moves
                // the row. fail_job already ran and unlocked the row; it
                // does not touch attempts.
                let sql = format!(
                    "UPDATE {schema}._private_jobs \
                     SET attempts = max_attempts, last_error = $2, updated_at = now() \
                     WHERE id = $1 AND locked_at IS NULL AND attempts < max_attempts",
                    schema = client.schema()
                );
                let reason = format!("Permanent failure (non-retryable error): {}", ctx.error);

                match sqlx::query(crate::audited_sql(sql))
                    .bind(job_id)
                    .bind(&reason)
                    .execute(client.pool())
                    .await
                {
                    Ok(result) if result.rows_affected() > 0 => {
                        log::info!(
                            "Short-circuited retries for non-retryable error (job_id: {}, task: {}, error: {})",
                            job_id,
                            task,
                            ctx.error
                        );
                    }
                    Ok(_) => {
                        // No row updated — likely the lock changed hands or
                        // the row was deleted between fetch and hook. Rare
                        // but not catastrophic; fall back to normal retry.
                        log::warn!(
                            "Permanent-failure short-circuit affected zero rows (job_id: {}, task: {})",
                            job_id,
                            task
                        );
                    }
                    Err(e) => {
                        // DB error — log and continue. graphile_worker's
                        // fail_job will run next and the job will retry once
                        // more (and likely fail again, hitting this hook on
                        // the next attempt).
                        log::warn!(
                            "Permanent-failure short-circuit failed; job will retry instead (job_id: {}, task: {}, error: {})",
                            job_id,
                            task,
                            e
                        );
                    }
                }
            }
        });
    }
}
