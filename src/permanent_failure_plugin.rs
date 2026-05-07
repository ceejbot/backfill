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
//! # Implementation note: why we can't use `permanently_fail_jobs`
//!
//! graphile_worker exposes a `permanently_fail_jobs(job_ids, reason)` SQL
//! function that does what we want, but it has a `WHERE locked_at IS NULL OR
//! locked_at < NOW() - 4h` guard to prevent racing with an active worker.
//! Inside the `JobFail` hook the worker (us) still owns the lock —
//! graphile_worker doesn't clear `locked_at` until `fail_job` runs *after*
//! our hook returns. So `permanently_fail_jobs` would silently no-op.
//!
//! We work around this with a direct UPDATE that filters by `locked_by =
//! ctx.worker_id` instead of the `locked_at IS NULL` guard. Same safety
//! property (only this worker can update its own row) but compatible with
//! the in-flight lock state.

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
                // pick this up (its predicate is `is_available`, which is a
                // generated column = `locked_at IS NULL AND attempts <
                // max_attempts`). The next DLQ processor tick will scan this
                // row and move it to the DLQ.
                //
                // We filter by `locked_by = $3` to ensure we only mutate the
                // row while we still own its lock. graphile_worker's
                // `fail_job` will run immediately after this hook returns and
                // will clear locked_at; the `attempts = max_attempts` we
                // wrote here will persist (fail_job doesn't touch attempts).
                let sql = format!(
                    "UPDATE {schema}._private_jobs \
                     SET attempts = max_attempts, last_error = $2, updated_at = now() \
                     WHERE id = $1 AND locked_by = $3",
                    schema = client.schema()
                );
                let reason = format!("Permanent failure (non-retryable error): {}", ctx.error);

                match sqlx::query(&sql)
                    .bind(job_id)
                    .bind(&reason)
                    .bind(&ctx.worker_id)
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
