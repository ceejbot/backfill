//! Built-in plugin that cleans up DLQ entries when jobs complete successfully.
//!
//! When a job that was previously in the DLQ completes successfully, this
//! plugin automatically removes the associated DLQ entry. This prevents the DLQ
//! requeue mechanism from repeatedly requeuing jobs that have already
//! succeeded.
//!
//! The cleanup is based on `job_key` - if a successful job has a `job_key`, any
//! DLQ entries with that same `job_key` are deleted.

use graphile_worker::{HookRegistry, JobComplete, JobCompleteContext, Plugin};

use crate::BackfillClient;

/// Plugin that cleans up DLQ entries when jobs with matching job_keys complete.
///
/// This plugin is automatically registered when the DLQ processor is enabled.
/// It ensures that once a requeued job succeeds, its DLQ entry is removed so
/// it won't be requeued again.
#[derive(Clone)]
pub struct DlqCleanupPlugin {
    client: BackfillClient,
}

impl DlqCleanupPlugin {
    /// Create a new DLQ cleanup plugin.
    pub fn new(client: BackfillClient) -> Self {
        Self { client }
    }
}

impl Plugin for DlqCleanupPlugin {
    fn register(self, hooks: &mut HookRegistry) {
        let client = self.client;
        hooks.on(JobComplete, move |ctx: JobCompleteContext| {
            let client = client.clone();
            async move {
                // Only clean up if the job has a job_key
                if let Some(job_key) = ctx.job.key() {
                    match client.delete_dlq_by_job_key(job_key).await {
                        Ok(deleted) if deleted > 0 => {
                            log::debug!(
                                "Cleaned up DLQ entry after successful job (job_key: {}, task: {})",
                                job_key,
                                ctx.job.task_identifier()
                            );
                        }
                        Ok(_) => {
                            // No DLQ entry to clean up, this is normal for jobs
                            // that never failed
                        }
                        Err(e) => {
                            // Log but don't fail - this is a best-effort cleanup
                            log::warn!(
                                "Failed to clean up DLQ entry after job success (job_key: {}, error: {})",
                                job_key,
                                e
                            );
                        }
                    }
                }
            }
        });
    }
}
