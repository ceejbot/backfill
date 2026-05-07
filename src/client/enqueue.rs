//! The queue functions implementations for backfill client.

use graphile_worker::{TaskHandler, WorkerUtils};
use serde::Serialize;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use super::BackfillClient;
use crate::{BackfillError, EnqueueOutcome, JobSpec};

/// Check if a GraphileWorkerError is a RowNotFound error.
/// This happens when add_job returns NULL because a job with the same
/// job_key is currently locked by a worker.
fn is_row_not_found(e: &graphile_worker::errors::GraphileWorkerError) -> bool {
    use graphile_worker::errors::GraphileWorkerError;
    matches!(e, GraphileWorkerError::SqlError(sqlx::Error::RowNotFound))
}

impl BackfillClient {
    /// Create a new BackfillClient with the given database URL.
    ///
    /// This will create a connection pool and initialize the GraphileWorker
    /// schema.
    pub async fn new(database_url: &str) -> Result<Self, BackfillError> {
        let pool = PgPoolOptions::new().max_connections(10).connect(database_url).await?;

        Self::with_pool_and_schema(pool, "graphile_worker".to_string()).await
    }

    /// Create a BackfillClient with the given database URL and custom schema.
    pub async fn new_with_schema(database_url: &str, schema: &str) -> Result<Self, BackfillError> {
        let pool = PgPoolOptions::new().max_connections(10).connect(database_url).await?;

        Self::with_pool_and_schema(pool, schema.to_string()).await
    }

    /// Create a BackfillClient with an existing connection pool.
    pub async fn with_pool(pool: PgPool) -> Result<Self, BackfillError> {
        Self::with_pool_and_schema(pool, "graphile_worker".to_string()).await
    }

    /// Create a BackfillClient with an existing connection pool and custom
    /// schema.
    pub async fn with_pool_and_schema(pool: PgPool, schema: String) -> Result<Self, BackfillError> {
        // Run migrations to ensure schema is set up
        graphile_worker::WorkerOptions::default()
            .schema(&schema)
            .pg_pool(pool.clone())
            .init()
            .await?;

        Ok(Self { pool, schema })
    }

    /// Get the underlying PostgreSQL connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the schema name being used by this client.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Create a WorkerUtils instance for job management.
    pub fn utils(&self) -> WorkerUtils {
        WorkerUtils::new(self.pool.clone(), self.schema.clone())
    }

    /// Enqueue a job with the specified task identifier and payload.
    ///
    /// # Arguments
    /// * `task_identifier` - The string identifier for the task type
    /// * `payload` - The job payload (must be JSON-serializable)
    /// * `spec` - Job specification including priority, scheduling, etc.
    ///
    /// # Returns
    /// `EnqueueOutcome::Enqueued(Job)` if the job was created or updated,
    /// `EnqueueOutcome::AlreadyInProgress { job_key }` if a job with the same
    /// key is currently being processed.
    pub async fn enqueue<T>(
        &self,
        task_identifier: &str,
        payload: &T,
        spec: JobSpec,
    ) -> Result<EnqueueOutcome, BackfillError>
    where
        T: Serialize,
    {
        let start = std::time::Instant::now();
        let utils = self.utils();
        let job_key = spec.job_key.clone();

        let result = utils
            .add_raw_job(task_identifier, serde_json::to_value(payload)?, spec.clone().into())
            .await;

        match result {
            Ok(job) => {
                // Record successful enqueue
                crate::metrics::record_db_operation("enqueue", "success");
                crate::metrics::record_db_operation_duration("enqueue", start.elapsed().as_secs_f64());

                // Record job enqueued metric. Use the bounded `metric_label()`
                // (either "parallel" or "serial") to avoid exploding the label
                // cardinality when callers use `Queue::serial_for(entity, id)`.
                crate::metrics::record_job_enqueued(spec.queue.metric_label(), task_identifier, spec.priority.0);

                log::debug!(
                    "Job enqueued (job_id: {}, task: {}, queue: {}, priority: {})",
                    job.id(),
                    task_identifier,
                    spec.queue.as_str(),
                    spec.priority.0
                );

                Ok(EnqueueOutcome::Enqueued(Box::new(job)))
            }
            Err(e) => {
                // Check if this is a RowNotFound error with a job_key
                // This indicates the job_key is already locked by a worker
                if is_row_not_found(&e) && job_key.is_some() {
                    // Job with this key is already locked/in progress
                    crate::metrics::record_db_operation("enqueue", "already_in_progress");
                    crate::metrics::record_db_operation_duration("enqueue", start.elapsed().as_secs_f64());

                    // Record the already_in_progress metric (bounded label)
                    crate::metrics::record_job_already_in_progress(spec.queue.metric_label(), task_identifier);

                    let key = job_key.clone().unwrap_or_else(|| "<unknown>".to_string());
                    log::debug!(
                        "Job already in progress (job_key: {}, task: {}, queue: {})",
                        key,
                        task_identifier,
                        spec.queue.as_str()
                    );

                    Ok(EnqueueOutcome::AlreadyInProgress {
                        job_key: job_key.unwrap_or_default(),
                    })
                } else {
                    crate::metrics::record_db_operation("enqueue", "error");
                    log::error!("Failed to enqueue job (task: {}, error: {})", task_identifier, e);
                    Err(e.into())
                }
            }
        }
    }

    /// Enqueue a job with a type-safe task handler.
    ///
    /// This method uses the task's IDENTIFIER constant and ensures the payload
    /// type matches the expected task type.
    pub async fn enqueue_task<T>(&self, task: T, spec: JobSpec) -> Result<EnqueueOutcome, BackfillError>
    where
        T: TaskHandler + Serialize,
    {
        let utils = self.utils();
        let job_key = spec.job_key.clone();

        match utils.add_job(task, spec.into()).await {
            Ok(job) => Ok(EnqueueOutcome::Enqueued(Box::new(job))),
            Err(e) if is_row_not_found(&e) && job_key.is_some() => Ok(EnqueueOutcome::AlreadyInProgress {
                job_key: job_key.unwrap_or_default(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    /// Remove a job by its unique key.
    pub async fn remove_job(&self, job_key: &str) -> Result<(), BackfillError> {
        let utils = self.utils();
        utils.remove_job(job_key).await?;
        Ok(())
    }

    /// Mark jobs as completed.
    pub async fn complete_jobs(&self, job_ids: &[i64]) -> Result<(), BackfillError> {
        let utils = self.utils();
        utils.complete_jobs(job_ids).await?;
        Ok(())
    }

    /// Permanently fail jobs with a reason.
    pub async fn fail_jobs(&self, job_ids: &[i64], reason: &str) -> Result<(), BackfillError> {
        let utils = self.utils();
        utils.permanently_fail_jobs(job_ids, reason).await?;
        Ok(())
    }
}
