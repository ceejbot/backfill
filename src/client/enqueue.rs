//! The queue functions implementations for backfill client.

use graphile_worker::{Job, TaskHandler, WorkerUtils};
use serde::Serialize;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use super::BackfillClient;
use crate::{BackfillError, JobSpec};

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
    /// The Job struct containing the job ID and metadata.
    pub async fn enqueue<T>(&self, task_identifier: &str, payload: &T, spec: JobSpec) -> Result<Job, BackfillError>
    where
        T: Serialize,
    {
        let start = std::time::Instant::now();
        let utils = self.utils();

        let result = utils
            .add_raw_job(task_identifier, serde_json::to_value(payload)?, spec.clone().into())
            .await;

        match result {
            Ok(job) => {
                // Record successful enqueue
                crate::metrics::record_db_operation("enqueue", "success");
                crate::metrics::record_db_operation_duration("enqueue", start.elapsed().as_secs_f64());

                // Record job enqueued metric
                crate::metrics::record_job_enqueued(spec.queue.as_str(), task_identifier, spec.priority.0);

                log::debug!(
                    "Job enqueued (job_id: {}, task: {}, queue: {}, priority: {})",
                    job.id(),
                    task_identifier,
                    spec.queue.as_str(),
                    spec.priority.0
                );

                Ok(job)
            }
            Err(e) => {
                crate::metrics::record_db_operation("enqueue", "error");
                log::error!("Failed to enqueue job (task: {}, error: {})", task_identifier, e);
                Err(e.into())
            }
        }
    }

    /// Enqueue a job with a type-safe task handler.
    ///
    /// This method uses the task's IDENTIFIER constant and ensures the payload
    /// type matches the expected task type.
    pub async fn enqueue_task<T>(&self, task: T, spec: JobSpec) -> Result<Job, BackfillError>
    where
        T: TaskHandler + Serialize,
    {
        let utils = self.utils();
        let job = utils.add_job(task, spec.into()).await?;
        Ok(job)
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
