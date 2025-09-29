use graphile_worker::{Job, JobKeyMode, TaskHandler, WorkerUtils};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use crate::{BackfillError, DlqFilter, DlqJob, DlqJobList, DlqStats, JobSpec, Priority, Queue};

/// High-level client for the backfill job queue system.
pub struct BackfillClient {
    pool: PgPool,
    schema: String,
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

    /// Get the schema name being used.
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
        let utils = self.utils();
        let job = utils
            .add_raw_job(task_identifier, serde_json::to_value(payload)?, spec.into())
            .await?;

        Ok(job)
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

    /// Initialize the DLQ table if it doesn't exist.
    /// This creates the necessary schema for dead letter queue functionality.
    ///
    /// For production environments with controlled migrations, consider using
    /// the SQL migration files instead: `docs/dlq_schema.sql` contains the
    /// complete schema definition that can be run with psql or your
    /// preferred migration tool. See `docs/DLQ_MIGRATIONS.md` for detailed
    /// instructions.
    pub async fn init_dlq(&self) -> Result<(), BackfillError> {
        // Create the table first
        let create_table_query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.backfill_dlq (
                id BIGSERIAL PRIMARY KEY,
                -- Original job information
                original_job_id BIGINT,
                task_identifier VARCHAR(200) NOT NULL,
                payload JSONB NOT NULL,

                -- Job specification when originally enqueued
                queue_name VARCHAR(100) NOT NULL DEFAULT 'default',
                priority INTEGER NOT NULL DEFAULT 0,
                job_key VARCHAR(200),
                max_attempts INTEGER,

                -- Failure information
                failure_reason TEXT NOT NULL,
                failure_count INTEGER NOT NULL DEFAULT 1,
                last_error JSONB,

                -- Timestamps
                original_created_at TIMESTAMPTZ,
                original_run_at TIMESTAMPTZ,
                failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                -- Admin tracking
                requeued_count INTEGER NOT NULL DEFAULT 0,
                last_requeued_at TIMESTAMPTZ,
                notes TEXT
            )
        "#,
            self.schema
        );

        sqlx::query(&create_table_query).execute(&self.pool).await?;

        // Create indexes separately
        let indexes = vec![
            format!(
                "CREATE INDEX IF NOT EXISTS idx_backfill_dlq_task_identifier ON {}.backfill_dlq (task_identifier)",
                self.schema
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_backfill_dlq_failed_at ON {}.backfill_dlq (failed_at DESC)",
                self.schema
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_backfill_dlq_queue_name ON {}.backfill_dlq (queue_name)",
                self.schema
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_backfill_dlq_job_key ON {}.backfill_dlq (job_key) WHERE job_key IS NOT NULL",
                self.schema
            ),
        ];

        for index_query in indexes {
            sqlx::query(&index_query).execute(&self.pool).await?;
        }

        Ok(())
    }

    /// List jobs in the dead letter queue with pagination and filtering.
    pub async fn list_dlq_jobs(&self, filter: DlqFilter) -> Result<DlqJobList, BackfillError> {
        let mut query_builder = sqlx::QueryBuilder::new("SELECT ");
        query_builder.push(
            r#"
            id, original_job_id, task_identifier, payload, queue_name, priority,
            job_key, max_attempts, failure_reason, failure_count, last_error,
            original_created_at, original_run_at, failed_at, requeued_count,
            last_requeued_at, notes
        "#,
        );
        query_builder
            .push(" FROM ")
            .push(&self.schema)
            .push(".backfill_dlq WHERE 1=1");

        // Apply filters
        if let Some(task) = &filter.task_identifier {
            query_builder.push(" AND task_identifier = ").push_bind(task);
        }
        if let Some(queue) = &filter.queue_name {
            query_builder.push(" AND queue_name = ").push_bind(queue);
        }
        if let Some(from) = filter.failed_after {
            query_builder.push(" AND failed_at >= ").push_bind(from);
        }
        if let Some(to) = filter.failed_before {
            query_builder.push(" AND failed_at <= ").push_bind(to);
        }

        // Order and pagination
        query_builder.push(" ORDER BY failed_at DESC");
        query_builder.push(" LIMIT ").push_bind(filter.limit.unwrap_or(50));
        query_builder.push(" OFFSET ").push_bind(filter.offset.unwrap_or(0));

        let rows = query_builder.build().fetch_all(&self.pool).await?;

        let jobs: Vec<DlqJob> = rows
            .into_iter()
            .map(|row| DlqJob {
                id: row.get("id"),
                original_job_id: row.get("original_job_id"),
                task_identifier: row.get("task_identifier"),
                payload: row.get("payload"),
                queue_name: row.get("queue_name"),
                priority: row.get("priority"),
                job_key: row.get("job_key"),
                max_attempts: row.get("max_attempts"),
                failure_reason: row.get("failure_reason"),
                failure_count: row.get("failure_count"),
                last_error: row.get("last_error"),
                original_created_at: row.get("original_created_at"),
                original_run_at: row.get("original_run_at"),
                failed_at: row.get("failed_at"),
                requeued_count: row.get("requeued_count"),
                last_requeued_at: row.get("last_requeued_at"),
                notes: row.get("notes"),
            })
            .collect();

        // Get total count for pagination (simplified - could be optimized)
        let count_query = format!("SELECT COUNT(*) FROM {}.backfill_dlq", self.schema);
        let total: i64 = sqlx::query_scalar(&count_query)
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        Ok(DlqJobList {
            jobs,
            total: total as u32,
            offset: filter.offset.unwrap_or(0),
            limit: filter.limit.unwrap_or(50),
        })
    }

    /// Get a single DLQ job by ID.
    pub async fn get_dlq_job(&self, dlq_id: i64) -> Result<Option<DlqJob>, BackfillError> {
        let query = format!(
            r#"
            SELECT id, original_job_id, task_identifier, payload, queue_name, priority,
                   job_key, max_attempts, failure_reason, failure_count, last_error,
                   original_created_at, original_run_at, failed_at, requeued_count,
                   last_requeued_at, notes
            FROM {}.backfill_dlq
            WHERE id = $1
        "#,
            self.schema
        );

        let row = sqlx::query(&query).bind(dlq_id).fetch_optional(&self.pool).await?;

        Ok(row.map(|row| DlqJob {
            id: row.get("id"),
            original_job_id: row.get("original_job_id"),
            task_identifier: row.get("task_identifier"),
            payload: row.get("payload"),
            queue_name: row.get("queue_name"),
            priority: row.get("priority"),
            job_key: row.get("job_key"),
            max_attempts: row.get("max_attempts"),
            failure_reason: row.get("failure_reason"),
            failure_count: row.get("failure_count"),
            last_error: row.get("last_error"),
            original_created_at: row.get("original_created_at"),
            original_run_at: row.get("original_run_at"),
            failed_at: row.get("failed_at"),
            requeued_count: row.get("requeued_count"),
            last_requeued_at: row.get("last_requeued_at"),
            notes: row.get("notes"),
        }))
    }

    /// Requeue a job from the DLQ back to the main queue.
    pub async fn requeue_dlq_job(&self, dlq_id: i64, notes: Option<String>) -> Result<Job, BackfillError> {
        // Get the DLQ job
        let dlq_job = self
            .get_dlq_job(dlq_id)
            .await?
            .ok_or_else(|| BackfillError::DlqJobNotFound(dlq_id))?;

        // Create job spec from DLQ job data
        let queue = match dlq_job.queue_name.as_str() {
            "fast" => Queue::Fast,
            "bulk" => Queue::Bulk,
            "dead_letter" => Queue::DeadLetter,
            name => Queue::Custom(name.to_string()),
        };

        let spec = JobSpec {
            run_at: None, // Run immediately
            priority: Priority(dlq_job.priority as i16),
            queue,
            max_attempts: dlq_job.max_attempts,
            retry_policy: None, // Use default retry policy
            job_key: dlq_job.job_key.clone(),
            job_key_mode: JobKeyMode::Replace,
        };

        // Enqueue the job
        let job = self.enqueue(&dlq_job.task_identifier, &dlq_job.payload, spec).await?;

        // Update the DLQ record
        let update_query = format!(
            r#"
            UPDATE {}.backfill_dlq
            SET requeued_count = requeued_count + 1,
                last_requeued_at = NOW(),
                notes = $1
            WHERE id = $2
        "#,
            self.schema
        );

        sqlx::query(&update_query)
            .bind(&notes)
            .bind(dlq_id)
            .execute(&self.pool)
            .await?;

        Ok(job)
    }

    /// Delete a job from the DLQ permanently.
    pub async fn delete_dlq_job(&self, dlq_id: i64) -> Result<bool, BackfillError> {
        let query = format!("DELETE FROM {}.backfill_dlq WHERE id = $1", self.schema);
        let result = sqlx::query(&query).bind(dlq_id).execute(&self.pool).await?;

        Ok(result.rows_affected() > 0)
    }

    /// Get DLQ statistics for monitoring and dashboards.
    pub async fn dlq_stats(&self) -> Result<DlqStats, BackfillError> {
        let query = format!(
            r#"
            SELECT
                COUNT(*) as total_jobs,
                COUNT(DISTINCT task_identifier) as unique_tasks,
                COUNT(DISTINCT queue_name) as unique_queues,
                COALESCE(AVG(failure_count)::FLOAT8, 0) as avg_failure_count,
                COALESCE(SUM(requeued_count), 0) as total_requeued,
                MIN(failed_at) as oldest_failure,
                MAX(failed_at) as newest_failure
            FROM {}.backfill_dlq
        "#,
            self.schema
        );

        let row = sqlx::query(&query).fetch_one(&self.pool).await?;

        let task_breakdown_query = format!(
            r#"
            SELECT task_identifier, COUNT(*) as count
            FROM {}.backfill_dlq
            GROUP BY task_identifier
            ORDER BY count DESC
            LIMIT 10
        "#,
            self.schema
        );

        let task_rows = sqlx::query(&task_breakdown_query).fetch_all(&self.pool).await?;

        let task_breakdown: Vec<(String, u32)> = task_rows
            .into_iter()
            .map(|row| (row.get("task_identifier"), row.get::<i64, _>("count") as u32))
            .collect();

        Ok(DlqStats {
            total_jobs: row.get::<i64, _>("total_jobs") as u32,
            unique_tasks: row.get::<i64, _>("unique_tasks") as u32,
            unique_queues: row.get::<i64, _>("unique_queues") as u32,
            avg_failure_count: row.get::<Option<f64>, _>("avg_failure_count").unwrap_or(0.0),
            total_requeued: row.get::<i64, _>("total_requeued") as u32,
            oldest_failure: row.get("oldest_failure"),
            newest_failure: row.get("newest_failure"),
            task_breakdown,
        })
    }

    /// Add a job to the DLQ. This is typically called by the worker when a job
    /// fails permanently after exhausting all retries.
    pub async fn add_to_dlq(
        &self,
        original_job: &Job,
        failure_reason: &str,
        last_error: Option<serde_json::Value>,
    ) -> Result<DlqJob, BackfillError> {
        // Note: Job doesn't have queue_name field, so we'll use "default" for now
        // In production, this would need to be tracked elsewhere or passed as a
        // parameter
        let queue_name = "default";

        let insert_query = format!(
            r#"
            INSERT INTO {}.backfill_dlq (
                original_job_id, task_identifier, payload, queue_name, priority,
                job_key, max_attempts, failure_reason, failure_count, last_error,
                original_created_at, original_run_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING *
        "#,
            self.schema
        );

        let row = sqlx::query(&insert_query)
            .bind(original_job.id())
            .bind(original_job.task_identifier())
            .bind(original_job.payload())
            .bind(queue_name)
            .bind(*original_job.priority())
            .bind(original_job.key())
            .bind(original_job.max_attempts())
            .bind(failure_reason)
            .bind(original_job.attempts())
            .bind(&last_error)
            .bind(original_job.created_at())
            .bind(original_job.run_at())
            .fetch_one(&self.pool)
            .await?;

        Ok(DlqJob {
            id: row.get("id"),
            original_job_id: row.get("original_job_id"),
            task_identifier: row.get("task_identifier"),
            payload: row.get("payload"),
            queue_name: row.get("queue_name"),
            priority: row.get("priority"),
            job_key: row.get("job_key"),
            max_attempts: row.get("max_attempts"),
            failure_reason: row.get("failure_reason"),
            failure_count: row.get("failure_count"),
            last_error: row.get("last_error"),
            original_created_at: row.get("original_created_at"),
            original_run_at: row.get("original_run_at"),
            failed_at: row.get("failed_at"),
            requeued_count: row.get("requeued_count"),
            last_requeued_at: row.get("last_requeued_at"),
            notes: row.get("notes"),
        })
    }
}
