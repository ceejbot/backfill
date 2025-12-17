//! Dead letter queue types

use chrono::{DateTime, Utc};
use graphile_worker::{Job, JobKeyMode};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use super::BackfillClient;
use crate::{BackfillError, JobSpec, Priority, Queue};

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

impl BackfillClient {
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
            // Unique constraint on job_key for UPSERT support - prevents duplicate DLQ entries
            // when a requeued job fails again. Only applies to non-NULL job_keys.
            format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_backfill_dlq_job_key_unique ON {}.backfill_dlq (job_key) WHERE job_key IS NOT NULL",
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

        // Record DLQ age metrics for monitoring
        let now = Utc::now();
        for job in &jobs {
            let age_seconds = (now - job.failed_at).num_seconds() as f64;
            crate::metrics::record_dlq_age(&job.task_identifier, age_seconds);
        }

        // Get total count for pagination (simplified - could be optimized)
        let count_query = format!("SELECT COUNT(*) FROM {}.backfill_dlq", self.schema);
        let total: i64 = sqlx::query_scalar(&count_query).fetch_one(&self.pool).await?;

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

        Ok(row.map(|row| {
            let job = DlqJob {
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
            };

            // Record DLQ age metric
            let age_seconds = (Utc::now() - job.failed_at).num_seconds() as f64;
            crate::metrics::record_dlq_age(&job.task_identifier, age_seconds);

            job
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
        let outcome = self
            .enqueue(&dlq_job.task_identifier, &dlq_job.payload, spec.clone())
            .await?;

        let job = match outcome {
            crate::EnqueueOutcome::Enqueued(job) => job,
            crate::EnqueueOutcome::AlreadyInProgress { job_key } => {
                return Err(BackfillError::RuntimeError(format!(
                    "Cannot requeue DLQ job {}: a job with key '{}' is already in progress",
                    dlq_id, job_key
                )));
            }
        };

        // Record metrics
        crate::metrics::record_dlq_job_requeued(&dlq_job.task_identifier, spec.queue.as_str());

        log::info!(
            "Job requeued from DLQ (dlq_id: {}, job_id: {}, task: {})",
            dlq_id,
            job.id(),
            dlq_job.task_identifier
        );

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

        Ok(*job)
    }

    /// Delete a job from the DLQ permanently.
    pub async fn delete_dlq_job(&self, dlq_id: i64) -> Result<bool, BackfillError> {
        // Get the job first to record task identifier in metrics
        let task_identifier = if let Some(job) = self.get_dlq_job(dlq_id).await? {
            Some(job.task_identifier.clone())
        } else {
            None
        };

        let query = format!("DELETE FROM {}.backfill_dlq WHERE id = $1", self.schema);
        let result = sqlx::query(&query).bind(dlq_id).execute(&self.pool).await?;

        let deleted = result.rows_affected() > 0;

        if deleted && let Some(task) = task_identifier {
            crate::metrics::record_dlq_job_deleted(&task);
            log::info!("Job deleted from DLQ (dlq_id: {}, task: {})", dlq_id, task);
        }

        Ok(deleted)
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

        let stats = DlqStats {
            total_jobs: row.get::<i64, _>("total_jobs") as u32,
            unique_tasks: row.get::<i64, _>("unique_tasks") as u32,
            unique_queues: row.get::<i64, _>("unique_queues") as u32,
            avg_failure_count: row.get::<Option<f64>, _>("avg_failure_count").unwrap_or(0.0),
            total_requeued: row.get::<i64, _>("total_requeued") as u32,
            oldest_failure: row.get("oldest_failure"),
            newest_failure: row.get("newest_failure"),
            task_breakdown: task_breakdown.clone(),
        };

        // Update DLQ size gauge metrics
        crate::metrics::update_dlq_size(stats.total_jobs);

        // Update per-task breakdown
        for (task, count) in &task_breakdown {
            crate::metrics::update_dlq_size_by_task(task, *count);
        }

        Ok(stats)
    }

    /// Add a job to the DLQ. This is typically called internally when a job
    /// fails permanently after exhausting all retries.
    pub async fn add_to_dlq(
        &self,
        original_job: &Job,
        failure_reason: &str,
        last_error: Option<serde_json::Value>,
    ) -> Result<DlqJob, BackfillError> {
        let start = std::time::Instant::now();

        // Query queue_name from job_queue_id
        let queue_name = if let Some(queue_id) = original_job.job_queue_id() {
            let query = format!(
                "SELECT queue_name FROM {}._private_job_queues WHERE id = $1",
                self.schema
            );
            sqlx::query_scalar::<_, String>(&query)
                .bind(queue_id)
                .fetch_optional(&self.pool)
                .await?
                .unwrap_or_else(|| "default".to_string())
        } else {
            "default".to_string()
        };

        // Use UPSERT to handle the case where a requeued job fails again.
        // If a DLQ entry with the same job_key already exists, update it
        // instead of creating a duplicate. This ensures one DLQ entry per
        // logical job and keeps failed_at current for cooldown calculations.
        let upsert_query = format!(
            r#"
            INSERT INTO {}.backfill_dlq (
                original_job_id, task_identifier, payload, queue_name, priority,
                job_key, max_attempts, failure_reason, failure_count, last_error,
                original_created_at, original_run_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (job_key) WHERE job_key IS NOT NULL DO UPDATE SET
                failed_at = NOW(),
                failure_count = {schema}.backfill_dlq.failure_count + EXCLUDED.failure_count,
                failure_reason = EXCLUDED.failure_reason,
                last_error = EXCLUDED.last_error,
                original_job_id = EXCLUDED.original_job_id
            RETURNING *
        "#,
            self.schema,
            schema = self.schema
        );

        let row = sqlx::query(&upsert_query)
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

        let dlq_job = DlqJob {
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
        };

        // Record metrics
        crate::metrics::record_db_operation("dlq_add", "success");
        crate::metrics::record_db_operation_duration("dlq_add", start.elapsed().as_secs_f64());
        crate::metrics::record_dlq_job_added(&dlq_job.queue_name, &dlq_job.task_identifier, &dlq_job.failure_reason);

        log::info!(
            "Job moved to DLQ (dlq_id: {}, task: {}, failure_reason: {})",
            dlq_job.id,
            dlq_job.task_identifier,
            dlq_job.failure_reason
        );

        Ok(dlq_job)
    }

    /// Scan for permanently failed jobs and move them to the Dead Letter Queue.
    ///
    /// This method looks for jobs that have exhausted their retry attempts and
    /// moves them to the DLQ for manual inspection and potential reprocessing.
    ///
    /// Returns the number of jobs moved to the DLQ.
    pub async fn process_failed_jobs(&self) -> Result<u32, BackfillError> {
        // Find jobs that have failed permanently (attempts >= max_attempts)
        // and haven't been processed yet
        let find_failed_jobs_query = format!(
            r#"
            SELECT jobs.id, tasks.identifier AS task_identifier,
                   job_queues.queue_name, jobs.priority, jobs.key as job_key,
                   jobs.max_attempts, jobs.attempts, jobs.last_error,
                   jobs.created_at, jobs.run_at, jobs.updated_at, jobs.payload
            FROM {}._private_jobs AS jobs
            INNER JOIN {}._private_tasks AS tasks ON tasks.id = jobs.task_id
            LEFT JOIN {}._private_job_queues AS job_queues ON job_queues.id = jobs.job_queue_id
            WHERE jobs.attempts >= jobs.max_attempts
              AND jobs.max_attempts > 0
              AND jobs.id NOT IN (SELECT COALESCE(original_job_id, -1) FROM {}.backfill_dlq)
            ORDER BY jobs.updated_at ASC
            LIMIT 100
        "#,
            self.schema, self.schema, self.schema, self.schema
        );

        let failed_jobs = sqlx::query(&find_failed_jobs_query).fetch_all(&self.pool).await?;

        let mut moved_count = 0;

        for job_row in failed_jobs {
            let job_id: i64 = job_row.get("id");
            let task_identifier: String = job_row.get("task_identifier");
            let payload: serde_json::Value = job_row.get("payload");
            let queue_name: Option<String> = job_row.get("queue_name");
            let queue_name = queue_name.unwrap_or_else(|| "default".to_string());
            let priority: i16 = job_row.get("priority");
            let job_key: Option<String> = job_row.get("job_key");
            let max_attempts: i16 = job_row.get("max_attempts");
            let attempts: i16 = job_row.get("attempts");
            let last_error: Option<String> = job_row.get("last_error");
            let created_at: chrono::DateTime<chrono::Utc> = job_row.get("created_at");
            let run_at: chrono::DateTime<chrono::Utc> = job_row.get("run_at");

            // Convert last_error from TEXT to JSONB for DLQ table
            let last_error_json = last_error.map(serde_json::Value::String);

            // Move to DLQ using UPSERT to handle requeued jobs that fail again
            let upsert_dlq_query = format!(
                r#"
                INSERT INTO {schema}.backfill_dlq (
                    original_job_id, task_identifier, payload, queue_name, priority,
                    job_key, max_attempts, failure_reason, failure_count, last_error,
                    original_created_at, original_run_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (job_key) WHERE job_key IS NOT NULL DO UPDATE SET
                    failed_at = NOW(),
                    failure_count = {schema}.backfill_dlq.failure_count + EXCLUDED.failure_count,
                    failure_reason = EXCLUDED.failure_reason,
                    last_error = EXCLUDED.last_error,
                    original_job_id = EXCLUDED.original_job_id
            "#,
                schema = self.schema
            );

            let failure_reason = format!("Job exceeded maximum retry attempts ({}/{})", attempts, max_attempts);

            let upsert_result = sqlx::query(&upsert_dlq_query)
                .bind(job_id)
                .bind(&task_identifier)
                .bind(&payload)
                .bind(&queue_name)
                .bind(priority)
                .bind(&job_key)
                .bind(max_attempts as i32)
                .bind(failure_reason)
                .bind(attempts as i32)
                .bind(&last_error_json)
                .bind(created_at)
                .bind(run_at)
                .execute(&self.pool)
                .await;

            match upsert_result {
                Ok(_) => {
                    // Successfully moved to DLQ, now remove from main jobs table
                    let delete_query = format!("DELETE FROM {}._private_jobs WHERE id = $1", self.schema);
                    match sqlx::query(&delete_query).bind(job_id).execute(&self.pool).await {
                        Ok(_) => {
                            moved_count += 1;
                            log::info!(
                                "Successfully moved failed job to DLQ (job_id: {}, task: {}, attempts: {}/{})",
                                job_id,
                                task_identifier,
                                attempts,
                                max_attempts
                            );
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to delete job from main table after DLQ insertion (job_id: {}, task: {}, error: {})",
                                job_id,
                                task_identifier,
                                e
                            );
                            // Consider this a partial failure - job is in DLQ
                            // but also still in main table
                        }
                    }
                }
                Err(e) => {
                    log::error!(
                        "Failed to insert job into DLQ (job_id: {}, task: {}, error: {})",
                        job_id,
                        task_identifier,
                        e
                    );
                }
            }
        }

        if moved_count > 0 {
            log::info!("DLQ processing completed (moved_count: {})", moved_count);
        }

        Ok(moved_count)
    }

    /// Process failed jobs continuously in a background task.
    ///
    /// This spawns a background task that periodically scans for failed jobs
    /// and moves them to the DLQ. The task runs until the provided cancellation
    /// token is triggered.
    ///
    /// # Arguments
    /// * `interval` - How often to scan for failed jobs
    /// * `cancellation_token` - Token to signal when to stop the background
    ///   task
    ///
    /// # Returns
    /// A JoinHandle for the background task
    pub fn start_dlq_processor(
        &self,
        interval: std::time::Duration,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let client = self.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            log::info!(
                "Starting DLQ processor background task (interval_seconds: {})",
                interval.as_secs()
            );

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        log::info!("DLQ processor shutting down");
                        break;
                    }
                    _ = interval_timer.tick() => {
                        match client.process_failed_jobs().await {
                            Ok(count) if count > 0 => {
                                log::info!("DLQ processor moved failed jobs (moved_jobs: {})", count);
                            }
                            Ok(_) => {
                                // No jobs moved, no need to log
                            }
                            Err(e) => {
                                log::error!("DLQ processor encountered error: {}", e);
                            }
                        }
                    }
                }
            }
        })
    }
}
