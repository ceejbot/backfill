//! Axum HTTP API for backfill administration
//!
//! This module provides a library-first approach to adding backfill management
//! endpoints to existing Axum applications. Users can mount the admin router
//! at any path they choose.
//!
//! # Examples
//!
//! ## Basic Integration
//! ```rust,no_run
//! use axum::{Router, extract::State};
//! use backfill::{BackfillClient, admin::{BackfillAdminState, create_admin_router}};
//!
//! #[derive(Clone)]
//! struct AppState {
//!     backfill: BackfillClient,
//!     // ... your other app state
//! }
//!
//! impl BackfillAdminState for AppState {
//!     fn backfill_client(&self) -> &BackfillClient {
//!         &self.backfill
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let state = AppState {
//!         backfill: BackfillClient::new("postgresql://localhost/mydb").await?,
//!     };
//!
//!     let app: Router = Router::new()
//!         .nest("/admin/backfill", create_admin_router())
//!         .with_state(state);
//!
//!     // ... serve your app
//!     Ok(())
//! }
//! ```

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::{delete, get, post};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::BackfillClient;
use crate::client::DlqFilter;

/// Trait that application state must implement to use the admin API
///
/// This trait allows the admin router to access the BackfillClient from
/// your application's state object, regardless of how you've structured it.
pub trait BackfillAdminState: Clone + Send + Sync + 'static {
    /// Get a reference to the BackfillClient
    fn backfill_client(&self) -> &BackfillClient;
}

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Queue status information
#[derive(Serialize)]
pub struct QueueStatus {
    pub queue_name: String,
    pub pending_jobs: i64,
    pub active_jobs: i64,
    pub completed_jobs: i64,
    pub failed_jobs: i64,
}

/// Overall system status
#[derive(Serialize)]
pub struct SystemStatus {
    pub queues: Vec<QueueStatus>,
    pub dlq_enabled: bool,
    pub dlq_job_count: i64,
    pub total_jobs: i64,
}

/// Get statistics for a specific queue from the database
async fn get_queue_stats(
    pool: &sqlx::PgPool,
    schema: &str,
    queue_name: &str,
) -> Result<QueueStatus, (StatusCode, String)> {
    // Query for pending jobs (not locked, ready to run)
    let pending_query = format!(
        r#"
        SELECT COUNT(*)
        FROM {}._private_jobs j
        LEFT JOIN {}._private_job_queues q ON j.job_queue_id = q.id
        WHERE COALESCE(q.queue_name, 'default') = $1
          AND j.locked_at IS NULL
          AND j.run_at <= NOW()
          AND j.attempts < j.max_attempts
        "#,
        schema, schema
    );

    let pending_jobs = sqlx::query_scalar::<_, i64>(&pending_query)
        .bind(queue_name)
        .fetch_one(pool)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to query pending jobs: {}", e),
            )
        })?;

    // Query for active jobs (currently locked by workers)
    let active_query = format!(
        r#"
        SELECT COUNT(*)
        FROM {}._private_jobs j
        LEFT JOIN {}._private_job_queues q ON j.job_queue_id = q.id
        WHERE COALESCE(q.queue_name, 'default') = $1
          AND j.locked_at IS NOT NULL
        "#,
        schema, schema
    );

    let active_jobs = sqlx::query_scalar::<_, i64>(&active_query)
        .bind(queue_name)
        .fetch_one(pool)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to query active jobs: {}", e),
            )
        })?;

    // Query for failed jobs (attempts >= max_attempts)
    let failed_query = format!(
        r#"
        SELECT COUNT(*)
        FROM {}._private_jobs j
        LEFT JOIN {}._private_job_queues q ON j.job_queue_id = q.id
        WHERE COALESCE(q.queue_name, 'default') = $1
          AND j.attempts >= j.max_attempts
          AND j.max_attempts > 0
        "#,
        schema, schema
    );

    let failed_jobs = sqlx::query_scalar::<_, i64>(&failed_query)
        .bind(queue_name)
        .fetch_one(pool)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to query failed jobs: {}", e),
            )
        })?;

    Ok(QueueStatus {
        queue_name: queue_name.to_string(),
        pending_jobs,
        active_jobs,
        completed_jobs: 0, // GraphileWorker deletes completed jobs - use metrics system
        failed_jobs,
    })
}

/// Get all queue names from the database
async fn get_all_queue_names(pool: &sqlx::PgPool, schema: &str) -> Result<Vec<String>, (StatusCode, String)> {
    let query = format!(
        "SELECT DISTINCT queue_name FROM {}._private_job_queues ORDER BY queue_name",
        schema
    );

    let queue_names = sqlx::query_scalar::<_, String>(&query)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to query queue names: {}", e),
            )
        })?;

    // Include "default" if there are any jobs without a queue
    let has_default_jobs_query = format!(
        "SELECT EXISTS(SELECT 1 FROM {}._private_jobs WHERE job_queue_id IS NULL)",
        schema
    );

    let has_default_jobs = sqlx::query_scalar::<_, bool>(&has_default_jobs_query)
        .fetch_one(pool)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to check for default queue jobs: {}", e),
            )
        })?;

    let mut all_queues = queue_names;
    if has_default_jobs && !all_queues.contains(&"default".to_string()) {
        all_queues.insert(0, "default".to_string());
    }

    Ok(all_queues)
}

/// Job enqueueing request
#[derive(Deserialize)]
pub struct EnqueueJobRequest {
    pub task_identifier: String,
    pub payload: serde_json::Value,
    pub queue: Option<String>,
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub job_key: Option<String>,
    pub run_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Job enqueueing response
#[derive(Serialize)]
pub struct EnqueueJobResponse {
    pub job_id: String,
    pub status: String,
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
}

/// DLQ query parameters
#[derive(Deserialize)]
pub struct DlqQueryParams {
    pub limit: Option<i32>,
    pub offset: Option<i32>,
    pub queue_name: Option<String>,
    pub task_identifier: Option<String>,
}

/// Error response format
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl ErrorResponse {
    fn new(error: impl std::fmt::Display, code: impl Into<String>) -> Self {
        Self {
            error: error.to_string(),
            code: code.into(),
            timestamp: chrono::Utc::now(),
        }
    }
}

/// DLQ cleanup request
#[derive(Deserialize)]
pub struct DlqCleanupRequest {
    pub older_than_days: Option<i64>,
    pub task_identifier: Option<String>,
    pub queue_name: Option<String>,
    pub max_jobs: Option<i32>,
    pub dry_run: Option<bool>,
}

/// DLQ cleanup response
#[derive(Serialize)]
pub struct DlqCleanupResponse {
    pub deleted_count: u32,
    pub dry_run: bool,
    pub cutoff_date: chrono::DateTime<chrono::Utc>,
    pub affected_tasks: Vec<String>,
}

/// DLQ batch requeue request
#[derive(Deserialize)]
pub struct DlqBatchRequeueRequest {
    pub task_identifier: Option<String>,
    pub queue_name: Option<String>,
    pub failed_after: Option<chrono::DateTime<chrono::Utc>>,
    pub failed_before: Option<chrono::DateTime<chrono::Utc>>,
    pub notes: Option<String>,
    pub max_jobs: Option<i32>,
    pub dry_run: Option<bool>,
    pub throttle_ms: Option<u64>,
}

/// Batch operation error details
#[derive(Serialize)]
pub struct BatchOperationError {
    pub dlq_id: i64,
    pub task_identifier: String,
    pub error: String,
}

/// DLQ batch requeue response
#[derive(Serialize)]
pub struct DlqBatchRequeueResponse {
    pub requeued_count: u32,
    pub failed_count: u32,
    pub dry_run: bool,
    pub affected_tasks: Vec<String>,
    pub errors: Vec<BatchOperationError>,
}

/// DLQ batch delete request
#[derive(Deserialize)]
pub struct DlqBatchDeleteRequest {
    pub task_identifier: Option<String>,
    pub queue_name: Option<String>,
    pub failed_after: Option<chrono::DateTime<chrono::Utc>>,
    pub failed_before: Option<chrono::DateTime<chrono::Utc>>,
    pub max_jobs: Option<i32>,
    pub dry_run: Option<bool>,
}

/// DLQ batch delete response
#[derive(Serialize)]
pub struct DlqBatchDeleteResponse {
    pub deleted_count: u32,
    pub dry_run: bool,
    pub affected_tasks: Vec<String>,
}

// =============================================================================
// Lock Diagnostics Types
// =============================================================================

/// Information about a single queue lock
#[derive(Serialize)]
pub struct QueueLockInfo {
    pub queue_name: String,
    pub locked_at: chrono::DateTime<chrono::Utc>,
    pub locked_by: String,
    pub locked_minutes: f64,
    pub is_stale: bool,
}

/// Information about a single job lock
#[derive(Serialize)]
pub struct JobLockInfo {
    pub job_id: i64,
    pub task_identifier: String,
    pub locked_at: chrono::DateTime<chrono::Utc>,
    pub locked_by: String,
    pub locked_minutes: f64,
    pub attempts: i16,
    pub max_attempts: i16,
    pub is_stale: bool,
}

/// Overall lock status response
#[derive(Serialize)]
pub struct LocksStatusResponse {
    pub queue_locks: Vec<QueueLockInfo>,
    pub job_locks: Vec<JobLockInfo>,
    pub stale_queue_lock_count: usize,
    pub stale_job_lock_count: usize,
    pub queue_lock_stale_threshold_minutes: u64,
    pub job_lock_stale_threshold_minutes: u64,
    pub health_status: String,
}

/// Request to manually trigger stale lock cleanup
#[derive(Deserialize)]
pub struct LocksCleanupRequest {
    /// Override the queue lock stale threshold (minutes)
    pub queue_lock_threshold_minutes: Option<u64>,
    /// Override the job lock stale threshold (minutes)
    pub job_lock_threshold_minutes: Option<u64>,
    /// If true, only report what would be cleaned without actually cleaning
    pub dry_run: Option<bool>,
}

/// Response from manual lock cleanup
#[derive(Serialize)]
pub struct LocksCleanupResponse {
    pub queue_locks_released: u64,
    pub job_locks_released: u64,
    pub failed_jobs_deleted: u64,
    pub dry_run: bool,
}

/// Row type for queue lock queries
type QueueLockRow = (String, chrono::DateTime<chrono::Utc>, String, f64);

/// Row type for job lock queries (id, task_identifier, locked_at, locked_by,
/// locked_minutes, attempts, max_attempts)
type JobLockRow = (i64, String, chrono::DateTime<chrono::Utc>, String, f64, i16, i16);

/// Create the admin router that can be mounted in any Axum application
///
/// This router provides comprehensive backfill management endpoints:
/// - Health checks and system status
/// - Job enqueueing and management
/// - Dead Letter Queue (DLQ) management
/// - Queue statistics and monitoring
///
/// The router is designed to be well-behaved:
/// - Consistent JSON API responses
/// - Proper HTTP status codes
/// - Structured error handling
/// - Request/response logging
/// - CORS-ready (users can add middleware)
pub fn create_admin_router<S>() -> Router<S>
where
    S: BackfillAdminState,
{
    Router::new()
        // Health and status endpoints
        .route("/health", get(health_check))
        .route("/status", get(system_status::<S>))
        // Job management endpoints
        .route("/jobs", post(enqueue_job::<S>))
        .route("/jobs/{job_id}", get(get_job::<S>))
        .route("/jobs/{job_id}/cancel", delete(cancel_job::<S>))
        // Queue management endpoints
        .route("/queues", get(list_queues::<S>))
        .route("/queues/{queue_name}/stats", get(queue_stats::<S>))
        // Dead Letter Queue management
        .route("/dlq", get(list_dlq_jobs::<S>))
        .route("/dlq/stats", get(dlq_stats::<S>))
        .route("/dlq/{dlq_id}", get(get_dlq_job::<S>))
        .route("/dlq/{dlq_id}", delete(delete_dlq_job::<S>))
        .route("/dlq/{dlq_id}/requeue", post(requeue_dlq_job::<S>))
        .route("/dlq/cleanup", post(cleanup_dlq::<S>))
        .route("/dlq/batch-requeue", post(batch_requeue_dlq_jobs::<S>))
        .route("/dlq/batch-delete", post(batch_delete_dlq_jobs::<S>))
        // Lock diagnostics and cleanup
        .route("/locks/status", get(locks_status::<S>))
        .route("/locks/cleanup", post(locks_cleanup::<S>))
}

/// Health check endpoint - GET /health
async fn health_check() -> Result<Json<HealthResponse>, StatusCode> {
    let response = HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: chrono::Utc::now(),
    };

    Ok(Json(response))
}

/// System status endpoint - GET /status
async fn system_status<S>(State(state): State<S>) -> Result<Json<SystemStatus>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();
    let pool = client.pool();
    let schema = client.schema();

    // Get all queue names
    let queue_names = get_all_queue_names(pool, schema)
        .await
        .map_err(|(status, msg)| (status, Json(ErrorResponse::new(msg, "QUERY_ERROR"))))?;

    // Get stats for each queue
    let mut queue_stats = Vec::new();
    for queue_name in queue_names {
        let stats = get_queue_stats(pool, schema, &queue_name)
            .await
            .map_err(|(status, msg)| (status, Json(ErrorResponse::new(msg, "QUERY_ERROR"))))?;
        queue_stats.push(stats);
    }

    // Get DLQ stats
    let dlq_stats = client.dlq_stats().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(
                format!("Failed to get DLQ stats: {}", e),
                "DLQ_ERROR",
            )),
        )
    })?;

    // Calculate total jobs across all queues
    let total_jobs: i64 = queue_stats
        .iter()
        .map(|q| q.pending_jobs + q.active_jobs + q.failed_jobs)
        .sum();

    let status = SystemStatus {
        queues: queue_stats,
        dlq_enabled: true,
        dlq_job_count: dlq_stats.total_jobs as i64,
        total_jobs,
    };

    info!("Retrieved system status: {} queues", status.queues.len());
    Ok(Json(status))
}

/// Enqueue job endpoint - POST /jobs
async fn enqueue_job<S>(
    State(state): State<S>,
    Json(req): Json<EnqueueJobRequest>,
) -> Result<Json<EnqueueJobResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();

    info!("Enqueueing job: task={}, queue={:?}", req.task_identifier, req.queue);

    // Build job spec from request
    let mut spec = crate::JobSpec::default();

    if let Some(queue) = req.queue {
        // Empty string or "parallel" means parallel execution; anything else is a
        // serial queue
        spec.queue = match queue.as_str() {
            "" | "parallel" => crate::Queue::Parallel,
            name => crate::Queue::Serial(name.to_string()),
        };
    }

    if let Some(priority) = req.priority {
        spec.priority = crate::Priority(priority as i16);
    }

    if let Some(max_attempts) = req.max_attempts {
        spec.max_attempts = Some(max_attempts);
    }

    if let Some(job_key) = req.job_key {
        spec.job_key = Some(job_key);
    }

    if let Some(run_at) = req.run_at {
        spec.run_at = Some(run_at);
    }

    match client.enqueue(&req.task_identifier, &req.payload, spec).await {
        Ok(crate::EnqueueOutcome::Enqueued(job)) => {
            let response = EnqueueJobResponse {
                job_id: job.id().to_string(),
                status: "enqueued".to_string(),
                enqueued_at: chrono::Utc::now(),
            };

            info!("Successfully enqueued job: id={}", job.id());
            Ok(Json(response))
        }
        Ok(crate::EnqueueOutcome::AlreadyInProgress { job_key }) => {
            // Return 409 Conflict for already in progress
            warn!("Job already in progress: job_key={}", job_key);
            Err((
                StatusCode::CONFLICT,
                Json(ErrorResponse::new(
                    format!("Job with key '{}' is already in progress", job_key),
                    "JOB_ALREADY_IN_PROGRESS",
                )),
            ))
        }
        Err(e) => {
            error!("Failed to enqueue job: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e, "ENQUEUE_FAILED")),
            ))
        }
    }
}

/// Get job endpoint - GET /jobs/:job_id
async fn get_job<S>(
    State(_state): State<S>,
    Path(job_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    // This would require querying the GraphileWorker jobs table
    // For now, return not implemented
    warn!("Get job endpoint not yet implemented: job_id={}", job_id);
    Err((
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse::new(
            "Job retrieval not yet implemented",
            "NOT_IMPLEMENTED",
        )),
    ))
}

/// Cancel job endpoint - DELETE /jobs/:job_id
async fn cancel_job<S>(
    State(_state): State<S>,
    Path(job_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    // This would require updating the GraphileWorker jobs table
    // For now, return not implemented
    warn!("Cancel job endpoint not yet implemented: job_id={}", job_id);
    Err((
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse::new(
            "Job cancellation not yet implemented",
            "NOT_IMPLEMENTED",
        )),
    ))
}

/// List queues endpoint - GET /queues
async fn list_queues<S>(State(state): State<S>) -> Result<Json<Vec<QueueStatus>>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();
    let pool = client.pool();
    let schema = client.schema();

    // Get all queue names
    let queue_names = get_all_queue_names(pool, schema)
        .await
        .map_err(|(status, msg)| (status, Json(ErrorResponse::new(msg, "QUERY_ERROR"))))?;

    // Get stats for each queue
    let mut queue_stats = Vec::new();
    for queue_name in queue_names {
        let stats = get_queue_stats(pool, schema, &queue_name)
            .await
            .map_err(|(status, msg)| (status, Json(ErrorResponse::new(msg, "QUERY_ERROR"))))?;
        queue_stats.push(stats);
    }

    info!("Listed {} queues", queue_stats.len());
    Ok(Json(queue_stats))
}

/// Queue stats endpoint - GET /queues/:queue_name/stats
async fn queue_stats<S>(
    State(state): State<S>,
    Path(queue_name): Path<String>,
) -> Result<Json<QueueStatus>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();
    let stats = get_queue_stats(client.pool(), client.schema(), &queue_name)
        .await
        .map_err(|(status, msg)| (status, Json(ErrorResponse::new(msg, "QUERY_ERROR"))))?;

    info!("Retrieved stats for queue: {}", queue_name);
    Ok(Json(stats))
}

/// List DLQ jobs endpoint - GET /dlq
async fn list_dlq_jobs<S>(
    State(state): State<S>,
    Query(params): Query<DlqQueryParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();

    let filter = DlqFilter {
        queue_name: params.queue_name,
        task_identifier: params.task_identifier,
        failed_after: None,
        failed_before: None,
        limit: Some(params.limit.unwrap_or(50)),
        offset: Some(params.offset.unwrap_or(0)),
    };

    info!(
        "Listing DLQ jobs with filter: limit={:?}, offset={:?}",
        filter.limit, filter.offset
    );

    match client.list_dlq_jobs(filter).await {
        Ok(job_list) => Ok(Json(serde_json::json!({
            "jobs": job_list.jobs,
            "total_count": job_list.total,
            "has_more": (job_list.offset + job_list.limit) < job_list.total as i32
        }))),
        Err(e) => {
            error!("Failed to list DLQ jobs: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e, "DLQ_LIST_FAILED")),
            ))
        }
    }
}

/// DLQ stats endpoint - GET /dlq/stats
async fn dlq_stats<S>(State(state): State<S>) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();

    match client.dlq_stats().await {
        Ok(stats) => {
            info!("Retrieved DLQ stats: total_jobs={}", stats.total_jobs);
            Ok(Json(serde_json::to_value(stats).map_err(|e| {
                error!("Failed to serialize DLQ stats: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new("Serialization error", "SERIALIZATION_FAILED")),
                )
            })?))
        }
        Err(e) => {
            error!("Failed to get DLQ stats: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e, "DLQ_STATS_FAILED")),
            ))
        }
    }
}

/// Get DLQ job endpoint - GET /dlq/:dlq_id
async fn get_dlq_job<S>(
    State(state): State<S>,
    Path(dlq_id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();

    match client.get_dlq_job(dlq_id).await {
        Ok(Some(job)) => {
            info!("Retrieved DLQ job: id={}", dlq_id);
            Ok(Json(serde_json::to_value(job).map_err(|e| {
                error!("Failed to serialize DLQ job: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new("Serialization error", "SERIALIZATION_FAILED")),
                )
            })?))
        }
        Ok(None) => {
            warn!("DLQ job not found: id={}", dlq_id);
            Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new("DLQ job not found", "DLQ_JOB_NOT_FOUND")),
            ))
        }
        Err(e) => {
            error!("Failed to get DLQ job {}: {}", dlq_id, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e, "DLQ_GET_FAILED")),
            ))
        }
    }
}

/// Delete DLQ job endpoint - DELETE /dlq/:dlq_id
async fn delete_dlq_job<S>(
    State(state): State<S>,
    Path(dlq_id): Path<i64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();

    match client.delete_dlq_job(dlq_id).await {
        Ok(true) => {
            info!("Successfully deleted DLQ job: id={}", dlq_id);
            Ok(StatusCode::NO_CONTENT)
        }
        Ok(false) => {
            warn!("DLQ job not found for deletion: id={}", dlq_id);
            Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new("DLQ job not found", "DLQ_JOB_NOT_FOUND")),
            ))
        }
        Err(e) => {
            error!("Failed to delete DLQ job {}: {}", dlq_id, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e, "DLQ_DELETE_FAILED")),
            ))
        }
    }
}

/// Requeue DLQ job endpoint - POST /dlq/:dlq_id/requeue
async fn requeue_dlq_job<S>(
    State(state): State<S>,
    Path(dlq_id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();

    match client.requeue_dlq_job(dlq_id, None).await {
        Ok(job) => {
            info!(
                "Successfully requeued DLQ job: dlq_id={}, new_job_id={}",
                dlq_id,
                job.id()
            );
            Ok(Json(serde_json::json!({
                "job_id": job.id(),
                "status": "requeued",
                "requeued_at": chrono::Utc::now()
            })))
        }
        Err(e) if e.to_string().contains("not found") => {
            warn!("DLQ job not found for requeue: id={}", dlq_id);
            Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new("DLQ job not found", "DLQ_JOB_NOT_FOUND")),
            ))
        }
        Err(e) => {
            error!("Failed to requeue DLQ job {}: {}", dlq_id, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e, "DLQ_REQUEUE_FAILED")),
            ))
        }
    }
}

/// Cleanup DLQ endpoint - POST /dlq/cleanup
async fn cleanup_dlq<S>(
    State(state): State<S>,
    Json(req): Json<DlqCleanupRequest>,
) -> Result<Json<DlqCleanupResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();

    // Default to 90 days if not specified
    let days = req.older_than_days.unwrap_or(90);
    let cutoff_date = chrono::Utc::now() - chrono::Duration::days(days);
    let max_jobs = req.max_jobs.unwrap_or(1000).min(10000);
    let dry_run = req.dry_run.unwrap_or(false);

    // Build filter for list query
    let filter = DlqFilter {
        task_identifier: req.task_identifier.clone(),
        queue_name: req.queue_name.clone(),
        failed_before: Some(cutoff_date),
        limit: Some(max_jobs),
        ..Default::default()
    };

    // Get jobs to delete
    let jobs_to_delete = client.list_dlq_jobs(filter).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(format!("Failed to query DLQ: {}", e), "QUERY_ERROR")),
        )
    })?;

    if dry_run {
        let affected_tasks: Vec<String> = jobs_to_delete
            .jobs
            .iter()
            .map(|j| j.task_identifier.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        return Ok(Json(DlqCleanupResponse {
            deleted_count: jobs_to_delete.jobs.len() as u32,
            dry_run: true,
            cutoff_date,
            affected_tasks,
        }));
    }

    // Delete jobs
    let mut deleted_count = 0;
    let mut affected_tasks = std::collections::HashSet::new();

    for job in &jobs_to_delete.jobs {
        if client.delete_dlq_job(job.id).await.unwrap_or(false) {
            deleted_count += 1;
            affected_tasks.insert(job.task_identifier.clone());
        }
    }

    info!("DLQ cleanup completed: deleted {} jobs", deleted_count);

    Ok(Json(DlqCleanupResponse {
        deleted_count,
        dry_run: false,
        cutoff_date,
        affected_tasks: affected_tasks.into_iter().collect(),
    }))
}

/// Batch requeue DLQ jobs endpoint - POST /dlq/batch-requeue
async fn batch_requeue_dlq_jobs<S>(
    State(state): State<S>,
    Json(req): Json<DlqBatchRequeueRequest>,
) -> Result<Json<DlqBatchRequeueResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();
    let max_jobs = req.max_jobs.unwrap_or(100).min(1000);
    let dry_run = req.dry_run.unwrap_or(false);
    let throttle_ms = req.throttle_ms.unwrap_or(50).min(5000);

    // Build filter
    let filter = DlqFilter {
        task_identifier: req.task_identifier.clone(),
        queue_name: req.queue_name.clone(),
        failed_after: req.failed_after,
        failed_before: req.failed_before,
        limit: Some(max_jobs),
        ..Default::default()
    };

    // Get jobs to requeue
    let jobs_to_requeue = client.list_dlq_jobs(filter).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(format!("Failed to query DLQ: {}", e), "QUERY_ERROR")),
        )
    })?;

    if dry_run {
        let affected_tasks: Vec<String> = jobs_to_requeue
            .jobs
            .iter()
            .map(|j| j.task_identifier.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        return Ok(Json(DlqBatchRequeueResponse {
            requeued_count: jobs_to_requeue.jobs.len() as u32,
            failed_count: 0,
            dry_run: true,
            affected_tasks,
            errors: vec![],
        }));
    }

    // Requeue jobs with throttling
    let mut requeued_count = 0;
    let mut failed_count = 0;
    let mut errors = Vec::new();
    let mut affected_tasks = std::collections::HashSet::new();

    for job in &jobs_to_requeue.jobs {
        match client.requeue_dlq_job(job.id, req.notes.clone()).await {
            Ok(_) => {
                requeued_count += 1;
                affected_tasks.insert(job.task_identifier.clone());

                // Throttle to avoid overwhelming the queue
                if throttle_ms > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(throttle_ms)).await;
                }
            }
            Err(e) => {
                failed_count += 1;
                errors.push(BatchOperationError {
                    dlq_id: job.id,
                    task_identifier: job.task_identifier.clone(),
                    error: e.to_string(),
                });
            }
        }
    }

    info!(
        "DLQ batch requeue completed: {} succeeded, {} failed",
        requeued_count, failed_count
    );

    Ok(Json(DlqBatchRequeueResponse {
        requeued_count,
        failed_count,
        dry_run: false,
        affected_tasks: affected_tasks.into_iter().collect(),
        errors,
    }))
}

/// Batch delete DLQ jobs endpoint - POST /dlq/batch-delete
async fn batch_delete_dlq_jobs<S>(
    State(state): State<S>,
    Json(req): Json<DlqBatchDeleteRequest>,
) -> Result<Json<DlqBatchDeleteResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();
    let max_jobs = req.max_jobs.unwrap_or(100).min(1000);
    let dry_run = req.dry_run.unwrap_or(false);

    // Build filter
    let filter = DlqFilter {
        task_identifier: req.task_identifier.clone(),
        queue_name: req.queue_name.clone(),
        failed_after: req.failed_after,
        failed_before: req.failed_before,
        limit: Some(max_jobs),
        ..Default::default()
    };

    // Get jobs to delete
    let jobs_to_delete = client.list_dlq_jobs(filter).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(format!("Failed to query DLQ: {}", e), "QUERY_ERROR")),
        )
    })?;

    if dry_run {
        let affected_tasks: Vec<String> = jobs_to_delete
            .jobs
            .iter()
            .map(|j| j.task_identifier.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        return Ok(Json(DlqBatchDeleteResponse {
            deleted_count: jobs_to_delete.jobs.len() as u32,
            dry_run: true,
            affected_tasks,
        }));
    }

    // Delete jobs
    let mut deleted_count = 0;
    let mut affected_tasks = std::collections::HashSet::new();

    for job in &jobs_to_delete.jobs {
        if client.delete_dlq_job(job.id).await.unwrap_or(false) {
            deleted_count += 1;
            affected_tasks.insert(job.task_identifier.clone());
        }
    }

    info!("DLQ batch delete completed: deleted {} jobs", deleted_count);

    Ok(Json(DlqBatchDeleteResponse {
        deleted_count,
        dry_run: false,
        affected_tasks: affected_tasks.into_iter().collect(),
    }))
}

// =============================================================================
// Lock Diagnostics Endpoints
// =============================================================================

/// Default stale threshold for queue locks (5 minutes)
const DEFAULT_QUEUE_LOCK_STALE_MINUTES: u64 = 5;
/// Default stale threshold for job locks (30 minutes)
const DEFAULT_JOB_LOCK_STALE_MINUTES: u64 = 30;

/// Get current lock status - GET /locks/status
///
/// Returns information about all currently held queue and job locks,
/// identifying which ones are potentially stale and blocking workers.
async fn locks_status<S>(State(state): State<S>) -> Result<Json<LocksStatusResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();
    let pool = client.pool();
    let schema = client.schema();

    // Query queue locks
    let queue_locks_query = format!(
        r#"
        SELECT
            queue_name,
            locked_at,
            locked_by,
            EXTRACT(EPOCH FROM (NOW() - locked_at)) / 60.0 as locked_minutes
        FROM {}._private_job_queues
        WHERE locked_at IS NOT NULL
        ORDER BY locked_at ASC
        "#,
        schema
    );

    let queue_locks: Vec<QueueLockRow> = sqlx::query_as(&queue_locks_query).fetch_all(pool).await.map_err(|e| {
        error!("Failed to query queue locks: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(
                format!("Failed to query queue locks: {}", e),
                "QUERY_ERROR",
            )),
        )
    })?;

    // Query job locks (with task identifier)
    let job_locks_query = format!(
        r#"
        SELECT
            j.id,
            t.identifier as task_identifier,
            j.locked_at,
            j.locked_by,
            EXTRACT(EPOCH FROM (NOW() - j.locked_at)) / 60.0 as locked_minutes,
            j.attempts,
            j.max_attempts
        FROM {}._private_jobs j
        JOIN {}._private_tasks t ON j.task_id = t.id
        WHERE j.locked_at IS NOT NULL
        ORDER BY j.locked_at ASC
        LIMIT 100
        "#,
        schema, schema
    );

    let job_locks: Vec<JobLockRow> = sqlx::query_as(&job_locks_query).fetch_all(pool).await.map_err(|e| {
        error!("Failed to query job locks: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(
                format!("Failed to query job locks: {}", e),
                "QUERY_ERROR",
            )),
        )
    })?;

    // Convert to response types
    let queue_lock_infos: Vec<QueueLockInfo> = queue_locks
        .into_iter()
        .map(|(queue_name, locked_at, locked_by, locked_minutes)| {
            let is_stale = locked_minutes > DEFAULT_QUEUE_LOCK_STALE_MINUTES as f64;
            QueueLockInfo {
                queue_name,
                locked_at,
                locked_by,
                locked_minutes,
                is_stale,
            }
        })
        .collect();

    let job_lock_infos: Vec<JobLockInfo> = job_locks
        .into_iter()
        .map(
            |(job_id, task_identifier, locked_at, locked_by, locked_minutes, attempts, max_attempts)| {
                let is_stale = locked_minutes > DEFAULT_JOB_LOCK_STALE_MINUTES as f64;
                JobLockInfo {
                    job_id,
                    task_identifier,
                    locked_at,
                    locked_by,
                    locked_minutes,
                    attempts,
                    max_attempts,
                    is_stale,
                }
            },
        )
        .collect();

    let stale_queue_count = queue_lock_infos.iter().filter(|l| l.is_stale).count();
    let stale_job_count = job_lock_infos.iter().filter(|l| l.is_stale).count();

    // Determine health status
    let health_status = if stale_queue_count > 0 {
        "unhealthy - stale queue locks detected (workers may be blocked)"
    } else if stale_job_count > 5 {
        "warning - multiple stale job locks detected"
    } else if stale_job_count > 0 {
        "warning - stale job locks detected"
    } else {
        "healthy"
    };

    info!(
        "Lock status: {} queue locks ({} stale), {} job locks ({} stale)",
        queue_lock_infos.len(),
        stale_queue_count,
        job_lock_infos.len(),
        stale_job_count
    );

    Ok(Json(LocksStatusResponse {
        queue_locks: queue_lock_infos,
        job_locks: job_lock_infos,
        stale_queue_lock_count: stale_queue_count,
        stale_job_lock_count: stale_job_count,
        queue_lock_stale_threshold_minutes: DEFAULT_QUEUE_LOCK_STALE_MINUTES,
        job_lock_stale_threshold_minutes: DEFAULT_JOB_LOCK_STALE_MINUTES,
        health_status: health_status.to_string(),
    }))
}

/// Manually trigger stale lock cleanup - POST /locks/cleanup
///
/// Releases stale queue and job locks. Use this for emergency cleanup
/// when workers are blocked by stale locks from crashed workers.
async fn locks_cleanup<S>(
    State(state): State<S>,
    Json(req): Json<LocksCleanupRequest>,
) -> Result<Json<LocksCleanupResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();

    let queue_lock_timeout = std::time::Duration::from_secs(
        req.queue_lock_threshold_minutes
            .unwrap_or(DEFAULT_QUEUE_LOCK_STALE_MINUTES)
            * 60,
    );
    let job_lock_timeout =
        std::time::Duration::from_secs(req.job_lock_threshold_minutes.unwrap_or(DEFAULT_JOB_LOCK_STALE_MINUTES) * 60);
    let dry_run = req.dry_run.unwrap_or(false);

    if dry_run {
        // For dry run, query what would be cleaned
        let pool = client.pool();
        let schema = client.schema();

        let queue_count_query = format!(
            "SELECT COUNT(*) FROM {}._private_job_queues WHERE locked_at IS NOT NULL AND locked_at < NOW() - INTERVAL '{} seconds'",
            schema,
            queue_lock_timeout.as_secs()
        );
        let queue_count: (i64,) = sqlx::query_as(&queue_count_query).fetch_one(pool).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(format!("Query failed: {}", e), "QUERY_ERROR")),
            )
        })?;

        let job_count_query = format!(
            "SELECT COUNT(*) FROM {}._private_jobs WHERE locked_at IS NOT NULL AND locked_at < NOW() - INTERVAL '{} seconds'",
            schema,
            job_lock_timeout.as_secs()
        );
        let job_count: (i64,) = sqlx::query_as(&job_count_query).fetch_one(pool).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(format!("Query failed: {}", e), "QUERY_ERROR")),
            )
        })?;

        let failed_count_query = format!(
            "SELECT COUNT(*) FROM {}._private_jobs WHERE attempts >= max_attempts AND locked_at IS NULL",
            schema
        );
        let failed_count: (i64,) = sqlx::query_as(&failed_count_query).fetch_one(pool).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(format!("Query failed: {}", e), "QUERY_ERROR")),
            )
        })?;

        info!(
            "Lock cleanup dry run: would release {} queue locks, {} job locks, delete {} failed jobs",
            queue_count.0, job_count.0, failed_count.0
        );

        return Ok(Json(LocksCleanupResponse {
            queue_locks_released: queue_count.0 as u64,
            job_locks_released: job_count.0 as u64,
            failed_jobs_deleted: failed_count.0 as u64,
            dry_run: true,
        }));
    }

    // Actually perform cleanup
    let (queue_released, job_released, failed_deleted) = client
        .startup_cleanup_with_timeouts(queue_lock_timeout, job_lock_timeout)
        .await
        .map_err(|e| {
            error!("Lock cleanup failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(format!("Cleanup failed: {}", e), "CLEANUP_FAILED")),
            )
        })?;

    info!(
        "Lock cleanup completed: {} queue locks, {} job locks, {} failed jobs",
        queue_released, job_released, failed_deleted
    );

    Ok(Json(LocksCleanupResponse {
        queue_locks_released: queue_released,
        job_locks_released: job_released,
        failed_jobs_deleted: failed_deleted,
        dry_run: false,
    }))
}
