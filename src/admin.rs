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
//!     let app = Router::new()
//!         .nest("/admin/backfill", create_admin_router())
//!         .with_state(state);
//!
//!     // ... serve your app
//!     Ok(())
//! }
//! ```

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::{client::DlqFilter, BackfillClient};

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
        .route("/health", get(health_check::<S>))
        .route("/status", get(system_status::<S>))
        
        // Job management endpoints
        .route("/jobs", post(enqueue_job::<S>))
        .route("/jobs/:job_id", get(get_job::<S>))
        .route("/jobs/:job_id/cancel", delete(cancel_job::<S>))
        
        // Queue management endpoints  
        .route("/queues", get(list_queues::<S>))
        .route("/queues/:queue_name/stats", get(queue_stats::<S>))
        
        // Dead Letter Queue management
        .route("/dlq", get(list_dlq_jobs::<S>))
        .route("/dlq/stats", get(dlq_stats::<S>))
        .route("/dlq/:dlq_id", get(get_dlq_job::<S>))
        .route("/dlq/:dlq_id", delete(delete_dlq_job::<S>))
        .route("/dlq/:dlq_id/requeue", post(requeue_dlq_job::<S>))
        .route("/dlq/cleanup", post(cleanup_dlq::<S>))
}

/// Health check endpoint - GET /health
async fn health_check<S>() -> Result<Json<HealthResponse>, StatusCode>
where
    S: BackfillAdminState,
{
    let response = HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: chrono::Utc::now(),
    };
    
    Ok(Json(response))
}

/// System status endpoint - GET /status  
async fn system_status<S>(
    State(state): State<S>,
) -> Result<Json<SystemStatus>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let _client = state.backfill_client();
    
    // For now, return a basic status. In a real implementation, you'd query
    // the database for actual queue statistics
    let status = SystemStatus {
        queues: vec![
            QueueStatus {
                queue_name: "fast".to_string(),
                pending_jobs: 0,
                active_jobs: 0, 
                completed_jobs: 0,
                failed_jobs: 0,
            },
            QueueStatus {
                queue_name: "bulk".to_string(),
                pending_jobs: 0,
                active_jobs: 0,
                completed_jobs: 0, 
                failed_jobs: 0,
            },
        ],
        dlq_enabled: true,
        dlq_job_count: 0,
        total_jobs: 0,
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
        spec.queue = match queue.as_str() {
            "fast" => crate::Queue::Fast,
            "bulk" => crate::Queue::Bulk,
            "dead_letter" => crate::Queue::DeadLetter,
            custom => crate::Queue::Custom(custom.to_string()),
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
        Ok(job) => {
            let response = EnqueueJobResponse {
                job_id: job.id().to_string(),
                status: "enqueued".to_string(),
                enqueued_at: chrono::Utc::now(),
            };
            
            info!("Successfully enqueued job: id={}", job.id());
            Ok(Json(response))
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
        Json(ErrorResponse::new("Job retrieval not yet implemented", "NOT_IMPLEMENTED")),
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
        Json(ErrorResponse::new("Job cancellation not yet implemented", "NOT_IMPLEMENTED")),
    ))
}

/// List queues endpoint - GET /queues
async fn list_queues<S>(
    State(_state): State<S>,
) -> Result<Json<Vec<QueueStatus>>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    // This would query the actual queue statistics from the database
    // For now, return static data
    let queues = vec![
        QueueStatus {
            queue_name: "fast".to_string(),
            pending_jobs: 0,
            active_jobs: 0,
            completed_jobs: 0,
            failed_jobs: 0,
        },
        QueueStatus {
            queue_name: "bulk".to_string(),
            pending_jobs: 0,
            active_jobs: 0,
            completed_jobs: 0,
            failed_jobs: 0,
        },
    ];
    
    info!("Listed {} queues", queues.len());
    Ok(Json(queues))
}

/// Queue stats endpoint - GET /queues/:queue_name/stats
async fn queue_stats<S>(
    State(_state): State<S>,
    Path(queue_name): Path<String>,
) -> Result<Json<QueueStatus>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    // This would query the actual queue statistics from the database
    // For now, return static data
    let stats = QueueStatus {
        queue_name: queue_name.clone(),
        pending_jobs: 0,
        active_jobs: 0,
        completed_jobs: 0,
        failed_jobs: 0,
    };
    
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
    
    info!("Listing DLQ jobs with filter: limit={:?}, offset={:?}", filter.limit, filter.offset);
    
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
async fn dlq_stats<S>(
    State(state): State<S>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let client = state.backfill_client();
    
    match client.dlq_stats().await {
        Ok(stats) => {
            info!("Retrieved DLQ stats: total_jobs={}", stats.total_jobs);
            Ok(Json(serde_json::to_value(stats).unwrap()))
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
            Ok(Json(serde_json::to_value(job).unwrap()))
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
            info!("Successfully requeued DLQ job: dlq_id={}, new_job_id={}", dlq_id, job.id());
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
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)>
where
    S: BackfillAdminState,
{
    let _client = state.backfill_client();
    
    // This would implement batch cleanup of old DLQ jobs
    // For now, return a placeholder
    warn!("DLQ cleanup endpoint not yet implemented");
    Err((
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse::new("DLQ cleanup not yet implemented", "NOT_IMPLEMENTED")),
    ))
}
