//! Axum + Worker integration example
//!
//! This example demonstrates integrating the WorkerRunner with an Axum HTTP
//! server using the tokio::select! pattern suitable for Kubernetes deployment.
//!
//! Features demonstrated:
//! - Job enqueueing via HTTP endpoints
//! - Multiple job types with different retry strategies
//! - Long-running external service jobs (up to 15 minutes)
//! - Bulk processing with thousands of jobs
//! - Backfill discovery pattern
//! - Graceful shutdown handling
//!
//! To run this example:
//! ```bash
//! DATABASE_URL=postgresql://localhost/backfill cargo run --example axum_integration
//! ```

use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::{get, post};
use backfill::{
    BackfillClient, BackfillError, IntoTaskHandlerResult, JobSpec, Priority, Queue, QueueConfig, RetryPolicy,
    TaskHandler, WorkerConfig, WorkerContext, WorkerError, WorkerRunner,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Application state shared between Axum and the worker
#[derive(Clone)]
struct AppState {
    client: BackfillClient,
}

/// Long-running external system job (up to 15 minutes)
#[derive(Debug, Serialize, Deserialize)]
pub struct ExternalSystemJob {
    pub system_name: String,
    pub operation: String,
    pub payload: serde_json::Value,
    pub timeout_minutes: u32,
    pub fallback_systems: Vec<String>,
}

impl TaskHandler for ExternalSystemJob {
    const IDENTIFIER: &'static str = "external_system";

    async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        info!(
            "Starting external system job: system='{}', operation='{}', attempt={}",
            self.system_name,
            self.operation,
            ctx.job().attempts()
        );

        // Simulate complex fallback strategies
        let result = match self.call_primary_system().await {
            Ok(response) => Ok(response),
            Err(primary_error) => {
                warn!("Primary system failed: {}, trying fallbacks", primary_error);
                self.call_fallback_systems().await
            }
        };

        match result {
            Ok(_response) => {
                info!("External system job completed successfully");
                Ok(())
            }
            Err(error_msg) => {
                error!("All external systems failed: {}", error_msg);
                Err(WorkerError::ExternalServiceFailure {
                    service: self.system_name,
                    error: error_msg,
                })
            }
        }
    }
}

impl ExternalSystemJob {
    async fn call_primary_system(&self) -> Result<serde_json::Value, String> {
        // Simulate calling primary external service
        let delay = Duration::from_secs(self.timeout_minutes as u64 * 5); // 5 seconds per "minute"
        tokio::time::sleep(delay).await;

        // Simulate 30% failure rate for demonstration
        if fastrand::f32() < 0.3 {
            Err(format!("Primary system {} failed", self.system_name))
        } else {
            Ok(serde_json::json!({"status": "success", "system": self.system_name}))
        }
    }

    async fn call_fallback_systems(&self) -> Result<serde_json::Value, String> {
        for fallback in &self.fallback_systems {
            info!("Trying fallback system: {}", fallback);
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Simulate 70% success rate for fallbacks
            if fastrand::f32() < 0.7 {
                return Ok(serde_json::json!({"status": "success", "system": fallback}));
            }
        }

        Err("All fallback systems failed".to_string())
    }
}

/// Bulk HTTP request job for internal services
#[derive(Debug, Serialize, Deserialize)]
pub struct BulkHttpRequestJob {
    pub endpoint: String,
    pub batch_data: Vec<serde_json::Value>,
    pub is_low_priority: bool,
}

impl TaskHandler for BulkHttpRequestJob {
    const IDENTIFIER: &'static str = "bulk_http_request";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        info!(
            "Processing bulk HTTP request: endpoint='{}', batch_size={}, low_priority={}",
            self.endpoint,
            self.batch_data.len(),
            self.is_low_priority
        );

        // Simulate processing each item in the batch
        for (idx, _item) in self.batch_data.iter().enumerate() {
            if idx % 100 == 0 {
                info!("Processed {} of {} items", idx, self.batch_data.len());
            }

            // Simulate HTTP request
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        info!("Bulk HTTP request completed: {} items processed", self.batch_data.len());
        Ok::<(), WorkerError>(())
    }
}

/// Backfill discovery job that finds work and enqueues more jobs
#[derive(Debug, Serialize, Deserialize)]
pub struct BackfillDiscoveryJob {
    pub table_name: String,
    pub last_processed_id: Option<i64>,
    pub batch_size: usize,
}

impl TaskHandler for BackfillDiscoveryJob {
    const IDENTIFIER: &'static str = "backfill_discovery";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        info!("Running backfill discovery for table: {}", self.table_name);

        // Simulate querying database for items needing backfill
        let items = self.find_items_needing_backfill().await?;

        info!("Found {} items needing backfill", items.len());

        // Enqueue individual backfill jobs at low priority
        for item in &items {
            let _bulk_job = BulkHttpRequestJob {
                endpoint: format!("/api/backfill/{}", self.table_name),
                batch_data: vec![item.clone()],
                is_low_priority: true,
            };

            // Use the client from the context
            // Note: In practice, you'd get the client from your app state
            // This is a simplified example
            info!("Would enqueue backfill job for item: {:?}", item);
        }

        // Schedule next discovery run if we found a full batch
        if items.len() == self.batch_size {
            info!("Scheduling next backfill discovery run");
            // In practice, you'd enqueue another BackfillDiscoveryJob here
        }

        Ok::<(), WorkerError>(())
    }
}

impl BackfillDiscoveryJob {
    async fn find_items_needing_backfill(&self) -> Result<Vec<serde_json::Value>, WorkerError> {
        // Simulate database query
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Generate fake items that need backfill
        let mut items = Vec::new();
        let start_id = self.last_processed_id.unwrap_or(1000);

        for i in 0..self.batch_size.min(50) {
            // Limit to 50 for demo
            items.push(serde_json::json!({
                "id": start_id + i as i64,
                "data": format!("item_{}", i),
                "needs_backfill": true
            }));
        }

        Ok(items)
    }
}

/// HTTP API handlers
async fn health_check() -> &'static str {
    "OK"
}

#[derive(Deserialize)]
struct EnqueueExternalJobRequest {
    system_name: String,
    operation: String,
    payload: serde_json::Value,
    timeout_minutes: Option<u32>,
}

async fn enqueue_external_job(
    State(state): State<AppState>,
    Json(req): Json<EnqueueExternalJobRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let job = ExternalSystemJob {
        system_name: req.system_name,
        operation: req.operation,
        payload: req.payload,
        timeout_minutes: req.timeout_minutes.unwrap_or(5),
        fallback_systems: vec!["backup_system_1".to_string(), "backup_system_2".to_string()],
    };

    // Use aggressive retry policy for external systems
    let spec = JobSpec {
        priority: Priority::FAST_HIGH,
        queue: Queue::Fast,
        max_attempts: Some(12), // Aggressive retries
        retry_policy: Some(RetryPolicy::aggressive()),
        ..Default::default()
    };

    match state.client.enqueue("external_system", &job, spec).await {
        Ok(job) => Ok(Json(serde_json::json!({
            "job_id": job.id(),
            "status": "enqueued"
        }))),
        Err(e) => {
            error!("Failed to enqueue external job: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Deserialize)]
struct EnqueueBulkJobRequest {
    endpoint: String,
    batch_data: Vec<serde_json::Value>,
    is_low_priority: Option<bool>,
}

async fn enqueue_bulk_job(
    State(state): State<AppState>,
    Json(req): Json<EnqueueBulkJobRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let job = BulkHttpRequestJob {
        endpoint: req.endpoint,
        batch_data: req.batch_data,
        is_low_priority: req.is_low_priority.unwrap_or(false),
    };

    // Use bulk queue with conservative retries
    let spec = JobSpec {
        priority: if job.is_low_priority {
            Priority::BULK_LOW
        } else {
            Priority::BULK_DEFAULT
        },
        queue: Queue::Bulk,
        retry_policy: Some(RetryPolicy::conservative()),
        ..Default::default()
    };

    match state.client.enqueue("bulk_http_request", &job, spec).await {
        Ok(job) => Ok(Json(serde_json::json!({
            "job_id": job.id(),
            "status": "enqueued",
            "batch_size": job.payload().get("batch_data").and_then(|v| v.as_array().map(|a| a.len()))
        }))),
        Err(e) => {
            error!("Failed to enqueue bulk job: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn start_backfill_discovery(State(state): State<AppState>) -> Result<Json<serde_json::Value>, StatusCode> {
    let job = BackfillDiscoveryJob {
        table_name: "user_data".to_string(),
        last_processed_id: None,
        batch_size: 100,
    };

    let spec = JobSpec {
        priority: Priority::BULK_LOW,
        queue: Queue::Bulk,
        ..Default::default()
    };

    match state.client.enqueue("backfill_discovery", &job, spec).await {
        Ok(job) => Ok(Json(serde_json::json!({
            "job_id": job.id(),
            "status": "backfill_discovery_started"
        }))),
        Err(e) => {
            error!("Failed to start backfill discovery: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Create the Axum router
fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/api/jobs/external", post(enqueue_external_job))
        .route("/api/jobs/bulk", post(enqueue_bulk_job))
        .route("/api/backfill/start", post(start_backfill_discovery))
        .with_state(state)
}

/// Run the Axum server
async fn run_axum_server(state: AppState, cancellation_token: CancellationToken) -> Result<(), BackfillError> {
    let app = create_router(state);
    let listener = TcpListener::bind("127.0.0.1:3000")
        .await
        .map_err(|e| BackfillError::BindError(e.to_string()))?;

    info!("Axum server listening on http://127.0.0.1:3000");

    let server = axum::serve(listener, app);

    tokio::select! {
        result = server => {
            result.map_err(|e| BackfillError::RuntimeError(format!("Axum server error: {}", e)))
        }
        _ = cancellation_token.cancelled() => {
            info!("Axum server shutting down");
            Ok(())
        }
    }
}

/// Handle graceful shutdown signals
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received SIGINT"),
        _ = terminate => info!("Received SIGTERM"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup tracing
    tracing_subscriber::fmt::init();

    info!("Starting Axum + Worker integration example");

    // Configuration
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost/backfill".to_string());

    // Create shared client
    let client = BackfillClient::new(&database_url).await?;

    // Create app state
    let app_state = AppState { client: client.clone() };

    // Create worker configuration with different queue strategies
    let worker_config = WorkerConfig {
        database_url: database_url.clone(),
        schema: "graphile_worker".to_string(),
        queue_configs: vec![
            // Fast queue for external systems - fewer workers due to long duration
            QueueConfig::named_queue("fast", 3),
            // Bulk queue for high-throughput internal work
            QueueConfig::named_queue("bulk", 50),
            // Default queue for mixed workloads
            QueueConfig::default_queue(10),
        ],
        poll_interval: Duration::from_millis(100), // Fast polling for responsiveness
        dlq_processor_interval: Some(Duration::from_secs(30)), // Quick DLQ processing
    };

    // Create and configure worker
    let worker = WorkerRunner::builder(worker_config)
        .await?
        .define_job::<ExternalSystemJob>()
        .define_job::<BulkHttpRequestJob>()
        .define_job::<BackfillDiscoveryJob>()
        .build()
        .await?;

    info!(
        "Worker configured: {} queues, DLQ enabled: {}",
        worker.worker_count(),
        worker.dlq_processor_enabled()
    );

    // Create shutdown coordination
    let shutdown_token = CancellationToken::new();

    // Use tokio::select! for Kubernetes-style fail-fast deployment
    tokio::select! {
        // Run Axum server
        result = run_axum_server(app_state, shutdown_token.clone()) => {
            error!("Axum server failed: {:?}", result);
            result?; // Propagate error to cause pod restart
        }

        // Run worker
        result = worker.run_until_cancelled(shutdown_token.clone()) => {
            error!("Worker failed: {:?}", result);
            result?; // Propagate error to cause pod restart
        }

        // Wait for shutdown signal
        _ = wait_for_shutdown_signal() => {
            info!("Shutdown signal received, stopping all services gracefully");
            shutdown_token.cancel();

            // Give services time to stop gracefully (K8s gives us 30s by default)
            tokio::time::sleep(Duration::from_secs(25)).await;
            info!("Graceful shutdown completed");
        }
    }

    Ok(())
}
