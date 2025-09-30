#![cfg(feature = "axum")]
//! Example Axum server with integrated backfill admin API
//!
//! This example demonstrates how to integrate the backfill admin API into
//! an existing Axum application. The admin endpoints are mounted at
//! `/admin/backfill` and provide comprehensive job queue management
//! capabilities.
//!
//! To run this example:
//! ```bash
//! DATABASE_URL=postgresql://localhost/backfill cargo run --example admin_server --features axum
//! ```
//!
//! Then test the API endpoints:
//! ```bash
//! # Health check
//! curl http://localhost:3000/admin/backfill/health
//!
//! # System status
//! curl http://localhost:3000/admin/backfill/status
//!
//! # Enqueue a job
//! curl -X POST http://localhost:3000/admin/backfill/jobs \
//!   -H "Content-Type: application/json" \
//!   -d '{"task_identifier": "send_email", "payload": {"to": "user@example.com"}, "queue": "fast"}'
//!
//! # List DLQ jobs
//! curl http://localhost:3000/admin/backfill/dlq?limit=10
//!
//! # Get DLQ stats
//! curl http://localhost:3000/admin/backfill/dlq/stats
//! ```

use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::{get, post};
use backfill::admin::{BackfillAdminState, create_admin_router};
use backfill::{BackfillClient, IntoTaskHandlerResult, JobSpec, Priority, Queue, TaskHandler, WorkerContext};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info, warn};

/// Application state that implements BackfillAdminState
#[derive(Clone)]
struct AppState {
    backfill: BackfillClient,
}

impl BackfillAdminState for AppState {
    fn backfill_client(&self) -> &BackfillClient {
        &self.backfill
    }
}

/// Example job for demonstration
#[derive(Debug, Serialize, Deserialize)]
struct SendEmailJob {
    to: String,
    subject: Option<String>,
    body: Option<String>,
}

impl TaskHandler for SendEmailJob {
    const IDENTIFIER: &'static str = "send_email";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        info!("Sending email to: {}", self.to);

        // Simulate email sending
        tokio::time::sleep(Duration::from_millis(100)).await;

        if !self.to.contains('@') {
            return Err(backfill::WorkerError::InvalidInput {
                message: format!("Invalid email address: {}", self.to),
            });
        }

        info!("Email sent successfully to: {}", self.to);
        Ok(())
    }
}

/// Example API endpoint that uses backfill
async fn send_notification(
    State(state): State<AppState>,
    Json(email_req): Json<SendEmailJob>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let spec = JobSpec {
        priority: Priority::FAST_HIGH,
        queue: Queue::Fast,
        job_key: Some(format!("email_{}", email_req.to)),
        ..Default::default()
    };

    match state.backfill.enqueue("send_email", &email_req, spec).await {
        Ok(job) => Ok(Json(serde_json::json!({
            "status": "enqueued",
            "job_id": job.id(),
            "message": format!("Email queued for {}", email_req.to)
        }))),
        Err(e) => {
            error!("Failed to enqueue email: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Application info endpoint
async fn app_info() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "app": "Backfill Admin Server Example",
        "version": env!("CARGO_PKG_VERSION"),
        "endpoints": {
            "app_info": "GET /info",
            "send_notification": "POST /api/notify",
            "admin_api": "GET /admin/backfill/*"
        }
    }))
}

/// Create the main application router
fn create_app(state: AppState) -> Router {
    Router::new()
        // Application endpoints
        .route("/info", get(app_info))
        .route("/api/notify", post(send_notification))
        // Mount the backfill admin API at /admin/backfill
        .nest("/admin/backfill", create_admin_router())
        // Add CORS middleware for browser access (optional)
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods([
                    axum::http::Method::GET,
                    axum::http::Method::POST,
                    axum::http::Method::DELETE,
                ])
                .allow_headers(tower_http::cors::Any),
        )
        // Add logging middleware
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state)
}

/// Handle graceful shutdown
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

    info!("Starting backfill admin server example");

    // Get database URL from environment
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost/backfill".to_string());

    // Create backfill client
    let client = BackfillClient::new(&database_url).await?;

    // Initialize DLQ if needed
    if let Err(e) = client.init_dlq().await {
        warn!("Failed to initialize DLQ (may already exist): {}", e);
    }

    // Create application state
    let state = AppState { backfill: client };

    // Create router
    let app = create_app(state);

    // Start server
    let listener = TcpListener::bind("127.0.0.1:3000").await?;
    let addr = listener.local_addr()?;

    info!("Server listening on http://{}", addr);
    info!("API endpoints:");
    info!("  App info:       GET  http://{}/info", addr);
    info!("  Send email:     POST http://{}/api/notify", addr);
    info!("  Admin health:   GET  http://{}/admin/backfill/health", addr);
    info!("  Admin status:   GET  http://{}/admin/backfill/status", addr);
    info!("  Enqueue job:    POST http://{}/admin/backfill/jobs", addr);
    info!("  DLQ list:       GET  http://{}/admin/backfill/dlq", addr);
    info!("  DLQ stats:      GET  http://{}/admin/backfill/dlq/stats", addr);

    let server = axum::serve(listener, app);

    // Run server with graceful shutdown
    tokio::select! {
        result = server => {
            error!("Server failed: {:?}", result);
            result?;
        }
        _ = wait_for_shutdown_signal() => {
            info!("Graceful shutdown initiated");
        }
    }

    info!("Server stopped");
    Ok(())
}
