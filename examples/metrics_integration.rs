//! Example showing Prometheus metrics integration with backfill
//!
//! This example demonstrates:
//! - Installing a Prometheus metrics recorder
//! - Exposing metrics via HTTP endpoint
//! - Using backfill with automatic metrics collection
//! - Querying metrics for monitoring
//!
//! To run this example:
//! ```bash
//! cargo run --example metrics_integration --features axum
//! ```
//!
//! Then in another terminal:
//! ```bash
//! # View metrics
//! curl http://localhost:9000/metrics
//!
//! # Enqueue some jobs
//! curl -X POST http://localhost:3000/admin/backfill/jobs \
//!   -H "Content-Type: application/json" \
//!   -d '{"task_identifier": "example_job", "payload": {"message": "test"}}'
//! ```

use std::time::Duration;

use axum::Router;
use axum::routing::get;
use backfill::admin::{BackfillAdminState, create_admin_router};
use backfill::{
    BackfillClient, IntoTaskHandlerResult, JobSpec, Priority, Queue, TaskHandler, WorkerContext, WorkerError,
};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Application state
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
struct ExampleJob {
    message: String,
    delay_ms: Option<u64>,
    should_fail: Option<bool>,
}

impl TaskHandler for ExampleJob {
    const IDENTIFIER: &'static str = "example_job";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        info!("Processing job: {}", self.message);

        if let Some(delay) = self.delay_ms {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }

        if self.should_fail.unwrap_or(false) {
            return Err(WorkerError::JobFailed {
                message: "Job configured to fail".to_string(),
            });
        }

        info!("Job completed: {}", self.message);
        Ok::<(), WorkerError>(())
    }
}

/// Metrics endpoint handler
async fn metrics_handler(prometheus_handle: PrometheusHandle) -> String {
    prometheus_handle.render()
}

/// Create the metrics server
async fn metrics_server(prometheus_handle: PrometheusHandle) {
    let app = Router::new().route("/metrics", get(move || metrics_handler(prometheus_handle.clone())));

    let listener = TcpListener::bind("0.0.0.0:9000")
        .await
        .expect("Failed to bind metrics server");

    info!("📊 Metrics server listening on http://0.0.0.0:9000/metrics");

    axum::serve(listener, app).await.expect("Metrics server failed");
}

/// Create the admin API server
async fn admin_server(state: AppState) {
    let app = Router::new()
        .nest("/admin/backfill", create_admin_router())
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
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("Failed to bind admin server");

    info!("🚀 Admin API listening on http://0.0.0.0:3000/admin/backfill");
    info!("   Health:   GET  http://0.0.0.0:3000/admin/backfill/health");
    info!("   Status:   GET  http://0.0.0.0:3000/admin/backfill/status");
    info!("   Jobs:     POST http://0.0.0.0:3000/admin/backfill/jobs");
    info!("   DLQ:      GET  http://0.0.0.0:3000/admin/backfill/dlq");
    info!("   DLQ Stats: GET  http://0.0.0.0:3000/admin/backfill/dlq/stats");

    axum::serve(listener, app).await.expect("Admin server failed");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup tracing
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    info!("🎯 Backfill Metrics Integration Example");
    info!("");

    // Configure Prometheus exporter with custom histogram buckets for job metrics
    let builder = PrometheusBuilder::new()
        // Job duration buckets (0.01s to 10 minutes)
        .set_buckets_for_metric(
            Matcher::Prefix("backfill.jobs.duration".to_string()),
            &[0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0],
        )?
        // Queue wait time buckets (0.1s to 1 hour)
        .set_buckets_for_metric(
            Matcher::Prefix("backfill.jobs.wait_time".to_string()),
            &[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0],
        )?
        // Database operation buckets (1ms to 5s)
        .set_buckets_for_metric(
            Matcher::Prefix("backfill.db.operation_duration".to_string()),
            &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
        )?
        // DLQ age buckets (5 minutes to 7 days)
        .set_buckets_for_metric(
            Matcher::Prefix("backfill.dlq.age".to_string()),
            &[300.0, 1800.0, 3600.0, 21600.0, 86400.0, 604800.0],
        )?;

    let prometheus_handle = builder.install_recorder()?;

    info!("✅ Prometheus metrics recorder installed");

    // Get database URL
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost/backfill".to_string());

    // Create backfill client
    let client = BackfillClient::new(&database_url).await?;

    // Initialize DLQ
    if let Err(e) = client.init_dlq().await {
        warn!("Failed to initialize DLQ (may already exist): {}", e);
    }

    info!("✅ Backfill client initialized");

    // Enqueue some example jobs to generate metrics
    info!("");
    info!("📝 Enqueueing example jobs...");

    // High priority job
    client
        .enqueue(
            "example_job",
            &ExampleJob {
                message: "High priority fast job".to_string(),
                delay_ms: Some(100),
                should_fail: None,
            },
            JobSpec {
                priority: Priority::FAST_HIGH,
                queue: Queue::Fast,
                ..Default::default()
            },
        )
        .await?;

    // Bulk job
    client
        .enqueue(
            "example_job",
            &ExampleJob {
                message: "Bulk processing job".to_string(),
                delay_ms: Some(500),
                should_fail: None,
            },
            JobSpec {
                priority: Priority::BULK_DEFAULT,
                queue: Queue::Bulk,
                ..Default::default()
            },
        )
        .await?;

    // Emergency job
    client
        .enqueue(
            "example_job",
            &ExampleJob {
                message: "Emergency job".to_string(),
                delay_ms: Some(50),
                should_fail: None,
            },
            JobSpec {
                priority: Priority::EMERGENCY,
                queue: Queue::Fast,
                ..Default::default()
            },
        )
        .await?;

    info!("✅ Example jobs enqueued");

    // Get DLQ stats to populate gauges
    let stats = client.dlq_stats().await?;
    info!("📊 DLQ stats: {} jobs", stats.total_jobs);

    let state = AppState { backfill: client };

    // Spawn metrics server
    tokio::spawn(metrics_server(prometheus_handle));

    // Give servers a moment to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("");
    info!("🎉 System ready!");
    info!("");
    info!("Try these commands:");
    info!("  # View all metrics");
    info!("  curl http://localhost:9000/metrics");
    info!("");
    info!("  # View job metrics only");
    info!("  curl http://localhost:9000/metrics | grep backfill_jobs");
    info!("");
    info!("  # View DLQ metrics");
    info!("  curl http://localhost:9000/metrics | grep backfill_dlq");
    info!("");
    info!("  # Enqueue more jobs via admin API");
    info!("  curl -X POST http://localhost:3000/admin/backfill/jobs \\");
    info!("    -H 'Content-Type: application/json' \\");
    info!(
        "    -d '{{\"task_identifier\": \"example_job\", \"payload\": {{\"message\": \"test\"}}, \"queue\": \"fast\"}}'"
    );
    info!("");
    info!("Press Ctrl+C to stop");

    // Run admin server (blocks)
    admin_server(state).await;

    Ok(())
}
