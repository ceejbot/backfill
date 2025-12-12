//! Admin API integration tests
//!
//! These tests verify the HTTP endpoints provided by the Admin API.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use backfill::BackfillClient;
use backfill::admin::{BackfillAdminState, create_admin_router};
use serde::Deserialize;
use serde_json::{Value, json};
use tower::util::ServiceExt; // for oneshot
use uuid::Uuid;

/// Test application state that implements BackfillAdminState
#[derive(Clone)]
struct TestAppState {
    client: BackfillClient,
}

impl BackfillAdminState for TestAppState {
    fn backfill_client(&self) -> &BackfillClient {
        &self.client
    }
}

async fn setup_test_app(test_name: &str) -> Router {
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost/backfill_test".to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");

    // Use a unique schema for each test to avoid conflicts
    let unique_id = Uuid::new_v4().simple();
    let schema = format!("test_{}_{}", test_name, unique_id);
    let client = BackfillClient::with_pool_and_schema(pool, schema)
        .await
        .expect("Failed to create test client");

    // Initialize DLQ for tests that need it
    client.init_dlq().await.expect("Failed to init DLQ");

    let state = TestAppState { client };
    create_admin_router().with_state(state)
}

/// Helper to make JSON request bodies
fn json_body(value: Value) -> Body {
    Body::from(serde_json::to_vec(&value).expect("Failed to serialize JSON"))
}

/// Helper to parse JSON response
async fn parse_json_response<T: for<'de> Deserialize<'de>>(body: Body) -> T {
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("Failed to read body");
    serde_json::from_slice(&bytes).expect("Failed to parse JSON response")
}

#[tokio::test]
async fn test_health_check() {
    let app = setup_test_app("health").await;

    let request = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert_eq!(body["status"], "healthy");
    assert!(body["version"].is_string());
    assert!(body["timestamp"].is_string());
}

#[tokio::test]
async fn test_enqueue_job_via_api() {
    let app = setup_test_app("enqueue_api").await;

    let payload = json!({
        "task_identifier": "test_task",
        "payload": {"message": "hello", "number": 42},
        "priority": 50,
        "queue_name": "fast"
    });

    let request = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("content-type", "application/json")
        .body(json_body(payload))
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert!(body["job_id"].is_string());
}

#[tokio::test]
async fn test_get_job_returns_not_implemented() {
    let app = setup_test_app("get_job").await;

    let request = Request::builder()
        .uri("/jobs/123")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn test_cancel_job_returns_not_implemented() {
    let app = setup_test_app("cancel_job").await;

    let request = Request::builder()
        .method("DELETE")
        .uri("/jobs/123/cancel")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn test_list_queues() {
    let app = setup_test_app("list_queues").await;

    let request = Request::builder()
        .uri("/queues")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert!(body.is_array()); // Response is an array directly
}

#[tokio::test]
async fn test_queue_stats() {
    let app = setup_test_app("queue_stats").await;

    let request = Request::builder()
        .uri("/queues/fast/stats")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert_eq!(body["queue_name"], "fast");
    assert!(body["pending_jobs"].is_number());
    assert!(body["active_jobs"].is_number());
}

#[tokio::test]
async fn test_dlq_stats() {
    let app = setup_test_app("dlq_stats").await;

    let request = Request::builder()
        .uri("/dlq/stats")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert!(body["total_jobs"].is_number());
    assert!(body["unique_tasks"].is_number());
}

#[tokio::test]
async fn test_list_dlq_jobs() {
    let app = setup_test_app("list_dlq").await;

    let request = Request::builder()
        .uri("/dlq")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert!(body["jobs"].is_array());
    assert!(body["total_count"].is_number());
    assert_eq!(body["total_count"], 0); // Empty DLQ
}

#[tokio::test]
async fn test_list_dlq_jobs_with_pagination() {
    let app = setup_test_app("list_dlq_pagination").await;

    let request = Request::builder()
        .uri("/dlq?limit=10&offset=0")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert!(body["jobs"].is_array());
    assert!(body["total_count"].is_number());
    // Note: response doesn't include limit/offset echo
}

#[tokio::test]
async fn test_get_dlq_job_not_found() {
    let app = setup_test_app("get_dlq_404").await;

    let request = Request::builder()
        .uri("/dlq/999999")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_dlq_job_not_found() {
    let app = setup_test_app("delete_dlq_404").await;

    let request = Request::builder()
        .method("DELETE")
        .uri("/dlq/999999")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_requeue_dlq_job_not_found() {
    let app = setup_test_app("requeue_dlq_404").await;

    let request = Request::builder()
        .method("POST")
        .uri("/dlq/999999/requeue")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_batch_requeue_dlq_jobs() {
    let app = setup_test_app("batch_requeue").await;

    let payload = json!({
        "dlq_ids": [1, 2, 3]
    });

    let request = Request::builder()
        .method("POST")
        .uri("/dlq/batch-requeue")
        .header("content-type", "application/json")
        .body(json_body(payload))
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert!(body["requeued_count"].is_number());
    assert_eq!(body["requeued_count"], 0); // No jobs to requeue
}

#[tokio::test]
async fn test_batch_delete_dlq_jobs() {
    let app = setup_test_app("batch_delete").await;

    let payload = json!({
        "dlq_ids": [1, 2, 3]
    });

    let request = Request::builder()
        .method("POST")
        .uri("/dlq/batch-delete")
        .header("content-type", "application/json")
        .body(json_body(payload))
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert!(body["deleted_count"].is_number());
    assert_eq!(body["deleted_count"], 0); // No jobs to delete
}

#[tokio::test]
async fn test_system_status() {
    let app = setup_test_app("system_status").await;

    let request = Request::builder()
        .uri("/status")
        .body(Body::empty())
        .expect("Failed to build request");

    let response = app.oneshot(request).await.expect("Failed to call endpoint");

    assert_eq!(response.status(), StatusCode::OK);

    let body = parse_json_response::<Value>(response.into_body()).await;
    assert!(body["queues"].is_array());
    assert!(body["dlq_enabled"].is_boolean());
    assert!(body["dlq_job_count"].is_number());
    assert!(body["total_jobs"].is_number());
}

// Note: We skip the integration test of enqueueing and then checking queue
// stats because it would require complex router state management. The
// individual endpoints are tested above.
