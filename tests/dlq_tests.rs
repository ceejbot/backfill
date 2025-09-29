//! Dead Letter Queue integration tests

use backfill::{BackfillClient, BackfillError, DlqFilter};

async fn setup_test_client(test_name: &str) -> BackfillClient {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost/backfill_test".to_string());
    
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Use a unique schema for each test to avoid conflicts
    let schema = format!("test_{}", test_name);
    BackfillClient::with_pool_and_schema(pool, schema)
        .await
        .expect("Failed to create test client")
}

#[tokio::test]
async fn test_dlq_initialization() {
    let client = setup_test_client("dlq_init").await;
    
    // Initialize DLQ should succeed
    let result = client.init_dlq().await;
    assert!(result.is_ok(), "DLQ initialization failed: {:?}", result);
    
    // Should be able to call multiple times without error
    let result2 = client.init_dlq().await;
    assert!(result2.is_ok(), "Second DLQ initialization failed: {:?}", result2);
}

#[tokio::test]
async fn test_dlq_stats_empty() {
    let client = setup_test_client("dlq_stats").await;
    
    // Initialize DLQ
    client.init_dlq().await.unwrap();
    
    // Get stats from empty DLQ
    let stats = client.dlq_stats().await.unwrap();
    
    assert_eq!(stats.total_jobs, 0);
    assert_eq!(stats.unique_tasks, 0);
    assert_eq!(stats.unique_queues, 0);
    assert_eq!(stats.avg_failure_count, 0.0);
    assert_eq!(stats.total_requeued, 0);
    assert!(stats.oldest_failure.is_none());
    assert!(stats.newest_failure.is_none());
    assert!(stats.task_breakdown.is_empty());
}

#[tokio::test]
async fn test_dlq_list_empty() {
    let client = setup_test_client("dlq_list").await;
    
    // Initialize DLQ
    client.init_dlq().await.unwrap();
    
    // List jobs from empty DLQ
    let filter = DlqFilter::default();
    let list = client.list_dlq_jobs(filter).await.unwrap();
    
    assert_eq!(list.jobs.len(), 0);
    assert_eq!(list.total, 0);
    assert_eq!(list.offset, 0);
    assert_eq!(list.limit, 50); // default limit
}

#[tokio::test]
async fn test_dlq_get_nonexistent_job() {
    let client = setup_test_client("dlq_get").await;
    
    // Initialize DLQ
    client.init_dlq().await.unwrap();
    
    // Try to get a job that doesn't exist
    let job = client.get_dlq_job(99999).await.unwrap();
    assert!(job.is_none());
}

#[tokio::test]
async fn test_dlq_delete_nonexistent_job() {
    let client = setup_test_client("dlq_delete").await;
    
    // Initialize DLQ
    client.init_dlq().await.unwrap();
    
    // Try to delete a job that doesn't exist
    let deleted = client.delete_dlq_job(99999).await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn test_dlq_requeue_nonexistent_job() {
    let client = setup_test_client("dlq_requeue").await;
    
    // Initialize DLQ
    client.init_dlq().await.unwrap();
    
    // Try to requeue a job that doesn't exist
    let result = client.requeue_dlq_job(99999, Some("test note".to_string())).await;
    
    match result {
        Err(BackfillError::DlqJobNotFound(id)) => {
            assert_eq!(id, 99999);
        }
        other => panic!("Expected DlqJobNotFound error, got: {:?}", other),
    }
}
