//! Worker runner integration tests

use std::time::Duration;

use backfill::{
    BackfillError, IntoTaskHandlerResult, QueueConfig, TaskHandler, WorkerConfig, WorkerContext, WorkerError,
    WorkerRunner,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

/// Simple test job for worker tests
#[derive(Debug, Serialize, Deserialize)]
struct SimpleTestJob {
    message: String,
}

impl TaskHandler for SimpleTestJob {
    const IDENTIFIER: &'static str = "simple_test_job";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        Ok::<(), WorkerError>(())
    }
}

/// Get test database URL
fn get_test_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost/backfill_test".to_string())
}

#[tokio::test]
async fn test_worker_config_default() {
    let config = WorkerConfig::default();

    assert_eq!(config.database_url, "postgresql://localhost:5432/backfill");
    assert_eq!(config.schema, "graphile_worker");
    assert_eq!(config.queue_configs.len(), 1);
    assert_eq!(config.poll_interval, Duration::from_millis(200));
    assert_eq!(config.dlq_processor_interval, Some(Duration::from_secs(60)));
}

#[tokio::test]
async fn test_worker_config_builder() {
    let config = WorkerConfig::new("postgresql://localhost/test")
        .with_schema("custom_schema")
        .with_poll_interval(Duration::from_millis(100))
        .with_dlq_processor_interval(Some(Duration::from_secs(30)))
        .with_queues(vec![
            QueueConfig::default_queue(5),
            QueueConfig::named_queue("fast", 10),
        ]);

    assert_eq!(config.database_url, "postgresql://localhost/test");
    assert_eq!(config.schema, "custom_schema");
    assert_eq!(config.poll_interval, Duration::from_millis(100));
    assert_eq!(config.dlq_processor_interval, Some(Duration::from_secs(30)));
    assert_eq!(config.queue_configs.len(), 2);
}

#[tokio::test]
async fn test_worker_config_disable_dlq_processor() {
    let config = WorkerConfig::new("postgresql://localhost/test").with_dlq_processor_interval(None);

    assert_eq!(config.dlq_processor_interval, None);
}

#[tokio::test]
async fn test_queue_config_default_queue() {
    let config = QueueConfig::default_queue(10);

    assert_eq!(config.name, None);
    assert_eq!(config.concurrency, 10);
    assert_eq!(config.priority_range, None);
}

#[tokio::test]
async fn test_queue_config_named_queue() {
    let config = QueueConfig::named_queue("bulk", 20);

    assert_eq!(config.name, Some("bulk".to_string()));
    assert_eq!(config.concurrency, 20);
    assert_eq!(config.priority_range, None);
}

#[tokio::test]
async fn test_queue_config_priority_queue() {
    let config = QueueConfig::priority_queue("urgent", 5, -100, 100);

    assert_eq!(config.name, Some("urgent".to_string()));
    assert_eq!(config.concurrency, 5);
    assert_eq!(config.priority_range, Some((-100, 100)));
}

#[tokio::test]
async fn test_worker_runner_builder_creation() -> Result<(), BackfillError> {
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema("test_worker_builder")
        .with_dlq_processor_interval(None); // Disable DLQ for this test

    let builder = WorkerRunner::builder(config).await?;
    let worker = builder.define_job::<SimpleTestJob>().build().await?;

    assert_eq!(worker.worker_count(), 1);
    assert!(!worker.dlq_processor_enabled());

    Ok(())
}

#[tokio::test]
async fn test_worker_runner_multiple_job_types() -> Result<(), BackfillError> {
    #[derive(Debug, Serialize, Deserialize)]
    struct TestJobA {
        value: i32,
    }

    impl TaskHandler for TestJobA {
        const IDENTIFIER: &'static str = "test_job_a";

        async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
            Ok::<(), WorkerError>(())
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestJobB {
        name: String,
    }

    impl TaskHandler for TestJobB {
        const IDENTIFIER: &'static str = "test_job_b";

        async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
            Ok::<(), WorkerError>(())
        }
    }

    let config = WorkerConfig::new(get_test_database_url())
        .with_schema("test_worker_multi")
        .with_dlq_processor_interval(None);

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<TestJobA>()
        .define_job::<TestJobB>()
        .build()
        .await?;

    assert_eq!(worker.worker_count(), 1);

    Ok(())
}

#[tokio::test]
async fn test_worker_runner_with_multiple_queues() -> Result<(), BackfillError> {
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema("test_worker_queues")
        .with_queues(vec![
            QueueConfig::default_queue(5),
            QueueConfig::named_queue("fast", 10),
            QueueConfig::named_queue("bulk", 3),
        ])
        .with_dlq_processor_interval(None);

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    assert_eq!(worker.worker_count(), 3);

    Ok(())
}

#[tokio::test]
async fn test_worker_runner_with_dlq_enabled() -> Result<(), BackfillError> {
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema("test_worker_dlq")
        .with_dlq_processor_interval(Some(Duration::from_secs(30)));

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    assert!(worker.dlq_processor_enabled());

    Ok(())
}

#[tokio::test]
async fn test_worker_runner_client_access() -> Result<(), BackfillError> {
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema("test_worker_client")
        .with_dlq_processor_interval(None);

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    // Access the client
    let client = worker.client();

    // Verify we can use the client to enqueue a job
    let test_job = SimpleTestJob {
        message: "test".to_string(),
    };

    let outcome = client.enqueue("simple_test_job", &test_job, Default::default()).await?;
    let job = outcome.expect("enqueuing a job should succeed");

    assert!(*job.id() > 0);

    Ok(())
}

#[tokio::test]
async fn test_worker_runner_spawn_and_cancel() -> Result<(), BackfillError> {
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema("test_worker_spawn")
        .with_dlq_processor_interval(None)
        .with_poll_interval(Duration::from_millis(50)); // Fast polling for quick test

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    let cancellation_token = CancellationToken::new();
    let handle = worker.spawn_background(cancellation_token.clone());

    // Let it run briefly
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel it
    cancellation_token.cancel();

    // Wait for it to stop
    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;

    assert!(result.is_ok(), "Worker should stop within timeout");
    assert!(
        result.expect("timeout").is_ok(),
        "Worker should complete without errors"
    );

    Ok(())
}

#[tokio::test]
async fn test_worker_runner_run_until_cancelled() -> Result<(), BackfillError> {
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema("test_worker_run_until")
        .with_dlq_processor_interval(None)
        .with_poll_interval(Duration::from_millis(50));

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    let cancellation_token = CancellationToken::new();
    let token_clone = cancellation_token.clone();

    // Spawn a task to cancel after a short delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        token_clone.cancel();
    });

    // Run the worker
    let result = tokio::time::timeout(Duration::from_secs(5), worker.run_until_cancelled(cancellation_token)).await;

    assert!(result.is_ok(), "Worker should stop within timeout");
    assert!(
        result.expect("timeout").is_ok(),
        "Worker should complete without errors"
    );

    Ok(())
}

#[tokio::test]
async fn test_worker_runner_process_available_jobs() -> Result<(), BackfillError> {
    use backfill::enqueue_fast;
    use uuid::Uuid;

    // Use unique schema for test isolation
    let schema = format!("test_process_{}", Uuid::new_v4().simple());
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema(&schema)
        .with_dlq_processor_interval(None);

    // Create worker and client
    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    let client = worker.client();

    // Enqueue some test jobs
    let job1 = SimpleTestJob {
        message: "Test 1".to_string(),
    };
    let job2 = SimpleTestJob {
        message: "Test 2".to_string(),
    };
    let job3 = SimpleTestJob {
        message: "Test 3".to_string(),
    };

    enqueue_fast(client, SimpleTestJob::IDENTIFIER, &job1, None).await?;
    enqueue_fast(client, SimpleTestJob::IDENTIFIER, &job2, None).await?;
    enqueue_fast(client, SimpleTestJob::IDENTIFIER, &job3, None).await?;

    // Process all available jobs
    let processed = worker.process_available_jobs().await?;

    // Note: Returns 0 because job counting isn't implemented yet
    // But the jobs should still be processed successfully
    assert_eq!(processed, 0);

    // Verify jobs were actually processed by checking the database
    // Jobs should be completed and removed from the queue
    let remaining: (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM {}.\"_private_jobs\" WHERE is_available = true",
        schema
    ))
    .fetch_one(client.pool())
    .await?;

    assert_eq!(remaining.0, 0, "All jobs should have been processed");

    // Clean up schema
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema))
        .execute(client.pool())
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_worker_runner_invalid_database_url() {
    let config = WorkerConfig::new("postgresql://invalid:99999/nonexistent")
        .with_schema("test_invalid")
        .with_dlq_processor_interval(None);

    let result = WorkerRunner::builder(config).await;

    assert!(result.is_err(), "Should fail with invalid database URL");
}

#[tokio::test]
async fn test_worker_options_builder_concurrency() -> Result<(), BackfillError> {
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema("test_concurrency")
        .with_queues(vec![QueueConfig::default_queue(15)])
        .with_dlq_processor_interval(None);

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    assert_eq!(worker.worker_count(), 1);

    Ok(())
}

#[tokio::test]
async fn test_worker_with_custom_poll_interval() -> Result<(), BackfillError> {
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema("test_poll_interval")
        .with_poll_interval(Duration::from_millis(500))
        .with_dlq_processor_interval(None);

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    assert_eq!(worker.worker_count(), 1);

    Ok(())
}
