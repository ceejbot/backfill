//! Worker runner integration tests

use std::time::Duration;

use backfill::{
    BackfillError, IntoTaskHandlerResult, TaskHandler, WorkerConfig, WorkerContext, WorkerError, WorkerRunner,
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
    assert_eq!(config.concurrency, 10);
    assert_eq!(config.poll_interval, Duration::from_millis(200));
    assert_eq!(config.dlq_processor_interval, Some(Duration::from_secs(60)));
}

#[tokio::test]
async fn test_worker_config_builder() {
    let config = WorkerConfig::new("postgresql://localhost/test")
        .with_schema("custom_schema")
        .with_poll_interval(Duration::from_millis(100))
        .with_dlq_processor_interval(Some(Duration::from_secs(30)))
        .with_concurrency(10);

    assert_eq!(config.database_url, "postgresql://localhost/test");
    assert_eq!(config.schema, "custom_schema");
    assert_eq!(config.poll_interval, Duration::from_millis(100));
    assert_eq!(config.dlq_processor_interval, Some(Duration::from_secs(30)));
    assert_eq!(config.concurrency, 10);
}

#[tokio::test]
async fn test_worker_config_disable_dlq_processor() {
    let config = WorkerConfig::new("postgresql://localhost/test").with_dlq_processor_interval(None);

    assert_eq!(config.dlq_processor_interval, None);
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

    // Process all available jobs. Returns () — for job counts, plug in a
    // JobComplete hook before building the worker.
    worker.process_available_jobs().await?;

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
        .with_concurrency(15)
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

// =============================================================================
// Queue Concurrency Tests
// =============================================================================

use backfill::{JobSpec, Queue};

/// Test that parallel jobs can be locked concurrently (no queue lock blocking)
#[tokio::test]
async fn test_parallel_jobs_concurrent_locking() -> Result<(), BackfillError> {
    use uuid::Uuid;

    let schema = format!("test_parallel_lock_{}", Uuid::new_v4().simple());
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema(&schema)
        .with_dlq_processor_interval(None);

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    let client = worker.client();
    let pool = client.pool();

    // Enqueue 3 parallel jobs
    for i in 0..3 {
        let job = SimpleTestJob {
            message: format!("Parallel job {}", i),
        };
        client
            .enqueue(SimpleTestJob::IDENTIFIER, &job, JobSpec::default())
            .await?;
    }

    // Simulate locking all 3 jobs with different worker IDs
    // This mimics what graphile_worker does when fetching jobs
    let locked_count: (i64,) = sqlx::query_as(&format!(
        r#"
        WITH locked AS (
            UPDATE {schema}."_private_jobs"
            SET locked_at = NOW(), locked_by = 'test_worker_1'
            WHERE id = (
                SELECT id FROM {schema}."_private_jobs"
                WHERE is_available = true AND job_queue_id IS NULL
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
        )
        SELECT COUNT(*) FROM locked
        "#,
        schema = schema
    ))
    .fetch_one(pool)
    .await?;

    assert_eq!(locked_count.0, 1, "Should lock first parallel job");

    // Lock second job with different worker
    let locked_count2: (i64,) = sqlx::query_as(&format!(
        r#"
        WITH locked AS (
            UPDATE {schema}."_private_jobs"
            SET locked_at = NOW(), locked_by = 'test_worker_2'
            WHERE id = (
                SELECT id FROM {schema}."_private_jobs"
                WHERE is_available = true AND job_queue_id IS NULL AND locked_at IS NULL
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
        )
        SELECT COUNT(*) FROM locked
        "#,
        schema = schema
    ))
    .fetch_one(pool)
    .await?;

    assert_eq!(locked_count2.0, 1, "Should lock second parallel job concurrently");

    // Lock third job with different worker
    let locked_count3: (i64,) = sqlx::query_as(&format!(
        r#"
        WITH locked AS (
            UPDATE {schema}."_private_jobs"
            SET locked_at = NOW(), locked_by = 'test_worker_3'
            WHERE id = (
                SELECT id FROM {schema}."_private_jobs"
                WHERE is_available = true AND job_queue_id IS NULL AND locked_at IS NULL
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
        )
        SELECT COUNT(*) FROM locked
        "#,
        schema = schema
    ))
    .fetch_one(pool)
    .await?;

    assert_eq!(locked_count3.0, 1, "Should lock third parallel job concurrently");

    // Verify all 3 are locked simultaneously
    let total_locked: (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM {}.\"_private_jobs\" WHERE locked_at IS NOT NULL",
        schema
    ))
    .fetch_one(pool)
    .await?;

    assert_eq!(total_locked.0, 3, "All 3 parallel jobs should be locked simultaneously");

    // Clean up
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema))
        .execute(pool)
        .await?;

    Ok(())
}

/// Test that serial jobs block each other (queue lock prevents concurrent
/// execution)
#[tokio::test]
async fn test_serial_jobs_queue_locking() -> Result<(), BackfillError> {
    use uuid::Uuid;

    let schema = format!("test_serial_lock_{}", Uuid::new_v4().simple());
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema(&schema)
        .with_dlq_processor_interval(None);

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    let client = worker.client();
    let pool = client.pool();

    // Enqueue 3 serial jobs in the SAME queue
    for i in 0..3 {
        let job = SimpleTestJob {
            message: format!("Serial job {}", i),
        };
        client
            .enqueue(
                SimpleTestJob::IDENTIFIER,
                &job,
                JobSpec {
                    queue: Queue::serial("serial-queue"),
                    ..Default::default()
                },
            )
            .await?;
    }

    // Get the queue ID
    let queue_id: i32 = sqlx::query_scalar(&format!(
        "SELECT id FROM {}.\"_private_job_queues\" WHERE queue_name = 'serial-queue'",
        schema
    ))
    .fetch_one(pool)
    .await?;

    // Lock the queue (simulating first worker acquiring it)
    sqlx::query(&format!(
        "UPDATE {}.\"_private_job_queues\" SET locked_at = NOW(), locked_by = 'test_worker_1' WHERE id = $1",
        schema
    ))
    .bind(queue_id)
    .execute(pool)
    .await?;

    // Lock the first job
    let first_job_locked: (i64,) = sqlx::query_as(&format!(
        r#"
        WITH locked AS (
            UPDATE {schema}."_private_jobs"
            SET locked_at = NOW(), locked_by = 'test_worker_1'
            WHERE id = (
                SELECT id FROM {schema}."_private_jobs"
                WHERE is_available = true AND job_queue_id = $1
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
        )
        SELECT COUNT(*) FROM locked
        "#,
        schema = schema
    ))
    .bind(queue_id)
    .fetch_one(pool)
    .await?;

    assert_eq!(first_job_locked.0, 1, "Should lock first serial job");

    // Now try to fetch another job from the same queue with a different worker
    // This should fail because the queue is locked (SKIP LOCKED will skip it)
    let second_fetch: (i64,) = sqlx::query_as(&format!(
        r#"
        SELECT COUNT(*) FROM {schema}."_private_jobs" j
        WHERE j.is_available = true
          AND j.locked_at IS NULL
          AND j.job_queue_id IN (
              SELECT id FROM {schema}."_private_job_queues"
              WHERE locked_at IS NULL  -- Queue must not be locked
          )
        "#,
        schema = schema
    ))
    .fetch_one(pool)
    .await?;

    assert_eq!(
        second_fetch.0, 0,
        "No more jobs should be fetchable while queue is locked"
    );

    // Verify queue is locked
    let queue_locked: Option<String> = sqlx::query_scalar(&format!(
        "SELECT locked_by FROM {}.\"_private_job_queues\" WHERE id = $1",
        schema
    ))
    .bind(queue_id)
    .fetch_one(pool)
    .await?;

    assert_eq!(
        queue_locked,
        Some("test_worker_1".to_string()),
        "Queue should be locked by first worker"
    );

    // Clean up
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema))
        .execute(pool)
        .await?;

    Ok(())
}

/// Test that different serial queues are independent (can run concurrently)
#[tokio::test]
async fn test_different_serial_queues_independent() -> Result<(), BackfillError> {
    use uuid::Uuid;

    let schema = format!("test_multi_serial_{}", Uuid::new_v4().simple());
    let config = WorkerConfig::new(get_test_database_url())
        .with_schema(&schema)
        .with_dlq_processor_interval(None);

    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<SimpleTestJob>()
        .build()
        .await?;

    let client = worker.client();
    let pool = client.pool();

    // Enqueue jobs in different serial queues
    for queue_name in ["queue-a", "queue-b", "queue-c"] {
        let job = SimpleTestJob {
            message: format!("Job in {}", queue_name),
        };
        client
            .enqueue(
                SimpleTestJob::IDENTIFIER,
                &job,
                JobSpec {
                    queue: Queue::serial(queue_name),
                    ..Default::default()
                },
            )
            .await?;
    }

    // Lock queue-a
    sqlx::query(&format!(
        "UPDATE {}.\"_private_job_queues\" SET locked_at = NOW(), locked_by = 'worker_a' WHERE queue_name = 'queue-a'",
        schema
    ))
    .execute(pool)
    .await?;

    // Lock queue-b
    sqlx::query(&format!(
        "UPDATE {}.\"_private_job_queues\" SET locked_at = NOW(), locked_by = 'worker_b' WHERE queue_name = 'queue-b'",
        schema
    ))
    .execute(pool)
    .await?;

    // Queue-c should still be available
    let available_queues: (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM {}.\"_private_job_queues\" WHERE locked_at IS NULL",
        schema
    ))
    .fetch_one(pool)
    .await?;

    assert_eq!(available_queues.0, 1, "Queue-c should still be available");

    // Verify queue-c is the unlocked one
    let unlocked_queue: String = sqlx::query_scalar(&format!(
        "SELECT queue_name FROM {}.\"_private_job_queues\" WHERE locked_at IS NULL",
        schema
    ))
    .fetch_one(pool)
    .await?;

    assert_eq!(unlocked_queue, "queue-c");

    // Clean up
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema))
        .execute(pool)
        .await?;

    Ok(())
}
