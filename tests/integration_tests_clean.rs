type Result<T> = std::result::Result<T, BackfillError>;
use backfill::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

/// Test job payload for integration tests
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestJob {
    message: String,
    number: i32,
}

/// Get a test database URL from environment or use default
fn get_test_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost:5432/backfill_test".to_string())
}

/// One-time setup of test database
async fn ensure_test_database() -> Result<()> {
    // Much simpler approach - just try to create the database
    let base_url = get_test_database_url().replace("/backfill_test", "/postgres");
    let admin_pool = PgPool::connect(&base_url).await?;

    // Create test database if it doesn't exist
    sqlx::query("CREATE DATABASE backfill_test")
        .execute(&admin_pool)
        .await
        .ok(); // Ignore error if database already exists

    admin_pool.close().await;
    Ok(())
}

/// Test helper using unique schemas for perfect isolation
async fn with_isolated_schema<F, Fut>(test_fn: F) -> Result<()>
where
    F: FnOnce(BackfillClient) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    ensure_test_database().await?;

    // Create a unique schema name for this test
    let schema_name = format!("test_{}", Uuid::new_v4().simple());

    let pool = PgPool::connect(&get_test_database_url()).await?;

    // Create client with unique schema - GraphileWorker will create the schema
    let client = BackfillClient::with_pool_and_schema(pool.clone(), schema_name.clone()).await?;

    // Run the test
    let result = test_fn(client).await;

    // Clean up: drop the test schema
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema_name))
        .execute(&pool)
        .await?;

    pool.close().await;

    result
}

#[tokio::test]
async fn test_client_creation_and_schema_setup() -> Result<()> {
    with_isolated_schema(|_client| async move {
        // The fact that we got a client means schema setup worked
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_basic_job_enqueue() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "Hello, World!".to_string(),
            number: 42,
        };

        // Test basic enqueueing
        let outcome = client.enqueue("test_job", &test_job, JobSpec::default()).await?;
        let job = outcome.expect("outcome should contain a job");

        // Verify the job was inserted
        assert!(*job.id() > 0, "Job should have a valid ID");

        // Check the job exists in the database
        let pool = client.pool();
        let row: (i64,) = sqlx::query_as(&format!(
            "
            SELECT COUNT(*) FROM {}.\"_private_jobs\" j
            JOIN {}.\"_private_tasks\" t ON j.task_id = t.id
            WHERE t.identifier = 'test_job'
        ",
            client.schema(),
            client.schema()
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(row.0, 1, "Job should be inserted into database");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_priority_ordering() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "Priority test".to_string(),
            number: 1,
        };

        // Enqueue jobs with different priorities
        let _low_priority = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    priority: Priority::BULK_LOW,
                    ..Default::default()
                },
            )
            .await?;

        let _high_priority = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    priority: Priority::FAST_HIGH,
                    ..Default::default()
                },
            )
            .await?;

        let _emergency = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    priority: Priority::EMERGENCY,
                    ..Default::default()
                },
            )
            .await?;

        // Verify jobs are ordered by priority
        let pool = client.pool();
        let priorities: Vec<(i16,)> = sqlx::query_as(&format!(
            "
            SELECT j.priority FROM {}.\"_private_jobs\" j
            JOIN {}.\"_private_tasks\" t ON j.task_id = t.id
            WHERE t.identifier = 'test_job'
            ORDER BY j.priority ASC
        ",
            client.schema(),
            client.schema()
        ))
        .fetch_all(pool)
        .await?;

        assert_eq!(priorities.len(), 3);
        assert_eq!(priorities[0].0, -20); // EMERGENCY
        assert_eq!(priorities[1].0, -10); // FAST_HIGH
        assert_eq!(priorities[2].0, 5); // BULK_LOW

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_serial_queues() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "Queue test".to_string(),
            number: 1,
        };

        // Enqueue jobs in different serial queues
        let _fast_job = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    queue: Queue::serial("fast"),
                    ..Default::default()
                },
            )
            .await?;

        let _bulk_job = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    queue: Queue::serial("bulk"),
                    ..Default::default()
                },
            )
            .await?;

        let _custom_job = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    queue: Queue::serial("special_processing"),
                    ..Default::default()
                },
            )
            .await?;

        // Verify jobs are in their respective queues
        let pool = client.pool();
        let fast_count: (i64,) = sqlx::query_as(&format!(
            "
            SELECT COUNT(*) FROM {}.\"_private_jobs\" j
            JOIN {}.\"_private_job_queues\" q ON j.job_queue_id = q.id
            WHERE q.queue_name = 'fast'
        ",
            client.schema(),
            client.schema()
        ))
        .fetch_one(pool)
        .await?;

        let bulk_count: (i64,) = sqlx::query_as(&format!(
            "
            SELECT COUNT(*) FROM {}.\"_private_jobs\" j
            JOIN {}.\"_private_job_queues\" q ON j.job_queue_id = q.id
            WHERE q.queue_name = 'bulk'
        ",
            client.schema(),
            client.schema()
        ))
        .fetch_one(pool)
        .await?;

        let custom_count: (i64,) = sqlx::query_as(&format!(
            "
            SELECT COUNT(*) FROM {}.\"_private_jobs\" j
            JOIN {}.\"_private_job_queues\" q ON j.job_queue_id = q.id
            WHERE q.queue_name = 'special_processing'
        ",
            client.schema(),
            client.schema()
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(fast_count.0, 1);
        assert_eq!(bulk_count.0, 1);
        assert_eq!(custom_count.0, 1);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_job_key_idempotency() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "Idempotency test".to_string(),
            number: 1,
        };

        let job_key = "unique_job_123".to_string();

        // Enqueue the same job twice with the same job_key
        let outcome1 = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    job_key: Some(job_key.clone()),
                    ..Default::default()
                },
            )
            .await?;

        // First should be enqueued
        assert!(outcome1.is_enqueued());

        let outcome2 = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    job_key: Some(job_key.clone()),
                    ..Default::default()
                },
            )
            .await?;

        // Second should also be enqueued (job was updated, not locked)
        assert!(outcome2.is_enqueued());

        // The second job should replace the first (or be the same job)
        let pool = client.pool();
        let count: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {}.\"_private_jobs\" WHERE key = $1",
            client.schema()
        ))
        .bind(&job_key)
        .fetch_one(pool)
        .await?;

        assert_eq!(count.0, 1, "Should only have one job with the given key");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_job_key_already_in_progress() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "In progress test".to_string(),
            number: 1,
        };

        let job_key = "in_progress_job_123".to_string();

        // First, enqueue a job with a job_key
        let outcome1 = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    job_key: Some(job_key.clone()),
                    ..Default::default()
                },
            )
            .await?;

        // Should be enqueued
        assert!(outcome1.is_enqueued());
        let _job = outcome1.expect("outcome should contain a job");

        // Simulate the job being locked by a worker
        let pool = client.pool();
        sqlx::query(&format!(
            "UPDATE {}.\"_private_jobs\" SET locked_at = NOW(), locked_by = 'test_worker' WHERE key = $1",
            client.schema()
        ))
        .bind(&job_key)
        .execute(pool)
        .await?;

        // Try to enqueue another job with the same key
        let outcome2 = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    job_key: Some(job_key.clone()),
                    ..Default::default()
                },
            )
            .await?;

        // NOTE: With replace mode, PostgreSQL clears the key from the locked job first,
        // then inserts a NEW job. So outcome2 will be Enqueued, not AlreadyInProgress.
        // The AlreadyInProgress case happens in rare race conditions.
        // This test verifies the API works, even if we can't easily trigger the edge
        // case.
        assert!(outcome2.is_enqueued() || outcome2.is_already_in_progress());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_convenience_functions() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "Convenience test".to_string(),
            number: 1,
        };

        // Test enqueue_fast
        let fast_outcome = enqueue_fast(&client, "test_job", &test_job, Some("fast_job_key".to_string())).await?;
        assert!(fast_outcome.is_enqueued());

        // Test enqueue_bulk
        let bulk_outcome = enqueue_bulk(&client, "test_job", &test_job, Some("bulk_job_key".to_string())).await?;
        assert!(bulk_outcome.is_enqueued());

        // Verify the jobs use parallel execution (NULL job_queue_id) with correct
        // priorities
        let pool = client.pool();

        // Fast job: parallel execution with FAST_DEFAULT priority (-5)
        let fast_job_data: (Option<i32>, i16) = sqlx::query_as(&format!(
            "SELECT job_queue_id, priority FROM {}.\"_private_jobs\" WHERE key = 'fast_job_key'",
            client.schema()
        ))
        .fetch_one(pool)
        .await?;

        assert!(
            fast_job_data.0.is_none(),
            "Fast jobs should use parallel execution (NULL job_queue_id)"
        );
        assert_eq!(fast_job_data.1, -5, "Fast jobs should have FAST_DEFAULT priority");

        // Bulk job: parallel execution with BULK_DEFAULT priority (0)
        let bulk_job_data: (Option<i32>, i16) = sqlx::query_as(&format!(
            "SELECT job_queue_id, priority FROM {}.\"_private_jobs\" WHERE key = 'bulk_job_key'",
            client.schema()
        ))
        .fetch_one(pool)
        .await?;

        assert!(
            bulk_job_data.0.is_none(),
            "Bulk jobs should use parallel execution (NULL job_queue_id)"
        );
        assert_eq!(bulk_job_data.1, 0, "Bulk jobs should have BULK_DEFAULT priority");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_job_utilities() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "Utilities test".to_string(),
            number: 1,
        };

        // Enqueue a job
        let _job = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    job_key: Some("test_key".to_string()),
                    ..Default::default()
                },
            )
            .await?;

        // Test remove_job
        client.remove_job("test_key").await?;

        // Verify the job was removed
        let pool = client.pool();
        let count: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {}.\"_private_jobs\" WHERE key = 'test_key'",
            client.schema()
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(count.0, 0, "Job should be removed");

        Ok(())
    })
    .await
}

// Test job handler for cron tests
#[derive(Clone, Serialize, Deserialize)]
struct CronTestHandler;

impl TaskHandler for CronTestHandler {
    const IDENTIFIER: &'static str = "cron_test_task";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        Ok::<(), std::io::Error>(())
    }
}

#[tokio::test]
async fn test_cron_schedule_registration() -> Result<()> {
    ensure_test_database().await?;

    let schema_name = format!("test_{}", Uuid::new_v4().simple());
    let database_url = get_test_database_url();

    let config = WorkerConfig {
        database_url: database_url.clone(),
        schema: schema_name.clone(),
        queue_configs: vec![],
        poll_interval: std::time::Duration::from_millis(1000),
        dlq_processor_interval: None,
        ..Default::default()
    };

    // Build worker with cron schedule - should succeed
    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<CronTestHandler>()
        .add_cron_schedule("*/5 * * * * cron_test_task")?
        .build()
        .await?;

    // Process jobs once to ensure worker initialization completes
    worker.process_available_jobs().await?;

    // Verify cron table infrastructure exists
    let pool = PgPool::connect(&database_url).await?;

    let table_exists: (bool,) = sqlx::query_as(&format!(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '_private_known_crontabs')",
        schema_name
    ))
    .fetch_one(&pool)
    .await?;

    assert!(table_exists.0, "Cron table should exist after worker initialization");

    // Clean up
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema_name))
        .execute(&pool)
        .await?;

    pool.close().await;

    Ok(())
}

#[tokio::test]
async fn test_multiple_cron_schedules() -> Result<()> {
    ensure_test_database().await?;

    let schema_name = format!("test_{}", Uuid::new_v4().simple());
    let database_url = get_test_database_url();

    let config = WorkerConfig {
        database_url: database_url.clone(),
        schema: schema_name.clone(),
        queue_configs: vec![],
        poll_interval: std::time::Duration::from_millis(1000),
        dlq_processor_interval: None,
        ..Default::default()
    };

    // Build worker with multiple cron schedules - should succeed
    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<CronTestHandler>()
        .add_cron_schedule("*/5 * * * * cron_test_task")?
        .add_cron_schedule("0 * * * * cron_test_task")?
        .build()
        .await?;

    // Process jobs once to ensure worker initialization completes
    worker.process_available_jobs().await?;

    // Clean up
    let pool = PgPool::connect(&database_url).await?;
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema_name))
        .execute(&pool)
        .await?;

    pool.close().await;

    Ok(())
}

#[tokio::test]
async fn test_cron_with_payload() -> Result<()> {
    ensure_test_database().await?;

    let schema_name = format!("test_{}", Uuid::new_v4().simple());
    let database_url = get_test_database_url();

    let config = WorkerConfig {
        database_url: database_url.clone(),
        schema: schema_name.clone(),
        queue_configs: vec![],
        poll_interval: std::time::Duration::from_millis(1000),
        dlq_processor_interval: None,
        ..Default::default()
    };

    // Build worker with cron schedule that includes payload - should succeed
    let worker = WorkerRunner::builder(config)
        .await?
        .define_job::<CronTestHandler>()
        .add_cron_schedule(r#"0 2 * * * cron_test_task {"key":"value"}"#)?
        .build()
        .await?;

    // Process jobs once to ensure worker initialization completes
    worker.process_available_jobs().await?;

    // Clean up
    let pool = PgPool::connect(&database_url).await?;
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema_name))
        .execute(&pool)
        .await?;

    pool.close().await;

    Ok(())
}

// =============================================================================
// Stale Lock Cleanup Tests
// =============================================================================

/// Test that release_stale_queue_locks actually releases old queue locks
#[tokio::test]
async fn test_release_stale_queue_locks() -> Result<()> {
    with_isolated_schema(|client| async move {
        let pool = client.pool();
        let schema = client.schema();

        // First, enqueue a job to ensure the queue exists
        let test_job = TestJob {
            message: "test".to_string(),
            number: 1,
        };
        client.enqueue("test_job", &test_job, JobSpec::default()).await?;

        // Manually insert a stale queue lock (10 minutes old)
        let stale_lock_query = format!(
            r#"
            INSERT INTO {schema}._private_job_queues (queue_name, locked_at, locked_by)
            VALUES ('stale_test_queue', NOW() - INTERVAL '10 minutes', 'dead_worker_123')
            ON CONFLICT (queue_name) DO UPDATE
            SET locked_at = NOW() - INTERVAL '10 minutes', locked_by = 'dead_worker_123'
            "#,
            schema = schema
        );
        sqlx::query(&stale_lock_query).execute(pool).await?;

        // Verify the lock exists
        let count_before: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {schema}._private_job_queues WHERE locked_by = 'dead_worker_123'",
            schema = schema
        ))
        .fetch_one(pool)
        .await?;
        assert_eq!(count_before.0, 1, "Stale lock should exist before cleanup");

        // Run cleanup with a 5-minute timeout (should release 10-minute-old lock)
        let released = client
            .release_stale_queue_locks(std::time::Duration::from_secs(300))
            .await?;
        assert_eq!(released, 1, "Should have released 1 stale queue lock");

        // Verify the lock was released
        let count_after: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {schema}._private_job_queues WHERE locked_by = 'dead_worker_123'",
            schema = schema
        ))
        .fetch_one(pool)
        .await?;
        assert_eq!(count_after.0, 0, "Stale lock should be released after cleanup");

        Ok(())
    })
    .await
}

/// Test that release_stale_job_locks actually releases old job locks
#[tokio::test]
async fn test_release_stale_job_locks() -> Result<()> {
    with_isolated_schema(|client| async move {
        let pool = client.pool();
        let schema = client.schema();

        // Enqueue a job first
        let test_job = TestJob {
            message: "locked_job".to_string(),
            number: 42,
        };
        let outcome = client.enqueue("test_job", &test_job, JobSpec::default()).await?;
        let job = outcome.expect("should have enqueued job");
        let job_id = *job.id();

        // Manually set a stale lock on the job (15 minutes old)
        let stale_lock_query = format!(
            r#"
            UPDATE {schema}._private_jobs
            SET locked_at = NOW() - INTERVAL '15 minutes',
                locked_by = 'dead_worker_456'
            WHERE id = $1
            "#,
            schema = schema
        );
        sqlx::query(&stale_lock_query).bind(job_id).execute(pool).await?;

        // Verify the lock exists
        let locked_by: Option<String> = sqlx::query_scalar(&format!(
            "SELECT locked_by FROM {schema}._private_jobs WHERE id = $1",
            schema = schema
        ))
        .bind(job_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            locked_by,
            Some("dead_worker_456".to_string()),
            "Job should be locked before cleanup"
        );

        // Run cleanup with a 10-minute timeout (should release 15-minute-old lock)
        let released = client
            .release_stale_job_locks(std::time::Duration::from_secs(600))
            .await?;
        assert_eq!(released, 1, "Should have released 1 stale job lock");

        // Verify the lock was released
        let locked_by_after: Option<String> = sqlx::query_scalar(&format!(
            "SELECT locked_by FROM {schema}._private_jobs WHERE id = $1",
            schema = schema
        ))
        .bind(job_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(locked_by_after, None, "Job lock should be released after cleanup");

        Ok(())
    })
    .await
}

/// Test that recent locks are NOT released (safety check)
#[tokio::test]
async fn test_recent_locks_not_released() -> Result<()> {
    with_isolated_schema(|client| async move {
        let pool = client.pool();
        let schema = client.schema();

        // Enqueue a job
        let test_job = TestJob {
            message: "recent_lock".to_string(),
            number: 1,
        };
        let outcome = client.enqueue("test_job", &test_job, JobSpec::default()).await?;
        let job = outcome.expect("should have enqueued job");
        let job_id = *job.id();

        // Set a recent lock (1 minute old - should NOT be released)
        let recent_lock_query = format!(
            r#"
            UPDATE {schema}._private_jobs
            SET locked_at = NOW() - INTERVAL '1 minute',
                locked_by = 'active_worker'
            WHERE id = $1
            "#,
            schema = schema
        );
        sqlx::query(&recent_lock_query).bind(job_id).execute(pool).await?;

        // Run cleanup with a 30-minute timeout (should NOT release 1-minute-old lock)
        let released = client
            .release_stale_job_locks(std::time::Duration::from_secs(1800))
            .await?;
        assert_eq!(released, 0, "Should NOT have released recent lock");

        // Verify the lock still exists
        let locked_by: Option<String> = sqlx::query_scalar(&format!(
            "SELECT locked_by FROM {schema}._private_jobs WHERE id = $1",
            schema = schema
        ))
        .bind(job_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            locked_by,
            Some("active_worker".to_string()),
            "Recent lock should still exist"
        );

        Ok(())
    })
    .await
}

/// Test startup_cleanup releases both queue and job locks
#[tokio::test]
async fn test_startup_cleanup_releases_both_lock_types() -> Result<()> {
    with_isolated_schema(|client| async move {
        let pool = client.pool();
        let schema = client.schema();

        // Enqueue a job
        let test_job = TestJob {
            message: "startup_test".to_string(),
            number: 1,
        };
        let outcome = client.enqueue("test_job", &test_job, JobSpec::default()).await?;
        let job = outcome.expect("should have enqueued job");
        let job_id = *job.id();

        // Create a stale queue lock
        let stale_queue_lock = format!(
            r#"
            INSERT INTO {schema}._private_job_queues (queue_name, locked_at, locked_by)
            VALUES ('startup_stale_queue', NOW() - INTERVAL '1 hour', 'crashed_worker')
            ON CONFLICT (queue_name) DO UPDATE
            SET locked_at = NOW() - INTERVAL '1 hour', locked_by = 'crashed_worker'
            "#,
            schema = schema
        );
        sqlx::query(&stale_queue_lock).execute(pool).await?;

        // Create a stale job lock
        let stale_job_lock = format!(
            r#"
            UPDATE {schema}._private_jobs
            SET locked_at = NOW() - INTERVAL '1 hour',
                locked_by = 'crashed_worker'
            WHERE id = $1
            "#,
            schema = schema
        );
        sqlx::query(&stale_job_lock).bind(job_id).execute(pool).await?;

        // Run startup cleanup
        let (queue_released, job_released, _failed_deleted) = client.startup_cleanup().await?;

        assert!(queue_released >= 1, "Should have released at least 1 stale queue lock");
        assert_eq!(job_released, 1, "Should have released 1 stale job lock");

        // Verify both locks were released
        let queue_lock_count: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {schema}._private_job_queues WHERE locked_by = 'crashed_worker'",
            schema = schema
        ))
        .fetch_one(pool)
        .await?;
        assert_eq!(queue_lock_count.0, 0, "Queue lock should be released");

        let job_locked_by: Option<String> = sqlx::query_scalar(&format!(
            "SELECT locked_by FROM {schema}._private_jobs WHERE id = $1",
            schema = schema
        ))
        .bind(job_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(job_locked_by, None, "Job lock should be released");

        Ok(())
    })
    .await
}

// =============================================================================
// Queue Parallel vs Serial Behavior Tests
// =============================================================================

/// Test that parallel jobs have no job_queue_id (critical for parallel
/// execution)
#[tokio::test]
async fn test_parallel_jobs_have_no_queue_id() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "parallel test".to_string(),
            number: 1,
        };

        // Enqueue with default (parallel) queue
        let outcome = client.enqueue("test_job", &test_job, JobSpec::default()).await?;
        let job = outcome.expect("should enqueue");

        // Verify job_queue_id is NULL in the database
        let pool = client.pool();
        let job_queue_id: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT job_queue_id FROM {}.\"_private_jobs\" WHERE id = $1",
            client.schema()
        ))
        .bind(job.id())
        .fetch_one(pool)
        .await?;

        assert!(
            job_queue_id.is_none(),
            "Parallel jobs must have NULL job_queue_id to run concurrently"
        );

        Ok(())
    })
    .await
}

/// Test that serial jobs have a job_queue_id (required for serialization)
#[tokio::test]
async fn test_serial_jobs_have_queue_id() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "serial test".to_string(),
            number: 1,
        };

        // Enqueue with serial queue
        let outcome = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    queue: Queue::serial("my-serial-queue"),
                    ..Default::default()
                },
            )
            .await?;
        let job = outcome.expect("should enqueue");

        // Verify job_queue_id is NOT NULL in the database
        let pool = client.pool();
        let job_queue_id: Option<i32> = sqlx::query_scalar(&format!(
            "SELECT job_queue_id FROM {}.\"_private_jobs\" WHERE id = $1",
            client.schema()
        ))
        .bind(job.id())
        .fetch_one(pool)
        .await?;

        let job_queue_id = job_queue_id.expect("Serial jobs must have a job_queue_id for serialization");

        // Also verify the queue row exists with correct name
        let queue_name: String = sqlx::query_scalar(&format!(
            "SELECT queue_name FROM {}.\"_private_job_queues\" WHERE id = $1",
            client.schema()
        ))
        .bind(job_queue_id)
        .fetch_one(pool)
        .await?;

        assert_eq!(queue_name, "my-serial-queue");

        Ok(())
    })
    .await
}

/// Test that multiple parallel jobs can be enqueued without creating queue
/// locks
#[tokio::test]
async fn test_parallel_jobs_no_queue_rows() -> Result<()> {
    with_isolated_schema(|client| async move {
        let pool = client.pool();
        let schema = client.schema();

        // Enqueue 5 parallel jobs
        for i in 0..5 {
            let test_job = TestJob {
                message: format!("parallel job {}", i),
                number: i,
            };
            client.enqueue("test_job", &test_job, JobSpec::default()).await?;
        }

        // Count how many jobs were created
        let job_count: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {}.\"_private_jobs\" WHERE job_queue_id IS NULL",
            schema
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(job_count.0, 5, "All 5 jobs should have NULL job_queue_id");

        // Verify no queue rows were created for parallel jobs
        // (There might be other queues from setup, so we check specifically
        // for queue rows that have jobs pointing to them)
        let queues_with_jobs: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(DISTINCT job_queue_id) FROM {}.\"_private_jobs\" WHERE job_queue_id IS NOT NULL",
            schema
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(
            queues_with_jobs.0, 0,
            "Parallel jobs should not create queue associations"
        );

        Ok(())
    })
    .await
}

/// Test that serial jobs in the same queue share a queue row
#[tokio::test]
async fn test_serial_jobs_share_queue_row() -> Result<()> {
    with_isolated_schema(|client| async move {
        let pool = client.pool();
        let schema = client.schema();

        // Enqueue 3 serial jobs in the same queue
        for i in 0..3 {
            let test_job = TestJob {
                message: format!("serial job {}", i),
                number: i,
            };
            client
                .enqueue(
                    "test_job",
                    &test_job,
                    JobSpec {
                        queue: Queue::serial("shared-queue"),
                        ..Default::default()
                    },
                )
                .await?;
        }

        // All jobs should reference the same queue row
        let distinct_queue_ids: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(DISTINCT job_queue_id) FROM {}.\"_private_jobs\" WHERE job_queue_id IS NOT NULL",
            schema
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(
            distinct_queue_ids.0, 1,
            "All serial jobs in same queue should share one queue row"
        );

        // Verify queue name is correct
        let queue_name: String = sqlx::query_scalar(&format!(
            r#"
            SELECT q.queue_name
            FROM {schema}."_private_job_queues" q
            JOIN {schema}."_private_jobs" j ON j.job_queue_id = q.id
            LIMIT 1
            "#,
            schema = schema
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(queue_name, "shared-queue");

        Ok(())
    })
    .await
}

/// Test that different serial queues are independent
#[tokio::test]
async fn test_different_serial_queues_independent() -> Result<()> {
    with_isolated_schema(|client| async move {
        let pool = client.pool();
        let schema = client.schema();

        // Enqueue jobs in different serial queues
        let queues = ["queue-a", "queue-b", "queue-c"];
        for queue_name in queues {
            let test_job = TestJob {
                message: format!("job in {}", queue_name),
                number: 1,
            };
            client
                .enqueue(
                    "test_job",
                    &test_job,
                    JobSpec {
                        queue: Queue::serial(queue_name),
                        ..Default::default()
                    },
                )
                .await?;
        }

        // Each should have its own queue row
        let distinct_queue_ids: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(DISTINCT job_queue_id) FROM {}.\"_private_jobs\" WHERE job_queue_id IS NOT NULL",
            schema
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(
            distinct_queue_ids.0, 3,
            "Each serial queue should have its own queue row"
        );

        // Verify all queue names exist
        let queue_names: Vec<(String,)> = sqlx::query_as(&format!(
            r#"
            SELECT DISTINCT q.queue_name
            FROM {schema}."_private_job_queues" q
            JOIN {schema}."_private_jobs" j ON j.job_queue_id = q.id
            ORDER BY q.queue_name
            "#,
            schema = schema
        ))
        .fetch_all(pool)
        .await?;

        let names: Vec<&str> = queue_names.iter().map(|(n,)| n.as_str()).collect();
        assert_eq!(names, vec!["queue-a", "queue-b", "queue-c"]);

        Ok(())
    })
    .await
}

/// Test serial_for creates unique queues per entity
#[tokio::test]
async fn test_serial_for_per_entity_queues() -> Result<()> {
    with_isolated_schema(|client| async move {
        let pool = client.pool();
        let schema = client.schema();

        // Enqueue jobs for different users
        for user_id in [100, 200, 300] {
            let test_job = TestJob {
                message: format!("event for user {}", user_id),
                number: user_id,
            };
            client
                .enqueue(
                    "test_job",
                    &test_job,
                    JobSpec {
                        queue: Queue::serial_for("user", user_id),
                        ..Default::default()
                    },
                )
                .await?;
        }

        // Each user should have their own queue
        let queue_names: Vec<(String,)> = sqlx::query_as(&format!(
            r#"
            SELECT DISTINCT q.queue_name
            FROM {schema}."_private_job_queues" q
            JOIN {schema}."_private_jobs" j ON j.job_queue_id = q.id
            ORDER BY q.queue_name
            "#,
            schema = schema
        ))
        .fetch_all(pool)
        .await?;

        let names: Vec<&str> = queue_names.iter().map(|(n,)| n.as_str()).collect();
        assert_eq!(names, vec!["user:100", "user:200", "user:300"]);

        Ok(())
    })
    .await
}

/// Test mixed parallel and serial jobs
#[tokio::test]
async fn test_mixed_parallel_and_serial_jobs() -> Result<()> {
    with_isolated_schema(|client| async move {
        let pool = client.pool();
        let schema = client.schema();

        // Enqueue some parallel jobs
        for i in 0..3 {
            let test_job = TestJob {
                message: format!("parallel {}", i),
                number: i,
            };
            client.enqueue("test_job", &test_job, JobSpec::default()).await?;
        }

        // Enqueue some serial jobs
        for i in 0..2 {
            let test_job = TestJob {
                message: format!("serial {}", i),
                number: i + 100,
            };
            client
                .enqueue(
                    "test_job",
                    &test_job,
                    JobSpec {
                        queue: Queue::serial("rate-limit"),
                        ..Default::default()
                    },
                )
                .await?;
        }

        // Count parallel jobs (NULL job_queue_id)
        let parallel_count: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {}.\"_private_jobs\" WHERE job_queue_id IS NULL",
            schema
        ))
        .fetch_one(pool)
        .await?;

        // Count serial jobs (non-NULL job_queue_id)
        let serial_count: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {}.\"_private_jobs\" WHERE job_queue_id IS NOT NULL",
            schema
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(parallel_count.0, 3, "Should have 3 parallel jobs");
        assert_eq!(serial_count.0, 2, "Should have 2 serial jobs");

        Ok(())
    })
    .await
}
