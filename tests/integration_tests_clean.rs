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
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://ceej@localhost:5432/backfill_test".to_string())
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
        let job = client.enqueue("test_job", &test_job, JobSpec::default()).await?;

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
async fn test_named_queues() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "Queue test".to_string(),
            number: 1,
        };

        // Enqueue jobs in different queues
        let _fast_job = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    queue: Queue::Fast,
                    ..Default::default()
                },
            )
            .await?;

        let _bulk_job = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    queue: Queue::Bulk,
                    ..Default::default()
                },
            )
            .await?;

        let _custom_job = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    queue: Queue::Custom("special_processing".to_string()),
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
        let _job1 = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    job_key: Some(job_key.clone()),
                    ..Default::default()
                },
            )
            .await?;

        let _job2 = client
            .enqueue(
                "test_job",
                &test_job,
                JobSpec {
                    job_key: Some(job_key.clone()),
                    ..Default::default()
                },
            )
            .await?;

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
async fn test_convenience_functions() -> Result<()> {
    with_isolated_schema(|client| async move {
        let test_job = TestJob {
            message: "Convenience test".to_string(),
            number: 1,
        };

        // Test enqueue_fast
        let _fast_job = enqueue_fast(&client, "test_job", &test_job, Some("fast_job_key".to_string())).await?;

        // Test enqueue_bulk
        let _bulk_job = enqueue_bulk(&client, "test_job", &test_job, Some("bulk_job_key".to_string())).await?;

        // Verify the jobs are in the correct queues with correct priorities
        let pool = client.pool();
        let fast_job_data: (String, i16) = sqlx::query_as(&format!(
            "
            SELECT q.queue_name, j.priority
            FROM {}.\"_private_jobs\" j
            JOIN {}.\"_private_job_queues\" q ON j.job_queue_id = q.id
            WHERE j.key = 'fast_job_key'
        ",
            client.schema(),
            client.schema()
        ))
        .fetch_one(pool)
        .await?;

        let bulk_job_data: (String, i16) = sqlx::query_as(&format!(
            "
            SELECT q.queue_name, j.priority
            FROM {}.\"_private_jobs\" j
            JOIN {}.\"_private_job_queues\" q ON j.job_queue_id = q.id
            WHERE j.key = 'bulk_job_key'
        ",
            client.schema(),
            client.schema()
        ))
        .fetch_one(pool)
        .await?;

        assert_eq!(fast_job_data.0, "fast");
        assert_eq!(fast_job_data.1, -5); // FAST_DEFAULT

        assert_eq!(bulk_job_data.0, "bulk");
        assert_eq!(bulk_job_data.1, 0); // BULK_DEFAULT

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
