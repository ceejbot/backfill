//! Example demonstrating Dead Letter Queue (DLQ) management
//!
//! This example shows how to:
//! - Monitor DLQ statistics
//! - List and filter DLQ jobs
//! - Inspect individual failures
//! - Requeue jobs after fixing issues
//! - Clean up old DLQ entries
//!
//! To run this example:
//! ```bash
//! DATABASE_URL=postgresql://localhost/backfill cargo run --example dlq_management
//! ```

use std::time::Duration as StdDuration;

use backfill::{BackfillClient, BackfillError, DlqFilter};
use chrono::{Duration, Utc};

/// Display DLQ statistics for monitoring
async fn show_dlq_stats(client: &BackfillClient) -> Result<(), BackfillError> {
    log::info!("=== DLQ Statistics ===");

    let stats = client.dlq_stats().await?;

    log::info!("Total jobs in DLQ: {}", stats.total_jobs);
    log::info!("Unique task types: {}", stats.unique_tasks);
    log::info!("Unique queues: {}", stats.unique_queues);
    log::info!("Average failure count: {:.1}", stats.avg_failure_count);
    log::info!("Total requeued: {}", stats.total_requeued);

    if let Some(oldest) = stats.oldest_failure {
        log::info!("Oldest failure: {}", oldest.format("%Y-%m-%d %H:%M:%S"));
    }

    if let Some(newest) = stats.newest_failure {
        log::info!("Newest failure: {}", newest.format("%Y-%m-%d %H:%M:%S"));
    }

    if !stats.task_breakdown.is_empty() {
        log::info!("\nTop failing job types:");
        for (task, count) in stats.task_breakdown.iter().take(5) {
            log::info!("  {}: {} failures", task, count);
        }
    }

    Ok(())
}

/// List recent DLQ jobs with filtering
async fn list_recent_failures(client: &BackfillClient, task_filter: Option<String>) -> Result<(), BackfillError> {
    log::info!("\n=== Recent DLQ Jobs ===");

    let filter = DlqFilter {
        task_identifier: task_filter,
        limit: Some(10),
        offset: Some(0),
        ..Default::default()
    };

    let result = client.list_dlq_jobs(filter).await?;

    log::info!("Found {} jobs (showing 10 of {})", result.jobs.len(), result.total);

    for job in result.jobs {
        log::info!("\n  DLQ ID: {}", job.id);
        log::info!("  Task: {}", job.task_identifier);
        log::info!("  Queue: {}", job.queue_name);
        log::info!("  Failed at: {}", job.failed_at.format("%Y-%m-%d %H:%M:%S"));
        log::info!("  Failure count: {}", job.failure_count);
        log::info!("  Reason: {}", job.failure_reason);

        if job.requeued_count > 0 {
            log::info!("  Requeued {} times", job.requeued_count);
            if let Some(last_requeue) = job.last_requeued_at {
                log::info!("  Last requeue: {}", last_requeue.format("%Y-%m-%d %H:%M:%S"));
            }
        }

        if let Some(notes) = &job.notes {
            log::info!("  Notes: {}", notes);
        }
    }

    Ok(())
}

/// Inspect a specific DLQ job in detail
async fn inspect_dlq_job(client: &BackfillClient, dlq_id: i64) -> Result<(), BackfillError> {
    log::info!("\n=== Inspecting DLQ Job {} ===", dlq_id);

    let job = client.get_dlq_job(dlq_id).await?;

    match job {
        Some(job) => {
            log::info!("Task identifier: {}", job.task_identifier);
            log::info!("Queue: {}", job.queue_name);
            log::info!("Priority: {}", job.priority);
            log::info!("Failed at: {}", job.failed_at.format("%Y-%m-%d %H:%M:%S"));
            log::info!("Failure count: {}", job.failure_count);
            log::info!("Max attempts: {:?}", job.max_attempts);
            log::info!("Failure reason: {}", job.failure_reason);

            if let Some(original_id) = job.original_job_id {
                log::info!("Original job ID: {}", original_id);
            }

            if let Some(key) = &job.job_key {
                log::info!("Job key: {}", key);
            }

            log::info!("\nPayload:");
            log::info!("{}", serde_json::to_string_pretty(&job.payload)?);

            if let Some(error) = &job.last_error {
                log::info!("\nLast error:");
                log::info!("{}", serde_json::to_string_pretty(error)?);
            }

            log::info!("\nRequeue history:");
            log::info!("  Times requeued: {}", job.requeued_count);
            if let Some(last_requeue) = job.last_requeued_at {
                log::info!("  Last requeued: {}", last_requeue.format("%Y-%m-%d %H:%M:%S"));
            }

            if let Some(notes) = &job.notes {
                log::info!("\nAdmin notes:\n{}", notes);
            }
        }
        None => {
            log::warn!("DLQ job {} not found", dlq_id);
        }
    }

    Ok(())
}

/// Requeue jobs of a specific type after fixing an issue
#[allow(dead_code)]
async fn batch_requeue_by_task(
    client: &BackfillClient,
    task_identifier: &str,
    note: &str,
) -> Result<u32, BackfillError> {
    log::info!("\n=== Requeuing {} Jobs ===", task_identifier);

    let filter = DlqFilter {
        task_identifier: Some(task_identifier.to_string()),
        limit: Some(100),
        ..Default::default()
    };

    let jobs = client.list_dlq_jobs(filter).await?;
    let mut requeued = 0;
    let total = jobs.jobs.len();

    log::info!("Found {} jobs to requeue", total);

    for job in jobs.jobs {
        log::info!("Requeuing job {}: {}", job.id, job.task_identifier);

        match client.requeue_dlq_job(job.id, Some(note.to_string())).await {
            Ok(new_job) => {
                log::info!("  ✓ Requeued as job {}", new_job.id());
                requeued += 1;

                // Throttle to avoid overwhelming the system
                tokio::time::sleep(StdDuration::from_millis(100)).await;
            }
            Err(e) => {
                log::error!("  ✗ Failed to requeue: {}", e);
            }
        }
    }

    log::info!("Successfully requeued {}/{} jobs", requeued, total);
    Ok(requeued)
}

/// Clean up old DLQ entries that are no longer relevant
#[allow(dead_code)]
async fn cleanup_old_jobs(client: &BackfillClient, older_than_days: i64) -> Result<u32, BackfillError> {
    log::info!("\n=== Cleaning Up Old DLQ Jobs ===");

    let cutoff = Utc::now() - Duration::days(older_than_days);
    log::info!("Deleting jobs older than {}", cutoff.format("%Y-%m-%d"));

    let filter = DlqFilter {
        failed_before: Some(cutoff),
        limit: Some(100),
        ..Default::default()
    };

    let jobs = client.list_dlq_jobs(filter).await?;
    let mut deleted = 0;
    let total = jobs.jobs.len();

    log::info!("Found {} jobs to clean up", total);

    for job in jobs.jobs {
        log::info!(
            "Deleting job {}: {} (failed {})",
            job.id,
            job.task_identifier,
            job.failed_at.format("%Y-%m-%d")
        );

        match client.delete_dlq_job(job.id).await {
            Ok(true) => {
                log::info!("  ✓ Deleted");
                deleted += 1;
            }
            Ok(false) => {
                log::warn!("  ✗ Job not found (may have been deleted)");
            }
            Err(e) => {
                log::error!("  ✗ Failed to delete: {}", e);
            }
        }
    }

    log::info!("Successfully deleted {}/{} jobs", deleted, total);
    Ok(deleted)
}

/// Demonstrate typical DLQ management workflows
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    log::info!("Starting DLQ management example");

    // Connect to database
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost/backfill".to_string());

    let client = BackfillClient::new(&database_url).await?;

    // Ensure DLQ is initialized
    client.init_dlq().await?;

    // Workflow 1: Check overall DLQ health
    show_dlq_stats(&client).await?;

    // Workflow 2: List recent failures
    list_recent_failures(&client, None).await?;

    // Workflow 3: Filter by task type
    list_recent_failures(&client, Some("send_email".to_string())).await?;

    // Workflow 4: Inspect a specific job (if any exist)
    let stats = client.dlq_stats().await?;
    if stats.total_jobs > 0 {
        // Get the first job ID
        let filter = DlqFilter {
            limit: Some(1),
            ..Default::default()
        };
        let jobs = client.list_dlq_jobs(filter).await?;

        if let Some(job) = jobs.jobs.first() {
            inspect_dlq_job(&client, job.id).await?;
        }
    }

    // Workflow 5: Example of batch requeuing (commented out for safety)
    // After deploying a fix, you would run:
    // batch_requeue_by_task(
    //     &client,
    //     "send_email",
    //     "Requeued after fixing email validation bug in v2.1.0"
    // ).await?;

    // Workflow 6: Example of cleanup (commented out for safety)
    // Remove jobs older than 90 days:
    // cleanup_old_jobs(&client, 90).await?;

    log::info!("\n=== DLQ Management Example Complete ===");
    log::info!("Uncomment the batch_requeue_by_task or cleanup_old_jobs calls");
    log::info!("to test those workflows with your actual data.");

    Ok(())
}
