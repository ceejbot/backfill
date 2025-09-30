# Dead Letter Queue (DLQ) Guide

The Dead Letter Queue (DLQ) is a critical operational feature for handling jobs that cannot be processed successfully. This guide covers everything you need to know about using and managing the DLQ in backfill.

## Table of Contents

- [What is the DLQ?](#what-is-the-dlq)
- [Why You Need a DLQ](#why-you-need-a-dlq)
- [How It Works](#how-it-works)
- [Setup and Configuration](#setup-and-configuration)
- [Using the Client API](#using-the-client-api)
- [Using the HTTP Admin API](#using-the-http-admin-api)
- [Operational Best Practices](#operational-best-practices)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Common Workflows](#common-workflows)
- [Troubleshooting](#troubleshooting)

## What is the DLQ?

The Dead Letter Queue is a specialized queue where jobs that fail permanently are moved for manual inspection and reprocessing. Unlike temporary failures that trigger automatic retries, jobs in the DLQ have either:

1. **Exhausted all retry attempts** - Jobs that failed repeatedly despite the configured `max_attempts`
2. **Encountered unrecoverable errors** - Jobs with configuration problems, invalid data, or permanent system issues

The DLQ acts as a safety net, ensuring no job fails silently and all failures are visible for investigation and remediation.

## Why You Need a DLQ

Without a DLQ, permanently failed jobs would either:
- Disappear silently, causing data loss or missed operations
- Remain in the main queue forever, consuming resources
- Require manual database queries to discover and diagnose

The DLQ provides:

### Operational Visibility
- **Failure tracking** - See exactly which jobs failed and why
- **Pattern detection** - Identify systemic issues affecting multiple jobs
- **Audit trail** - Maintain records of failures for compliance and debugging

### Recovery Options
- **Manual intervention** - Fix underlying issues before reprocessing
- **Selective requeue** - Retry individual jobs after fixes are deployed
- **Data correction** - Modify job payloads before retrying
- **Cleanup** - Remove jobs that are no longer relevant

### System Health
- **Prevents queue pollution** - Failed jobs don't block healthy jobs
- **Resource management** - Failed jobs don't consume worker capacity
- **Alert integration** - Monitor DLQ growth for early problem detection

## How It Works

### Automatic DLQ Movement

Backfill automatically moves jobs to the DLQ when they meet failure criteria:

```rust
// Jobs are automatically moved to DLQ when:
// 1. Job attempts >= max_attempts
// 2. Job has exhausted all configured retries

// Example: This job will be moved to DLQ after 5 failed attempts
let spec = JobSpec {
    max_attempts: Some(5),
    priority: Priority::FAST_HIGH,
    ..Default::default()
};

client.enqueue("process_payment", &payload, spec).await?;
```

### DLQ Processor

The DLQ processor runs as a background task, periodically scanning for failed jobs:

```rust
// Configure DLQ processor interval
let worker_config = WorkerConfig {
    database_url: "postgresql://localhost/mydb".to_string(),
    schema: "graphile_worker".to_string(),
    // Scan for failed jobs every 60 seconds
    dlq_processor_interval: Some(Duration::from_secs(60)),
    ..Default::default()
};
```

**How it works:**
1. Every `dlq_processor_interval`, the processor queries for failed jobs
2. Jobs where `attempts >= max_attempts` are identified
3. Jobs are copied to the `backfill_dlq` table with failure metadata
4. Original jobs are deleted from the main `jobs` table
5. Operation is logged for audit purposes

### DLQ Job Metadata

Each DLQ entry preserves complete job context:

```rust
pub struct DlqJob {
    pub id: i64,                                  // DLQ-specific ID
    pub original_job_id: Option<i64>,             // Original job ID
    pub task_identifier: String,                  // Job type (e.g., "send_email")
    pub payload: serde_json::Value,               // Original job data
    pub queue_name: String,                       // Which queue it ran in
    pub priority: i32,                            // Original priority
    pub job_key: Option<String>,                  // Deduplication key
    pub max_attempts: Option<i32>,                // Configured retry limit
    pub failure_reason: String,                   // Human-readable reason
    pub failure_count: i32,                       // How many times it failed
    pub last_error: Option<serde_json::Value>,    // Last error details
    pub original_created_at: Option<DateTime>,    // When job was created
    pub original_run_at: Option<DateTime>,        // When job was scheduled
    pub failed_at: DateTime<Utc>,                 // When moved to DLQ
    pub requeued_count: i32,                      // Times requeued from DLQ
    pub last_requeued_at: Option<DateTime>,       // Last requeue time
    pub notes: Option<String>,                    // Admin notes
}
```

## Setup and Configuration

### Database Schema Setup

**Option 1: Automatic (Development)**

```rust
use backfill::BackfillClient;

let client = BackfillClient::new("postgresql://localhost/mydb").await?;
client.init_dlq().await?;  // Creates schema if needed
```

**Option 2: Migration-Based (Production)**

```bash
# Using psql
psql -d your_database -f docs/dlq_schema.sql

# With custom schema name
sed 's/graphile_worker/your_schema/g' docs/dlq_schema.sql | psql -d your_database
```

See [`DLQ_MIGRATIONS.md`](DLQ_MIGRATIONS.md) for integration with migration tools (Diesel, SQLx, Refinery).

### Worker Configuration

Enable DLQ processing in your worker:

```rust
use backfill::{WorkerConfig, WorkerRunner, QueueConfig};
use std::time::Duration;

let config = WorkerConfig {
    database_url: "postgresql://localhost/mydb".to_string(),
    schema: "graphile_worker".to_string(),
    
    queue_configs: vec![
        QueueConfig::named_queue("fast", 10),
        QueueConfig::named_queue("bulk", 5),
    ],
    
    // Scan for failed jobs every 60 seconds
    dlq_processor_interval: Some(Duration::from_secs(60)),
    
    poll_interval: Duration::from_millis(200),
};

let worker = WorkerRunner::builder(config)
    .await?
    .define_job::<YourJobHandler>()
    .build()
    .await?;
```

### Environment Configuration

```bash
# DLQ processor scan interval (default: 60 seconds)
export DLQ_PROCESSOR_INTERVAL_SECS=30

# Disable DLQ processor entirely (not recommended)
# Simply omit DLQ_PROCESSOR_INTERVAL_SECS or set dlq_processor_interval to None
```

## Using the Client API

The `BackfillClient` provides a complete Rust API for DLQ operations.

### Listing DLQ Jobs

```rust
use backfill::{BackfillClient, client::DlqFilter};

let client = BackfillClient::new("postgresql://localhost/mydb").await?;

// List all DLQ jobs with pagination
let filter = DlqFilter {
    limit: Some(50),
    offset: Some(0),
    ..Default::default()
};
let result = client.list_dlq_jobs(filter).await?;

println!("Found {} jobs (total: {})", result.jobs.len(), result.total);
for job in result.jobs {
    println!("Job {}: {} failed at {}", job.id, job.task_identifier, job.failed_at);
}
```

### Filtering DLQ Jobs

```rust
use chrono::{Utc, Duration};

// Find all failed email jobs from the last 24 hours
let filter = DlqFilter {
    task_identifier: Some("send_email".to_string()),
    failed_after: Some(Utc::now() - Duration::hours(24)),
    limit: Some(100),
    ..Default::default()
};
let email_failures = client.list_dlq_jobs(filter).await?;

// Find failures in a specific queue
let filter = DlqFilter {
    queue_name: Some("fast".to_string()),
    limit: Some(50),
    ..Default::default()
};
let fast_queue_failures = client.list_dlq_jobs(filter).await?;
```

### Getting DLQ Statistics

```rust
// Get comprehensive DLQ statistics
let stats = client.dlq_stats().await?;

println!("DLQ Statistics:");
println!("  Total jobs: {}", stats.total_jobs);
println!("  Unique task types: {}", stats.unique_tasks);
println!("  Average failure count: {:.1}", stats.avg_failure_count);
println!("  Total requeued: {}", stats.total_requeued);

if let Some(oldest) = stats.oldest_failure {
    println!("  Oldest failure: {}", oldest);
}

println!("\nTop failing job types:");
for (task, count) in stats.task_breakdown {
    println!("  {}: {} failures", task, count);
}
```

### Inspecting Individual Jobs

```rust
// Get detailed information about a specific DLQ job
let dlq_job = client.get_dlq_job(dlq_id).await?;

if let Some(job) = dlq_job {
    println!("Task: {}", job.task_identifier);
    println!("Failed at: {}", job.failed_at);
    println!("Failure count: {}", job.failure_count);
    println!("Reason: {}", job.failure_reason);
    println!("Payload: {}", serde_json::to_string_pretty(&job.payload)?);
    
    if let Some(error) = job.last_error {
        println!("Last error: {}", serde_json::to_string_pretty(&error)?);
    }
}
```

### Requeuing Jobs

```rust
// Requeue a job after fixing the underlying issue
let job = client.requeue_dlq_job(
    dlq_id, 
    Some("Fixed email validation bug in v2.1.0".to_string())
).await?;

println!("Job requeued with ID: {}", job.id());

// The original DLQ entry remains with updated requeue tracking:
// - requeued_count incremented
// - last_requeued_at updated
// - notes field updated
```

### Deleting DLQ Jobs

```rust
// Permanently delete a DLQ job that's no longer needed
let deleted = client.delete_dlq_job(dlq_id).await?;

if deleted {
    println!("DLQ job {} deleted", dlq_id);
} else {
    println!("DLQ job {} not found", dlq_id);
}
```

### Manual DLQ Addition

While typically automatic, you can manually add jobs to the DLQ:

```rust
use graphile_worker::Job;

// This is usually only needed for custom error handling logic
client.add_to_dlq(
    &original_job,
    "Custom failure reason: External API permanently unavailable",
    Some(serde_json::json!({"error": "API returned 410 Gone"}))
).await?;
```

## Using the HTTP Admin API

The backfill admin API provides RESTful endpoints for DLQ management. Enable it with the `axum` feature.

### Setup

```rust
use axum::Router;
use backfill::admin::{BackfillAdminState, create_admin_router};

#[derive(Clone)]
struct AppState {
    backfill: BackfillClient,
}

impl BackfillAdminState for AppState {
    fn backfill_client(&self) -> &BackfillClient {
        &self.backfill
    }
}

let app = Router::new()
    .nest("/admin/backfill", create_admin_router())
    .with_state(AppState { backfill: client });
```

See [`ADMIN_API.md`](ADMIN_API.md) for complete API documentation.

### API Endpoints

**List DLQ Jobs**
```bash
# Get first 50 DLQ jobs
curl "http://localhost:3000/admin/backfill/dlq?limit=50&offset=0"

# Filter by task type
curl "http://localhost:3000/admin/backfill/dlq?task_identifier=send_email"

# Filter by queue
curl "http://localhost:3000/admin/backfill/dlq?queue_name=fast"
```

**Get DLQ Statistics**
```bash
curl http://localhost:3000/admin/backfill/dlq/stats
```

**Get Specific Job**
```bash
curl http://localhost:3000/admin/backfill/dlq/123
```

**Requeue Job**
```bash
curl -X POST http://localhost:3000/admin/backfill/dlq/123/requeue
```

**Delete Job**
```bash
curl -X DELETE http://localhost:3000/admin/backfill/dlq/123
```

### Response Format

All endpoints return JSON with consistent structure:

**Success Response:**
```json
{
  "jobs": [...],
  "total_count": 150,
  "has_more": true
}
```

**Error Response:**
```json
{
  "error": "DLQ job not found",
  "code": "DLQ_JOB_NOT_FOUND",
  "timestamp": "2024-01-01T12:00:00Z"
}
```

## Operational Best Practices

### 1. Monitor DLQ Growth

Set up alerts when DLQ size exceeds thresholds:

```sql
-- Monitor DLQ size
SELECT COUNT(*) FROM graphile_worker.backfill_dlq;

-- Alert if growth is rapid
SELECT COUNT(*) FROM graphile_worker.backfill_dlq 
WHERE failed_at > NOW() - INTERVAL '1 hour';
```

**Alert Thresholds:**
- Warning: > 10 jobs/hour for more than 2 hours
- Critical: > 50 jobs/hour or > 500 total jobs

### 2. Regular DLQ Review Process

Establish a routine for reviewing and processing DLQ jobs:

```rust
// Daily DLQ review script
async fn daily_dlq_review(client: &BackfillClient) -> Result<(), Box<dyn std::error::Error>> {
    let stats = client.dlq_stats().await?;
    
    println!("Daily DLQ Report:");
    println!("  Total: {} jobs", stats.total_jobs);
    
    // Group by task type for focused investigation
    for (task, count) in stats.task_breakdown {
        println!("  {}: {} failures", task, count);
        
        // Get sample failures for this task type
        let filter = DlqFilter {
            task_identifier: Some(task.clone()),
            limit: Some(5),
            ..Default::default()
        };
        let samples = client.list_dlq_jobs(filter).await?;
        
        // Analyze failure patterns
        for job in samples.jobs {
            println!("    Job {}: {}", job.id, job.failure_reason);
        }
    }
    
    Ok(())
}
```

### 3. Categorize Failures

Different failure types require different responses:

| Failure Type | Response Strategy | Example |
|--------------|------------------|---------|
| **Data Quality** | Fix data validation, requeue | Invalid email format |
| **External Service** | Wait for service recovery, batch requeue | API timeout |
| **Code Bug** | Deploy fix, requeue affected jobs | Null pointer error |
| **Configuration** | Update config, requeue | Wrong API endpoint |
| **Permanent** | Delete job, notify stakeholders | User account deleted |

### 4. Implement Requeue Strategies

```rust
// Batch requeue jobs after deploying a fix
async fn batch_requeue(
    client: &BackfillClient,
    task_identifier: &str,
    since: DateTime<Utc>
) -> Result<u32, Box<dyn std::error::Error>> {
    let filter = DlqFilter {
        task_identifier: Some(task_identifier.to_string()),
        failed_after: Some(since),
        limit: Some(100),
        ..Default::default()
    };
    
    let mut requeued = 0;
    let jobs = client.list_dlq_jobs(filter).await?;
    
    for job in jobs.jobs {
        match client.requeue_dlq_job(job.id, Some(format!(
            "Requeued after fix deployed at {}",
            Utc::now()
        ))).await {
            Ok(_) => {
                requeued += 1;
                println!("Requeued job {}", job.id);
            }
            Err(e) => {
                eprintln!("Failed to requeue job {}: {}", job.id, e);
            }
        }
        
        // Avoid overwhelming the system
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    Ok(requeued)
}
```

### 5. Add Administrative Notes

Use the notes field to document investigations and actions:

```rust
// When requeuing, document why
client.requeue_dlq_job(
    dlq_id,
    Some(format!(
        "Bug fixed in v2.1.0 (commit abc123). \
         Root cause: Email validation regex was too strict. \
         Requeued by: ops@example.com"
    ))
).await?;
```

### 6. Set Retention Policies

Clean up old DLQ entries to prevent unbounded growth:

```sql
-- Archive jobs older than 90 days
INSERT INTO graphile_worker.backfill_dlq_archive 
SELECT * FROM graphile_worker.backfill_dlq 
WHERE failed_at < NOW() - INTERVAL '90 days';

DELETE FROM graphile_worker.backfill_dlq 
WHERE failed_at < NOW() - INTERVAL '90 days';
```

### 7. Test Failure Scenarios

Include DLQ behavior in your testing:

```rust
#[tokio::test]
async fn test_job_moves_to_dlq_after_max_attempts() {
    let client = test_client().await;
    
    // Enqueue job that will fail
    let spec = JobSpec {
        max_attempts: Some(3),
        ..Default::default()
    };
    let job = client.enqueue("failing_job", &payload, spec).await?;
    
    // Run worker (job will fail 3 times)
    run_worker_until_empty().await;
    
    // Verify job moved to DLQ
    let dlq_filter = DlqFilter {
        task_identifier: Some("failing_job".to_string()),
        ..Default::default()
    };
    let dlq_jobs = client.list_dlq_jobs(dlq_filter).await?;
    
    assert_eq!(dlq_jobs.jobs.len(), 1);
    assert_eq!(dlq_jobs.jobs[0].failure_count, 3);
}
```

## Monitoring and Alerting

### Key Metrics

**DLQ Size Metrics:**
```sql
-- Current DLQ size
SELECT COUNT(*) as dlq_size 
FROM graphile_worker.backfill_dlq;

-- DLQ growth rate (jobs/hour)
SELECT COUNT(*) as jobs_last_hour
FROM graphile_worker.backfill_dlq 
WHERE failed_at > NOW() - INTERVAL '1 hour';

-- Age of oldest unhandled failure
SELECT NOW() - MIN(failed_at) as oldest_failure_age
FROM graphile_worker.backfill_dlq 
WHERE requeued_count = 0;
```

**Failure Pattern Metrics:**
```sql
-- Failure rate by task type
SELECT 
    task_identifier,
    COUNT(*) as failure_count,
    AVG(failure_count) as avg_attempts,
    MAX(failed_at) as most_recent_failure
FROM graphile_worker.backfill_dlq 
WHERE failed_at > NOW() - INTERVAL '24 hours'
GROUP BY task_identifier
ORDER BY failure_count DESC;

-- Queue-specific failure rates
SELECT 
    queue_name,
    COUNT(*) as failures
FROM graphile_worker.backfill_dlq 
GROUP BY queue_name;
```

### Alerting Rules

**Prometheus/Grafana Example:**
```yaml
groups:
  - name: backfill_dlq
    interval: 60s
    rules:
      - alert: DLQHighGrowthRate
        expr: rate(dlq_jobs_total[1h]) > 10
        for: 2h
        labels:
          severity: warning
        annotations:
          summary: "DLQ growing rapidly"
          description: "More than 10 jobs/hour entering DLQ for 2+ hours"
      
      - alert: DLQSizeCritical
        expr: dlq_jobs_total > 500
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "DLQ size critical"
          description: "DLQ has {{ $value }} jobs"
      
      - alert: DLQOldFailures
        expr: time() - dlq_oldest_unhandled_timestamp > 86400
        labels:
          severity: warning
        annotations:
          summary: "Old DLQ jobs unhandled"
          description: "DLQ has jobs older than 24 hours"
```

### Dashboard Queries

Use the Rust client for monitoring dashboards:

```rust
use backfill::BackfillClient;

async fn dlq_dashboard_data(client: &BackfillClient) -> Result<DashboardData, Box<dyn std::error::Error>> {
    let stats = client.dlq_stats().await?;
    
    Ok(DashboardData {
        total_jobs: stats.total_jobs,
        unique_tasks: stats.unique_tasks,
        avg_failure_count: stats.avg_failure_count,
        total_requeued: stats.total_requeued,
        oldest_failure: stats.oldest_failure,
        newest_failure: stats.newest_failure,
        task_breakdown: stats.task_breakdown,
    })
}
```

## Common Workflows

### Workflow 1: Investigating a Spike

```bash
# 1. Check DLQ stats to identify the problem
curl http://localhost:3000/admin/backfill/dlq/stats

# 2. List recent failures
curl "http://localhost:3000/admin/backfill/dlq?limit=20" | jq '.jobs[] | {id, task: .task_identifier, reason: .failure_reason}'

# 3. Get detailed view of a specific failure
curl http://localhost:3000/admin/backfill/dlq/123 | jq '.payload, .last_error'

# 4. Check logs for the time period when failures occurred
# (use your logging system)

# 5. Deploy fix, then requeue affected jobs
for id in $(curl "http://localhost:3000/admin/backfill/dlq?task_identifier=send_email" | jq -r '.jobs[].id'); do
    curl -X POST "http://localhost:3000/admin/backfill/dlq/$id/requeue"
    sleep 0.1
done
```

### Workflow 2: Handling External Service Outages

```rust
// When an external service is down, jobs will accumulate in DLQ

// 1. Monitor the situation
let stats = client.dlq_stats().await?;
println!("DLQ has {} jobs", stats.total_jobs);

// 2. Once service recovers, batch requeue
let filter = DlqFilter {
    task_identifier: Some("call_external_api".to_string()),
    failed_after: Some(outage_start_time),
    limit: Some(100),
    ..Default::default()
};

let mut page = 0;
loop {
    let filter = DlqFilter {
        offset: Some(page * 100),
        ..filter.clone()
    };
    
    let jobs = client.list_dlq_jobs(filter).await?;
    
    if jobs.jobs.is_empty() {
        break;
    }
    
    for job in jobs.jobs {
        client.requeue_dlq_job(job.id, Some("Requeued after service recovery".to_string())).await?;
        tokio::time::sleep(Duration::from_millis(50)).await; // Throttle
    }
    
    page += 1;
}
```

### Workflow 3: Data Cleanup

```rust
// Remove jobs that are no longer relevant

// Find old jobs from deprecated task types
let filter = DlqFilter {
    task_identifier: Some("old_legacy_task".to_string()),
    ..Default::default()
};

let jobs = client.list_dlq_jobs(filter).await?;

for job in jobs.jobs {
    // Verify it's safe to delete
    if job.failed_at < cutoff_date {
        client.delete_dlq_job(job.id).await?;
        println!("Deleted old job: {}", job.id);
    }
}
```

## Troubleshooting

### Jobs Not Moving to DLQ

**Symptom:** Failed jobs remain in main queue or disappear

**Possible Causes:**
1. DLQ processor not running
2. `dlq_processor_interval` not configured
3. Jobs don't have `max_attempts` set
4. Database permissions issues

**Solutions:**
```rust
// Verify DLQ processor is enabled
let worker_config = WorkerConfig {
    dlq_processor_interval: Some(Duration::from_secs(60)), // Must be Some(...)
    ..config
};

// Ensure jobs have max_attempts
let spec = JobSpec {
    max_attempts: Some(5), // Required for DLQ movement
    ..Default::default()
};

// Manually trigger DLQ processing (for testing)
let moved = client.process_failed_jobs().await?;
println!("Moved {} jobs to DLQ", moved);
```

### DLQ Growing Too Fast

**Symptom:** Hundreds of jobs entering DLQ quickly

**Investigation:**
```rust
let stats = client.dlq_stats().await?;

// Check which task types are failing
for (task, count) in stats.task_breakdown {
    println!("{}: {} failures", task, count);
}

// Get recent failures for the top failing task
let filter = DlqFilter {
    task_identifier: Some(top_failing_task),
    limit: Some(10),
    ..Default::default()
};
let recent = client.list_dlq_jobs(filter).await?;

// Check error patterns
for job in recent.jobs {
    println!("{}: {}", job.id, job.failure_reason);
    if let Some(error) = job.last_error {
        println!("  Error: {}", error);
    }
}
```

**Common Causes:**
- External service outage (temporary)
- Code bug affecting all jobs of a type (requires hotfix)
- Configuration error (check environment variables)
- Database connection issues (check connection pool)

### Requeue Not Working

**Symptom:** Requeued jobs immediately fail again

**Checklist:**
1. Was the underlying issue actually fixed?
2. Is the fix deployed to all worker instances?
3. Are there dependencies (external services) still down?
4. Check worker logs for new error messages

```rust
// Add detailed logging to requeued jobs
let job = client.requeue_dlq_job(dlq_id, Some(format!(
    "Requeued at {} after deploying fix. Monitor closely.",
    Utc::now()
))).await?;

// Monitor the requeued job
tokio::time::sleep(Duration::from_secs(10)).await;

// Check if it failed again
let check = client.get_dlq_job(dlq_id).await?;
if let Some(job) = check {
    if job.requeued_count > 1 {
        println!("WARNING: Job failed again after requeue!");
    }
}
```

### Database Performance Issues

**Symptom:** DLQ queries are slow

**Solutions:**
```sql
-- Ensure indexes exist
\d graphile_worker.backfill_dlq

-- Should show indexes on:
-- - task_identifier
-- - failed_at
-- - queue_name
-- - job_key (partial, where NOT NULL)

-- If indexes are missing, create them:
CREATE INDEX IF NOT EXISTS idx_backfill_dlq_task_identifier 
ON graphile_worker.backfill_dlq (task_identifier);

CREATE INDEX IF NOT EXISTS idx_backfill_dlq_failed_at 
ON graphile_worker.backfill_dlq (failed_at DESC);

-- For very large DLQ tables, consider partitioning by failed_at
```

## See Also

- [Admin API Documentation](ADMIN_API.md) - Complete HTTP API reference
- [DLQ Migrations Guide](DLQ_MIGRATIONS.md) - Database schema setup
- [Database Setup](DATABASE_SETUP.md) - PostgreSQL configuration
- [Testing Guide](TESTING.md) - Testing DLQ functionality

## Support

For issues, questions, or feature requests related to the DLQ, please see the main README for contribution guidelines.
