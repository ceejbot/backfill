# Dead Letter Queue SQL Migrations

This directory contains SQL migration files for setting up the Dead Letter Queue (DLQ) functionality in the backfill library.

## Files

- **`dlq_schema.sql`** - Complete DLQ schema with table creation, indexes, and documentation

## Usage Options

### Option 1: Automatic Initialization (Recommended)

The backfill library can automatically initialize the DLQ schema when needed:

```rust
use backfill::BackfillClient;

// Default schema ("graphile_worker"):
let client = BackfillClient::new("postgresql://localhost/mydb").await?;

// Or, with a custom schema:
let client = BackfillClient::new_with_schema(
    "postgresql://localhost/mydb",
    "my_schema",
).await?;

// This will create the DLQ table if it doesn't exist
client.init_dlq().await?;
```

### Option 2: Manual Migration with SQL Files

For production environments where you prefer explicit migration control:

#### Using psql

```bash
# Replace schema name if needed
sed 's/graphile_worker/your_schema_name/g' docs/dlq_schema.sql | psql -d your_database

# Or run directly if using the default schema
psql -d your_database -f docs/dlq_schema.sql
```

#### Using migration tools

**With diesel-cli:**
```bash
# Copy the SQL content into a diesel migration
diesel migration generate add_dlq_schema
# Copy dlq_schema.sql content to the up.sql file
```

**With sqlx-cli:**
```bash
# Create a new migration
sqlx migrate add add_dlq_schema
# Copy dlq_schema.sql content to the migration file
```

**With refinery or other tools:**
```bash
# Copy dlq_schema.sql to your migrations directory
cp docs/dlq_schema.sql migrations/001_add_dlq_schema.sql
```

## Schema Customization

### Changing the Schema Name

The default schema is `graphile_worker` to match graphile-worker conventions. To use a different schema:

```bash
# Replace all occurrences of the schema name
sed 's/graphile_worker/your_custom_schema/g' docs/dlq_schema.sql > custom_dlq_schema.sql
psql -d your_database -f custom_dlq_schema.sql
```

### Adding Custom Columns

You can extend the DLQ table with additional columns for your specific needs:

```sql
-- Add custom columns after initial schema creation
ALTER TABLE your_schema.backfill_dlq 
ADD COLUMN tenant_id TEXT,
ADD COLUMN environment TEXT DEFAULT 'production',
ADD COLUMN custom_metadata JSONB;

-- Add corresponding indexes
CREATE INDEX idx_backfill_dlq_tenant 
    ON your_schema.backfill_dlq (tenant_id);
```

## Schema Overview

The DLQ table provides comprehensive job failure tracking. For the full
DDL, see [`docs/dlq_schema.sql`](dlq_schema.sql) or the `init_dlq()`
implementation in `src/client/dlq.rs`.

### Core fields
- **`id`** — DLQ entry primary key (BIGSERIAL)
- **`original_job_id`** — id of the source row in `_private_jobs`
- **`task_identifier`** — task name (e.g. `"send_email"`)
- **`payload`** — original job payload (JSONB)
- **`queue_name`** — `""` for parallel-origin jobs, the queue name for
  serial-origin jobs (preserved across requeue)
- **`priority`**, **`job_key`**, **`max_attempts`** — original job spec

### Failure analysis
- **`failure_reason`** — human-readable summary
- **`failure_count`** — DLQ-touch counter (1 on first landing, +1 each
  subsequent requeue-then-fail, via UPSERT on `job_key`)
- **`last_error`** — most recent error (JSONB)
- **`failed_at`** — when the row landed in the DLQ
- **`original_created_at`** / **`original_run_at`** — original timestamps

### Administrative features
- **`requeued_count`** — times the row has been requeued from the DLQ
- **`last_requeued_at`** — most recent requeue
- **`notes`** — free-form admin notes

### Performance Indexes

The schema includes optimized indexes for:
- **Admin interfaces** - Fast filtering by task type, queue, and time
- **Statistics queries** - Efficient aggregation for monitoring
- **Requeue operations** - Quick lookup for admin actions

## Integration with Backfill Library

Once the schema is created, the library provides full admin functionality:

```rust
// Get DLQ statistics
let stats = client.dlq_stats().await?;
println!("Total DLQ jobs: {}", stats.total_jobs);

// List failed jobs with filtering
let filter = DlqFilter {
    task_identifier: Some("send_email".to_string()),
    queue_name: Some("notifications".to_string()),
    limit: Some(50),
    offset: Some(0),
};
let jobs = client.list_dlq_jobs(filter).await?;

// Requeue a job for retry
client.requeue_dlq_job(job_id, None).await?;

// Delete permanently failed jobs
client.delete_dlq_job(job_id).await?;
```

## Monitoring and Alerts

The DLQ schema is designed for easy monitoring integration:

```sql
-- Monitor DLQ growth
SELECT COUNT(*) FROM your_schema.backfill_dlq 
WHERE failed_at > NOW() - INTERVAL '1 hour';

-- Check failure patterns
SELECT task_identifier, COUNT(*), AVG(failure_count)
FROM your_schema.backfill_dlq 
GROUP BY task_identifier
ORDER BY COUNT(*) DESC;

-- Recent high-failure jobs
SELECT * FROM your_schema.backfill_dlq 
WHERE failure_count > 5 
AND failed_at > NOW() - INTERVAL '24 hours'
ORDER BY failed_at DESC;
```

Set up alerts when DLQ job counts exceed thresholds or when specific job types consistently fail.
