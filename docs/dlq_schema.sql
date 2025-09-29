-- Dead Letter Queue (DLQ) Schema for Backfill Library
-- This file contains the complete SQL schema for the Dead Letter Queue functionality.
-- 
-- Usage:
--   Replace 'graphile_worker' with your desired schema name before running.
--   This matches the default schema used by graphile-worker and backfill library.
--
-- Example with psql:
--   psql -d your_database -f dlq_schema.sql
--
-- Example with a custom schema:
--   sed 's/graphile_worker/your_schema_name/g' dlq_schema.sql | psql -d your_database

-- Create the Dead Letter Queue table
-- This table stores jobs that have exceeded their maximum retry attempts
CREATE TABLE IF NOT EXISTS graphile_worker.backfill_dlq (
    -- Unique identifier for the DLQ entry
    id BIGSERIAL PRIMARY KEY,
    
    -- Original job identification and routing
    original_job_id BIGINT,                        -- Original job ID from jobs table (can be NULL)
    task_identifier VARCHAR(200) NOT NULL,         -- Job type/handler name
    payload JSONB NOT NULL,                        -- Original job payload
    
    -- Job specification when originally enqueued
    queue_name VARCHAR(100) NOT NULL DEFAULT 'default', -- Queue the job was processed in
    priority INTEGER NOT NULL DEFAULT 0,               -- Job priority level
    job_key VARCHAR(200),                              -- Optional job deduplication key
    max_attempts INTEGER,                              -- Maximum attempts before DLQ
    
    -- Failure tracking and analysis
    failure_reason TEXT NOT NULL,                     -- Why the job failed permanently
    failure_count INTEGER NOT NULL DEFAULT 1,         -- Total number of failures before DLQ
    last_error JSONB,                                 -- Last error details as JSON
    
    -- Timestamps
    original_created_at TIMESTAMPTZ,                  -- When original job was created
    original_run_at TIMESTAMPTZ,                      -- Original scheduled run time
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),     -- When job was moved to DLQ
    
    -- Administrative tracking
    requeued_count INTEGER NOT NULL DEFAULT 0,        -- Number of times requeued from DLQ
    last_requeued_at TIMESTAMPTZ,                     -- When last requeued (NULL if never)
    notes TEXT                                        -- Admin notes or additional context
);

-- Create indexes for efficient querying and admin operations

-- Primary access patterns for admin interfaces
CREATE INDEX IF NOT EXISTS idx_backfill_dlq_task_identifier 
    ON graphile_worker.backfill_dlq (task_identifier);

CREATE INDEX IF NOT EXISTS idx_backfill_dlq_failed_at 
    ON graphile_worker.backfill_dlq (failed_at DESC);

CREATE INDEX IF NOT EXISTS idx_backfill_dlq_queue_name 
    ON graphile_worker.backfill_dlq (queue_name);

-- Index for job key lookups (only when job_key is not NULL)
CREATE INDEX IF NOT EXISTS idx_backfill_dlq_job_key 
    ON graphile_worker.backfill_dlq (job_key) 
    WHERE job_key IS NOT NULL;

-- Comments for documentation
COMMENT ON TABLE graphile_worker.backfill_dlq IS 
    'Dead Letter Queue for jobs that have exceeded maximum retry attempts. Used for manual intervention and debugging.';

COMMENT ON COLUMN graphile_worker.backfill_dlq.original_job_id IS 
    'Original job ID from the jobs table when the job was moved to DLQ (can be NULL)';

COMMENT ON COLUMN graphile_worker.backfill_dlq.task_identifier IS 
    'Job type/handler name, used for routing and admin filtering';

COMMENT ON COLUMN graphile_worker.backfill_dlq.payload IS 
    'Original job payload as JSON, preserved for requeuing or analysis';

COMMENT ON COLUMN graphile_worker.backfill_dlq.queue_name IS 
    'Queue name where the job was being processed';

COMMENT ON COLUMN graphile_worker.backfill_dlq.priority IS 
    'Job priority level (higher numbers = higher priority)';

COMMENT ON COLUMN graphile_worker.backfill_dlq.job_key IS 
    'Optional job deduplication key for preventing duplicate jobs';

COMMENT ON COLUMN graphile_worker.backfill_dlq.max_attempts IS 
    'Maximum retry attempts that were configured when job failed';

COMMENT ON COLUMN graphile_worker.backfill_dlq.failure_reason IS 
    'High-level reason why the job failed permanently';

COMMENT ON COLUMN graphile_worker.backfill_dlq.failure_count IS 
    'Total number of failures before the job was moved to DLQ';

COMMENT ON COLUMN graphile_worker.backfill_dlq.last_error IS 
    'Last error details as JSON for debugging and analysis';

COMMENT ON COLUMN graphile_worker.backfill_dlq.original_created_at IS 
    'Timestamp when the original job was created';

COMMENT ON COLUMN graphile_worker.backfill_dlq.original_run_at IS 
    'Timestamp when the original job was scheduled to run';

COMMENT ON COLUMN graphile_worker.backfill_dlq.failed_at IS 
    'Timestamp when the job was moved to the Dead Letter Queue';

COMMENT ON COLUMN graphile_worker.backfill_dlq.requeued_count IS 
    'Number of times this job has been requeued from the DLQ for retry';

COMMENT ON COLUMN graphile_worker.backfill_dlq.last_requeued_at IS 
    'Timestamp of the most recent requeue operation (NULL if never requeued)';

COMMENT ON COLUMN graphile_worker.backfill_dlq.notes IS 
    'Administrative notes or additional context for manual intervention';
