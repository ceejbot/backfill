# Incomplete Features & TODOs

This document tracks partially-implemented features and known limitations in the backfill codebase.

**Last Updated:** 2025-09-30

## Recent Changes

### Metrics System (2025-09-30)
✅ **COMPLETED** - Metrics system updated with:
- All metric names now use underscores for Prometheus compatibility (`backfill_jobs_enqueued` not `backfill.jobs.enqueued`)
- Automatic metrics for job enqueuing, DLQ operations, and database operations
- Public `metrics` module with `JobMetrics` helper for easy manual instrumentation
- Comprehensive documentation in `docs/METRICS.md`

**Note:** Job lifecycle metrics (start/complete/fail/duration) require manual instrumentation due to GraphileWorker's lack of lifecycle hooks. Helper utilities provided.

## High Priority - Core Library Features

### 1. WorkerRunner::process_available_jobs()
**Location:** `src/worker.rs:467-478`

**Current State:** Stub implementation that returns 0 without doing anything

**What Needs to be Implemented:**
- Query the job count from the database
- Process available jobs without starting persistent polling loop
- Return actual count of jobs processed

**Intended Use Case:** Batch processing or testing scenarios where you want to process the current job queue without running a persistent worker

**Current Workaround:** Documentation suggests using `run_until_cancelled` with a short timeout instead

---

## Medium Priority - Admin API Features

These are all feature-gated behind the `axum` feature flag.

### 2. Admin API: get_job()
**Location:** `src/admin.rs:285-298`

**Current State:** Returns HTTP 501 NOT_IMPLEMENTED

**What Needs to be Implemented:**
- Query GraphileWorker jobs table by job_id
- Return job details including payload, status, attempts, etc.

**Endpoint:** GET /jobs/:job_id

### 3. Admin API: cancel_job()
**Location:** `src/admin.rs:301-318`

**Current State:** Returns HTTP 501 NOT_IMPLEMENTED

**What Needs to be Implemented:**
- Update GraphileWorker jobs table to mark job as cancelled
- Prevent job from being picked up by workers

**Endpoint:** DELETE /jobs/:job_id

**Notes:** May need to research GraphileWorker's cancellation mechanism

### 4. Admin API: system_status()
**Location:** `src/admin.rs:185-213`

**Current State:** Returns hardcoded static data with all zeros

**What Needs to be Implemented:**
- Query database for actual queue statistics
- Calculate pending_jobs, active_jobs, completed_jobs, failed_jobs per queue

**Endpoint:** GET /status

**Current Return:**
```rust
QueueStatus {
    queue_name: "fast" | "bulk",
    pending_jobs: 0,  // hardcoded
    active_jobs: 0,   // hardcoded
    completed_jobs: 0, // hardcoded
    failed_jobs: 0,   // hardcoded
}
```

### 5. Admin API: list_queues()
**Location:** `src/admin.rs:321-346`

**Current State:** Returns hardcoded list of "fast" and "bulk" queues with zero counts

**What Needs to be Implemented:**
- Query database for actual queue list (dynamic queue discovery)
- Get real statistics for each queue
- Consider custom queues beyond just "fast" and "bulk"

**Endpoint:** GET /queues

### 6. Admin API: queue_stats()
**Location:** `src/admin.rs:349-368`

**Current State:** Returns hardcoded zeros for all metrics

**What Needs to be Implemented:**
- Query database for specific queue statistics
- Return actual counts for pending/active/completed/failed jobs

**Endpoint:** GET /queues/:queue_name/stats

### 7. Admin API: dlq_cleanup()
**Location:** `src/admin.rs:545-562`

**Current State:** Returns HTTP 501 NOT_IMPLEMENTED

**What Needs to be Implemented:**
- Batch cleanup of old DLQ jobs
- Parameters for cleanup criteria (age, status, task type, etc.)
- Return count of cleaned up jobs

**Endpoint:** POST /dlq/cleanup

**Notes:** Should probably accept query parameters or JSON body to specify cleanup criteria

---

## Low Priority - Known Limitations/Workarounds

### 8. DLQ pagination optimization
**Location:** `src/client/dlq.rs:238`

**Current State:** Uses separate COUNT query for pagination, could be more efficient

**Code Comment:** `// Get total count for pagination (simplified - could be optimized)`

**Possible Optimizations:**
- Use window functions to get count with main query
- Cache count with short TTL
- Accept approximate counts for large result sets
- Use cursor-based pagination instead of offset/limit

---

## Documentation TODOs

### 11. BackfillClient schema field documentation
**Location:** `src/client/mod.rs:13-14`

**Current State:**
```rust
/// a psql connection pool
pool: sqlx::PgPool,
/// TODO document
schema: String,
```

**Needs:** Proper documentation explaining:
- What the schema field is (GraphileWorker schema name)
- Default value ("graphile_worker")
- Why it's configurable
- Example usage

### 12. queries.rs placeholder
**Location:** `src/client/queries.rs:117-118`

**Current State:** File contains query documentation/examples but note says:
```rust
// Note: Actual query implementations would go here if DATABASE_URL is available
// For now, this serves as documentation and examples for users to implement
```

**Status:** May be intentional - serves as documentation/reference

**Consider:** Clarify purpose of this file or move examples to proper docs

---

## Dead Code

### 13. JobHandlerConfig.identifier field
**Location:** `src/worker.rs:205-206`

**Current State:**
```rust
#[derive(Clone)]
pub(crate) struct JobHandlerConfig {
    #[allow(dead_code)]
    pub identifier: String,
    pub builder_fn: fn(WorkerOptions) -> WorkerOptions,
}
```

**Issue:** `identifier` field is never read

**Decision Needed:**
- Remove the field if truly unnecessary
- Use it for debugging/logging purposes
- Use it for metrics tagging
- Keep for future use (document why)

---

## Summary by Priority

**High Priority (blocks core functionality):**
- WorkerRunner::process_available_jobs() - batch processing use case

**Medium Priority (admin API features):**
- 6 admin endpoints returning stub/static data
- All feature-gated, so not blocking main library use

**Low Priority (workarounds exist):**
- 1 known limitation with acceptable workaround
- 2 documentation TODOs
- 1 dead code cleanup

**Total Items:** 11
