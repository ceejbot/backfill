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
- 2 admin endpoints returning stub/static data
- All feature-gated, so not blocking main library use

**Low Priority (workarounds exist):**
- 1 known limitation with acceptable workaround
- 2 documentation TODOs
- 1 dead code cleanup

**Total Items:** 7
