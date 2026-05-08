# Backfill Admin API

> **⚠️ EXPERIMENTAL**: The Admin API is currently experimental and subject to change. Several endpoints return stub data or are not fully implemented. For production use, rely on the core job enqueueing and worker APIs which are stable. See [API Status](#api-status) below for details.

The Backfill Admin API provides a library-first HTTP API for managing job queues, monitoring system health, and administering the Dead Letter Queue (DLQ). The API is designed to be easily integrated into existing Axum applications.

## Quick Start

Enable the `axum` feature in your `Cargo.toml`:

```toml
[dependencies]
backfill = { version = "2", features = ["axum"] }
```

## Basic Integration

```rust
use axum::Router;
use backfill::{
    admin::{BackfillAdminState, create_admin_router},
    BackfillClient,
};

#[derive(Clone)]
struct AppState {
    backfill: BackfillClient,
    // ... your other app state
}

impl BackfillAdminState for AppState {
    fn backfill_client(&self) -> &BackfillClient {
        &self.backfill
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState {
        backfill: BackfillClient::new("postgresql://localhost/mydb").await?,
    };

    let app = Router::new()
        .nest("/admin/backfill", create_admin_router())
        .with_state(state);

    // ... serve your app
    Ok(())
}
```

## API Endpoints

### Health and Status

- **GET `/health`** - Health check endpoint
  ```json
  {
    "status": "healthy",
    "version": "0.1.0",
    "timestamp": "2024-01-01T00:00:00Z"
  }
  ```

- **GET `/status`** - System status overview. The `queues` array is
  populated by querying `_private_job_queues` (one entry per known
  serial queue name) plus a synthetic `"default"` entry if any
  parallel-queue jobs exist. `completed_jobs` is always reported as 0
  — graphile_worker deletes completed rows from `_private_jobs`, so
  that number is only available via the metrics system, not the DB.
  ```json
  {
    "queues": [
      {
        "queue_name": "default",
        "pending_jobs": 5,
        "active_jobs": 2,
        "completed_jobs": 0,
        "failed_jobs": 3
      }
    ],
    "dlq_enabled": true,
    "dlq_job_count": 3,
    "total_jobs": 10
  }
  ```

### Job Management

- **POST `/jobs`** - Enqueue a new job. The `queue` field accepts:
  `"parallel"` or `""` for parallel execution (the default — jobs run
  concurrently across workers), or any other string for a serial queue
  with that name (one job at a time, cluster-wide).
  ```json
  {
    "task_identifier": "send_email",
    "payload": {"to": "user@example.com", "subject": "Welcome!"},
    "queue": "parallel",
    "priority": -10,
    "max_attempts": 5,
    "job_key": "email_12345",
    "run_at": "2024-01-01T12:00:00Z"
  }
  ```

- **GET `/jobs/:job_id`** - *Not implemented* — returns 501.
- **DELETE `/jobs/:job_id`** - *Not implemented* — returns 501.

### Queue Management

- **GET `/queues`** - List all queues with statistics
- **GET `/queues/:queue_name/stats`** - Get detailed stats for a specific queue

### Dead Letter Queue (DLQ) Management

- **GET `/dlq`** - List DLQ jobs with pagination
  - Query parameters: `limit`, `offset`, `queue_name`, `task_identifier`
  - `queue_name` matches stored values: empty string for parallel-origin
    jobs, the queue name for serial-origin jobs.
  ```json
  {
    "jobs": [
      {
        "id": 123,
        "original_job_id": 9876,
        "task_identifier": "send_email",
        "payload": {"to": "invalid@email"},
        "queue_name": "",
        "priority": -10,
        "failure_count": 1,
        "failed_at": "2024-01-01T10:00:00Z",
        "last_error": "Invalid email address"
      }
    ],
    "total_count": 50,
    "has_more": true
  }
  ```

- **GET `/dlq/stats`** - DLQ statistics
  ```json
  {
    "total_jobs": 50,
    "unique_tasks": 8,
    "unique_queues": 3,
    "avg_failure_count": 1.4,
    "total_requeued": 12,
    "oldest_failure": "2024-01-01T09:00:00Z",
    "newest_failure": "2024-01-01T11:00:00Z",
    "task_breakdown": [["send_email", 30], ["process_order", 12]]
  }
  ```

- **GET `/dlq/:dlq_id`** - Get specific DLQ job details
- **DELETE `/dlq/:dlq_id`** - Permanently delete a DLQ job
- **POST `/dlq/:dlq_id/requeue`** - Move a DLQ job back to the main queue
  ```json
  {
    "job_id": 12345,
    "status": "requeued",
    "requeued_at": "2024-01-01T12:00:00Z"
  }
  ```

- **POST `/dlq/cleanup`** - Bulk-delete DLQ jobs older than a cutoff
  (default 90 days). Body: `{ "older_than_days": 30, "task_identifier":
  "send_email", "max_jobs": 1000, "dry_run": true }` (all fields
  optional).
- **POST `/dlq/batch-requeue`** - Requeue many DLQ jobs in one request,
  with throttling and dry-run support.
- **POST `/dlq/batch-delete`** - Bulk-delete DLQ jobs by filter (separate
  from `/dlq/cleanup`, which is age-based).

### Lock Diagnostics

- **GET `/locks/status`** - Current queue and job locks, with a
  `health_status` summary (healthy / warning / unhealthy) and stale-lock
  counts. Useful for diagnosing crashed workers.
- **POST `/locks/cleanup`** - Manually release stale locks and optionally
  delete permanently-failed jobs. Supports `dry_run` and override
  thresholds.

## Error Handling

All endpoints return consistent error responses:

```json
{
  "error": "Job not found",
  "code": "JOB_NOT_FOUND",
  "timestamp": "2024-01-01T12:00:00Z"
}
```

Common error codes:
- `ENQUEUE_FAILED` - Job enqueueing failed
- `JOB_NOT_FOUND` - Specified job doesn't exist
- `DLQ_JOB_NOT_FOUND` - DLQ job not found
- `DLQ_LIST_FAILED` - Failed to list DLQ jobs
- `DLQ_STATS_FAILED` - Failed to get DLQ statistics
- `NOT_IMPLEMENTED` - Feature not yet implemented

## Security Considerations

The admin API provides powerful administrative capabilities. Consider:

1. **Authentication** - Add authentication middleware before mounting the router
2. **Authorization** - Restrict access to appropriate users/roles
3. **Rate Limiting** - Prevent abuse of job enqueueing endpoints
4. **Network Security** - Bind to internal interfaces only or use reverse proxy

Example with basic auth middleware:

```rust
use tower_http::auth::RequireAuthorizationLayer;

let app = Router::new()
    .nest(
        "/admin/backfill", 
        create_admin_router()
            .layer(RequireAuthorizationLayer::bearer("your-secret-token"))
    )
    .with_state(state);
```

## CORS Support

The API is designed to work well with browser-based admin UIs. Add CORS middleware as needed:

```rust
use tower_http::cors::{CorsLayer, Any};

let app = Router::new()
    .nest("/admin/backfill", create_admin_router())
    .layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST, Method::DELETE])
            .allow_headers(Any)
    )
    .with_state(state);
```

## Monitoring Integration

The admin API endpoints are perfect for:
- Building admin dashboards
- Health check monitoring
- Alerting on DLQ growth
- Performance monitoring
- Automated queue management

## Best Practices

1. **Graceful Degradation** - Handle API failures gracefully in your application
2. **Caching** - Cache status/stats responses for high-traffic scenarios  
3. **Pagination** - Always use pagination for DLQ listing in production
4. **Logging** - The API includes structured logging for all operations
5. **Monitoring** - Monitor API response times and error rates
6. **Testing** - Test your BackfillAdminState implementation thoroughly

## Example: Complete Admin Server

See `examples/admin_server.rs` for a complete example showing:
- How to integrate the admin API with your application
- Adding CORS and logging middleware
- Environment-based configuration
- Graceful shutdown handling
- Example job handlers

Run the example:

```bash
DATABASE_URL=postgresql://localhost/backfill cargo run --example admin_server --features axum
```

Then test the endpoints:

```bash
# Health check
curl http://localhost:3000/admin/backfill/health

# Enqueue a job
curl -X POST http://localhost:3000/admin/backfill/jobs \
  -H "Content-Type: application/json" \
  -d '{"task_identifier": "send_email", "payload": {"to": "user@example.com"}}'

# List DLQ jobs
curl "http://localhost:3000/admin/backfill/dlq?limit=10"
```

## API Status

The following table reflects the current state of every route registered
in `src/admin.rs`:

| Endpoint | Method | Status | Notes |
|----------|--------|--------|-------|
| `/health` | GET | ✅ Stable | Static response with crate version |
| `/status` | GET | ✅ Stable | Live queue + DLQ stats from the DB. `completed_jobs` is always `0` (graphile_worker deletes completed rows) |
| `/jobs` | POST | ✅ Stable | Full enqueue with priority, max_attempts, job_key, run_at |
| `/jobs/{job_id}` | GET | ⚠️ Not implemented | Returns 501 |
| `/jobs/{job_id}/cancel` | DELETE | ⚠️ Not implemented | Returns 501 |
| `/queues` | GET | ✅ Stable | Live list of queue names with per-queue counts |
| `/queues/{queue_name}/stats` | GET | ✅ Stable | Live counts for a single queue |
| `/dlq` | GET | ✅ Stable | Filtering + pagination |
| `/dlq/stats` | GET | ✅ Stable | Aggregate counters and per-task breakdown |
| `/dlq/{dlq_id}` | GET | ✅ Stable | |
| `/dlq/{dlq_id}` | DELETE | ✅ Stable | |
| `/dlq/{dlq_id}/requeue` | POST | ✅ Stable | |
| `/dlq/cleanup` | POST | ✅ Stable | Age-based bulk delete with `dry_run` |
| `/dlq/batch-requeue` | POST | ✅ Stable | Filter-based bulk requeue with throttling |
| `/dlq/batch-delete` | POST | ✅ Stable | Filter-based bulk delete |
| `/locks/status` | GET | ✅ Stable | Queue + job locks with stale-lock health summary |
| `/locks/cleanup` | POST | ✅ Stable | Manual stale-lock cleanup with `dry_run` |

The two unimplemented endpoints (`GET /jobs/{id}` and `DELETE
/jobs/{id}/cancel`) sit on graphile_worker semantics that aren't cleanly
expressible against the public API; they remain stubs until upstream
exposes the necessary primitives. See `src/admin.rs` for implementation
details if you'd like to contribute.
