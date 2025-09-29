# Backfill Admin API

The Backfill Admin API provides a library-first HTTP API for managing job queues, monitoring system health, and administering the Dead Letter Queue (DLQ). The API is designed to be easily integrated into existing Axum applications.

## Quick Start

Enable the `axum` feature in your `Cargo.toml`:

```toml
[dependencies]
backfill = { version = "0.1", features = ["axum"] }
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

- **GET `/status`** - System status overview
  ```json
  {
    "queues": [
      {
        "queue_name": "fast",
        "pending_jobs": 5,
        "active_jobs": 2,
        "completed_jobs": 100,
        "failed_jobs": 3
      }
    ],
    "dlq_enabled": true,
    "dlq_job_count": 3,
    "total_jobs": 110
  }
  ```

### Job Management

- **POST `/jobs`** - Enqueue a new job
  ```json
  {
    "task_identifier": "send_email",
    "payload": {"to": "user@example.com", "subject": "Welcome!"},
    "queue": "fast",
    "priority": -10,
    "max_attempts": 5,
    "job_key": "email_12345",
    "run_at": "2024-01-01T12:00:00Z"
  }
  ```

- **GET `/jobs/:job_id`** - Get job details *(coming soon)*
- **DELETE `/jobs/:job_id/cancel`** - Cancel a pending job *(coming soon)*

### Queue Management

- **GET `/queues`** - List all queues with statistics
- **GET `/queues/:queue_name/stats`** - Get detailed stats for a specific queue

### Dead Letter Queue (DLQ) Management

- **GET `/dlq`** - List DLQ jobs with pagination
  - Query parameters: `limit`, `offset`, `queue_name`, `task_identifier`
  ```json
  {
    "jobs": [
      {
        "id": 123,
        "original_job_id": "550e8400-e29b-41d4-a716-446655440000",
        "task_identifier": "send_email",
        "payload": {"to": "invalid@email"},
        "queue_name": "fast",
        "priority": -10,
        "failure_count": 5,
        "failed_at": "2024-01-01T10:00:00Z",
        "last_error": {"error": "Invalid email address"}
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
    "oldest_failed_at": "2024-01-01T09:00:00Z",
    "newest_failed_at": "2024-01-01T11:00:00Z"
  }
  ```

- **GET `/dlq/:dlq_id`** - Get specific DLQ job details
- **DELETE `/dlq/:dlq_id`** - Permanently delete a DLQ job
- **POST `/dlq/:dlq_id/requeue`** - Move a DLQ job back to the main queue
  ```json
  {
    "job_id": "550e8400-e29b-41d4-a716-446655440001",
    "status": "requeued",
    "requeued_at": "2024-01-01T12:00:00Z"
  }
  ```

- **POST `/dlq/cleanup`** - Bulk cleanup old DLQ jobs *(coming soon)*

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
