# SQLx Compile-Time Query Verification Setup

SQLx provides excellent compile-time verification of database queries, ensuring your SQL is correct before your code even runs. This is one of Rust's greatest strengths for database applications.

## Overview

SQLx macros like `sqlx::query!()` and `sqlx::query_as!()` verify your SQL queries at compile time by:
- Connecting to your database during compilation
- Parsing and validating SQL syntax
- Checking table/column existence
- Verifying parameter types and counts
- Generating type-safe Rust code

## Setup Options

### Option 1: Online Mode (Recommended for CI)

Set the `DATABASE_URL` environment variable to enable compile-time verification:

```bash
# Local development
export DATABASE_URL="postgresql://localhost:5432/backfill"

# Or in .env file (copy from .env.example)
DATABASE_URL=postgresql://localhost:5432/backfill
```

**Pros:**
- Always up-to-date with current schema
- Works great in CI with test databases
- No additional files to maintain

**Cons:**
- Requires database connection during compilation
- Slower compilation times

### Option 2: Offline Mode (Recommended for Distribution)

Generate query metadata offline using:

```bash
# First, ensure DATABASE_URL is set
export DATABASE_URL="postgresql://localhost:5432/backfill"

# Generate the metadata
cargo sqlx prepare

# This creates .sqlx/sqlx-data.json
```

**Pros:**
- No database required for compilation
- Faster compilation
- Can be committed to version control
- Good for distributing crates

**Cons:**
- Must remember to regenerate after schema changes
- Metadata can become stale

### Option 3: Hybrid Approach (Best of Both Worlds)

Use online mode in development/CI and offline mode for releases:

```bash
# Development: Use online mode
export DATABASE_URL="postgresql://localhost:5432/backfill"
cargo build

# Before release: Generate offline metadata
cargo sqlx prepare
git add .sqlx/sqlx-data.json
git commit -m "Update SQLx query metadata"

# CI: Check metadata is current
cargo sqlx prepare --check
```

## Example Usage

### Basic Query

```rust
use sqlx::{PgPool, query!};

async fn get_job_count(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let result = query!(
        "SELECT COUNT(*) as count FROM graphile_worker.jobs"
    )
    .fetch_one(pool)
    .await?;
    
    Ok(result.count.unwrap_or(0))
}
```

### Query with Parameters

```rust
async fn get_failed_jobs(pool: &PgPool, max_attempts: i16) -> Result<Vec<JobInfo>, sqlx::Error> {
    let rows = query!(
        r#"
        SELECT id, task_identifier, queue_name, attempts, created_at
        FROM graphile_worker.jobs 
        WHERE attempts >= $1 
        ORDER BY created_at DESC
        "#,
        max_attempts
    )
    .fetch_all(pool)
    .await?;
    
    let jobs = rows.into_iter().map(|row| JobInfo {
        id: row.id,
        task_identifier: row.task_identifier,
        queue_name: row.queue_name,
        attempts: row.attempts,
        created_at: row.created_at,
    }).collect();
    
    Ok(jobs)
}
```

### Insert with Returning

```rust
async fn create_dlq_job(
    pool: &PgPool,
    task_id: &str,
    payload: &serde_json::Value,
    failure_reason: &str
) -> Result<i64, sqlx::Error> {
    let result = query!(
        r#"
        INSERT INTO graphile_worker.backfill_dlq 
        (task_identifier, payload, failure_reason)
        VALUES ($1, $2, $3)
        RETURNING id
        "#,
        task_id,
        payload,
        failure_reason
    )
    .fetch_one(pool)
    .await?;
    
    Ok(result.id)
}
```

## Database Schema Requirements

For compile-time verification to work, SQLx needs access to your database schema. Ensure:

1. **Database exists and is accessible**
2. **GraphileWorker tables are set up** (run migrations first)  
3. **DLQ tables exist** (run `client.init_dlq()` or manual migration)
4. **Connection permissions** allow schema introspection

## Setting Up for This Project

### 1. Create Database

```bash
# Create database
createdb backfill

# Or with custom settings
createdb -h localhost -p 5432 -U postgres backfill
```

### 2. Run Migrations

```bash
# Set DATABASE_URL
export DATABASE_URL="postgresql://localhost:5432/backfill"

# Run the example to set up tables
cargo run --example basic_worker
```

### 3. Verify Setup

```bash
# This should work without errors
cargo check

# Or generate offline metadata
cargo sqlx prepare
```

## CI/CD Integration

### GitHub Actions

```yaml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: backfill
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
    
    steps:
    - uses: actions/checkout@v4
    - uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        
    - name: Check SQLx queries
      run: cargo sqlx prepare --check
      env:
        DATABASE_URL: postgresql://postgres:postgres@localhost:5432/backfill
        
    - name: Run tests
      run: cargo test
      env:
        DATABASE_URL: postgresql://postgres:postgres@localhost:5432/backfill
```

### Using Offline Mode in CI

If you prefer offline mode:

```yaml
- name: Check SQLx metadata is current
  run: cargo sqlx prepare --check
  # No DATABASE_URL needed - uses .sqlx/sqlx-data.json
```

## Troubleshooting

### "database schema is out of date"

```bash
# Regenerate metadata
cargo sqlx prepare
```

### "failed to connect to database"

```bash
# Check connection
psql $DATABASE_URL -c "SELECT 1;"

# Verify GraphileWorker schema exists
psql $DATABASE_URL -c "\\dt graphile_worker.*"
```

### "relation does not exist"

Ensure all tables are created:

```bash
# Run example to create tables
cargo run --example basic_worker

# Or manually create DLQ table
cargo run --example admin_server
```

### Migration Strategy for Existing Projects

1. **Start with read-only queries** - less risk
2. **Convert critical paths first** - highest value
3. **Use hybrid approach** - online for development, offline for releases
4. **Add CI checks gradually** - prevent regressions

## Best Practices

1. **Always regenerate metadata** after schema changes
2. **Commit .sqlx/sqlx-data.json** for reproducible builds
3. **Use descriptive query comments** for complex queries
4. **Test both online and offline modes** in CI
5. **Keep DATABASE_URL in .env** for local development

## Performance Impact

- **Compile time**: +10-30% with online mode, minimal with offline
- **Runtime**: Zero overhead - queries are pre-compiled
- **Binary size**: Slightly larger due to generated code
- **Safety**: Eliminates entire classes of runtime errors

The safety benefits far outweigh the small compilation cost!
