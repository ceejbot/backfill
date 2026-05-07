# Database Setup and GraphileWorker Contract

This document explains how the `backfill` library handles database setup and the contract between your application and GraphileWorker.

## Automatic Schema Management

**No manual database migrations required!** 

The `backfill` library uses GraphileWorker, which automatically creates and manages its own database schema. When you initialize a `BackfillClient`, the following happens automatically:

1. **Schema Creation**: GraphileWorker creates a `graphile_worker` schema (or your custom schema name)
2. **Table Creation**: All necessary tables are created automatically
3. **Migration Management**: GraphileWorker handles its own schema versioning and migrations

## Database Schema Structure

GraphileWorker creates the following tables automatically:

### Core Tables

- **`_private_jobs`** - Stores individual jobs with their payload, priority, and execution state
- **`_private_tasks`** - Stores task type definitions (task identifiers)
- **`_private_job_queues`** - Manages named queues for organizing work
- **`_private_known_crontabs`** - Handles recurring/scheduled tasks
- **`migrations`** - Tracks GraphileWorker's own schema migrations

### Key Schema Details

```sql
-- Jobs table (simplified view)
_private_jobs:
  - id: bigint (primary key)
  - task_id: integer (references _private_tasks.id)
  - job_queue_id: integer (references _private_job_queues.id) 
  - payload: json
  - priority: smallint (-32768 to 32767, lower = higher priority)
  - run_at: timestamptz
  - attempts: smallint
  - max_attempts: smallint
  - key: text (unique identifier for deduplication)
  - created_at: timestamptz
  - updated_at: timestamptz

-- Task definitions
_private_tasks:
  - id: integer (primary key)
  - identifier: text (unique, your task name like \"send_email\")

-- Queue definitions  
_private_job_queues:
  - id: integer (primary key)
  - queue_name: text (unique, like \"fast\", \"bulk\", \"emails\")
```

## Database Requirements

### Minimum Requirements

- **PostgreSQL 11+** (GraphileWorker requirement)
- **Connection permissions**: Your database user must be able to:
  - Create schemas (`CREATE SCHEMA`)
  - Create tables, indexes, and functions
  - Insert, update, delete, and select data

### Recommended Setup

```sql
-- Create a dedicated database for your application
CREATE DATABASE myapp;

-- Create a dedicated user with appropriate permissions
CREATE USER myapp_worker WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE myapp TO myapp_worker;

-- Grant schema creation permissions
GRANT CREATE ON DATABASE myapp TO myapp_worker;
```

## Connection String Examples

```rust
use backfill::BackfillClient;

// Basic connection (GraphileWorker will create the schema)
let client = BackfillClient::new(\"postgresql://user:password@localhost/mydb\").await?;

// With custom schema name
let client = BackfillClient::new_with_schema(
    \"postgresql://user:password@localhost/mydb\",
    \"custom_worker_schema\"
).await?;

// With existing connection pool
let pool = PgPoolOptions::new()
    .max_connections(10)
    .connect(\"postgresql://user:password@localhost/mydb\")
    .await?;
let client = BackfillClient::with_pool(pool).await?;
```

## What Happens on First Run

When you call any of the `BackfillClient` constructors:

1. **Schema Check**: GraphileWorker checks if its schema exists
2. **Automatic Migration**: If not, it creates the schema and all tables
3. **Version Tracking**: Future runs will apply any necessary schema updates automatically
4. **Ready to Use**: Your client is immediately ready to enqueue and process jobs

## Development vs Production

### Development
- GraphileWorker can create the schema on-demand
- No separate migration step needed
- Great for rapid iteration

### Production
- Same automatic behavior, but consider:
  - **Database Permissions**: Ensure your app has schema creation rights, or
  - **Pre-initialization**: Run schema setup during deployment with elevated permissions
  - **Monitoring**: Monitor the `graphile_worker.migrations` table for schema changes

### Production Deployment Options

**Option 1: App-managed (Recommended)**
```rust
// Your application automatically handles schema setup
let client = BackfillClient::new(&database_url).await?;
```

**Option 2: Deployment-managed**
```bash
# During deployment, run schema initialization separately
cargo run --bin setup_database  # Your custom setup binary
cargo run --bin my_app          # Your main application
```

## Schema Customization

### Custom Schema Name
```rust
let client = BackfillClient::new_with_schema(
    &database_url, 
    \"my_job_queue\"  // Custom schema name
).await?;
```

### Multiple Environments
```rust
// Development
let client = BackfillClient::new_with_schema(&url, \"worker_dev\").await?;

// Staging  
let client = BackfillClient::new_with_schema(&url, \"worker_staging\").await?;

// Production
let client = BackfillClient::new_with_schema(&url, \"worker_prod\").await?;
```

## Troubleshooting

### Permission Errors
```
Error: permission denied to create schema \"graphile_worker\"
```
**Solution**: Grant `CREATE` permission to your database user:
```sql
GRANT CREATE ON DATABASE mydb TO myuser;
```

### Connection Issues
```
Error: connection to server on socket failed
```
**Solution**: Verify your connection string and PostgreSQL service:
```bash
# Test direct connection
psql \"postgresql://user:password@localhost/mydb\"

# Check if PostgreSQL is running
pg_isready -h localhost -p 5432
```

### Schema Conflicts
```
Error: relation \"graphile_worker._private_jobs\" already exists
```
**Solution**: This usually indicates a partial schema. Clean up and retry:
```sql
DROP SCHEMA IF EXISTS graphile_worker CASCADE;
```

## Integration with Existing Applications

### Alongside Other Tables
GraphileWorker uses its own schema, so it won't interfere with your existing application tables:

```
mydb:
├── public schema (your app tables)
│   ├── users
│   ├── orders  
│   └── products
└── graphile_worker schema (job queue)
    ├── _private_jobs
    ├── _private_tasks
    └── _private_job_queues
```

### Shared Database
Multiple applications can share the same PostgreSQL instance by using different schema names:

```rust
// App 1
let client = BackfillClient::new_with_schema(&url, \"app1_jobs\").await?;

// App 2  
let client = BackfillClient::new_with_schema(&url, \"app2_jobs\").await?;
```

## SQLx usage

> **Note on backfill itself**: backfill uses runtime SQLx queries
> (`sqlx::query()` and friends) rather than the compile-time
> `sqlx::query!()` macros. This is a deliberate choice — backfill's SQL
> targets dynamic schema names (the user's chosen schema, not a fixed
> identifier), which the compile-time macros don't support cleanly. So
> `DATABASE_URL` is **not** required at compile time, there is no `.sqlx/`
> metadata to maintain, and schema mismatches surface at runtime (the
> integration test suite is the safety net).
>
> The setup below is a recommendation for **your own** queries against
> backfill's tables, in code you write that uses SQLx. If you don't write
> any `sqlx::query!()` / `sqlx::query_as!()` macros yourself, you can
> skip this section.

This library uses SQLx for all database operations, which provides excellent compile-time verification of SQL queries. This is one of Rust's greatest strengths for database applications.

### Overview

SQLx macros like `sqlx::query!()` and `sqlx::query_as!()` verify your SQL queries at compile time by:
- Connecting to your database during compilation
- Parsing and validating SQL syntax
- Checking table/column existence
- Verifying parameter types and counts
- Generating type-safe Rust code

### Setup Options

#### Option 1: Online Mode (Recommended for Development)

Set the `DATABASE_URL` environment variable to enable compile-time verification:

```bash
# Local development
export DATABASE_URL="postgresql://localhost:5432/backfill"

# Or in .env file
DATABASE_URL=postgresql://localhost:5432/backfill
```

**Pros:**
- Always up-to-date with current schema
- Works great in CI with test databases
- No additional files to maintain

**Cons:**
- Requires database connection during compilation
- Slower compilation times

#### Option 2: Offline Mode (Recommended for Distribution)

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

#### Option 3: Hybrid Approach (Best of Both Worlds)

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

### CI/CD Integration

Example GitHub Actions workflow:

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

### Best Practices

1. **Always regenerate metadata** after schema changes with `cargo sqlx prepare`
2. **Commit .sqlx/sqlx-data.json** for reproducible builds
3. **Test both online and offline modes** in CI
4. **Keep DATABASE_URL in .env** for local development

### Performance Impact

- **Compile time**: +10-30% with online mode, minimal with offline
- **Runtime**: Zero overhead - queries are pre-compiled
- **Binary size**: Slightly larger due to generated code
- **Safety**: Eliminates entire classes of runtime errors

The safety benefits far outweigh the small compilation cost!

## Summary

- ✅ **No manual migrations needed** - GraphileWorker handles everything
- ✅ **Automatic schema creation** - Just provide a connection string
- ✅ **Production ready** - Built-in migration management
- ✅ **Flexible deployment** - Works in any environment setup
- ✅ **Non-intrusive** - Uses separate schema from your app tables
- ✅ **Type-safe queries** - SQLx compile-time verification for safety

The `backfill` library makes job queues as easy as connecting to a database!