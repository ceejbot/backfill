# Testing Strategy for Backfill Integration Tests

## Problem: Database Test Hygiene

You were absolutely right to point out the test hygiene issues! The original integration tests had several problems:

1. **Race conditions**: Multiple tests trying to create/drop the same schema simultaneously
2. **Slow and brittle cleanup**: Schema creation/deletion for each test
3. **Poor isolation**: Tests could interfere with each other

## Solution: Isolated Schema Pattern

After evaluating different approaches (transactions vs. isolated schemas vs. shared cleanup), we implemented **isolated schemas** using unique UUIDs:

### Why This Approach?

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **Transaction Rollback** | Fast, perfect isolation | Complex with GraphileWorker's DDL operations | ❌ Too complex |
| **Shared DB + Cleanup** | Simple | Race conditions, incomplete cleanup | ❌ Still problematic |
| **Isolated Schemas** | Perfect isolation, reliable cleanup, works with GraphileWorker | Slightly slower than transactions | ✅ **Best choice** |

### How It Works

```rust
async fn with_isolated_schema<F, Fut>(test_fn: F) -> Result<()>
where
    F: FnOnce(BackfillClient) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    ensure_test_database().await?;
    
    // Create a unique schema name for this test
    let schema_name = format!("test_{}", Uuid::new_v4().simple());
    
    let pool = PgPool::connect(&get_test_database_url()).await?;
    
    // Create client with unique schema - GraphileWorker will create the schema
    let client = BackfillClient::with_pool_and_schema(pool.clone(), schema_name.clone()).await?;
    
    // Run the test
    let result = test_fn(client).await;
    
    // Clean up: drop the test schema
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA IF EXISTS {} CASCADE", schema_name)))
        .execute(&pool)
        .await?;
        
    pool.close().await;
    
    result
}
```

### Benefits

✅ **Perfect Isolation**: Each test gets its own schema with UUID name like `test_a1b2c3d4e5f6`  
✅ **Reliable Cleanup**: Schema is always dropped, even if test panics  
✅ **Parallel Execution**: Tests can run in parallel without conflicts  
✅ **GraphileWorker Compatible**: Works seamlessly with GraphileWorker's schema creation  
✅ **Fast**: Schema creation is much faster than full database setup  

### Usage Pattern

```rust
#[tokio::test]
async fn test_some_feature() -> Result<()> {
    with_isolated_schema(|client| async move {
        // Your test code here
        // client has its own isolated schema
        let job = client.enqueue("task", &payload, spec).await?;
        // Test assertions...
        Ok(())
    }).await
}
```

## Why this beats the old shared-schema tests

The original integration tests reused a single schema across the whole
suite. They needed `--test-threads=1` to avoid duplicate-key collisions,
and partial cleanup left behind state that broke subsequent runs. The
isolated-schema pattern eliminates both: every test gets its own
`test_<uuid>` schema, dropped on exit even if the test panics. That makes
parallel execution safe by construction.

## Running the Tests

```bash
# Run the full suite in parallel against your local PostgreSQL
just test           # createdb backfill_test if needed, then nextest

# Run against the bundled docker-compose database (port 5433)
just test-docker

# Run a single test by name
just test-one test_basic_job_enqueue
```

## Alternative Approaches Considered

### 1. Transaction-Based Testing
**Initial idea**: Wrap each test in a transaction and rollback at the end.

```rust
// This approach had issues with GraphileWorker's DDL operations
async fn with_transaction<F>(test_fn: F) -> Result<()> {
    let tx = pool.begin().await?;
    // Run test within transaction
    let result = test_fn(pool).await;
    tx.rollback().await?; // Always rollback
    result
}
```

**Why we didn't use it**: GraphileWorker performs DDL operations (CREATE SCHEMA, CREATE TABLE) that don't work well within transactions that need to be rolled back. Also, the GraphileWorker client expects a connection pool, not a transaction.

### 2. Test Data Cleanup
**Alternative**: Keep shared schema, but clean up specific test data.

```rust
async fn cleanup_test_data(pool: &PgPool) -> Result<()> {
    sqlx::query("DELETE FROM graphile_worker._private_jobs").execute(pool).await?;
    // ... more cleanup
}
```

**Why we didn't use it**: Still has race conditions and incomplete cleanup edge cases.

## Best Practices

1. **Use isolated schemas** for integration tests that need database state
2. **Prefer unit tests** for business logic that can be tested without database
3. **Use transactions** for tests that need to test rollback behavior specifically
4. **Always use async move** in test closures to avoid lifetime issues
5. **Keep test names descriptive** and test one feature per test

## Future Improvements

1. **Connection pooling**: Could reuse connections across tests for better performance
2. **Parallel schema cleanup**: Batch cleanup operations
3. **Test fixtures**: Create reusable test data generators
4. **Benchmark tracking**: Monitor test performance over time

The isolated schema approach gives us the best of both worlds: perfect test isolation with reliable cleanup, while being compatible with GraphileWorker's architecture.