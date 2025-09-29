//! Compile-time checked SQLx queries
//!
//! This module contains examples of how to use SQLx macros for compile-time
//! verification of database queries. These provide type safety and guarantee
//! that your queries will work at runtime.
//!
//! ## Setup Requirements
//!
//! To use compile-time query verification, you need either:
//!
//! ### Option 1: Online Mode (Recommended)
//! Set the `DATABASE_URL` environment variable:
//! ```bash
//! export DATABASE_URL="postgresql://user:password@localhost:5432/database"
//! ```
//!
//! ### Option 2: Offline Mode
//! Generate query metadata using:
//! ```bash
//! cargo sqlx prepare
//! ```
//! This creates `.sqlx/sqlx-data.json` with query metadata.
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use sqlx::PgPool;
//! use crate::BackfillError;
//!
//! /// Example of compile-time checked query
//! pub async fn get_job_count_safe(pool: &PgPool) -> Result<i64, BackfillError> {
//!     let result = sqlx::query!(
//!         "SELECT COUNT(*) as count FROM graphile_worker.jobs"
//!     )
//!     .fetch_one(pool)
//!     .await?;
//!     
//!     Ok(result.count.unwrap_or(0))
//! }
//! 
//! /// Example with parameters
//! pub async fn get_jobs_by_task_safe(
//!     pool: &PgPool, 
//!     task_name: &str
//! ) -> Result<Vec<JobInfo>, BackfillError> {
//!     let rows = sqlx::query!(
//!         r#"
//!         SELECT id, task_identifier, queue_name, priority, created_at
//!         FROM graphile_worker.jobs 
//!         WHERE task_identifier = $1
//!         ORDER BY created_at DESC
//!         "#,
//!         task_name
//!     )
//!     .fetch_all(pool)
//!     .await?;
//!     
//!     let jobs = rows.into_iter().map(|row| JobInfo {
//!         id: row.id,
//!         task_identifier: row.task_identifier,
//!         queue_name: row.queue_name,
//!         priority: row.priority,
//!         created_at: row.created_at,
//!     }).collect();
//!     
//!     Ok(jobs)
//! }
//! ```
//!
//! ## Benefits
//!
//! - **Type Safety**: Column names and types are verified at compile time
//! - **Runtime Safety**: Guaranteed that queries will execute successfully  
//! - **Refactoring Safety**: Schema changes cause compile errors, not runtime errors
//! - **Performance**: No runtime query parsing overhead
//!
//! ## Migration Strategy
//!
//! For existing projects, migrate gradually:
//! 1. Start with high-risk queries (production critical paths)
//! 2. Use `sqlx::query!()` for SELECT queries first
//! 3. Move to `sqlx::query_as!()` for complex mappings
//! 4. Convert INSERT/UPDATE/DELETE queries last
//!
//! ## CI/CD Integration  
//!
//! Add to your CI pipeline:
//! ```yaml
//! # GitHub Actions example
//! - name: Check SQLx queries
//!   run: cargo sqlx prepare --check
//!   env:
//!     DATABASE_URL: ${{ secrets.DATABASE_URL }}
//! ```

use chrono::{DateTime, Utc};

/// Example job information structure
#[derive(Debug)]
pub struct JobInfo {
    pub id: i64,
    pub task_identifier: String,
    pub queue_name: Option<String>,
    pub priority: i32,
    pub created_at: DateTime<Utc>,
}

/// Example DLQ information structure for compile-time queries
#[derive(Debug)]
pub struct DlqJobInfo {
    pub id: i64,
    pub task_identifier: String,
    pub failed_at: DateTime<Utc>,
    pub failure_count: i32,
}

// Note: Actual query implementations would go here if DATABASE_URL is available
// For now, this serves as documentation and examples for users to implement
