//! The backfill client, split across a couple of files.

pub mod cleanup;
mod dlq;
mod enqueue;

pub use dlq::{DlqFilter, DlqJob, DlqJobList, DlqStats};

/// High-level client for the backfill job queue system.
#[derive(Clone)]
pub struct BackfillClient {
    /// PostgreSQL connection pool
    pool: sqlx::PgPool,
    /// The PostgreSQL schema name where GraphileWorker tables reside.
    /// Defaults to "graphile_worker". Configurable via `new_with_schema()`.
    schema: String,
}
