//! The backfill client, split across a couple of files.

mod cleanup;
mod dlq;
mod enqueue;

pub use cleanup::DEFAULT_STALE_LOCK_TIMEOUT;
pub use dlq::*;

/// High-level client for the backfill job queue system.
#[derive(Clone)]
pub struct BackfillClient {
    /// PostgreSQL connection pool
    pool: sqlx::PgPool,
    /// The PostgreSQL schema name where GraphileWorker tables reside.
    /// Defaults to "graphile_worker". Configurable via `new_with_schema()`.
    schema: String,
}
