//! The backfill client, split across a couple of files.

mod dlq;
mod enqueue;

pub use dlq::*;

/// High-level client for the backfill job queue system.
#[derive(Clone)]
pub struct BackfillClient {
    /// a psql connection pool
    pool: sqlx::PgPool,
    /// TODO document
    schema: String,
}
