//! Custom error types.

use thiserror::Error;

/// Core errors for the Backfill library
#[derive(Debug, Error)]
pub enum BackfillError {
    // Configuration errors
    #[error("Invalid FAST_QUEUE_CONCURRENCY: {0}")]
    FastQueueParseInt(String),
    #[error("Invalid BULK_QUEUE_CONCURRENCY: {0}")]
    BulkQueueParseInt(String),
    #[error("Invalid DEAD_LETTER_QUEUE_CONCURRENCY: {0}")]
    DeadLetterParseInt(String),
    #[error("Invalid POLL_INTERVAL_MS: {0}")]
    PollIntervalParseInt(String),
    #[error("Invalid SHUTDOWN_TIMEOUT_SECS: {0}")]
    ShutdownTimeoutParseInt(String),
    #[error("Invalid DLQ_PROCESSOR_INTERVAL_SECS: {0}")]
    DlqProcessorIntervalParseInt(String),

    // DLQ errors
    #[error("DLQ job not found with ID: {0}")]
    DlqJobNotFound(i64),

    // External library errors
    #[error(transparent)]
    GraphileWorker(#[from] graphile_worker::errors::GraphileWorkerError),
    #[error(transparent)]
    GraphileBuild(#[from] graphile_worker::builder::WorkerBuildError),
    #[error("Worker runtime error: {0}")]
    WorkerRuntime(String),
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
}

/// Worker-specific errors that can classify whether jobs should be retried
#[derive(Debug, Error)]
pub enum WorkerError {
    // Non-retryable errors (permanent failures)
    #[error("Invalid input provided: {message}")]
    InvalidInput { message: String },
    #[error("Malformed data: {message}")]
    MalformedData { message: String },
    #[error("Unauthorized access: {message}")]
    Unauthorized { message: String },
    #[error("Forbidden operation: {message}")]
    Forbidden { message: String },
    #[error("Resource not found: {message}")]
    ResourceNotFound { message: String },
    #[error("Validation failed: {message}")]
    ValidationFailed { message: String },

    // Retryable errors (temporary failures)
    #[error("Connection timeout: {message}")]
    ConnectionTimeout { message: String },
    #[error("Network error: {message}")]
    NetworkError { message: String },
    #[error("Service unavailable: {message}")]
    ServiceUnavailable { message: String },
    #[error("Rate limit exceeded: {message}")]
    RateLimitExceeded { message: String },
    #[error("Resource temporarily unavailable: {message}")]
    TemporaryUnavailable { message: String },

    // Generic wrapper for any other error (defaults to retryable)
    #[error("Job failed: {message}")]
    JobFailed { message: String },

    // Backfill library errors
    #[error(transparent)]
    Backfill(#[from] BackfillError),
}

impl WorkerError {
    /// Determine if this error should trigger a retry
    pub fn is_retryable(&self) -> bool {
        match self {
            // Non-retryable errors
            WorkerError::InvalidInput { .. }
            | WorkerError::MalformedData { .. }
            | WorkerError::Unauthorized { .. }
            | WorkerError::Forbidden { .. }
            | WorkerError::ResourceNotFound { .. }
            | WorkerError::ValidationFailed { .. } => false,

            // Retryable errors
            WorkerError::ConnectionTimeout { .. }
            | WorkerError::NetworkError { .. }
            | WorkerError::ServiceUnavailable { .. }
            | WorkerError::RateLimitExceeded { .. }
            | WorkerError::TemporaryUnavailable { .. }
            | WorkerError::JobFailed { .. } => true,

            // For BackfillError, defer to its own classification if it has one
            WorkerError::Backfill(_) => true, // Default to retryable for BackfillError
        }
    }

    /// Create a WorkerError from any error by classifying it based on the
    /// message
    pub fn from_any_error<E: std::error::Error>(error: E) -> Self {
        let message = error.to_string();
        Self::classify_from_message(message)
    }

    /// Classify an error based on its message content
    pub fn classify_from_message(message: String) -> Self {
        let msg_lower = message.to_lowercase();

        // Non-retryable patterns
        if msg_lower.contains("invalid input") || msg_lower.contains("invalid parameter") {
            return WorkerError::InvalidInput { message };
        }
        if msg_lower.contains("malformed") || msg_lower.contains("parse error") || msg_lower.contains("syntax error") {
            return WorkerError::MalformedData { message };
        }
        if msg_lower.contains("unauthorized") || msg_lower.contains("not authorized") {
            return WorkerError::Unauthorized { message };
        }
        if msg_lower.contains("forbidden") || msg_lower.contains("permission denied") {
            return WorkerError::Forbidden { message };
        }
        if msg_lower.contains("not found") || msg_lower.contains("does not exist") {
            return WorkerError::ResourceNotFound { message };
        }
        if msg_lower.contains("validation") && (msg_lower.contains("failed") || msg_lower.contains("error")) {
            return WorkerError::ValidationFailed { message };
        }
        if msg_lower.contains("bad request") || msg_lower.contains("request format") {
            return WorkerError::MalformedData { message };
        }

        // Retryable patterns
        if msg_lower.contains("timeout") || msg_lower.contains("timed out") {
            return WorkerError::ConnectionTimeout { message };
        }
        if msg_lower.contains("network") || msg_lower.contains("connection") || msg_lower.contains("dns") {
            return WorkerError::NetworkError { message };
        }
        if msg_lower.contains("unavailable") || msg_lower.contains("service down") {
            return WorkerError::ServiceUnavailable { message };
        }
        if msg_lower.contains("rate limit") || msg_lower.contains("too many requests") {
            return WorkerError::RateLimitExceeded { message };
        }
        if msg_lower.contains("busy") || msg_lower.contains("overloaded") || msg_lower.contains("retry") {
            return WorkerError::TemporaryUnavailable { message };
        }

        // Default to generic job failed (retryable)
        WorkerError::JobFailed { message }
    }
}

// Manual implementations for common error types to avoid conflicts
// Note: anyhow::Error conversion is handled in the worker binary where anyhow
// is available

impl From<serde_json::Error> for WorkerError {
    fn from(error: serde_json::Error) -> Self {
        WorkerError::MalformedData {
            message: error.to_string(),
        }
    }
}

impl From<std::io::Error> for WorkerError {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::TimedOut => WorkerError::ConnectionTimeout {
                message: error.to_string(),
            },
            std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted => WorkerError::NetworkError {
                message: error.to_string(),
            },
            std::io::ErrorKind::NotFound => WorkerError::ResourceNotFound {
                message: error.to_string(),
            },
            std::io::ErrorKind::PermissionDenied => WorkerError::Forbidden {
                message: error.to_string(),
            },
            _ => WorkerError::JobFailed {
                message: error.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_classification() {
        // Non-retryable errors
        let non_retryable_cases = vec![
            "Invalid input provided", "Malformed JSON data", "Unauthorized access", "Forbidden operation",
            "Resource not found", "Bad request format", "Validation failed", "Parse error in payload",
        ];

        for case in non_retryable_cases {
            let error = WorkerError::classify_from_message(case.to_string());
            assert!(!error.is_retryable(), "Expected '{}' to be non-retryable", case);
        }

        // Retryable errors
        let retryable_cases = vec![
            "Connection timeout",
            "Network unreachable",
            "Service unavailable",
            "Rate limit exceeded",
            "Too many requests",
            "Database connection failed",
        ];

        for case in retryable_cases {
            let error = WorkerError::classify_from_message(case.to_string());
            assert!(error.is_retryable(), "Expected '{}' to be retryable", case);
        }

        // Unknown errors default to retryable
        let error = WorkerError::classify_from_message("Some unknown error".to_string());
        assert!(error.is_retryable(), "Unknown errors should be retryable");
    }
}
