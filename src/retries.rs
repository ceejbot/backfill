//! Retry policy.
//!
//! `RetryPolicy` is a thin wrapper over `max_attempts`. The three presets
//! (`fast`, `aggressive`, `conservative`) differ only in attempt count;
//! they exist as readable shorthand at call sites.
//!
//! # Backoff timing is fixed by graphile_worker
//!
//! graphile_worker schedules every retry on a hard-coded SQL formula:
//!
//! ```text
//! run_at = greatest(now(), run_at) + (exp(least(attempts, 10)) * interval '1 second')
//! ```
//!
//! Concretely: ~1s after the 1st failure, ~2.7s after the 2nd, ~7.4s after
//! the 3rd, ~20s, ~55s, ~2.5min, ~6.7min, ~18min, ~49min, ~2.2h, ~6h (capped
//! at attempts ≥ 10). This formula applies to every job. There is no per-job
//! backoff customization. If upstream graphile_worker grows that capability
//! later, this struct will gain the relevant fields then.

/// Configuration for retry behaviour.
///
/// Currently only `max_attempts` is configurable. Backoff timing is fixed by
/// graphile_worker — see the module-level docs.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts.
    pub max_attempts: i32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 8 }
    }
}

impl RetryPolicy {
    /// Check if we should retry for the given attempt number.
    pub fn should_retry(&self, attempt: i32) -> bool {
        attempt < self.max_attempts
    }

    /// Get the total number of attempts (including initial attempt).
    pub fn total_attempts(&self) -> i32 {
        self.max_attempts + 1 // +1 for the initial attempt
    }

    /// Preset for high-priority jobs that need quick turnaround.
    ///
    /// `max_attempts = 3`.
    pub fn fast() -> Self {
        Self { max_attempts: 3 }
    }

    /// Preset for critical jobs that should keep retrying for a long time.
    ///
    /// `max_attempts = 12` — graphile_worker's exponential backoff caps at
    /// ~6h per retry once attempts ≥ 10, so this gives roughly half a day of
    /// retry coverage.
    pub fn aggressive() -> Self {
        Self { max_attempts: 12 }
    }

    /// Preset for bulk jobs where consistency matters more than latency.
    ///
    /// `max_attempts = 5`.
    pub fn conservative() -> Self {
        Self { max_attempts: 5 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_default_max_attempts() {
        assert_eq!(RetryPolicy::default().max_attempts, 8);
    }

    #[test]
    fn retry_policy_should_retry() {
        let policy = RetryPolicy { max_attempts: 3 };

        assert!(policy.should_retry(0));
        assert!(policy.should_retry(1));
        assert!(policy.should_retry(2));
        assert!(!policy.should_retry(3));
        assert!(!policy.should_retry(4));
    }

    #[test]
    fn retry_policy_total_attempts() {
        let policy = RetryPolicy { max_attempts: 3 };
        assert_eq!(policy.total_attempts(), 4); // +1 for the initial attempt
    }

    #[test]
    fn retry_policy_preset_attempt_counts() {
        assert_eq!(RetryPolicy::fast().max_attempts, 3);
        assert_eq!(RetryPolicy::aggressive().max_attempts, 12);
        assert_eq!(RetryPolicy::conservative().max_attempts, 5);
    }
}
