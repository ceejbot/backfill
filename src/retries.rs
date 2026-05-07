//! Retry policy.
//!
//! # ⚠️ Important: backoff timing is fixed, not configurable
//!
//! `RetryPolicy` exposes `initial_delay`, `max_delay`, `backoff_multiplier`,
//! and `jitter_factor` fields, but **graphile_worker does not honor them**.
//! graphile_worker schedules every retry on a hard-coded SQL formula:
//!
//! ```text
//! run_at = greatest(now(), run_at) + (exp(least(attempts, 10)) * interval '1 second')
//! ```
//!
//! Concretely: ~1s after the 1st failure, ~2.7s after the 2nd, ~7.4s after
//! the 3rd, ~20s, ~55s, ~2.5min, ~6.7min, ~18min, ~49min, ~2.2h, ~6h (capped
//! at attempts ≥ 10). This formula applies to every job regardless of which
//! `RetryPolicy` the caller configured.
//!
//! **The only `RetryPolicy` field that actually affects job behaviour is
//! `max_attempts`**, which is forwarded to graphile_worker's `JobSpec`.
//! The other fields are kept on the struct for source-compatibility and
//! forward-compatibility — if upstream graphile_worker ever exposes per-job
//! backoff config, the shape is already there.
//!
//! The `fast()`, `aggressive()`, and `conservative()` presets are kept for
//! convenience but in practice differ only in attempt count (3 / 12 / 5
//! respectively).

use std::time::Duration;

/// Configuration for retry behaviour.
///
/// **Only `max_attempts` affects runtime behaviour.** The other fields are
/// retained on the struct for source-compatibility (and so a future
/// graphile_worker upstream that supports per-job backoff config can
/// activate them without an API break) but graphile_worker computes retry
/// timing from a fixed SQL formula (`exp(min(attempts, 10))` seconds) — see
/// the module-level docs for details.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts. **Honored at runtime.**
    pub max_attempts: i32,
    /// Initial delay between retries. **NOT honored** — graphile_worker uses
    /// `exp(min(attempts, 10))` seconds regardless.
    pub initial_delay: Duration,
    /// Maximum delay between retries. **NOT honored** — graphile_worker's
    /// formula caps naturally at attempts ≥ 10 (~6 hours).
    pub max_delay: Duration,
    /// Backoff multiplier. **NOT honored** — graphile_worker's formula uses
    /// `exp()`, not a configurable multiplier.
    pub backoff_multiplier: f64,
    /// Jitter factor. **NOT honored** — graphile_worker's formula adds no
    /// jitter.
    pub jitter_factor: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(300), // 5 minutes
            backoff_multiplier: 2.0,
            jitter_factor: 0.1, // 10% jitter
        }
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
    /// `max_attempts = 3`. Other fields are stored but not honored — see the
    /// module-level docs. In practice this preset differs from `aggressive()`
    /// and `conservative()` only in attempt count.
    pub fn fast() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }

    /// Preset for critical jobs that should keep retrying for a long time.
    ///
    /// `max_attempts = 12` — graphile_worker's exponential backoff caps at
    /// ~6h per retry once attempts ≥ 10, so this gives roughly half a day of
    /// retry coverage. Other fields are stored but not honored.
    pub fn aggressive() -> Self {
        Self {
            max_attempts: 12,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(600),
            backoff_multiplier: 1.5,
            jitter_factor: 0.15,
        }
    }

    /// Preset for bulk jobs where consistency matters more than latency.
    ///
    /// `max_attempts = 5`. Other fields are stored but not honored.
    pub fn conservative() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(1800),
            backoff_multiplier: 2.5,
            jitter_factor: 0.2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 8);
        assert_eq!(policy.initial_delay, Duration::from_millis(1000));
        assert_eq!(policy.max_delay, Duration::from_secs(300));
        assert_eq!(policy.backoff_multiplier, 2.0);
        assert_eq!(policy.jitter_factor, 0.1);
    }

    #[test]
    fn retry_policy_should_retry() {
        let policy = RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };

        assert!(policy.should_retry(0));
        assert!(policy.should_retry(1));
        assert!(policy.should_retry(2));
        assert!(!policy.should_retry(3));
        assert!(!policy.should_retry(4));
    }

    #[test]
    fn retry_policy_total_attempts() {
        let policy = RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };
        assert_eq!(policy.total_attempts(), 4); // +1 for the initial attempt
    }

    #[test]
    fn retry_policy_presets_differ_only_in_attempt_count() {
        // The presets exist as readable shorthand for max_attempts; they
        // intentionally diverge on the other fields too, but those fields
        // don't reach graphile_worker, so for runtime behaviour only the
        // attempt count differs.
        assert_eq!(RetryPolicy::fast().max_attempts, 3);
        assert_eq!(RetryPolicy::aggressive().max_attempts, 12);
        assert_eq!(RetryPolicy::conservative().max_attempts, 5);
    }
}
