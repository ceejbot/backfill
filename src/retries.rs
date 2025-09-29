//! Retries: policy, implementation, and tests.

use std::time::Duration;

/// Configuration for exponential backoff retry policy.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_attempts: i32,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Backoff multiplier (typically 2.0 for exponential backoff)
    pub backoff_multiplier: f64,
    /// Add jitter to prevent thundering herd (0.0 to 1.0, default 0.1)
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
    /// Create a new RetryPolicy with custom settings
    pub fn new(max_attempts: i32, initial_delay: Duration, max_delay: Duration, backoff_multiplier: f64) -> Self {
        Self {
            max_attempts,
            initial_delay,
            max_delay,
            backoff_multiplier,
            jitter_factor: 0.1,
        }
    }

    /// Create a RetryPolicy with jitter configuration
    pub fn with_jitter(mut self, jitter_factor: f64) -> Self {
        self.jitter_factor = jitter_factor.clamp(0.0, 1.0);
        self
    }

    /// Calculate the delay for a specific attempt number (0-based)
    pub fn calculate_delay(&self, attempt: i32) -> Duration {
        if attempt >= self.max_attempts {
            return Duration::ZERO;
        }

        // Calculate base delay: initial_delay * multiplier^attempt
        let base_delay_ms = (self.initial_delay.as_millis() as f64) * self.backoff_multiplier.powi(attempt);

        // Cap at max_delay
        let capped_delay_ms = base_delay_ms.min(self.max_delay.as_millis() as f64);

        // Add jitter to prevent thundering herd
        let jitter = if self.jitter_factor > 0.0 {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            // Use a simple hash-based jitter for deterministic randomness
            let mut hasher = DefaultHasher::new();
            attempt.hash(&mut hasher);
            std::thread::current().id().hash(&mut hasher);

            let hash = hasher.finish();
            let jitter_multiplier = (hash % 1000) as f64 / 1000.0; // 0.0 to 1.0
            let jitter_range = capped_delay_ms * self.jitter_factor;
            jitter_range * (jitter_multiplier * 2.0 - 1.0) // -jitter_range to +jitter_range
        } else {
            0.0
        };

        let final_delay_ms = (capped_delay_ms + jitter).max(0.0);
        Duration::from_millis(final_delay_ms as u64)
    }

    /// Calculate the run_at time for a retry
    pub fn calculate_retry_time(
        &self,
        attempt: i32,
        base_time: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        let delay = self.calculate_delay(attempt);
        base_time + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX)
    }

    /// Check if we should retry for the given attempt number
    pub fn should_retry(&self, attempt: i32) -> bool {
        attempt < self.max_attempts
    }

    /// Get the total number of attempts (including initial attempt)
    pub fn total_attempts(&self) -> i32 {
        self.max_attempts + 1 // +1 for the initial attempt
    }

    /// Create a fast retry policy for high-priority jobs
    pub fn fast() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }

    /// Create an aggressive retry policy for critical jobs
    pub fn aggressive() -> Self {
        Self {
            max_attempts: 12,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(600), // 10 minutes
            backoff_multiplier: 1.5,
            jitter_factor: 0.15,
        }
    }

    /// Create a conservative retry policy for bulk jobs
    pub fn conservative() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(1800), // 30 minutes
            backoff_multiplier: 2.5,
            jitter_factor: 0.2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobSpec;

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
    fn retry_policy_calculate_delay() {
        let policy = RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10), 2.0);

        // First retry (attempt 0): 100ms
        let delay0 = policy.calculate_delay(0);
        assert!(delay0.as_millis() >= 90 && delay0.as_millis() <= 110); // Allow for jitter

        // Second retry (attempt 1): 200ms
        let delay1 = policy.calculate_delay(1);
        assert!(delay1.as_millis() >= 180 && delay1.as_millis() <= 220); // Allow for jitter

        // Third retry (attempt 2): 400ms
        let delay2 = policy.calculate_delay(2);
        assert!(delay2.as_millis() >= 360 && delay2.as_millis() <= 440); // Allow for jitter

        // No more retries (attempt 3): 0ms
        let delay3 = policy.calculate_delay(3);
        assert_eq!(delay3, Duration::ZERO);
    }

    #[test]
    fn retry_policy_should_retry() {
        let policy = RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10), 2.0);

        assert!(policy.should_retry(0)); // First retry
        assert!(policy.should_retry(1)); // Second retry
        assert!(policy.should_retry(2)); // Third retry
        assert!(!policy.should_retry(3)); // No more retries
        assert!(!policy.should_retry(4)); // Definitely no more retries
    }

    #[test]
    fn retry_policy_max_delay_cap() {
        let policy = RetryPolicy::new(
            10,
            Duration::from_millis(100),
            Duration::from_secs(1), // Cap at 1 second
            2.0,
        )
        .with_jitter(0.0); // No jitter for predictable testing

        // Attempt 0: 100ms
        assert_eq!(policy.calculate_delay(0), Duration::from_millis(100));

        // Attempt 3: 100 * 2^3 = 800ms
        assert_eq!(policy.calculate_delay(3), Duration::from_millis(800));

        // Attempt 4: 100 * 2^4 = 1600ms, but capped at 1000ms
        assert_eq!(policy.calculate_delay(4), Duration::from_millis(1000));

        // Attempt 10: Still capped at 1000ms
        assert_eq!(policy.calculate_delay(9), Duration::from_millis(1000));
    }

    #[test]
    fn retry_policy_presets() {
        let fast = RetryPolicy::fast();
        assert_eq!(fast.max_attempts, 3);
        assert_eq!(fast.initial_delay, Duration::from_millis(100));
        assert_eq!(fast.max_delay, Duration::from_secs(30));

        let aggressive = RetryPolicy::aggressive();
        assert_eq!(aggressive.max_attempts, 12);
        assert_eq!(aggressive.initial_delay, Duration::from_millis(500));
        assert_eq!(aggressive.max_delay, Duration::from_secs(600));

        let conservative = RetryPolicy::conservative();
        assert_eq!(conservative.max_attempts, 5);
        assert_eq!(conservative.initial_delay, Duration::from_secs(5));
        assert_eq!(conservative.max_delay, Duration::from_secs(1800));
    }

    #[test]
    fn job_spec_with_retry_policies() {
        let spec = JobSpec::default().with_fast_retries();
        assert_eq!(spec.max_attempts, Some(3));
        assert!(spec.retry_policy.is_some());

        let policy = spec.effective_retry_policy();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_delay, Duration::from_millis(100));

        // Test retry time calculation
        let base_time = chrono::Utc::now();
        let retry_time = spec
            .calculate_retry_time(0, base_time)
            .expect("we should get a time from this");
        assert!(retry_time > base_time);

        // Test when no more retries
        let no_retry = spec.calculate_retry_time(10, base_time);
        assert!(no_retry.is_none());
    }
}
