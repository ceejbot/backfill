//! Priorities and queues.

use serde::{Deserialize, Serialize};

/// Priority levels for jobs in the backfill system.
///
/// Lower numbers indicate higher priority (closer to front of queue).
/// Fast queue uses negative priorities, bulk queue uses positive priorities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Priority(pub i16);

impl Priority {
    /// Emergency priority for the fast queue (-20)
    pub const EMERGENCY: Priority = Priority(-20);
    /// High priority for the fast queue (-10)
    pub const FAST_HIGH: Priority = Priority(-10);
    /// Default priority for the fast queue (-5)
    pub const FAST_DEFAULT: Priority = Priority(-5);
    /// Low priority for bulk processing (0)
    pub const BULK_DEFAULT: Priority = Priority(0);
    /// Lower priority for bulk processing (5)
    pub const BULK_LOW: Priority = Priority(5);
    /// Lowest priority for bulk processing (10)
    pub const BULK_LOWEST: Priority = Priority(10);
}

impl Default for Priority {
    fn default() -> Self {
        Self::BULK_DEFAULT
    }
}

impl From<Priority> for i32 {
    fn from(priority: Priority) -> Self {
        priority.0 as i32
    }
}

impl From<Priority> for i16 {
    fn from(priority: Priority) -> Self {
        priority.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_ordering() {
        assert!(Priority::EMERGENCY < Priority::FAST_HIGH);
        assert!(Priority::FAST_HIGH < Priority::FAST_DEFAULT);
        assert!(Priority::FAST_DEFAULT < Priority::BULK_DEFAULT);
        assert!(Priority::BULK_DEFAULT < Priority::BULK_LOW);
        assert!(Priority::BULK_LOW < Priority::BULK_LOWEST);
    }

    #[test]
    fn priority_conversion() {
        assert_eq!(i32::from(Priority::EMERGENCY), -20);
        assert_eq!(i32::from(Priority::FAST_HIGH), -10);
        assert_eq!(i32::from(Priority::BULK_DEFAULT), 0);
        assert_eq!(i32::from(Priority::BULK_LOWEST), 10);
    }
}
