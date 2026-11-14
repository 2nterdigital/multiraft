//! Static inputs for the native preference startup of the local voter slots.
//!
//! Startup uses only the native `start_groups_with_preference` admission: the
//! layout names each slot's `preferred_initializer` and this plan supplies the
//! grace and recovery budget. This crate keeps no bootstrap record and adds no
//! initialization rule of its own.
use std::time::Duration;

/// Accepted [`SlotStartupPlan::bootstrap_grace`] range (the native admission bounds).
pub const BOOTSTRAP_GRACE_RANGE: std::ops::RangeInclusive<Duration> =
    Duration::from_millis(100)..=Duration::from_millis(2000);
/// Upper bound of [`SlotStartupPlan::recovery_timeout`] (the native cleanup budget).
pub const MAX_RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// How one [`crate::SlotNodeOwner::start`] brings up the local voter slots.
///
/// All local voter slots are registered first and then recovered concurrently
/// by one native `start_groups_with_preference` batch. Each slot carries the
/// layout's `preferred_initializer`. A slot with persisted local state only
/// recovers. A pristine slot initializes the layout membership: the preferred
/// initializer at once, any other voter only after `bootstrap_grace` if the
/// group is still uninitialized (native fallback, reported as
/// `fallback == true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotStartupPlan {
    /// Native grace before a non-preferred voter falls back to initializing.
    /// Must be within [`BOOTSTRAP_GRACE_RANGE`]; default 1000 ms.
    pub bootstrap_grace: Duration,
    /// Per-group recovery budget measured from native registration, nonzero
    /// and at most [`MAX_RECOVERY_TIMEOUT`]; default 30 s. The effective budget
    /// is additionally clamped to the `deadline` passed to `start`.
    pub recovery_timeout: Duration,
}

impl Default for SlotStartupPlan {
    fn default() -> Self {
        Self {
            bootstrap_grace: Duration::from_millis(1000),
            recovery_timeout: MAX_RECOVERY_TIMEOUT,
        }
    }
}

impl SlotStartupPlan {
    pub(crate) fn validate(&self) -> bool {
        BOOTSTRAP_GRACE_RANGE.contains(&self.bootstrap_grace)
            && !self.recovery_timeout.is_zero()
            && self.recovery_timeout <= MAX_RECOVERY_TIMEOUT
    }
}
