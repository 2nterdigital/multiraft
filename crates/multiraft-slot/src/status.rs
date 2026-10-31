//! Local recovery readiness does not assert quorum or leader readiness.
use crate::{InflightStatus, RuntimeError};
#[derive(Debug, Clone)]
pub struct SlotStatus {
    pub local_recovery_ready: bool,
    pub accepting: bool,
    pub inflight: InflightStatus,
}
#[derive(Debug, Clone)]
pub struct ShutdownReport {
    pub complete: bool,
    pub unknown_writes: u64,
    pub remaining: InflightStatus,
    pub error: Option<RuntimeError>,
}

/// Actual local native facts, sampled separately and potentially stale immediately.
#[derive(Debug, Clone)]
pub struct SlotObservedGroup {
    /// Full effective/committed membership identities, joint configurations and learners.
    pub membership: multiraft_net::GroupObservation,
    /// Actual local term, log tail and application applied progress.
    pub progress: multiraft_net::LocalGroupStatus,
    /// Actual snapshot, purge and compaction progress.
    pub storage: multiraft_net::LocalStorageStatus,
}
/// A missing or unavailable native source is never replaced by layout intent.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum SlotGroupObservation {
    /// This process currently has a recovered local native Group.
    Present(Box<SlotObservedGroup>),
    /// The native owner has no local Group for this Slot.
    LocalMissing,
    /// The local native source could not supply its facts in the requested budget.
    Unavailable,
}
