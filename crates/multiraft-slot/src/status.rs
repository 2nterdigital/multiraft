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
