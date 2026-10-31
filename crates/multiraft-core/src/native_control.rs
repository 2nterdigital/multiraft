//! Durable, typed native control entries, separate from opaque application bytes.
use crate::typ::StoredMembership;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// A monotonic operation number and immutable caller-selected identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationId {
    pub sequence: u64,
    pub nonce: [u8; 16],
}

/// Native control payload. Application proposal APIs never construct this variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeControl {
    Begin {
        specification: ReplacementSpec,
        executor_epoch: u64,
    },
    Claim {
        operation: OperationId,
        executor_epoch: u64,
    },
}

/// Immutable arguments of one exact-three replica replacement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplacementSpec {
    pub operation: OperationId,
    pub expected_start: StoredMembership,
    pub target_voters: BTreeSet<u64>,
    pub target_generation: u64,
}

/// Latest operation and monotonic rejection floor, retained through log purge.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeLedger {
    pub high_water: u64,
    pub current: Option<OperationRecord>,
}

/// Durable immutable operation arguments and the highest claimed executor epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRecord {
    pub specification: ReplacementSpec,
    pub executor_epoch: u64,
}

/// Deterministic native admission rejection; the enclosing Raft entry still applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlRejection {
    InvalidArguments,
    ConfigurationChanged,
    OperationConflict,
    StaleOperation,
    StaleExecutor,
    NoOperation,
}

/// Exact result encoded in the native proposal's effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlReply {
    Applied(NativeLedger),
    Rejected(ControlRejection),
}
