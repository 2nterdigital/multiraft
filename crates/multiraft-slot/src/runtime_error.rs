//! Bounded errors shared by local callers and the versioned application protocol.
use serde::{Deserialize, Serialize};

/// Stable, bounded native maintenance rejection facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[error("native maintenance rejected: {self:?}")]
pub enum MaintenanceFailure {
    UnknownGroup,
    Disabled,
    SizeLimit,
    UnsupportedCapture,
    StorageFailure,
    SubmissionFailed,
    ShuttingDown,
    Deadline,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum RuntimeError {
    #[error("slot runtime is closed")]
    Closed,
    #[error("slot admission is full")]
    Busy,
    #[error("invalid slot runtime configuration")]
    InvalidConfig,
    #[error("persistent slot manifest does not match")]
    ManifestMismatch,
    #[error("invalid slot request")]
    InvalidRequest,
    #[error("incompatible cluster layout")]
    LayoutMismatch,
    #[error("slot payload exceeds the supported limit")]
    PayloadTooLarge,
    #[error("not leader; hint={hint:?}")]
    NotLeader { hint: Option<u64> },
    #[error("slot service unavailable")]
    Unavailable,
    #[error("write outcome unknown; do not automatically retry")]
    Unknown,
    #[error("deadline expired before dispatch")]
    Deadline,
    #[error("application query failed")]
    Application,
    #[error(transparent)]
    Maintenance(MaintenanceFailure),
    #[error("slot task panicked; runtime fenced")]
    TaskPanicked,
}
impl RuntimeError {
    pub fn outcome_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }
}

pub(crate) fn native(error: multiraft_net::RuntimeError, write: bool) -> RuntimeError {
    use multiraft_net::RuntimeError as N;
    match error {
        N::Closed => RuntimeError::Closed,
        N::Busy => RuntimeError::Busy,
        N::InvalidConfig(_) => RuntimeError::InvalidConfig,
        N::Deadline {
            outcome_unknown: false,
            ..
        }
        | N::Interrupted {
            outcome_unknown: false,
            ..
        } => RuntimeError::Deadline,
        N::MaintenanceRejected(reason) => {
            use multiraft_net::CompactionRejection as C;
            let reason = match reason {
                C::Busy => return RuntimeError::Busy,
                C::UnknownGroup => MaintenanceFailure::UnknownGroup,
                C::Disabled => MaintenanceFailure::Disabled,
                C::SizeLimit => MaintenanceFailure::SizeLimit,
                C::UnsupportedCapture => MaintenanceFailure::UnsupportedCapture,
                C::StorageFailure => MaintenanceFailure::StorageFailure,
                C::SubmissionFailed => MaintenanceFailure::SubmissionFailed,
                C::ShuttingDown => MaintenanceFailure::ShuttingDown,
                C::Deadline => MaintenanceFailure::Deadline,
            };
            RuntimeError::Maintenance(reason)
        }
        N::Source(multiraft_core::MultiRaftError::NotLeader { hint }) => {
            RuntimeError::NotLeader { hint }
        }
        _ if write => RuntimeError::Unknown,
        _ => RuntimeError::Unavailable,
    }
}
