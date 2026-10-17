//! Bounded errors shared by local callers and the versioned application protocol.
use crate::{ClusterLayout, SlotId};
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
    /// Native preference startup failed and the partially started node was
    /// released. Local-only: `SlotNodeOwner::start` returns it; it is never
    /// produced by an application RPC. `cause` is the bounded native failure
    /// text including its source chain.
    #[error("slot startup failed: slot={slot:?} phase={phase} outcome_unknown={outcome_unknown}: {cause}")]
    StartupFailed {
        /// Slot whose group failed, when the native report names one.
        slot: Option<SlotId>,
        /// Native startup phase code, e.g. `native_wait`.
        phase: String,
        /// Whether initialization or peer-visible effects may have happened.
        outcome_unknown: bool,
        cause: String,
    },
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

/// Bound on [`RuntimeError::StartupFailed::cause`].
const MAX_CAUSE_BYTES: usize = 2048;

pub(crate) fn startup_failure(
    layout: &ClusterLayout,
    failure: &multiraft_net::StartupFailure,
) -> RuntimeError {
    use multiraft_net::StartupRejection as R;
    match failure.rejection {
        Some(R::InvalidInput) => return RuntimeError::InvalidConfig,
        Some(R::Closed) => return RuntimeError::Closed,
        Some(R::Busy | R::OwnerNotEmpty) | None => {}
    }
    let mut cause = failure.to_string();
    let mut source = std::error::Error::source(&failure.source);
    while let Some(error) = source {
        cause.push_str(": ");
        cause.push_str(&error.to_string());
        source = error.source();
    }
    if cause.len() > MAX_CAUSE_BYTES {
        let mut end = MAX_CAUSE_BYTES;
        while !cause.is_char_boundary(end) {
            end -= 1;
        }
        cause.truncate(end);
    }
    RuntimeError::StartupFailed {
        slot: failure
            .group_id
            .and_then(|group| layout.slot_for_group(group).ok()),
        phase: failure.phase.code().to_owned(),
        outcome_unknown: failure.outcome_unknown,
        cause,
    }
}
