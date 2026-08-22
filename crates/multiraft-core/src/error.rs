//! Public errors and propose results for MultiRaft.

use thiserror::Error;

use crate::GroupId;
use crate::NodeId;

/// Terminal outcome for a local group observation stream.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("group {group_id} observation is closed")]
pub struct ObservationClosed {
    group_id: GroupId,
}

impl ObservationClosed {
    /// Create a terminal observation error for `group_id`.
    pub const fn new(group_id: GroupId) -> Self {
        Self { group_id }
    }

    /// The group whose local observation stream has ended.
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }
}

/// Errors returned by MultiRaft propose / group APIs.
#[derive(Debug, Error)]
pub enum MultiRaftError {
    #[error("not leader; hint={hint:?}")]
    NotLeader { hint: Option<NodeId> },

    #[error("unknown group {0}")]
    UnknownGroup(u64),

    #[error("stale queries disabled (set ClusterConfig::enable_stale_queries)")]
    StaleQueriesDisabled,

    #[error("live standby snapshot install is unsupported")]
    LiveSnapshotInstallUnsupported,

    #[error(transparent)]
    ObservationClosed(#[from] ObservationClosed),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Successful propose: log index and term after quorum commit + apply
/// (linearizable write for that group).
#[derive(Debug, Clone)]
pub struct ProposeOk {
    pub index: u64,
    pub term: u64,
}

/// Result of a local FSM read for Standby service offload.
///
/// Never linearizable: `applied_index` is this node's last applied log only.
#[derive(Debug, Clone)]
pub struct StaleRead<T> {
    pub value: T,
    pub applied_index: u64,
    pub applied_term: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_and_stale_read() {
        let e = MultiRaftError::NotLeader { hint: Some(2) };
        assert!(e.to_string().contains("not leader"));
        assert!(MultiRaftError::UnknownGroup(7).to_string().contains("7"));
        assert!(MultiRaftError::StaleQueriesDisabled
            .to_string()
            .contains("stale"));
        let unsupported = MultiRaftError::LiveSnapshotInstallUnsupported;
        assert_eq!(
            unsupported.to_string(),
            "live standby snapshot install is unsupported"
        );
        let other: MultiRaftError = anyhow::anyhow!("boom").into();
        assert!(other.to_string().contains("boom"));
        let _ = format!("{:?}", ProposeOk { index: 1, term: 1 });
        let sr = StaleRead {
            value: 42u64,
            applied_index: 3,
            applied_term: 1,
        };
        assert_eq!(sr.value, 42);
        let _ = format!("{sr:?}");
    }

    #[test]
    fn observation_closed_is_typed_and_preserves_group() {
        fn assert_error<E: std::error::Error>() {}

        assert_error::<ObservationClosed>();

        let closed = ObservationClosed::new(42);
        assert_eq!(closed.group_id(), 42);
        assert_eq!(closed.to_string(), "group 42 observation is closed");

        let err: MultiRaftError = closed.into();
        match err {
            MultiRaftError::ObservationClosed(actual) => assert_eq!(actual, closed),
            other => panic!("expected ObservationClosed, got {other:?}"),
        }
    }
}
