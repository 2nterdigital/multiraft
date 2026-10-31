//! Read-only native authority with conservative validation around the durable ledger capture.
use super::*;
use multiraft_core::{typ::LogId, typ::StoredMembership, NativeLedger, ReadIndexFailure};
use openraft::storage::RaftStateMachine;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

/// A coherent local membership and control ledger after this invocation's ReadIndex.
///
/// This proves one Group's read point, not an atomic snapshot across Groups or a
/// reservation against later writes. Preserve the complete membership for stale checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalAuthoritativeMembership {
    /// Native Group addressed directly, without business routing.
    pub group_id: GroupId,
    /// Local replica that performed the ReadIndex confirmation.
    pub node_id: NodeId,
    /// Exact committed and applied membership, including log identity and node metadata.
    pub membership: StoredMembership,
    /// Durable operation rejection floor and latest record, including retained terminal proof.
    pub ledger: NativeLedger,
    /// Confirmed native read point. `None` is OpenRaft's empty-log read point.
    pub read_barrier: Option<LogId>,
    /// FSM position unchanged across the ledger capture, at or beyond the read point.
    pub applied: LogId,
}

/// Honest local outcomes; no outcome falls back to topology samples or forwarded reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum LocalAuthorityError {
    /// Owner fenced, native stopped, or shutdown interrupted the read.
    #[error("local authority is closed")]
    Closed,
    /// Immediate bounded request admission was full.
    #[error("local authority admission is full")]
    Busy,
    /// Caller budget expired; no native mutation was dispatched.
    #[error("local authority deadline expired")]
    Deadline,
    /// No actual local Group is registered. Reading does not construct one.
    #[error("local native group is missing")]
    LocalMissing,
    /// Local replica could not confirm leader authority. The hint grants no authority.
    #[error("local replica is not leader; hint={hint:?}")]
    NotLeader { hint: Option<NodeId> },
    /// Quorum/backend/FSM unavailable, unsupported ledger, or concurrent state change.
    #[error("local authority is unavailable")]
    Unavailable,
    /// Slot facade was addressed outside its fixed Slot map.
    #[error("invalid slot")]
    InvalidSlot,
}
impl From<MultiRaftError> for LocalAuthorityError {
    fn from(error: MultiRaftError) -> Self {
        match error {
            MultiRaftError::UnknownGroup(_) => Self::LocalMissing,
            MultiRaftError::NotLeader { hint } => Self::NotLeader { hint },
            MultiRaftError::ReadIndex(ReadIndexFailure::Deadline) => Self::Deadline,
            MultiRaftError::ReadIndex(ReadIndexFailure::Closed) => Self::Closed,
            _ => Self::Unavailable,
        }
    }
}

impl<S: StateMachine> MultiRaft<S> {
    /// Confirm ReadIndex and capture this actual local Group's membership and ledger.
    ///
    /// No startup, initialization, reservation, forwarding or membership change occurs.
    /// This raw facade has no runtime admission permit; owned consumers should use
    /// [`crate::RuntimeHandle::local_authoritative_membership`]. Synchronous disk
    /// callbacks must be bounded, as with other native reads.
    ///
    /// # Errors
    /// Returns typed missing/leader/deadline/closed outcomes. Unsupported ledgers,
    /// backend failure and any concurrent apply/configuration change are Unavailable.
    pub async fn local_authoritative_membership(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<LocalAuthoritativeMembership, LocalAuthorityError> {
        self.authority_check(deadline)?;
        let read = async {
            // Pin both pieces from one registered generation, never a Genesis declaration.
            let (raft, mut sm) = self
                .groups
                .lock()
                .unwrap()
                .get(&group)
                .map(|app| (app.raft.clone(), app.state_machine.clone()))
                .ok_or(LocalAuthorityError::LocalMissing)?;
            sm.application_ready()
                .map_err(|_| LocalAuthorityError::Unavailable)?;
            let read_barrier = read::confirm(&raft, deadline).await?;
            let committed_before = raft
                .with_raft_state(|state| state.membership_state.committed().as_ref().clone())
                .await
                .map_err(|_| LocalAuthorityError::Unavailable)?;
            let before = sm
                .applied_state()
                .await
                .map_err(|_| LocalAuthorityError::Unavailable)?;
            let ledger = sm
                .controlled_ledger()
                .await
                .map_err(|_| LocalAuthorityError::Unavailable)?;
            let after = sm
                .applied_state()
                .await
                .map_err(|_| LocalAuthorityError::Unavailable)?;
            let committed_after = raft
                .with_raft_state(|state| state.membership_state.committed().as_ref().clone())
                .await
                .map_err(|_| LocalAuthorityError::Unavailable)?;
            // The ledger and applied record mutate together under the FSM lock.
            // Unchanged full applied identities bracket the ledger; exact core/FSM
            // membership equality also excludes newer committed-but-unapplied configs.
            if before != after
                || before.1 != committed_before
                || committed_before != committed_after
            {
                return Err(LocalAuthorityError::Unavailable);
            }
            let applied = before
                .0
                .filter(|id| read_barrier.is_none_or(|barrier| *id >= barrier))
                .ok_or(LocalAuthorityError::Unavailable)?;
            sm.application_ready()
                .map_err(|_| LocalAuthorityError::Unavailable)?;
            self.authority_check(deadline)?;
            Ok(LocalAuthoritativeMembership {
                group_id: group,
                node_id: self.node_id,
                membership: committed_before,
                ledger,
                read_barrier,
                applied,
            })
        };
        tokio::time::timeout_at(deadline, read)
            .await
            .map_err(|_| LocalAuthorityError::Deadline)?
    }
    fn authority_check(&self, deadline: Instant) -> Result<(), LocalAuthorityError> {
        if self
            .snapshot_rt
            .stopping
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(LocalAuthorityError::Closed);
        }
        if Instant::now() >= deadline {
            return Err(LocalAuthorityError::Deadline);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
