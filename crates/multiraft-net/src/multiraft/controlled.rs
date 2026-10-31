//! Per-group owned control commands and authenticated durable replica lifecycle.
use super::*;
use crate::controlled_startup::{StartupAdmission, StartupAuthority, StartupAuthorityError};
use multiraft_core::{NativeLedger, OperationId, ReplacementSpec, TerminalProof};
use std::sync::Weak;

mod cleanup;
mod open;
mod owner;
mod source;
pub(super) use owner::GroupOwner;

/// A controlled open always distinguishes a new bootstrap from durable recovery.
#[derive(Debug, Clone)]
pub enum ControlledOpenMode {
    /// Initialize an explicitly new, pristine group exactly once.
    BootstrapNew { voters: BTreeSet<NodeId> },
    /// Reopen the admitted durable generation without initialization.
    Recover,
    /// Consume a reserved learner generation for this immutable operation.
    Target { operation: OperationId },
}

/// Immutable local generation and one-shot construction request.
#[derive(Debug, Clone)]
pub struct ControlledOpen {
    pub group_id: GroupId,
    pub generation: u64,
    pub identity: Vec<u8>,
    pub mode: ControlledOpenMode,
}

/// A serialized local native control actor; clones share its durable fences.
pub struct ControlledGroup<S: StateMachine> {
    node: Weak<MultiRaft<S>>,
    group: GroupId,
    generation: u64,
    identity: Vec<u8>,
    authority: Arc<dyn StartupAuthority>,
}
impl<S: StateMachine> Clone for ControlledGroup<S> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            group: self.group,
            generation: self.generation,
            identity: self.identity.clone(),
            authority: self.authority.clone(),
        }
    }
}
impl<S: StateMachine> std::fmt::Debug for ControlledGroup<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlledGroup")
            .field("group", &self.group)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// Executor identity pinned to one committed leader incarnation; never refreshed in place.
#[derive(Debug, Clone)]
pub struct ControlledExecutor {
    operation: OperationId,
    epoch: u64,
    leader: openraft::alias::CommittedLeaderIdOf<TypeConfig>,
}

/// Sealed terminal evidence produced only after native and target durable verification.
#[derive(Debug, Clone)]
pub struct ControlledTerminal {
    group: GroupId,
    source: NodeId,
    proof: TerminalProof,
    specification: ReplacementSpec,
}
impl ControlledTerminal {
    /// Exact committed/applied native terminal proof; reading it grants no authority.
    pub fn proof(&self) -> &TerminalProof {
        &self.proof
    }
}

/// A controlled command failure; uncertainty never releases durable operation ownership.
#[derive(Debug, thiserror::Error)]
pub enum ControlledError {
    #[error("controlled command deadline; outcome unknown: {outcome_unknown}")]
    Deadline { outcome_unknown: bool },
    #[error("controlled actor is closed")]
    Closed,
    #[error("controlled request rejected: {0}")]
    Rejected(&'static str),
    #[error("controlled storage: {0}")]
    Storage(#[from] std::io::Error),
    #[error("controlled startup authority: {0}")]
    Authority(#[from] StartupAuthorityError),
    #[error("native control: {0}")]
    Native(String),
}
impl From<MultiRaftError> for ControlledError {
    fn from(error: MultiRaftError) -> Self {
        Self::Native(error.to_string())
    }
}

impl<S: StateMachine> ControlledGroup<S> {
    pub fn group_id(&self) -> GroupId {
        self.group
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    fn node(&self) -> Result<Arc<MultiRaft<S>>, ControlledError> {
        self.node.upgrade().ok_or(ControlledError::Closed)
    }
    fn handle(
        node: &Arc<MultiRaft<S>>,
        request: &ControlledOpen,
        authority: Arc<dyn StartupAuthority>,
    ) -> Self {
        Self {
            node: Arc::downgrade(node),
            group: request.group_id,
            generation: request.generation,
            identity: request.identity.clone(),
            authority,
        }
    }
    /// Read the durable ledger after a new native ReadIndex confirmation.
    pub async fn ledger(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<NativeLedger, ControlledError> {
        let group = self.group;
        self.node()?
            .controlled_run(group, deadline, move |node| async move {
                let (raft, sm) = node.controlled_parts(group)?;
                raft.ensure_linearizable(ReadPolicy::ReadIndex)
                    .await
                    .map_err(|error| ControlledError::Native(error.to_string()))?;
                Ok(sm.controlled_ledger().await?)
            })
            .await
    }
    /// Exact native committed membership sampled inside the core after ReadIndex.
    pub async fn membership(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<multiraft_core::typ::StoredMembership, ControlledError> {
        let group = self.group;
        self.node()?
            .controlled_run(group, deadline, move |node| async move {
                let (raft, _) = node.controlled_parts(group)?;
                raft.ensure_linearizable(ReadPolicy::ReadIndex)
                    .await
                    .map_err(|error| ControlledError::Native(error.to_string()))?;
                raft.with_raft_state(|state| state.membership_state.committed().as_ref().clone())
                    .await
                    .map_err(|error| ControlledError::Native(error.to_string()))
            })
            .await
    }
}
