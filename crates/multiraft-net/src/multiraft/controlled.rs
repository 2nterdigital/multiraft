//! Per-group owned control commands and authenticated durable replica lifecycle.
use super::*;
use crate::controlled_startup::{StartupAdmission, StartupAuthority, StartupAuthorityError};
use multiraft_core::{NativeLedger, OperationId, ReplacementSpec, TerminalProof};
use std::sync::Weak;

mod bootstrap;
mod cleanup;
mod durable_record;
mod open;
mod owner;
pub mod portable;
mod seed;
mod seed_stream;
mod source;
mod storage_owner;
mod terminal;
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
/// Controlled FSMs must declare exclusive canonical live and snapshot storage paths.
/// Unsupported or differently owned disk namespaces are refused before native core startup.
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
    target_specification: Option<ReplacementSpec>,
}
impl<S: StateMachine> Clone for ControlledGroup<S> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            group: self.group,
            generation: self.generation,
            identity: self.identity.clone(),
            authority: self.authority.clone(),
            target_specification: self.target_specification.clone(),
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

/// A sealed fresh native checkpoint containing the current operation's committed Begin.
/// The descriptor holds no file or FSM lease; export acquires a source-owned streaming lease.
#[derive(Clone)]
pub struct ControlledSeed {
    specification: ReplacementSpec,
    record: multiraft_core::OperationRecord,
    vote: multiraft_core::typ::Vote,
    fingerprint: Option<[u8; 32]>,
    meta: multiraft_core::typ::SnapshotMeta,
    manifest: Vec<u8>,
    input: seed_stream::SeedInput,
}
impl std::fmt::Debug for ControlledSeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlledSeed")
            .field("operation", &self.specification.operation)
            .field("cut", &self.meta.last_log_id)
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
    source_generation: u64,
    fingerprint: Option<[u8; 32]>,
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
    #[error("native target ({outcome:?}): {message}")]
    TargetOutcome {
        outcome: portable::NativeTargetOutcome,
        message: String,
    },
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
            target_specification: None,
        }
    }
    /// Observe the durable generation receipt without establishing consensus authority.
    pub fn receipt(&self) -> Result<multiraft_store::ControlledGroupReceipt, ControlledError> {
        self.checked_receipt()
    }
    /// Read this replica's durable-applied ledger; this observation may be stale.
    pub async fn local_ledger(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<NativeLedger, ControlledError> {
        let this = self.clone();
        self.node()?
            .controlled_run(self.group, deadline, move |node| async move {
                this.checked_active()?;
                let (_, sm) = node.controlled_parts(this.group)?;
                Ok(sm.controlled_ledger().await?)
            })
            .await
    }
    /// Install through the same encoded native port used by independent processes.
    pub async fn open_target(
        &self,
        seed: ControlledSeed,
        admission: StartupAdmission,
        deadline: tokio::time::Instant,
    ) -> Result<Self, ControlledError> {
        let port = self.target_port(seed.specification.clone())?;
        port.install_seed(seed, admission, deadline).await?;
        Ok(self.clone())
    }
    /// Consume this reserved target using its exact durable identity and operation binding.
    pub(super) async fn open_target_input(
        &self,
        seed: ControlledSeed,
        admission: StartupAdmission,
        deadline: tokio::time::Instant,
    ) -> Result<Self, ControlledError> {
        let operation = seed.specification.operation;
        self.node()?
            .open_controlled_group_inner(
                ControlledOpen {
                    group_id: self.group,
                    generation: self.generation,
                    identity: self.identity.clone(),
                    mode: ControlledOpenMode::Target { operation },
                },
                self.authority.clone(),
                admission,
                Some(seed),
                deadline,
            )
            .await
    }
    /// Read the durable ledger after a new native ReadIndex confirmation.
    pub async fn ledger(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<NativeLedger, ControlledError> {
        let group = self.group;
        let this = self.clone();
        self.node()?
            .controlled_run(group, deadline, move |node| async move {
                this.checked_active()?;
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
        let this = self.clone();
        self.node()?
            .controlled_run(group, deadline, move |node| async move {
                this.checked_active()?;
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
