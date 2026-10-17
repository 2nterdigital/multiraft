//! Batch-native registration and initialization; no application policy.
use super::*;
use multiraft_core::{
    InitializationLogId, InitializationVote, InitializeDisposition, StartupProvenance,
};
use openraft::vote::RaftLeaderId;

/// Whether a pristine local voter may initialize the Group's membership.
///
/// `Allowed` keeps native startup behavior: a pristine voter initializes (the
/// preferred initializer immediately, others after the grace fallback).
/// `Forbidden` never initializes: the Group may only be caught up by an existing
/// leader (log or snapshot). A Forbidden-started **pristine** voter also arms the
/// inbound rejoin vote guard until it has applied the first observed leader
/// commit. Persisted local state never initializes and never arms the guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InitializePolicy {
    #[default]
    Allowed,
    Forbidden,
}

impl InitializePolicy {
    /// Bounded operational source label.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Forbidden => "forbidden",
        }
    }
}

pub(crate) fn digest_label(value: Option<[u8; 32]>) -> Option<String> {
    value.map(|bytes| bytes.iter().map(|b| format!("{b:02x}")).collect())
}

impl<S: StateMachine> MultiRaft<S> {
    pub(crate) fn startup_owner_empty(&self) -> bool {
        self.groups.lock().unwrap().is_empty()
    }
    pub(crate) fn startup_peer_ids(&self) -> BTreeSet<NodeId> {
        self.config.peers.iter().map(|(id, _)| *id).collect()
    }
    pub(crate) fn startup_voter(&self) -> bool {
        self.config.role != NodeRole::Standby
    }

    pub(crate) async fn register_startup_group(
        &self,
        group: GroupId,
        policy: InitializePolicy,
    ) -> Result<(StartupProvenance, tokio::time::Instant), MultiRaftError> {
        self.ensure_node_log().await?;
        let provenance = self.local_provenance(group)?;
        let guard = self.rejoin_guard_for(group, policy, provenance);
        let registered = self.spawn_local_group(group, guard).await?;
        Ok((provenance, registered))
    }

    /// Local storage provenance; call after `ensure_node_log`.
    pub(super) fn local_provenance(
        &self,
        group: GroupId,
    ) -> Result<StartupProvenance, MultiRaftError> {
        let mut provenance = StartupProvenance::Pristine;
        if !self.config.data_dir.as_os_str().is_empty() {
            provenance = self
                .log_provenance(group)
                .map_err(|e| MultiRaftError::Other(e.into()))?;
            let catalog = SnapshotCatalog::new(self.config.data_dir.join("snapshots"), 1);
            let snapshots = catalog
                .startup_provenance(group, self.config.max_snapshot_bytes)
                .map_err(|e| MultiRaftError::Other(e.into()))?;
            if snapshots == StartupProvenance::Persisted {
                provenance = snapshots;
            }
        }
        Ok(provenance)
    }

    /// Only a Forbidden pristine voter is guarded. Persisted voters kept their
    /// vote and log; guarding them would deadlock a full cold restart.
    pub(super) fn rejoin_guard_for(
        &self,
        group: GroupId,
        policy: InitializePolicy,
        provenance: StartupProvenance,
    ) -> crate::RejoinGuard {
        let armed = policy == InitializePolicy::Forbidden
            && provenance == StartupProvenance::Pristine
            && self.startup_voter();
        tracing::info!(target: "multiraft::startup", node_id = self.node_id, group_id = group,
            phase = "rejoin_guard", initialize_policy = policy.code(),
            startup_provenance = provenance.code(), rejoin_guard_armed = armed,
            "inbound vote guard decided before Group publication");
        if armed {
            crate::RejoinGuard::armed()
        } else {
            crate::RejoinGuard::disarmed()
        }
    }

    /// Forbidden pristine recovery: wait until applied covers the first observed
    /// leader commit. Native `wait_for_recovery` on an empty log pins its target to
    /// the first `cluster_committed`, which only AppendEntries `leader_commit`
    /// sets; the guard anchor (same message, or a snapshot lower bound) must be
    /// covered too. On success the guard is released.
    pub(crate) async fn wait_for_rejoin_recovery(
        &self,
        group: GroupId,
        timeout: Duration,
    ) -> Result<(), MultiRaftError> {
        let started = tokio::time::Instant::now();
        self.wait_for_recovery(group, timeout).await?;
        let (raft, guard) = {
            let groups = self.groups.lock().unwrap();
            let app = groups
                .get(&group)
                .ok_or(MultiRaftError::UnknownGroup(group))?;
            (app.raft.clone(), app.rejoin_guard.clone())
        };
        if guard.is_armed() {
            let remaining = timeout.saturating_sub(started.elapsed());
            let anchor = guard.anchor();
            let result = raft
                .wait(Some(remaining))
                .applied_index_at_least(anchor, "rejoin guard anchor applied")
                .await;
            if let Err(source) = result {
                self.ensure_recovery_running(group)?;
                let failure = match &source {
                    openraft::metrics::WaitError::Timeout(..) => {
                        multiraft_core::RecoveryFailure::Deadline
                    }
                    openraft::metrics::WaitError::ShuttingDown => {
                        multiraft_core::RecoveryFailure::Closed
                    }
                };
                return Err(multiraft_core::RecoveryError::new(
                    group,
                    multiraft_core::RecoveryStage::Await,
                    failure,
                    Some(source.into()),
                )
                .into());
            }
            let applied = raft
                .metrics()
                .borrow_watched()
                .last_applied
                .as_ref()
                .map(|id| id.index);
            guard.try_release(self.node_id, group, applied);
        }
        tracing::info!(target: "multiraft::recovery", operation = "owned_recovery", phase = "complete",
            node_id = self.node_id, group_id = group, recovery_basis = "first_leader_commit",
            rejoin_guard_armed = guard.is_armed(), anchor_index = ?guard.anchor(),
            "forbidden pristine Group caught up from an existing leader");
        Ok(())
    }
    pub(crate) async fn startup_eligible(&self, group: GroupId) -> Result<bool, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        raft.is_initialized()
            .await
            .map(|initialized| !initialized)
            .map_err(|e| {
                recovery::native_recovery_error(group, multiraft_core::RecoveryStage::Await, e)
            })
    }
    pub(crate) async fn startup_initialize(
        &self,
        group: GroupId,
        members: &[NodeId],
        input_digest: Option<[u8; 32]>,
    ) -> Result<InitializeDisposition, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let digest = digest_label(input_digest);
        tracing::info!(target: "multiraft::startup", node_id=self.node_id, group_id=group, startup_digest=digest.as_deref(), digest_known=digest.is_some(), phase="initialize_dispatch", initialization="unknown", outcome_unknown=true, "native initialize call begins; reply and campaign are separate");
        let attempt = self
            .election_source
            .as_ref()
            .map(|source| source.begin(group, crate::ElectionSourceEvent::InitializeStarted));
        let result = match raft.initialize(self.membership_nodes(members)).await {
            Ok(()) => Ok(InitializeDisposition::InitOk),
            Err(RaftError::APIError(InitializeError::NotAllowed(refusal))) => {
                let leader = refusal.vote.leader_id();
                Ok(InitializeDisposition::NotAllowed {
                    last_log_id: refusal.last_log_id.map(|log| {
                        let id = log.committed_leader_id();
                        InitializationLogId {
                            term: id.term(),
                            node_id: *id.node_id(),
                            index: log.index(),
                        }
                    }),
                    vote: InitializationVote {
                        term: leader.term(),
                        node_id: *leader.node_id(),
                        committed: refusal.vote.committed,
                    },
                })
            }
            Err(e) => Err(MultiRaftError::Other(e.into())),
        };
        let disposition = result
            .as_ref()
            .copied()
            .unwrap_or(InitializeDisposition::Unknown);
        tracing::info!(target: "multiraft::startup", node_id=self.node_id, group_id=group, startup_digest=digest.as_deref(), digest_known=digest.is_some(), phase="initialize_reply", initialization=disposition.code(), outcome_unknown=result.is_err(), "native initialize reply; quorum and campaign causality unknown");
        if let InitializeDisposition::NotAllowed { last_log_id, vote } = disposition {
            tracing::info!(target: "multiraft::startup", node_id=self.node_id, group_id=group, startup_digest=digest.as_deref(), digest_known=digest.is_some(), phase="initialize_refusal", initialization="not_allowed", last_log_known=last_log_id.is_some(), last_log_term=last_log_id.map(|v|v.term), last_log_node_id=last_log_id.map(|v|v.node_id), last_log_index=last_log_id.map(|v|v.index), vote_term=vote.term, vote_node_id=vote.node_id, vote_committed=vote.committed, "raw native initialization refusal facts");
        }
        if let Some(attempt) = attempt {
            attempt.finish(crate::ElectionSourceEvent::InitializeFinished { disposition });
        }
        result
    }
}
