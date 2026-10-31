//! Batch-native registration and initialization; no application policy.
use super::*;
use multiraft_core::{
    InitializationLogId, InitializationVote, InitializeDisposition, StartupProvenance,
};
use openraft::vote::RaftLeaderId;

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
    ) -> Result<(StartupProvenance, tokio::time::Instant), MultiRaftError> {
        self.ensure_node_log().await?;
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
        let registered = self.spawn_local_group(group).await?;
        Ok((provenance, registered))
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
        if self.is_controlled_group(group)? {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "controlled group rejects legacy initialization"
            )));
        }
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

impl<S: StateMachine> MultiRaft<S> {
    /// Called only by the owned creation pipeline; no initialization or glue claim.
    pub(crate) async fn recover_existing_group(
        &self,
        group: GroupId,
    ) -> Result<(), MultiRaftError> {
        if self.groups.lock().unwrap().contains_key(&group) {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "strict recovery requires an unopened local Group"
            )));
        }
        self.spawn_persisted_local_group(group).await?;
        Ok(())
    }

    pub(super) async fn check_persisted_startup(&self, group: GroupId) -> anyhow::Result<()> {
        use openraft::{storage::RaftLogStorage, RaftLogReader};
        anyhow::ensure!(
            recovery::durable_local_mode(&self.config),
            "strict recovery requires durable native storage"
        );
        anyhow::ensure!(
            self.log_provenance(group)? == StartupProvenance::Persisted,
            "strict recovery requires persisted native Group history"
        );
        let catalog = SnapshotCatalog::new(self.config.data_dir.join("snapshots"), 1);
        catalog.startup_provenance(group, self.config.max_snapshot_bytes)?;
        let snapshot = catalog.describe_native(group, self.config.max_snapshot_bytes)?;
        let mut log = self.open_native_log(group)?;
        let committed = log.read_committed().await?;
        let state = log.get_log_state().await?;
        let snapshot_membership = snapshot.as_ref().is_some_and(|snapshot| {
            snapshot.meta.last_log_id.is_some()
                && snapshot
                    .meta
                    .last_membership
                    .membership()
                    .voter_ids()
                    .next()
                    .is_some()
        });
        // A saved vote alone is not an initialized Group, even though provenance
        // correctly records that it must never be overwritten as pristine.
        anyhow::ensure!(
            committed.is_some() || snapshot_membership,
            "strict recovery requires committed native membership or checkpoint"
        );
        let vote = log
            .read_vote()
            .await?
            .ok_or_else(|| anyhow::anyhow!("strict recovery requires a persisted native vote"))?;
        let latest = state.last_log_id.max(committed).max(
            snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.meta.last_log_id),
        );
        anyhow::ensure!(
            latest.is_none_or(|id| vote.leader_id.to_committed() >= *id.committed_leader_id()),
            "strict recovery vote is older than persisted native history"
        );
        if snapshot_membership || state.last_purged_log_id.is_some() {
            // Generic disk FSM authority is available only after opening the
            // application. The next gate runs before constructing any Core.
            return Ok(());
        }
        let end = committed.expect("checked committed basis").index;
        let mut start = 0;
        while start <= end {
            let stop = start.saturating_add(256).min(end.saturating_add(1));
            let entries = log.try_get_log_entries(start..stop).await?;
            anyhow::ensure!(
                entries.len() as u64 == stop - start,
                "strict recovery has an incomplete committed log prefix"
            );
            if entries.iter().any(|entry| matches!(&entry.payload,
                openraft::EntryPayload::Membership(membership) if membership.voter_ids().next().is_some())) {
                return Ok(());
            }
            anyhow::ensure!(stop > start, "strict recovery log index overflow");
            start = stop;
        }
        Err(anyhow::anyhow!(
            "strict recovery has no committed initialized membership"
        ))
    }

    pub(super) async fn check_persisted_fsm_basis(
        &self,
        group: GroupId,
        log: &mut NativeRaftLogStore,
        sm: &StateMachineStore<S>,
    ) -> anyhow::Result<()> {
        use openraft::storage::{RaftLogStorage, RaftStateMachine};
        let state = log.get_log_state().await?;
        let Some(purged) = state.last_purged_log_id else {
            return Ok(());
        };
        let snapshot = sm.native_snapshot_info().await?;
        if snapshot.is_some_and(|snapshot| {
            snapshot.meta.last_log_id >= Some(purged)
                && snapshot
                    .meta
                    .last_membership
                    .membership()
                    .voter_ids()
                    .next()
                    .is_some()
        }) {
            return Ok(());
        }
        let mut sm = sm.clone();
        let (applied, membership) = sm.applied_state().await?;
        anyhow::ensure!(applied >= Some(purged) && membership.membership().voter_ids().next().is_some(),
            "strict recovery Group {group} has purged logs without a complete local FSM/checkpoint basis");
        Ok(())
    }
}
