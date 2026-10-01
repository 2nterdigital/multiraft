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
        let mut provenance = StartupProvenance::Pristine;
        if !self.config.data_dir.as_os_str().is_empty() {
            provenance = FileLogStoreOf::startup_provenance(
                self.config.data_dir.join(format!("group-{group}")),
            )
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
