//! Batch-native registration and initialization; no application policy.
use super::*;
use multiraft_core::{InitializeDisposition, StartupProvenance};

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
        tracing::info!(target: "multiraft::startup", node_id=self.node_id, group_id=group, input_digest=?input_digest, phase="initialize_dispatch", outcome="unknown", "native initialize call begins; reply and campaign are separate");
        let result = match raft.initialize(self.membership_nodes(members)).await {
            Ok(()) => Ok(InitializeDisposition::InitOk),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                Ok(InitializeDisposition::NotAllowed)
            }
            Err(e) => Err(MultiRaftError::Other(e.into())),
        };
        tracing::info!(target: "multiraft::startup", node_id=self.node_id, group_id=group, input_digest=?input_digest, phase="initialize_reply", disposition=?result.as_ref().ok(), "native initialize reply; quorum and campaign causality unknown");
        result
    }
}
