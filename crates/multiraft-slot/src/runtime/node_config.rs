//! Native configuration invariants for immutable genesis and trusted runtime peers.
use crate::{ClusterLayout, NodeId, RuntimeError, SlotLimits, SlotStartupPlan};
use multiraft_core::{ClusterConfig, LogStorageBackend, NodeRole, SnapshotMode};
use std::path::PathBuf;

pub struct SlotNodeConfig {
    pub layout: ClusterLayout,
    pub node_id: NodeId,
    pub data_dir: PathBuf,
    /// Timings and native snapshot policy limits may be tuned. Identity/storage
    /// fields are checked against the verified manifest before opening the node.
    pub native: ClusterConfig,
    pub limits: SlotLimits,
    /// Optional dial catalog and local binds; excluded from persistent identity.
    pub transport: Option<crate::SlotTransportConfig>,
    /// Bootstrap grace and recovery budget for the native preference startup;
    /// default 1000 ms grace and 30 s budget. See [`SlotStartupPlan`].
    pub startup: SlotStartupPlan,
}
impl SlotNodeConfig {
    pub fn new(layout: ClusterLayout, node_id: NodeId, data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let mut native = ClusterConfig::new(
            node_id,
            layout.nodes().iter().map(|n| (n.node_id, n.raft)).collect(),
        );
        native.data_dir = data_dir.clone();
        native.log_storage_backend = LogStorageBackend::RocksDb;
        native.snapshot_mode = SnapshotMode::NativeDurable;
        native.raft_transport_fingerprint = Some(layout.digest().0);
        Self {
            layout,
            node_id,
            data_dir,
            native,
            limits: SlotLimits::default(),
            transport: None,
            startup: SlotStartupPlan::default(),
        }
    }
    pub(super) fn validate(&self) -> Result<(), RuntimeError> {
        self.validate_inner(self.layout.nodes(), true)
    }
    pub(super) fn validate_strict(&self, nodes: &[crate::NodeAddress]) -> Result<(), RuntimeError> {
        self.validate_inner(nodes, false)
    }
    fn validate_inner(
        &self,
        nodes: &[crate::NodeAddress],
        require_genesis_node: bool,
    ) -> Result<(), RuntimeError> {
        self.limits.validate()?;
        if let Some(transport) = &self.transport {
            transport.validate_nodes(nodes)?;
        }
        // Slot owns transport overrides, rather than accepting hidden native overrides.
        if self.native.transport.is_some() {
            return Err(RuntimeError::InvalidConfig);
        }
        if require_genesis_node {
            self.layout
                .node(self.node_id)
                .map_err(|_| RuntimeError::InvalidConfig)?;
        }
        if !self.startup.validate() {
            return Err(RuntimeError::InvalidConfig);
        }
        let c = &self.native;
        let peers: Vec<_> = self
            .layout
            .nodes()
            .iter()
            .map(|n| (n.node_id, n.raft))
            .collect();
        if self.data_dir.as_os_str().is_empty()
            || c.data_dir != self.data_dir
            || c.node_id != self.node_id
            || c.peers != peers
            || c.raft_transport_fingerprint != Some(self.layout.digest().0)
            || c.log_storage_backend != LogStorageBackend::RocksDb
            || c.snapshot_mode != SnapshotMode::NativeDurable
            || c.role != NodeRole::Voter
            || c.daisy_upstream_base.is_some()
            || !c.standby_node_ids.is_empty()
            || c.admin_advertise_addr.is_some()
            || c.validate_snapshot_storage().is_err()
        {
            return Err(RuntimeError::InvalidConfig);
        }
        Ok(())
    }
}
