//! Explicit, durable controlled startup profile for fixed Slot replicas.
use crate::{RuntimeError, SlotId, StrictSlotStartup};
use multiraft_net::controlled_startup::{StartupAuthority, StartupCatalog};
use std::{collections::BTreeSet, sync::Arc};
use tokio::time::Instant;

/// Controlled identities are incompatible with the ordinary strict startup profile.
pub const CONTROLLED_SLOT_PROTOCOL_VERSION: u32 = 3;

/// Physical identity of an existing or explicitly new local generation.
#[derive(Debug, Clone)]
pub struct ControlledSlotReplica {
    pub slot: SlotId,
    pub generation: u64,
    pub identity: Vec<u8>,
    /// Fresh boot request identity; retry only the same physical admission outcome.
    pub request_id: [u8; 16],
}

/// Trusted catalog adapter and physical identities; catalog facts choose startup admission.
pub struct ControlledSlotStartup {
    pub startup: StrictSlotStartup,
    pub authority: Arc<dyn StartupAuthority>,
    pub replicas: Vec<ControlledSlotReplica>,
}
impl std::fmt::Debug for ControlledSlotStartup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlledSlotStartup")
            .field("startup", &self.startup)
            .field("replicas", &self.replicas)
            .finish_non_exhaustive()
    }
}
impl ControlledSlotStartup {
    /// Validate and derive the actual controlled common transport domain before owner startup.
    /// This establishes no catalog admission, peer authentication or quorum authority.
    pub fn transport_domain(
        &self,
        layout: &crate::ClusterLayout,
        local: crate::NodeId,
    ) -> Result<[u8; 32], RuntimeError> {
        let sparse = self.startup.disposition == crate::SlotStartupDisposition::Candidate
            && self.replicas.is_empty();
        self.startup.validate_profile_coverage(
            layout,
            local,
            CONTROLLED_SLOT_PROTOCOL_VERSION,
            !sparse,
        )?;
        Ok(self.startup.transport_digest(layout.digest()).0)
    }
    pub(crate) async fn catalogs(
        &self,
        layout: &crate::ClusterLayout,
        local: u64,
        deadline: Instant,
    ) -> Result<Vec<(SlotId, StartupCatalog)>, RuntimeError> {
        self.transport_domain(layout, local)?;
        let mut slots = BTreeSet::new();
        for replica in &self.replicas {
            layout
                .group_for_slot(replica.slot)
                .map_err(|_| RuntimeError::InvalidConfig)?;
            if replica.generation == 0
                || replica.identity.is_empty()
                || replica.request_id == [0; 16]
                || !slots.insert(replica.slot)
            {
                return Err(RuntimeError::InvalidConfig);
            }
        }
        let mut catalogs = Vec::new();
        for assignment in &layout.config().slots {
            let group = layout
                .group_for_slot(assignment.slot)
                .map_err(|_| RuntimeError::InvalidConfig)?;
            let catalog = tokio::time::timeout_at(deadline, self.authority.read(group))
                .await
                .map_err(|_| RuntimeError::Deadline)?
                .map_err(|_| RuntimeError::Unavailable)?;
            if catalog.group_id != group {
                return Err(RuntimeError::InvalidConfig);
            }
            if catalog.authorized.contains_key(&local)
                || catalog
                    .active_operation
                    .as_ref()
                    .is_some_and(|binding| binding.target.node_id == local)
            {
                self.startup
                    .validate_profile(layout, local, CONTROLLED_SLOT_PROTOCOL_VERSION)?;
            }
            catalogs.push((assignment.slot, catalog));
        }
        Ok(catalogs)
    }
}
