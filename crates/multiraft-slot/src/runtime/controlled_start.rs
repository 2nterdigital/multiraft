//! Catalog-driven replica selection, independent from ordinary Genesis placement startup.
use super::SlotStateMachine;
use crate::{ControlledSlotStartup, RuntimeError, SlotId, SlotStartupDisposition};
use multiraft_net::{
    controlled_startup::{
        StartupAuthority, StartupAuthorityResult, StartupCatalog, StartupRequest,
    },
    ControlledOpen, ControlledOpenMode, ManagedControlledGroup, ManagedTargetEndpoint,
    RuntimeHandle,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tokio::time::Instant;

pub(super) struct ControlledState<S: SlotStateMachine> {
    pub authority: Arc<dyn StartupAuthority>,
    pub groups: Mutex<BTreeMap<SlotId, ManagedControlledGroup<S>>>,
    pub endpoints: Mutex<BTreeMap<Vec<u8>, ManagedTargetEndpoint<S>>>,
    pub directory: Mutex<super::carriers::CarrierDirectory>,
    pub carrier_install: tokio::sync::Mutex<()>,
}
pub(super) async fn open<S: SlotStateMachine>(
    native: &RuntimeHandle<S>,
    layout: &crate::ClusterLayout,
    local: u64,
    input: ControlledSlotStartup,
    catalogs: Vec<(SlotId, StartupCatalog)>,
    deadline: Instant,
) -> Result<ControlledState<S>, RuntimeError> {
    let mut groups = BTreeMap::new();
    for (slot, catalog) in catalogs {
        let replica = input.replicas.iter().find(|r| r.slot == slot);
        let binding = catalog.active_operation.as_ref();
        let authorized = match binding {
            Some(b) if b.source == local => None,
            Some(b) if b.target.node_id == local => Some(b.target.generation),
            _ => catalog.authorized.get(&local).copied(),
        };
        let Some(replica) = replica else {
            if authorized.is_some() {
                return Err(RuntimeError::InvalidConfig);
            }
            continue;
        };
        if authorized.is_some_and(|generation| generation != replica.generation) {
            return Err(RuntimeError::InvalidConfig);
        }
        let group = layout
            .group_for_slot(slot)
            .map_err(|_| RuntimeError::InvalidConfig)?;
        if authorized.is_none() {
            // A bound/retired source may inspect its exact receipt, but never restore a Core.
            let inspected = native
                .inspect_controlled_group(
                    group,
                    replica.generation,
                    replica.identity.clone(),
                    input.authority.clone(),
                    deadline,
                )
                .await
                .map_err(|_| RuntimeError::Unavailable)?;
            groups.insert(slot, inspected);
            continue;
        }
        let mut deferred_target = None;
        if let Some(b) = binding.filter(|b| b.target.node_id == local) {
            if replica.identity != b.target.consumption_id {
                return Err(RuntimeError::InvalidConfig);
            }
            let inspected = native
                .inspect_controlled_group(
                    group,
                    replica.generation,
                    replica.identity.clone(),
                    input.authority.clone(),
                    deadline,
                )
                .await
                .map_err(|_| RuntimeError::Unavailable)?;
            let receipt = inspected
                .receipt(deadline)
                .await
                .map_err(|_| RuntimeError::Unavailable)?;
            // Reserved targets remain without Core until the same portable seed operation.
            if receipt.stage == multiraft_net::ControlledGroupStage::Reserved {
                deferred_target = Some(inspected);
            }
        }
        if let Some(target) = deferred_target {
            groups.insert(slot, target);
            continue;
        }
        let request = StartupRequest {
            group_id: group,
            node_id: local,
            generation: replica.generation,
            request_id: replica.request_id,
        };
        let admission = match tokio::time::timeout_at(deadline, input.authority.admit(request))
            .await
            .map_err(|_| RuntimeError::Deadline)?
            .map_err(|_| RuntimeError::Unavailable)?
        {
            StartupAuthorityResult::Applied(admission) => admission,
            _ => return Err(RuntimeError::Unavailable),
        };
        let mode = if input.startup.disposition == SlotStartupDisposition::Genesis {
            let voters: BTreeSet<_> = layout
                .voters(slot)
                .map_err(|_| RuntimeError::InvalidConfig)?
                .iter()
                .copied()
                .collect();
            if catalog.membership.log_id().is_some()
                || catalog.active_operation.is_some()
                || !catalog.completed.is_empty()
                || catalog.membership.membership().get_joint_config() != &vec![voters.clone()]
                || catalog
                    .membership
                    .membership()
                    .learner_ids()
                    .next()
                    .is_some()
                || catalog.authorized.keys().copied().collect::<BTreeSet<_>>() != voters
            {
                return Err(RuntimeError::InvalidConfig);
            }
            ControlledOpenMode::BootstrapNew { voters }
        } else {
            ControlledOpenMode::Recover
        };
        let opened = native
            .open_controlled_group(
                ControlledOpen {
                    group_id: group,
                    generation: replica.generation,
                    identity: replica.identity.clone(),
                    mode,
                },
                input.authority.clone(),
                admission,
                deadline,
            )
            .await
            .map_err(|error| {
                tracing::warn!(%error, group, "controlled Slot startup refused");
                RuntimeError::Unavailable
            })?;
        groups.insert(slot, opened);
    }
    Ok(ControlledState {
        authority: input.authority,
        groups: Mutex::new(groups),
        endpoints: Mutex::new(BTreeMap::new()),
        carrier_install: tokio::sync::Mutex::new(()),
        directory: Mutex::new(super::carriers::CarrierDirectory::new(
            input
                .startup
                .directory
                .nodes
                .iter()
                .map(|node| node.node_id)
                .collect(),
        )),
    })
}
