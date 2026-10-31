//! Dynamic local role observations and trusted discovery; neither grants consensus authority.
use super::{Shared, SlotHandle, SlotStateMachine};
use crate::{RuntimeError, SlotId};
use multiraft_net::{ControlledGroupStage, GroupObservation, LocalMembershipRole};
use std::collections::BTreeSet;
use tokio::time::Instant;

/// Actual local lifecycle/membership role, independent from the frozen Genesis layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalSlotRole {
    Missing,
    Learner,
    Voter,
    Retired,
    Unknown,
}
/// A local sample; native expected-leader guards remain mandatory for side effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRoleObservation {
    pub role: LocalSlotRole,
    pub generation: Option<u64>,
    pub membership: Option<GroupObservation>,
    /// Eligible only at the sampled actual native leader and voter state.
    pub leader_eligible: bool,
}
impl<S: SlotStateMachine> Shared<S> {
    pub(super) async fn role(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<SlotRoleObservation, RuntimeError> {
        let group = self
            .layout
            .group_for_slot(slot)
            .map_err(|_| RuntimeError::InvalidRequest)?;
        let mut generation = None;
        if let Some(controlled) = &self.controlled {
            let managed = controlled.groups.lock().unwrap().get(&slot).cloned();
            if let Some(managed) = managed {
                generation = Some(managed.generation());
                let receipt = managed
                    .receipt(deadline)
                    .await
                    .map_err(|_| RuntimeError::Unavailable)?;
                if matches!(
                    receipt.stage,
                    ControlledGroupStage::Retired
                        | ControlledGroupStage::LogsCleaned
                        | ControlledGroupStage::Cleaned
                ) {
                    return Ok(SlotRoleObservation {
                        role: LocalSlotRole::Retired,
                        generation,
                        membership: None,
                        leader_eligible: false,
                    });
                }
            }
        }
        match self.native.observe_group(group, deadline).await {
            Ok((sample, _)) => {
                let role = match sample.local_membership_role {
                    LocalMembershipRole::Voter => LocalSlotRole::Voter,
                    LocalMembershipRole::Learner => LocalSlotRole::Learner,
                    LocalMembershipRole::NotMember => LocalSlotRole::Missing,
                    _ => LocalSlotRole::Unknown,
                };
                let leader_eligible = role == LocalSlotRole::Voter
                    && sample.server_state == multiraft_net::GroupServerState::Leader;
                Ok(SlotRoleObservation {
                    role,
                    generation,
                    membership: Some(sample),
                    leader_eligible,
                })
            }
            Err(multiraft_net::RuntimeError::Source(
                multiraft_core::MultiRaftError::UnknownGroup(_),
            )) => Ok(SlotRoleObservation {
                role: LocalSlotRole::Missing,
                generation,
                membership: None,
                leader_eligible: false,
            }),
            Err(multiraft_net::RuntimeError::Closed) => Err(RuntimeError::Closed),
            Err(_) => Ok(SlotRoleObservation {
                role: LocalSlotRole::Unknown,
                generation,
                membership: None,
                leader_eligible: false,
            }),
        }
    }
    pub(crate) async fn routing_voters(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<Vec<u64>, RuntimeError> {
        let Some(controlled) = &self.controlled else {
            return self
                .layout
                .voters(slot)
                .map(|v| v.to_vec())
                .map_err(|_| RuntimeError::InvalidRequest);
        };
        let role = self.role(slot, deadline).await?;
        let mut voters = BTreeSet::new();
        if let Some(sample) = role.membership {
            for config in sample
                .committed_membership
                .voter_configs
                .iter()
                .chain(&sample.effective_membership.voter_configs)
            {
                voters.extend(config);
            }
        }
        // Discovery is permitted for a node with no local Core. Catalog presence never
        // changes its local role, and each destination still checks actual native authority.
        if voters.is_empty() {
            let group = self
                .layout
                .group_for_slot(slot)
                .map_err(|_| RuntimeError::InvalidRequest)?;
            let catalog = tokio::time::timeout_at(deadline, controlled.authority.read(group))
                .await
                .map_err(|_| RuntimeError::Deadline)?
                .map_err(|_| RuntimeError::Unavailable)?;
            if catalog.group_id != group {
                return Err(RuntimeError::Unavailable);
            }
            voters.extend(catalog.membership.membership().voter_ids());
            if let Some(binding) = catalog.active_operation {
                voters.extend(binding.specification.target_voters);
            }
        }
        voters.retain(|id| controlled.directory.contains(id));
        Ok(voters.into_iter().collect())
    }
    pub(crate) async fn locally_routable(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<bool, RuntimeError> {
        if self.controlled.is_none() {
            return self
                .layout
                .voters(slot)
                .map(|v| v.contains(&self.node_id))
                .map_err(|_| RuntimeError::InvalidRequest);
        }
        Ok(matches!(
            self.role(slot, deadline).await?.role,
            LocalSlotRole::Voter | LocalSlotRole::Learner
        ))
    }
}
impl<S: SlotStateMachine> SlotHandle<S> {
    /// Observe actual local generation/lifecycle and full joint configuration log identities, without ReadIndex.
    pub async fn local_role(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<SlotRoleObservation, RuntimeError> {
        let shared = self.shared()?;
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline);
        }
        let _guard = shared.budget.acquire(slot, 0)?;
        shared.role(slot, deadline).await
    }
    /// Sample actual current local voters across the fixed Slot map, including joint voters.
    pub async fn local_voter_slots(&self, deadline: Instant) -> Result<Vec<SlotId>, RuntimeError> {
        let mut slots = Vec::new();
        for assignment in &self.layout.config().slots {
            if self.local_role(assignment.slot, deadline).await?.role == LocalSlotRole::Voter {
                slots.push(assignment.slot);
            }
        }
        Ok(slots)
    }
}
