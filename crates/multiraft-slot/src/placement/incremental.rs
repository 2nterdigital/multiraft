//! Minimum-replacement RF3 assignment over the current committed voter sets.

use serde::{Deserialize, Serialize};

use super::{
    contiguous_bucket_table, preferred_leader, validated_layout, validated_nodes, PlacementInput,
    REPLICATION_FACTOR,
};
use crate::{NodeId, SlotAssignment, SlotError, SlotId};

mod assignment;

/// Actual committed voters for one zero-based Slot ordinal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CurrentPlacement {
    /// Slot ordinal in `0..slot_count`.
    pub slot: SlotId,
    /// Exactly three distinct voters, all present in the target candidates.
    pub voters: Vec<NodeId>,
}

/// Final balanced voters and best-effort leader preference for one Slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncrementalTarget {
    /// Slot ordinal in `0..slot_count`.
    pub slot: SlotId,
    /// Three distinct voters in ascending node order.
    pub voters: Vec<NodeId>,
    /// Preferred leader selected only after minimizing replica replacements.
    pub preferred_leader: NodeId,
}

/// One replacement with exact voter sets before and after this step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaReplacement {
    /// Slot whose replica moves.
    pub slot: SlotId,
    /// Existing voter removed in this step.
    pub source: NodeId,
    /// Candidate voter added in this step.
    pub target: NodeId,
    /// Canonical voters expected before this step, including earlier steps.
    pub expected_voters: Vec<NodeId>,
    /// Canonical voters after this step.
    pub result_voters: Vec<NodeId>,
}

/// Pure immutable assignment and its canonical ordered replacement chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncrementalPlacement {
    /// Final assignments in ascending Slot order.
    pub targets: Vec<IncrementalTarget>,
    /// Replacements ordered by Slot, then source and target node identity.
    pub replacements: Vec<ReplicaReplacement>,
}

/// Balance replicas with the fewest replacements, then select leader preferences.
///
/// Every node receives floor or ceil of `3 * slot_count / node_count` replicas.
/// Which nodes receive the remainder is optimized jointly with replica retention.
/// Equal-cost solutions use stable Slot/node ordering; input permutations cannot
/// change the result. Multiple replacements in a Slot form an exact voter chain.
/// This function performs no IO, generates no genesis layout, and changes no routing.
///
/// # Errors
/// Returns [`SlotError::Config`] for invalid placement metadata or candidates,
/// unequal weights, missing/duplicate/out-of-range Slots, or voters that are not
/// three distinct target candidates. Current voters must all remain candidates;
/// this policy supports expansion, not removal of source nodes.
pub fn plan_incremental(
    input: &PlacementInput,
    current: &[CurrentPlacement],
) -> Result<IncrementalPlacement, SlotError> {
    let nodes = validated_nodes(input)?;
    if nodes.iter().any(|node| node.weight != nodes[0].weight) {
        return Err(SlotError::Config(
            "incremental placement requires equal node weights".into(),
        ));
    }
    let buckets = contiguous_bucket_table(input.slot_count)?;
    if current.len() != input.slot_count as usize {
        return Err(SlotError::Config(
            "exactly one assignment per slot is required".into(),
        ));
    }
    if current
        .iter()
        .any(|slot| slot.voters.len() != REPLICATION_FACTOR)
    {
        return Err(SlotError::Config(
            "incremental placement requires exactly 3 voters per slot".into(),
        ));
    }
    // Reuse layout normalization and validation on actual membership, never a
    // newly generated genesis assignment. The bucket table is only validated.
    let slots = current
        .iter()
        .map(|slot| SlotAssignment {
            slot: slot.slot,
            voters: slot.voters.clone(),
            preferred_initializer: slot.voters[0],
            soft_preferred_leader: slot.voters[0],
        })
        .collect();
    let normalized = validated_layout(input, &nodes, buckets, slots)?;
    let current = &normalized.config().slots;
    let chosen = assignment::minimum_replacements(&nodes, current)?;
    let mut preferences = vec![0; nodes.len()];
    let mut targets = Vec::with_capacity(current.len());
    let mut replacements = Vec::new();
    for (slot, indices) in current.iter().zip(chosen) {
        let leader = preferred_leader(slot.slot.0, &indices, &nodes, &mut preferences);
        let voters: Vec<NodeId> = indices.iter().map(|&index| nodes[index].node_id).collect();
        let removed = slot.voters.iter().filter(|node| !voters.contains(node));
        let added = voters.iter().filter(|node| !slot.voters.contains(node));
        let mut expected_voters = slot.voters.clone();
        for (&source, &target) in removed.zip(added) {
            let mut result_voters = expected_voters.clone();
            result_voters.retain(|&node| node != source);
            result_voters.push(target);
            result_voters.sort_unstable();
            replacements.push(ReplicaReplacement {
                slot: slot.slot,
                source,
                target,
                expected_voters,
                result_voters: result_voters.clone(),
            });
            expected_voters = result_voters;
        }
        targets.push(IncrementalTarget {
            slot: slot.slot,
            voters,
            preferred_leader: nodes[leader].node_id,
        });
    }
    Ok(IncrementalPlacement {
        targets,
        replacements,
    })
}
