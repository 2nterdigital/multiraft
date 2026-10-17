//! Deterministic layout generation. Pure: no IO, no clock, no randomness.
//!
//! The same [`PlacementInput`] (in any node order) always yields the same
//! normalized layout, the same canonical JSON bytes, and the same digests.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::config::{
    ClusterLayout, GroupId, LayoutConfig, NodeAddress, NodeId, RouteVersion, SlotAssignment,
    SlotId, FORMAT_VERSION, MAX_NODES,
};
use crate::router::BUCKET_COUNT;
use crate::SlotError;

/// Replication factor of generated layouts. Fixed in this format.
pub const REPLICATION_FACTOR: usize = 3;

/// One candidate node. `weight` scales its share of replicas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementNode {
    pub node_id: NodeId,
    pub raft: SocketAddr,
    pub application_rpc: SocketAddr,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

fn default_weight() -> u32 {
    1
}

/// Generator input, typically read from a static node list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementInput {
    pub cluster_id: String,
    pub slot_count: u32,
    pub group_id_base: GroupId,
    pub nodes: Vec<PlacementNode>,
}

/// Contiguous `bucket -> slot` table: the first `256 % slot_count` slots own
/// `256 / slot_count + 1` buckets, the rest own `256 / slot_count`.
///
/// # Errors
/// [`SlotError::Config`] when `slot_count` is outside `1..=256`.
pub fn contiguous_bucket_table(slot_count: u32) -> Result<Vec<u32>, SlotError> {
    if slot_count == 0 || slot_count > BUCKET_COUNT {
        return Err(SlotError::Config("slot_count must be in 1..=256".into()));
    }
    let base = BUCKET_COUNT / slot_count;
    let wide = BUCKET_COUNT % slot_count;
    let mut table = Vec::with_capacity(BUCKET_COUNT as usize);
    for slot in 0..slot_count {
        let width = if slot < wide { base + 1 } else { base };
        table.extend(std::iter::repeat_n(slot, width as usize));
    }
    debug_assert_eq!(table.len(), BUCKET_COUNT as usize);
    Ok(table)
}

/// Generate and validate a v2 layout.
///
/// Replicas: for slot ordinal `0..slot_count`, the [`REPLICATION_FACTOR`]
/// nodes with the smallest `assigned_replicas / weight` (exact integer
/// cross-multiplication) are chosen; ties break on
/// `((ordinal + 1) + node_id) % node_count`, then `node_id`.
/// Preferred leader: among the slot's voters, the node with the fewest
/// preferences so far, with the same tie rule. `preferred_initializer` and
/// `soft_preferred_leader` are both set to that node.
///
/// # Errors
/// [`SlotError::Config`] for duplicate node ids or addresses, zero weights,
/// fewer than [`REPLICATION_FACTOR`] nodes, or any layout validation failure.
pub fn plan(input: &PlacementInput) -> Result<ClusterLayout, SlotError> {
    let invalid = |message: &str| SlotError::Config(message.to_owned());
    if input.nodes.len() < REPLICATION_FACTOR {
        return Err(invalid("placement needs at least 3 nodes for RF 3"));
    }
    if input.nodes.len() > MAX_NODES {
        return Err(invalid("node count must be in 3..=1024"));
    }
    let mut nodes = input.nodes.clone();
    nodes.sort_by_key(|node| node.node_id);
    if nodes
        .windows(2)
        .any(|pair| pair[0].node_id == pair[1].node_id)
    {
        return Err(invalid("duplicate node id"));
    }
    let mut addresses: Vec<SocketAddr> = nodes
        .iter()
        .flat_map(|node| [node.raft, node.application_rpc])
        .collect();
    addresses.sort_unstable();
    if addresses.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("duplicate node address"));
    }
    if nodes.iter().any(|node| node.weight == 0) {
        return Err(invalid("node weight must be >= 1"));
    }
    let buckets = contiguous_bucket_table(input.slot_count)?;
    let count = nodes.len() as u128;
    let tie = |ordinal: u32, node: &PlacementNode| {
        (u128::from(ordinal) + 1 + u128::from(node.node_id)) % count
    };
    let mut replicas = vec![0_u64; nodes.len()];
    let mut preferences = vec![0_u64; nodes.len()];
    let mut slots = Vec::with_capacity(input.slot_count as usize);
    for ordinal in 0..input.slot_count {
        let mut order: Vec<usize> = (0..nodes.len()).collect();
        order.sort_by(|&a, &b| {
            // replicas[a] / weight[a] vs replicas[b] / weight[b], without floats.
            let left = u128::from(replicas[a]) * u128::from(nodes[b].weight);
            let right = u128::from(replicas[b]) * u128::from(nodes[a].weight);
            left.cmp(&right)
                .then_with(|| tie(ordinal, &nodes[a]).cmp(&tie(ordinal, &nodes[b])))
                .then_with(|| nodes[a].node_id.cmp(&nodes[b].node_id))
        });
        let chosen = &order[..REPLICATION_FACTOR];
        for &index in chosen {
            replicas[index] += 1;
        }
        let leader = *chosen
            .iter()
            .min_by(|&&a, &&b| {
                preferences[a]
                    .cmp(&preferences[b])
                    .then_with(|| tie(ordinal, &nodes[a]).cmp(&tie(ordinal, &nodes[b])))
                    .then_with(|| nodes[a].node_id.cmp(&nodes[b].node_id))
            })
            .unwrap_or(&chosen[0]);
        preferences[leader] += 1;
        let mut voters: Vec<NodeId> = chosen.iter().map(|&index| nodes[index].node_id).collect();
        voters.sort_unstable();
        slots.push(SlotAssignment {
            slot: SlotId(ordinal),
            voters,
            preferred_initializer: nodes[leader].node_id,
            soft_preferred_leader: nodes[leader].node_id,
        });
    }
    ClusterLayout::new(LayoutConfig {
        cluster_id: input.cluster_id.clone(),
        format_version: FORMAT_VERSION,
        route_version: RouteVersion::Sha256Bucket256V1,
        bucket_count: BUCKET_COUNT,
        slot_count: input.slot_count,
        group_id_base: input.group_id_base,
        nodes: nodes
            .iter()
            .map(|node| NodeAddress {
                node_id: node.node_id,
                raft: node.raft,
                application_rpc: node.application_rpc,
            })
            .collect(),
        buckets,
        slots,
    })
}
