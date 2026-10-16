//! Normalized, validated persistent cluster layouts and deterministic routing.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::SlotError;

pub type NodeId = u64;
pub type GroupId = u64;
pub const FORMAT_VERSION: u32 = 1;
/// Input defense ceiling, not a tested runtime capacity.
pub const MAX_SLOTS: u32 = 65_536;
/// Input defense ceiling, not a tested runtime capacity.
pub const MAX_NODES: usize = 1_024;
/// Input defense ceiling, not a tested runtime capacity.
pub const MAX_VOTERS: usize = 1_024;
/// Inclusive key limit, fixed by hash version; empty keys are allowed.
pub const MAX_KEY_LEN: usize = 65_536;
const MAX_CLUSTER_ID_LEN: usize = 256;
const KEY_DOMAIN: &[u8] = b"multiraft-slot/key/sha256-v1\0";
const LAYOUT_DOMAIN: &[u8] = b"multiraft-slot/layout/v1\0";

/// Zero-based slot index, checked against a layout at use sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SlotId(pub u32);

/// Persisted hash contract. New variants require an explicit new-data migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HashVersion {
    Sha256V1,
}

/// Digest used for compatibility checks, not authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ManifestDigest(pub [u8; 32]);

/// Addresses for the Raft and application protocols are independent.
/// Both are persistent layout identity; changing an address requires new data.
/// IPv6 scope IDs are preserved. Nonzero IPv6 flowinfo is rejected because
/// the persisted socket string representation cannot retain it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeAddress {
    pub node_id: NodeId,
    pub raft: SocketAddr,
    pub application_rpc: SocketAddr,
}

/// Static voting membership of exactly one slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotAssignment {
    pub slot: SlotId,
    pub voters: Vec<NodeId>,
}

/// Unvalidated input. Runtime resource budgets intentionally do not appear here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutConfig {
    pub cluster_id: String,
    pub format_version: u32,
    pub hash_version: HashVersion,
    pub slot_count: u32,
    pub group_id_base: GroupId,
    pub nodes: Vec<NodeAddress>,
    pub slots: Vec<SlotAssignment>,
}

/// Immutable layout whose ordering and identity have been validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterLayout {
    config: LayoutConfig,
    digest: ManifestDigest,
}

impl ClusterLayout {
    /// Normalize input ordering and reject duplicates, missing slots, and unknown peers.
    pub fn new(mut config: LayoutConfig) -> Result<Self, SlotError> {
        let invalid = |message: &str| SlotError::Config(message.to_owned());
        if config.cluster_id.is_empty() || config.cluster_id.len() > MAX_CLUSTER_ID_LEN {
            return Err(invalid("cluster_id must contain 1..=256 UTF-8 bytes"));
        }
        if config.format_version != FORMAT_VERSION {
            return Err(invalid("unsupported format_version"));
        }
        if config.slot_count == 0 || config.slot_count > MAX_SLOTS {
            return Err(invalid("slot_count must be in 1..=65536"));
        }
        config
            .group_id_base
            .checked_add(u64::from(config.slot_count - 1))
            .ok_or_else(|| invalid("group id range overflows u64"))?;
        if config.nodes.is_empty() || config.nodes.len() > MAX_NODES {
            return Err(invalid("node count must be in 1..=1024"));
        }
        config.nodes.sort_by_key(|node| node.node_id);
        if config
            .nodes
            .windows(2)
            .any(|pair| pair[0].node_id == pair[1].node_id)
        {
            return Err(invalid("duplicate node id"));
        }
        for node in &config.nodes {
            for address in [node.raft, node.application_rpc] {
                if matches!(address, SocketAddr::V6(address) if address.flowinfo() != 0) {
                    return Err(invalid("nonzero IPv6 flowinfo cannot be persisted"));
                }
            }
        }
        if config.slots.len() != config.slot_count as usize {
            return Err(invalid("exactly one assignment per slot is required"));
        }
        config.slots.sort_by_key(|assignment| assignment.slot);
        for (index, assignment) in config.slots.iter_mut().enumerate() {
            if assignment.slot.0 as usize != index {
                return Err(invalid("duplicate, missing, or out-of-range slot"));
            }
            if assignment.voters.is_empty() || assignment.voters.len() > MAX_VOTERS {
                return Err(invalid("voter count must be in 1..=1024"));
            }
            assignment.voters.sort_unstable();
            if assignment.voters.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(invalid("duplicate voter"));
            }
            for voter in &assignment.voters {
                if config
                    .nodes
                    .binary_search_by_key(voter, |node| node.node_id)
                    .is_err()
                {
                    return Err(invalid("voter references an unknown node"));
                }
            }
        }
        let digest = layout_digest(&config);
        Ok(Self { config, digest })
    }

    pub fn config(&self) -> &LayoutConfig {
        &self.config
    }
    pub fn cluster_id(&self) -> &str {
        &self.config.cluster_id
    }
    pub fn nodes(&self) -> &[NodeAddress] {
        &self.config.nodes
    }
    pub fn slot_count(&self) -> u32 {
        self.config.slot_count
    }
    pub fn digest(&self) -> ManifestDigest {
        self.digest
    }

    pub fn verify_digest(&self, digest: ManifestDigest) -> Result<(), SlotError> {
        if digest == self.digest {
            Ok(())
        } else {
            Err(SlotError::ManifestMismatch("layout digest"))
        }
    }

    pub fn node(&self, id: NodeId) -> Result<&NodeAddress, SlotError> {
        self.config
            .nodes
            .binary_search_by_key(&id, |node| node.node_id)
            .map(|index| &self.config.nodes[index])
            .map_err(|_| SlotError::UnknownNode(id))
    }

    pub fn voters(&self, slot: SlotId) -> Result<&[NodeId], SlotError> {
        self.config
            .slots
            .get(slot.0 as usize)
            .map(|entry| entry.voters.as_slice())
            .ok_or(SlotError::UnknownSlot(slot.0))
    }

    pub fn group_for_slot(&self, slot: SlotId) -> Result<GroupId, SlotError> {
        self.voters(slot)?;
        // Construction checked the entire inclusive range.
        Ok(self.config.group_id_base + u64::from(slot.0))
    }

    pub fn slot_for_group(&self, group: GroupId) -> Result<SlotId, SlotError> {
        let index = group
            .checked_sub(self.config.group_id_base)
            .filter(|index| *index < u64::from(self.slot_count()))
            .ok_or(SlotError::UnknownGroup(group))?;
        Ok(SlotId(
            u32::try_from(index).map_err(|_| SlotError::UnknownGroup(group))?,
        ))
    }

    /// SHA256(domain || u64 big-endian byte length || key), first eight bytes
    /// interpreted big-endian modulo slot_count. Caller and leader never participate.
    pub fn slot_for_key(&self, key: &[u8]) -> Result<SlotId, SlotError> {
        if key.len() > MAX_KEY_LEN {
            return Err(SlotError::KeyTooLong {
                actual: key.len(),
                max: MAX_KEY_LEN,
            });
        }
        let mut hash = Sha256::new();
        hash.update(KEY_DOMAIN);
        hash.update((key.len() as u64).to_be_bytes());
        hash.update(key);
        let result = hash.finalize();
        let mut prefix = [0_u8; 8];
        prefix.copy_from_slice(&result[..8]);
        let slot = u64::from_be_bytes(prefix) % u64::from(self.slot_count());
        Ok(SlotId(u32::try_from(slot).map_err(|_| {
            SlotError::Config("slot exceeds u32".into())
        })?))
    }
}

fn layout_digest(config: &LayoutConfig) -> ManifestDigest {
    let mut hash = Sha256::new();
    hash.update(LAYOUT_DOMAIN);
    hash.update(config.format_version.to_be_bytes());
    hash.update(1_u32.to_be_bytes()); // Sha256V1 wire identifier.
    hash.update((config.cluster_id.len() as u64).to_be_bytes());
    hash.update(config.cluster_id.as_bytes());
    hash.update(config.slot_count.to_be_bytes());
    hash.update(config.group_id_base.to_be_bytes());
    hash.update((config.nodes.len() as u64).to_be_bytes());
    for node in &config.nodes {
        hash.update(node.node_id.to_be_bytes());
        for address in [node.raft, node.application_rpc] {
            let address = address.to_string();
            hash.update((address.len() as u64).to_be_bytes());
            hash.update(address.as_bytes());
        }
    }
    hash.update((config.slots.len() as u64).to_be_bytes());
    for assignment in &config.slots {
        hash.update(assignment.slot.0.to_be_bytes());
        hash.update((assignment.voters.len() as u64).to_be_bytes());
        for voter in &assignment.voters {
            hash.update(voter.to_be_bytes());
        }
    }
    ManifestDigest(hash.finalize().into())
}
