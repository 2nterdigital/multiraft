//! Normalized, validated persistent cluster layouts and deterministic routing.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::router::{KeyRouter, BUCKET_COUNT};
use crate::SlotError;

pub type NodeId = u64;
pub type GroupId = u64;
/// Layout format accepted by this build. Earlier formats are rejected, never converted.
pub const FORMAT_VERSION: u32 = 2;
/// Hard ceiling: slot ordinals must fit the `u8` routing table.
pub const MAX_SLOTS: u32 = BUCKET_COUNT;
/// Input defense ceiling, not a tested runtime capacity.
pub const MAX_NODES: usize = 1_024;
/// Input defense ceiling, not a tested runtime capacity.
pub const MAX_VOTERS: usize = 1_024;
const MAX_CLUSTER_ID_LEN: usize = 256;
const LAYOUT_DOMAIN: &[u8] = b"multiraft-slot/layout/v2\0";
const ROUTE_DOMAIN: &[u8] = b"multiraft-slot/route/v1\0";

/// Zero-based slot index, checked against a layout at use sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SlotId(pub u32);

/// Persisted routing contract. New variants require an explicit new-data migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RouteVersion {
    /// SHA256 key hash into 256 buckets, then a persisted bucket -> slot table.
    Sha256Bucket256V1,
}

impl RouteVersion {
    /// Stable identifier hashed into both digests.
    pub fn wire_id(self) -> u32 {
        match self {
            Self::Sha256Bucket256V1 => 1,
        }
    }
}

/// Full layout identity (addresses, membership, preferences, route).
/// Used for compatibility checks, not authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ManifestDigest(pub [u8; 32]);

/// Data identity: which key lands in which slot/group. Excludes addresses and preferences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RouteDigest(pub [u8; 32]);

impl ManifestDigest {
    pub fn to_hex(&self) -> String {
        hex(&self.0)
    }
}

impl RouteDigest {
    pub fn to_hex(&self) -> String {
        hex(&self.0)
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

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

/// Static voting membership and startup/leadership preferences of exactly one slot.
///
/// `preferred_initializer` chooses who initializes a pristine group first;
/// `soft_preferred_leader` is the leadership balancing target. They are
/// independent fields and both must be voters of the slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotAssignment {
    pub slot: SlotId,
    pub voters: Vec<NodeId>,
    pub preferred_initializer: NodeId,
    pub soft_preferred_leader: NodeId,
}

/// Unvalidated input. Runtime resource budgets intentionally do not appear here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutConfig {
    pub cluster_id: String,
    pub format_version: u32,
    pub route_version: RouteVersion,
    pub bucket_count: u32,
    pub slot_count: u32,
    pub group_id_base: GroupId,
    pub nodes: Vec<NodeAddress>,
    /// Exactly 256 entries: `buckets[bucket] = slot ordinal`.
    pub buckets: Vec<u32>,
    pub slots: Vec<SlotAssignment>,
}

#[derive(Deserialize)]
struct FormatProbe {
    format_version: Option<u32>,
}

impl LayoutConfig {
    /// Parse a layout artifact, rejecting other format versions with a clear
    /// error before strict field checking.
    ///
    /// # Errors
    /// [`SlotError::Config`] for malformed JSON, a missing or unsupported
    /// `format_version`, or unknown/missing fields.
    pub fn from_json(bytes: &[u8]) -> Result<Self, SlotError> {
        let probe: FormatProbe = serde_json::from_slice(bytes)
            .map_err(|error| SlotError::Config(format!("invalid layout JSON: {error}")))?;
        check_format_version(probe.format_version)?;
        serde_json::from_slice(bytes)
            .map_err(|error| SlotError::Config(format!("invalid layout: {error}")))
    }
}

pub(crate) fn check_format_version(version: Option<u32>) -> Result<(), SlotError> {
    match version {
        Some(FORMAT_VERSION) => Ok(()),
        Some(other) => Err(SlotError::Config(format!(
            "unsupported layout format_version {other} (this build accepts only {FORMAT_VERSION}; \
             earlier layouts require a new data space and are never converted)"
        ))),
        None => Err(SlotError::Config("layout format_version is missing".into())),
    }
}

/// Immutable layout whose ordering and identity have been validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterLayout {
    config: LayoutConfig,
    router: KeyRouter,
    route_digest: RouteDigest,
    digest: ManifestDigest,
}

impl ClusterLayout {
    /// Normalize input ordering and reject duplicates, missing slots, unknown
    /// peers, invalid route tables, and preferences outside a slot's voters.
    ///
    /// # Errors
    /// [`SlotError::Config`] describing the first violated rule.
    pub fn new(mut config: LayoutConfig) -> Result<Self, SlotError> {
        let invalid = |message: &str| SlotError::Config(message.to_owned());
        check_format_version(Some(config.format_version))?;
        if config.cluster_id.is_empty() || config.cluster_id.len() > MAX_CLUSTER_ID_LEN {
            return Err(invalid("cluster_id must contain 1..=256 UTF-8 bytes"));
        }
        if config.bucket_count != BUCKET_COUNT {
            return Err(invalid("bucket_count must be 256"));
        }
        if config.slot_count == 0 || config.slot_count > MAX_SLOTS {
            return Err(invalid("slot_count must be in 1..=256"));
        }
        let router = KeyRouter::new(&config.buckets, config.slot_count, config.group_id_base)?;
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
        let mut addresses = Vec::with_capacity(config.nodes.len() * 2);
        for node in &config.nodes {
            for address in [node.raft, node.application_rpc] {
                if matches!(address, SocketAddr::V6(address) if address.flowinfo() != 0) {
                    return Err(invalid("nonzero IPv6 flowinfo cannot be persisted"));
                }
                addresses.push(address);
            }
        }
        addresses.sort_unstable();
        if addresses.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("duplicate node address"));
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
            if assignment
                .voters
                .binary_search(&assignment.preferred_initializer)
                .is_err()
            {
                return Err(invalid("preferred_initializer must be a voter of its slot"));
            }
            if assignment
                .voters
                .binary_search(&assignment.soft_preferred_leader)
                .is_err()
            {
                return Err(invalid("soft_preferred_leader must be a voter of its slot"));
            }
        }
        let route_digest = route_digest(&config);
        let digest = layout_digest(&config, route_digest);
        Ok(Self {
            config,
            router,
            route_digest,
            digest,
        })
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
    pub fn group_id_base(&self) -> GroupId {
        self.config.group_id_base
    }
    /// The single routing entry point for this layout.
    pub fn router(&self) -> KeyRouter {
        self.router
    }
    /// Layout digest (alias of [`Self::layout_digest`]); the transport and manifest fingerprint.
    pub fn digest(&self) -> ManifestDigest {
        self.digest
    }
    /// Full layout identity, including addresses and preferences.
    pub fn layout_digest(&self) -> ManifestDigest {
        self.digest
    }
    /// Data identity: route version, bucket/slot counts, group base and table only.
    pub fn route_digest(&self) -> RouteDigest {
        self.route_digest
    }

    /// Canonical, stable JSON of the normalized layout (pretty, trailing newline).
    /// The same validated layout always yields byte-identical output.
    pub fn to_canonical_json(&self) -> Vec<u8> {
        // Struct field order is fixed and every collection was sorted by `new`.
        let mut bytes =
            serde_json::to_vec_pretty(&self.config).expect("layout serialization is infallible");
        bytes.push(b'\n');
        bytes
    }

    /// # Errors
    /// [`SlotError::ManifestMismatch`] when `digest` differs from the layout digest.
    pub fn verify_digest(&self, digest: ManifestDigest) -> Result<(), SlotError> {
        if digest == self.digest {
            Ok(())
        } else {
            Err(SlotError::ManifestMismatch("layout digest"))
        }
    }

    /// # Errors
    /// [`SlotError::ManifestMismatch`] when `digest` differs from the route digest.
    pub fn verify_route_digest(&self, digest: RouteDigest) -> Result<(), SlotError> {
        if digest == self.route_digest {
            Ok(())
        } else {
            Err(SlotError::ManifestMismatch("route digest"))
        }
    }

    pub fn node(&self, id: NodeId) -> Result<&NodeAddress, SlotError> {
        self.config
            .nodes
            .binary_search_by_key(&id, |node| node.node_id)
            .map(|index| &self.config.nodes[index])
            .map_err(|_| SlotError::UnknownNode(id))
    }

    fn assignment(&self, slot: SlotId) -> Result<&SlotAssignment, SlotError> {
        self.config
            .slots
            .get(slot.0 as usize)
            .ok_or(SlotError::UnknownSlot(slot.0))
    }

    pub fn voters(&self, slot: SlotId) -> Result<&[NodeId], SlotError> {
        self.assignment(slot).map(|entry| entry.voters.as_slice())
    }

    pub fn preferred_initializer(&self, slot: SlotId) -> Result<NodeId, SlotError> {
        self.assignment(slot)
            .map(|entry| entry.preferred_initializer)
    }

    pub fn soft_preferred_leader(&self, slot: SlotId) -> Result<NodeId, SlotError> {
        self.assignment(slot)
            .map(|entry| entry.soft_preferred_leader)
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

    /// Delegates to [`KeyRouter::slot_for_key`]. Caller and leader never participate.
    pub fn slot_for_key(&self, key: &[u8]) -> Result<SlotId, SlotError> {
        self.router.slot_for_key(key)
    }
}

/// `SHA256("multiraft-slot/route/v1\0" || u32 route_version || u32 bucket_count
/// || u32 slot_count || u64 group_id_base || 256 x u32 bucket entries)`, all big-endian.
fn route_digest(config: &LayoutConfig) -> RouteDigest {
    let mut hash = Sha256::new();
    hash.update(ROUTE_DOMAIN);
    hash.update(config.route_version.wire_id().to_be_bytes());
    hash.update(config.bucket_count.to_be_bytes());
    hash.update(config.slot_count.to_be_bytes());
    hash.update(config.group_id_base.to_be_bytes());
    for slot in &config.buckets {
        hash.update(slot.to_be_bytes());
    }
    RouteDigest(hash.finalize().into())
}

/// Covers every persisted field of the normalized layout plus the route digest.
fn layout_digest(config: &LayoutConfig, route: RouteDigest) -> ManifestDigest {
    let mut hash = Sha256::new();
    hash.update(LAYOUT_DOMAIN);
    hash.update(config.format_version.to_be_bytes());
    hash.update(config.route_version.wire_id().to_be_bytes());
    hash.update((config.cluster_id.len() as u64).to_be_bytes());
    hash.update(config.cluster_id.as_bytes());
    hash.update(config.slot_count.to_be_bytes());
    hash.update(config.group_id_base.to_be_bytes());
    hash.update(route.0);
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
        hash.update(assignment.preferred_initializer.to_be_bytes());
        hash.update(assignment.soft_preferred_leader.to_be_bytes());
    }
    ManifestDigest(hash.finalize().into())
}
