//! Explicit startup authority for isolated new-format data spaces.
use crate::{ClusterLayout, ManifestDigest, NodeAddress, RuntimeError, MAX_NODES};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// Strict Slot startup/manifest compatibility; independent from native controlled protocol versions.
pub const SLOT_PROTOCOL_VERSION: u32 = 2;

/// Startup selection supplied by the application after checking its data-space authority.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStartupDisposition {
    /// One-time creation in an empty local directory belonging to the frozen genesis cohort.
    Genesis,
    /// Recover only from complete local source creation facts and durable native Group state.
    RecoverExisting,
    /// Own listeners and a persistent identity without constructing any Group.
    Candidate,
}

/// Stable local identity, independent from mutable peer addresses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotLocalIdentity {
    /// Application-authorized data space.
    pub data_space_id: u64,
    /// Application-provided, persistent UUID for this data directory.
    pub data_dir_uuid: [u8; 16],
    /// Must equal [`SLOT_PROTOCOL_VERSION`].
    pub protocol_version: u32,
    /// Application storage/command compatibility tuple, hashed by the application.
    pub compatibility_digest: [u8; 32],
}

/// A checked application directory snapshot, distinct from the immutable genesis artifact.
///
/// The caller authenticates and persists this source; this adapter validates its identity
/// and coverage. Directory presence does not prove online status or native membership.
#[derive(Debug, Clone)]
pub struct TrustedSlotDirectory {
    /// Positive source revision; not a native membership epoch.
    pub version: u64,
    /// Must match the local data space identity.
    pub data_space_id: u64,
    /// Frozen origin layout digest, including the original addresses and preferences.
    pub genesis_digest: ManifestDigest,
    /// Advertised addresses, including this process and every genesis node id.
    pub nodes: Vec<NodeAddress>,
}

/// Inputs for [`crate::SlotNodeOwner::start_strict`].
#[derive(Debug, Clone)]
pub struct StrictSlotStartup {
    pub disposition: SlotStartupDisposition,
    pub identity: SlotLocalIdentity,
    pub directory: TrustedSlotDirectory,
}
impl StrictSlotStartup {
    /// Common strict wire identity; local UUIDs and mutable directory facts never enter it.
    pub(crate) fn transport_digest(&self, genesis: ManifestDigest) -> ManifestDigest {
        let mut hash = Sha256::new();
        hash.update(b"multiraft-slot/strict-transport/v1\0");
        hash.update(genesis.0);
        hash.update(self.identity.data_space_id.to_be_bytes());
        hash.update(self.identity.protocol_version.to_be_bytes());
        hash.update(self.identity.compatibility_digest);
        ManifestDigest(hash.finalize().into())
    }

    pub(crate) fn validate(&self, layout: &ClusterLayout, local: u64) -> Result<(), RuntimeError> {
        self.validate_profile(layout, local, SLOT_PROTOCOL_VERSION)
    }

    pub(crate) fn validate_profile(
        &self,
        layout: &ClusterLayout,
        local: u64,
        protocol: u32,
    ) -> Result<(), RuntimeError> {
        let directory = &self.directory;
        if self.identity.data_space_id == 0
            || self.identity.data_dir_uuid == [0; 16]
            || self.identity.protocol_version != protocol
            || self.identity.compatibility_digest == [0; 32]
            || directory.version == 0
            || directory.data_space_id != self.identity.data_space_id
            || directory.genesis_digest != layout.digest()
            || directory.nodes.is_empty()
            || directory.nodes.len() > MAX_NODES
        {
            return Err(RuntimeError::InvalidConfig);
        }
        let mut ids = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        for node in &directory.nodes {
            if !ids.insert(node.node_id) {
                return Err(RuntimeError::InvalidConfig);
            }
            for addr in [node.raft, node.application_rpc] {
                if addr.port() == 0
                    || addr.ip().is_unspecified()
                    || matches!(addr, std::net::SocketAddr::V6(a) if a.flowinfo() != 0)
                    || !addresses.insert(addr)
                {
                    return Err(RuntimeError::InvalidConfig);
                }
            }
        }
        if !ids.contains(&local) || layout.nodes().iter().any(|n| !ids.contains(&n.node_id)) {
            return Err(RuntimeError::InvalidConfig);
        }
        match self.disposition {
            SlotStartupDisposition::Genesis if layout.node(local).is_err() => {
                Err(RuntimeError::InvalidConfig)
            }
            SlotStartupDisposition::Candidate if layout.node(local).is_ok() => {
                Err(RuntimeError::InvalidConfig)
            }
            _ => Ok(()),
        }
    }
}
