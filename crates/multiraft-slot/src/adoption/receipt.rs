//! Self-contained historical Native receipts for trusted maintenance adapters.
use super::{LegacyPassportIdentity, LegacyReplica};
use crate::SlotLocalIdentity;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

/// Checked authorization bindings plus independently observed original file identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptionBinding {
    pub session_id: [u8; 16],
    pub authorization_id: [u8; 16],
    pub cold_manifest_sha256: [u8; 32],
    /// Trusted coordinator input, not a Native observation of PD or TiKV identity.
    pub authorized_physical_domain: [u8; 32],
    pub root: PathBuf,
    /// Local identity observed in the original Slot manifest, never inferred from a leader id.
    pub node_id: u64,
    pub data_space_id: u64,
    pub groups: BTreeSet<u64>,
    /// Normalized original layout digest, distinct from either raw file hash below.
    pub old_layout_digest: [u8; 32],
    pub old_slot_manifest_sha256: [u8; 32],
    pub old_passport_sha256: [u8; 32],
    pub inventory_digest: [u8; 32],
    /// Includes UUID, protocol and the shared application compatibility tuple.
    pub target_identity: SlotLocalIdentity,
    /// Hash of the exact target identity, including its local directory UUID.
    pub per_directory_identity_digest: [u8; 32],
}

/// Durable Native conversion result; this grants no shared Store or ingress permission.
/// Serialized values are historical data; deserialization alone proves no authorization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptionReceipt {
    pub authorization_digest: [u8; 32],
    pub binding: AdoptionBinding,
    pub legacy_space_identity: LegacyPassportIdentity,
    pub generation: u64,
    pub identities: BTreeMap<u64, Vec<u8>>,
    /// Shared transport domain; independent of the local directory UUID.
    pub transport_fingerprint: [u8; 32],
    /// Actual original configuration/log/applied evidence; never a fresh-genesis substitute.
    pub original_replicas: Vec<LegacyReplica>,
}
