//! Adoption records are separate from fresh-genesis and learner-seed anchors.
use super::{invalid, LegacyReplica};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
};

/// Immutable Native conversion anchor for an original persistent replica.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptionAnchor {
    pub version: u32,
    pub authorization_digest: [u8; 32],
    pub root: PathBuf,
    pub node: u64,
    pub generation: u64,
    pub identity: Vec<u8>,
    pub fingerprint: [u8; 32],
    pub original: LegacyReplica,
}

/// Completion is published only after all original group anchors are durable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptionComplete {
    pub version: u32,
    pub authorization_digest: [u8; 32],
    pub node: u64,
    pub root: PathBuf,
    pub anchors: BTreeMap<u64, [u8; 32]>,
}

/// Outside all deletable Raft log, FSM and snapshot group payloads.
pub fn adoption_directory(root: &Path) -> PathBuf {
    root.join("controlled-adoption")
}

/// Load an exact complete adoption anchor, refusing every incomplete conversion.
/// Absence of adoption records preserves the existing bootstrap/seed recovery path.
///
/// # Errors
/// Pending, conflicting, moved or corrupt records never grant construction authority.
pub fn load_adoption_anchor(
    root: &Path,
    node: u64,
    group: u64,
    generation: u64,
    identity: &[u8],
    fingerprint: Option<[u8; 32]>,
) -> io::Result<Option<AdoptionAnchor>> {
    let directory = adoption_directory(root);
    if !directory.try_exists()? {
        return Ok(None);
    }
    let complete: AdoptionComplete =
        serde_json::from_slice(&std::fs::read(directory.join("complete.json"))?)
            .map_err(io::Error::other)?;
    if complete.version != 1
        || complete.node != node
        || complete.root != root
        || root.canonicalize()? != root
    {
        return Err(invalid("adoption completion owner mismatch"));
    }
    // A later replica incarnation uses its own seed anchor after original cleanup.
    if generation != 1 || !complete.anchors.contains_key(&group) {
        return Ok(None);
    }
    let bytes = std::fs::read(directory.join(format!("group-{group}.anchor.json")))?;
    let anchor: AdoptionAnchor = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    let expected: [u8; 32] = Sha256::digest(&bytes).into();
    if complete.version != 1
        || anchor.version != 1
        || complete.node != node
        || anchor.node != node
        || complete.root != root
        || anchor.root != root
        || root.canonicalize()? != root
        || complete.authorization_digest != anchor.authorization_digest
        || complete.anchors.get(&group) != Some(&expected)
        || anchor.original.group != group
        || anchor.generation != 1
        || anchor.generation != generation
        || anchor.identity != identity
        || Some(anchor.fingerprint) != fingerprint
        || anchor.original.live != crate::DiskFsmDb::group_path(root, group)
        || anchor.original.workspace != crate::DiskFsmDb::workspace_path(&anchor.original.live)?
    {
        return Err(invalid(
            "adoption completion or original replica identity mismatch",
        ));
    }
    Ok(Some(anchor))
}
