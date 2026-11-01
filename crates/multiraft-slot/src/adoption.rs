//! Explicit cold legacy inspection; no ordinary startup adopts existing data.
mod conversion;
use crate::{ClusterLayout, LayoutConfig, ManifestDigest, RouteDigest, SlotId};
pub use conversion::{adopt_existing, AdoptionReceipt, UpgradeAuthorization};
use multiraft_store::adoption::{inspect_legacy_replica, LegacyReplica};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

/// Native-produced evidence over actual original files and persistent group identities.
/// Critical fields have no public constructor or deserializer.
#[derive(Debug, Clone)]
pub struct AdoptionEvidence {
    pub(crate) root: PathBuf,
    pub(crate) layout: ClusterLayout,
    pub(crate) node: u64,
    pub(crate) space: u64,
    pub(crate) inventory: [u8; 32],
    pub(crate) files: BTreeMap<PathBuf, [u8; 32]>,
    pub(crate) replicas: Vec<LegacyReplica>,
    pub(crate) manifest: Vec<u8>,
    pub(crate) passport: Vec<u8>,
}
impl AdoptionEvidence {
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn layout(&self) -> &ClusterLayout {
        &self.layout
    }
    pub fn node_id(&self) -> u64 {
        self.node
    }
    pub fn data_space_id(&self) -> u64 {
        self.space
    }
    pub fn inventory_digest(&self) -> [u8; 32] {
        self.inventory
    }
    /// Full native membership log identity, applied and committed facts, and observed paths.
    pub fn replicas(&self) -> &[LegacyReplica] {
        &self.replicas
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    layout: LayoutConfig,
    layout_digest: ManifestDigest,
    route_digest: RouteDigest,
    local_node_id: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Passport {
    format_version: u32,
    space_id: u64,
    cluster_id: String,
    layout_digest: String,
    route_version: u32,
    bucket_count: u32,
    route_digest: String,
    slot_count: u32,
    group_id_base: u64,
    max_skew_ms: u64,
    created_at_ms: u64,
}
pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
pub(crate) fn inventory(root: &Path) -> io::Result<BTreeMap<PathBuf, [u8; 32]>> {
    fn visit(
        root: &Path,
        directory: &Path,
        output: &mut BTreeMap<PathBuf, [u8; 32]>,
    ) -> io::Result<()> {
        for row in fs::read_dir(directory)? {
            let row = row?;
            let path = row.path();
            let kind = row.file_type()?;
            if kind.is_dir() {
                visit(root, &path, output)?;
            } else if kind.is_file() {
                output.insert(
                    path.strip_prefix(root)
                        .map_err(io::Error::other)?
                        .to_owned(),
                    Sha256::digest(fs::read(path)?).into(),
                );
            } else {
                return Err(invalid(
                    "legacy inventory contains a linked or special file",
                ));
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files)?;
    Ok(files)
}
pub(crate) fn digest(files: &BTreeMap<PathBuf, [u8; 32]>) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"multiraft-legacy-inventory-v1\0");
    for (path, value) in files {
        let path = path.to_string_lossy();
        hash.update((path.len() as u64).to_be_bytes());
        hash.update(path.as_bytes());
        hash.update(value);
    }
    hash.finalize().into()
}

/// Inspect a cold node's existing Slot root and adjacent OLD data-space passport.
/// Completed conversions return their sealed original evidence for receipt replay;
/// this historical proof is not an observation of subsequent native operation.
/// The caller must quiesce the complete stack and supply an unmodified backup copy.
/// No missing database, namespace, snapshot or manifest is created.
///
/// # Errors
/// Rejects missing, linked, changed or conflicting original data.
pub fn inspect_existing(root: &Path) -> io::Result<AdoptionEvidence> {
    if fs::symlink_metadata(root)?.file_type().is_symlink() || root.canonicalize()? != root {
        return Err(invalid("legacy root must be its exact canonical directory"));
    }
    if root.join("controlled-adoption/intent.json").exists() {
        return conversion::inspect_resume(root);
    }
    let files = inventory(root)?;
    let manifest = fs::read(root.join(crate::MANIFEST_FILE_NAME))?;
    let disk: Manifest = serde_json::from_slice(&manifest).map_err(io::Error::other)?;
    let layout = ClusterLayout::new(disk.layout).map_err(io::Error::other)?;
    layout.node(disk.local_node_id).map_err(io::Error::other)?;
    if disk.layout_digest != layout.digest() || disk.route_digest != layout.route_digest() {
        return Err(invalid("legacy layout digest mismatch"));
    }
    let passport_bytes = fs::read(
        root.parent()
            .ok_or_else(|| invalid("legacy parent missing"))?
            .join("data-space.json"),
    )?;
    let passport: Passport = serde_json::from_slice(&passport_bytes).map_err(io::Error::other)?;
    let config = layout.config();
    if passport.format_version != 5
        || passport.space_id == 0
        || passport.cluster_id != config.cluster_id
        || passport.layout_digest != layout.digest().to_hex()
        || passport.route_digest != layout.route_digest().to_hex()
        || passport.route_version != config.route_version.wire_id()
        || passport.bucket_count != config.bucket_count
        || passport.slot_count != config.slot_count
        || passport.group_id_base != config.group_id_base
        || passport.max_skew_ms == 0
        || passport.created_at_ms == 0
    {
        return Err(invalid(
            "legacy passport does not match actual Slot manifest",
        ));
    }
    let mut replicas = Vec::new();
    for assignment in &config.slots {
        if !assignment.voters.contains(&disk.local_node_id) {
            continue;
        }
        let group = layout
            .group_for_slot(SlotId(assignment.slot.0))
            .map_err(io::Error::other)?;
        let live = multiraft_store::DiskFsmDb::group_path(root, group);
        let workspace = multiraft_store::DiskFsmDb::workspace_path(&live)?;
        let replica = inspect_legacy_replica(&root.join("raft-rocksdb"), &live, &workspace, group)?;
        if replica
            .membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>()
            != assignment.voters
        {
            return Err(invalid(
                "actual original membership differs from manifest voters",
            ));
        }
        replicas.push(replica);
    }
    if replicas.is_empty() || inventory(root)? != files {
        return Err(invalid("legacy inventory changed during inspection"));
    }
    Ok(AdoptionEvidence {
        root: root.to_owned(),
        layout,
        node: disk.local_node_id,
        space: passport.space_id,
        inventory: digest(&files),
        files,
        replicas,
        manifest,
        passport: passport_bytes,
    })
}
