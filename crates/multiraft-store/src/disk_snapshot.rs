//! Streaming snapshots for disk-backed state machines
//! (`docs/specs/2026-10-19-disk-fsm-streaming-snapshot.md`).
//!
//! The OpenRaft snapshot bytes of a disk-backed Group are a small manifest: the
//! snapshot metadata plus the file list of one consistent checkpoint directory.
//! The checkpoint files never pass through memory as one image; senders stream
//! them in bounded chunks and receivers write them straight into a staging
//! directory, verify them, and only then let OpenRaft install the manifest.
mod swap;
mod transfer;
mod workspace;

pub use swap::{inject_install_fault, install_dir, recover_swap, InstallFault, Swapped};
pub(crate) use transfer::Incoming;
pub use transfer::{ChunkSource, DiskChunk, MAX_CHUNK_BYTES};
pub(crate) use workspace::{Published, TempCheckpoint, Workspace};

use multiraft_core::TypeConfig;
use multiraft_fsm::GroupId;
use openraft::alias::SnapshotMetaOf;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;

const MAGIC: &[u8] = b"multiraft-disk-snapshot-v1\n";
/// Every disk snapshot id starts with this prefix; whole-image ids never do.
pub const DISK_SNAPSHOT_ID_PREFIX: &str = "disk-v1-";
const MAX_FILES: usize = 100_000;
const MAX_MANIFEST_BYTES: usize = 32 * 1024 * 1024;

/// One file of a checkpoint directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestFile {
    pub name: String,
    pub size: u64,
}

/// The OpenRaft snapshot payload of a disk-backed Group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskManifest {
    pub group: GroupId,
    pub meta: SnapshotMetaOf<TypeConfig>,
    pub files: Vec<ManifestFile>,
    /// Sender-local checkpoint directory. Receivers never read it.
    pub source: PathBuf,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Whether OpenRaft snapshot bytes are a disk manifest rather than a whole image.
pub fn is_disk_snapshot(meta: &SnapshotMetaOf<TypeConfig>, data: &[u8]) -> bool {
    data.starts_with(MAGIC) && meta.snapshot_id.starts_with(DISK_SNAPSHOT_ID_PREFIX)
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != "."
        && name != ".."
        && !name.contains(['/', '\\', '\0'])
}

impl DiskManifest {
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut bytes = MAGIC.to_vec();
        serde_json::to_writer(&mut bytes, self).map_err(io::Error::other)?;
        Ok(bytes)
    }

    /// Decode and check structure: magic, id prefix, Group, file names and sizes.
    pub fn decode(bytes: &[u8], group: GroupId) -> io::Result<Self> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(invalid("disk snapshot manifest too large"));
        }
        let body = bytes
            .strip_prefix(MAGIC)
            .ok_or_else(|| invalid("not a disk snapshot manifest"))?;
        let manifest: Self =
            serde_json::from_slice(body).map_err(|error| invalid(error.to_string()))?;
        if manifest.group != group {
            return Err(invalid("disk snapshot belongs to another group"));
        }
        if !manifest
            .meta
            .snapshot_id
            .starts_with(DISK_SNAPSHOT_ID_PREFIX)
            || manifest.meta.snapshot_id.len() > 4096
        {
            return Err(invalid("invalid disk snapshot id"));
        }
        if manifest.meta.last_log_id.is_none() {
            return Err(invalid("disk snapshot without an applied position"));
        }
        if manifest.meta.last_membership.log_id().as_ref() > manifest.meta.last_log_id.as_ref() {
            return Err(invalid("membership is beyond the disk snapshot cut"));
        }
        if manifest.files.is_empty() || manifest.files.len() > MAX_FILES {
            return Err(invalid("invalid disk snapshot file count"));
        }
        let mut names = std::collections::BTreeSet::new();
        for file in &manifest.files {
            if !valid_name(&file.name) || !names.insert(file.name.as_str()) {
                return Err(invalid("invalid disk snapshot file name"));
            }
        }
        manifest.total_bytes()?;
        Ok(manifest)
    }

    pub fn total_bytes(&self) -> io::Result<u64> {
        self.files.iter().try_fold(0_u64, |total, file| {
            total
                .checked_add(file.size)
                .ok_or_else(|| invalid("disk snapshot size overflow"))
        })
    }
}

/// Process-unique suffix for workspace entries.
fn nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos() as u64);
    format!(
        "{time:016x}{:04x}{:08x}",
        std::process::id() & 0xffff,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests;
