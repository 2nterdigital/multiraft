//! Native generations extend the existing catalog. Standby entries remain isolated.
use super::{hex_sha256, write_fsync, SnapshotCatalog};
use crate::durability::sync_dir;
use multiraft_core::TypeConfig;
use multiraft_fsm::GroupId;
use openraft::alias::SnapshotMetaOf;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const VERSION: u32 = 1;
const META_LIMIT: usize = 1024 * 1024;
const BYTE_LIMIT: usize = 64 * 1024 * 1024;
static NEXT_STAGE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct NativeSnapshot {
    pub meta: SnapshotMetaOf<TypeConfig>,
    pub data: Vec<u8>,
}

/// Validated persistent facts without materializing the application image.
#[derive(Debug, Clone)]
pub struct NativeSnapshotInfo {
    pub meta: SnapshotMetaOf<TypeConfig>,
    pub size: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeMeta {
    version: u32,
    group: GroupId,
    meta: SnapshotMetaOf<TypeConfig>,
    size: usize,
    data_sha256: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Active {
    version: u32,
    group: GroupId,
    generation: String,
    metadata_sha256: String,
}

/// A fully synced candidate which has not yet become recovery authority.
/// Dropping this value leaves an inert generation; it never activates implicitly.
pub struct NativeSnapshotStage {
    group: GroupId,
    root: PathBuf,
    active: Active,
    meta: SnapshotMetaOf<TypeConfig>,
    max_bytes: usize,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn check_cap(cap: usize) -> io::Result<()> {
    if cap == 0 || cap > BYTE_LIMIT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid snapshot cap",
        ));
    }
    Ok(())
}

fn bounded_read(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(invalid("snapshot file is not regular"));
    }
    let file = fs::File::open(path)?;
    if file.metadata()?.len() > limit as u64 {
        return Err(invalid("snapshot file exceeds bound"));
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(invalid("snapshot file exceeds bound"));
    }
    Ok(bytes)
}

fn validate_meta(meta: &SnapshotMetaOf<TypeConfig>) -> io::Result<()> {
    if meta.snapshot_id.is_empty() || meta.snapshot_id.len() > 4096 {
        return Err(invalid("invalid snapshot identity"));
    }
    if let Some(membership_log) = meta.last_membership.log_id() {
        if meta
            .last_log_id
            .is_none_or(|last| membership_log.index > last.index)
        {
            return Err(invalid("membership is beyond snapshot cut"));
        }
    }
    Ok(())
}

impl SnapshotCatalog {
    fn native_root(&self, group: GroupId) -> PathBuf {
        self.root.join(group.to_string()).join("native-v1")
    }

    fn validate_native_paths(&self, group: GroupId) -> io::Result<()> {
        for path in [
            &self.root,
            &self.root.join(group.to_string()),
            &self.native_root(group),
        ] {
            match fs::symlink_metadata(path) {
                Ok(metadata) if !metadata.file_type().is_dir() => {
                    return Err(invalid("snapshot root is not a directory"))
                }
                Ok(_) => (),
                Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Load only the explicitly active generation; never guess an older fallback.
    pub fn load_native(
        &self,
        group: GroupId,
        max_bytes: usize,
    ) -> io::Result<Option<NativeSnapshot>> {
        let _guard = self
            .native_activation
            .lock()
            .map_err(|_| invalid("snapshot activation poisoned"))?;
        self.load_native_unlocked(group, max_bytes)
    }

    /// Validate the current generation using a fixed-size checksum buffer.
    /// Status readers never retain a whole application image in a ready future.
    pub fn describe_native(
        &self,
        group: GroupId,
        max_bytes: usize,
    ) -> io::Result<Option<NativeSnapshotInfo>> {
        use sha2::Digest;
        let _guard = self
            .native_activation
            .lock()
            .map_err(|_| invalid("snapshot activation poisoned"))?;
        check_cap(max_bytes)?;
        self.validate_native_paths(group)?;
        let root = self.native_root(group);
        let bytes = match bounded_read(&root.join("active.json"), META_LIMIT) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let active: Active = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        let (dir, metadata) = self.read_native_metadata(group, max_bytes, &root, &active)?;
        let path = dir.join("data.bin");
        let file_meta = fs::symlink_metadata(&path)?;
        if !file_meta.file_type().is_file() || file_meta.len() != metadata.size as u64 {
            return Err(invalid("snapshot data type or size mismatch"));
        }
        let mut reader = fs::File::open(path)?.take(metadata.size as u64 + 1);
        let mut hash = sha2::Sha256::new();
        let mut buffer = [0_u8; 8192];
        let mut size = 0;
        loop {
            let read = match reader.read(&mut buffer) {
                Ok(read) => read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            };
            if read == 0 {
                break;
            }
            size += read;
            hash.update(&buffer[..read]);
        }
        let checksum: String = hash
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if size != metadata.size || checksum != metadata.data_sha256 {
            return Err(invalid("snapshot data checksum or size mismatch"));
        }
        Ok(Some(NativeSnapshotInfo {
            meta: metadata.meta,
            size: size as u64,
        }))
    }

    fn load_native_unlocked(
        &self,
        group: GroupId,
        max_bytes: usize,
    ) -> io::Result<Option<NativeSnapshot>> {
        check_cap(max_bytes)?;
        self.validate_native_paths(group)?;
        let root = self.native_root(group);
        let active_bytes = match bounded_read(&root.join("active.json"), META_LIMIT) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let active: Active = serde_json::from_slice(&active_bytes).map_err(io::Error::other)?;
        self.read_native_generation(group, max_bytes, &root, &active)
            .map(Some)
    }

    fn read_native_generation(
        &self,
        group: GroupId,
        max_bytes: usize,
        root: &Path,
        active: &Active,
    ) -> io::Result<NativeSnapshot> {
        let (dir, metadata) = self.read_native_metadata(group, max_bytes, root, active)?;
        let data = bounded_read(&dir.join("data.bin"), metadata.size)?;
        if data.len() != metadata.size || hex_sha256(&data) != metadata.data_sha256 {
            return Err(invalid("snapshot data checksum or size mismatch"));
        }
        Ok(NativeSnapshot {
            meta: metadata.meta,
            data,
        })
    }

    fn read_native_metadata(
        &self,
        group: GroupId,
        max_bytes: usize,
        root: &Path,
        active: &Active,
    ) -> io::Result<(PathBuf, NativeMeta)> {
        if active.version != VERSION
            || active.group != group
            || active.generation.len() != 64
            || !active.generation.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid("incompatible snapshot manifest"));
        }
        let dir = root.join(&active.generation);
        if !fs::symlink_metadata(&dir)?.file_type().is_dir() {
            return Err(invalid("snapshot generation is not a directory"));
        }
        let metadata_bytes = bounded_read(&dir.join("native-meta.json"), META_LIMIT)?;
        if hex_sha256(&metadata_bytes) != active.metadata_sha256 {
            return Err(invalid("snapshot metadata checksum mismatch"));
        }
        let metadata: NativeMeta =
            serde_json::from_slice(&metadata_bytes).map_err(io::Error::other)?;
        if metadata.version != VERSION || metadata.group != group || metadata.size > max_bytes {
            return Err(invalid("incompatible snapshot metadata"));
        }
        validate_meta(&metadata.meta)?;
        Ok((dir, metadata))
    }

    /// Persist a candidate off the application lock, before restore/activation.
    pub fn stage_native(
        &self,
        group: GroupId,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: &[u8],
        max_bytes: usize,
    ) -> io::Result<NativeSnapshotStage> {
        check_cap(max_bytes)?;
        validate_meta(meta)?;
        if data.len() > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot capture limit",
            ));
        }
        let metadata = NativeMeta {
            version: VERSION,
            group,
            meta: meta.clone(),
            size: data.len(),
            data_sha256: hex_sha256(data),
        };
        let metadata_bytes = serde_json::to_vec(&metadata).map_err(io::Error::other)?;
        if metadata_bytes.len() > META_LIMIT {
            return Err(invalid("snapshot metadata exceeds bound"));
        }
        let generation = hex_sha256(&metadata_bytes);
        let root = self.native_root(group);
        self.validate_native_paths(group)?;
        fs::create_dir_all(&root)?;
        // Persist every new directory component down to the caller-owned root.
        sync_dir(&root)?;
        sync_dir(root.parent().unwrap())?;
        sync_dir(&self.root)?;
        if let Some(parent) = self.root.parent() {
            sync_dir(parent)?;
        }
        let stage = root.join(format!(
            ".stage-{}-{}",
            std::process::id(),
            NEXT_STAGE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&stage)?;
        write_fsync(&stage.join("data.bin"), data)?;
        tracing::debug!(target: "multiraft::native_catalog", phase = "data_synced", "native snapshot publication");
        write_fsync(&stage.join("native-meta.json"), &metadata_bytes)?;
        tracing::debug!(target: "multiraft::native_catalog", phase = "metadata_synced", "native snapshot publication");
        sync_dir(&stage)?;
        tracing::debug!(target: "multiraft::native_catalog", phase = "generation_synced", "native snapshot publication");
        let _guard = self
            .native_activation
            .lock()
            .map_err(|_| invalid("snapshot activation poisoned"))?;
        let final_dir = root.join(&generation);
        if final_dir.exists() {
            // Content addressing never permits overwriting an immutable generation.
            if bounded_read(&final_dir.join("native-meta.json"), META_LIMIT)? != metadata_bytes
                || bounded_read(&final_dir.join("data.bin"), max_bytes)? != data
            {
                return Err(invalid("existing snapshot generation is corrupt"));
            }
            fs::remove_dir_all(&stage)?;
        } else {
            fs::rename(&stage, &final_dir)?;
        }
        sync_dir(&root)?;
        tracing::debug!(target: "multiraft::native_catalog", phase = "generation_published", "native snapshot publication");
        Ok(NativeSnapshotStage {
            group,
            root,
            active: Active {
                version: VERSION,
                group,
                generation,
                metadata_sha256: hex_sha256(&metadata_bytes),
            },
            meta: meta.clone(),
            max_bytes,
        })
    }

    pub fn activate_native(&self, staged: NativeSnapshotStage) -> io::Result<()> {
        if staged.root != self.native_root(staged.group) {
            return Err(invalid("foreign snapshot stage"));
        }
        let _guard = self
            .native_activation
            .lock()
            .map_err(|_| invalid("snapshot activation poisoned"))?;
        self.validate_native_paths(staged.group)?;
        let _validated_candidate = self.read_native_generation(
            staged.group,
            staged.max_bytes,
            &staged.root,
            &staged.active,
        )?;
        if let Some(current) = self.load_native_unlocked(staged.group, staged.max_bytes)? {
            if current.meta.last_log_id > staged.meta.last_log_id {
                return Err(invalid("snapshot activation would regress"));
            }
            if current.meta.last_log_id == staged.meta.last_log_id {
                // A different snapshot ID is normal at the same cut, but membership is not.
                if current.meta.last_membership != staged.meta.last_membership {
                    return Err(invalid("conflicting snapshot membership at same cut"));
                }
            }
        }
        let previous = match bounded_read(&staged.root.join("active.json"), META_LIMIT) {
            Ok(bytes) => Some(serde_json::from_slice::<Active>(&bytes).map_err(io::Error::other)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let bytes = serde_json::to_vec(&staged.active).map_err(io::Error::other)?;
        let pending = staged.root.join("active.pending");
        write_fsync(&pending, &bytes)?;
        tracing::debug!(target: "multiraft::native_catalog", phase = "manifest_synced", "native snapshot publication");
        fs::rename(&pending, staged.root.join("active.json"))?;
        tracing::debug!(target: "multiraft::native_catalog", phase = "manifest_renamed", "native snapshot publication");
        sync_dir(&staged.root)?;
        tracing::debug!(target: "multiraft::native_catalog", phase = "active_synced", "native snapshot publication");
        // Readers share activation's lock and return owned bytes. No live reader
        // holds a file after this point. Other staged generations are untouched.
        if let Some(previous) = previous {
            if previous.generation != staged.active.generation {
                fs::remove_dir_all(staged.root.join(previous.generation))?;
                tracing::debug!(target: "multiraft::native_catalog", phase = "obsolete_removed", "native snapshot publication");
                sync_dir(&staged.root)?;
            }
        }
        Ok(())
    }

    pub fn publish_native(
        &self,
        group: GroupId,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: &[u8],
        max_bytes: usize,
    ) -> io::Result<()> {
        let stage = self.stage_native(group, meta, data, max_bytes)?;
        self.activate_native(stage)
    }
}
