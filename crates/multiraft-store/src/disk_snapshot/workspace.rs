//! One Group's snapshot workspace: the published checkpoint, capture temporaries,
//! sender leases, receive staging and the install marker.
//!
//! Layout (all names are direct children of the workspace root):
//! - `checkpoint-{index:020}-{nonce}/` plus `…manifest`: the published snapshot;
//!   the manifest is written last, so a directory without one is incomplete.
//! - `tmp-{nonce}/`: a capture not yet published.
//! - `staging-{nonce}/`: a transfer being received.
//! - `checkpoint-….send-{nonce}/`: a sender's hard-link lease.
//! - `retired-{nonce}/`: the previous live store during an install swap.
//! - `installed`: the applied position of the last swapped-in checkpoint.
use super::{invalid, nonce, DiskManifest, ManifestFile, DISK_SNAPSHOT_ID_PREFIX};
use crate::durability::sync_dir;
use multiraft_core::TypeConfig;
use multiraft_fsm::GroupId;
use openraft::alias::{LogIdOf, SnapshotMetaOf};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const CHECKPOINT: &str = "checkpoint-";
const MANIFEST_SUFFIX: &str = ".manifest";
const INSTALLED: &str = "installed";

#[derive(Debug, Clone)]
pub(crate) struct Workspace {
    root: PathBuf,
}

/// A captured checkpoint directory; removed on drop unless published.
#[derive(Debug)]
pub(crate) struct TempCheckpoint {
    path: Option<PathBuf>,
}
impl TempCheckpoint {
    pub(crate) fn path(&self) -> &Path {
        self.path.as_deref().expect("live temporary checkpoint")
    }
    fn take(mut self) -> PathBuf {
        self.path.take().expect("live temporary checkpoint")
    }
}
impl Drop for TempCheckpoint {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

/// The published snapshot of a workspace.
pub(crate) struct Published {
    pub(crate) bytes: Vec<u8>,
    pub(crate) manifest: DiskManifest,
}

pub(super) fn write_durable(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp-write");
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    sync_dir(path.parent().unwrap_or_else(|| Path::new(".")))
}

/// Sorted regular files of a checkpoint directory; fsyncs each file and the directory.
pub(super) fn list_synced(dir: &Path) -> io::Result<Vec<ManifestFile>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(invalid("checkpoint contains a non-regular entry"));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("checkpoint file name is not UTF-8"))?;
        let file = fs::File::open(entry.path())?;
        file.sync_all()?;
        files.push(ManifestFile {
            name,
            size: file.metadata()?.len(),
        });
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    sync_dir(dir)?;
    Ok(files)
}

impl Workspace {
    /// Create the root and remove everything except the newest complete
    /// published checkpoint, the install marker and swap leftovers (those
    /// belong to [`super::recover_swap`]). Call before any snapshot work starts.
    pub(crate) fn open(root: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        let workspace = Self { root };
        let keep = workspace.newest_complete()?;
        for entry in fs::read_dir(&workspace.root)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let keep_entry = name == INSTALLED
                || name.starts_with("retired-")
                || keep.as_ref().is_some_and(|stem| {
                    name == *stem || name == format!("{stem}{MANIFEST_SUFFIX}")
                });
            if !keep_entry {
                remove_entry(&entry.path())?;
            }
        }
        sync_dir(&workspace.root)?;
        Ok(workspace)
    }

    /// Stem (`checkpoint-…`) of the newest checkpoint whose directory and manifest both exist.
    fn newest_complete(&self) -> io::Result<Option<String>> {
        let mut best: Option<String> = None;
        for entry in fs::read_dir(&self.root)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            let Some(stem) = name.strip_suffix(MANIFEST_SUFFIX) else {
                continue;
            };
            if !stem.starts_with(CHECKPOINT) || !self.root.join(stem).is_dir() {
                continue;
            }
            if best.as_deref().is_none_or(|current| stem > current) {
                best = Some(stem.to_owned());
            }
        }
        Ok(best)
    }

    pub(crate) fn temp_checkpoint(&self) -> TempCheckpoint {
        TempCheckpoint {
            path: Some(self.root.join(format!("tmp-{}", nonce()))),
        }
    }

    pub(crate) fn staging_dir(&self) -> PathBuf {
        self.root.join(format!("staging-{}", nonce()))
    }

    /// The newest published checkpoint, if any.
    pub(crate) fn latest(&self, group: GroupId) -> io::Result<Option<Published>> {
        let Some(stem) = self.newest_complete()? else {
            return Ok(None);
        };
        let bytes = fs::read(self.root.join(format!("{stem}{MANIFEST_SUFFIX}")))?;
        let manifest = DiskManifest::decode(&bytes, group)?;
        if manifest.source != self.root.join(&stem) {
            return Err(invalid("published disk snapshot moved"));
        }
        Ok(Some(Published { bytes, manifest }))
    }

    /// Publish `capture` (taken at `meta`) as the current snapshot and remove
    /// older ones. A newer published snapshot wins and the capture is dropped.
    pub(crate) fn publish(
        &self,
        group: GroupId,
        capture: TempCheckpoint,
        mut meta: SnapshotMetaOf<TypeConfig>,
    ) -> io::Result<Published> {
        if let Some(current) = self.latest(group)? {
            if current.manifest.meta.last_log_id >= meta.last_log_id {
                return Ok(current);
            }
        }
        let index = meta
            .last_log_id
            .map(|id| id.index)
            .ok_or_else(|| invalid("disk snapshot without an applied position"))?;
        let suffix = nonce();
        let stem = format!("{CHECKPOINT}{index:020}-{suffix}");
        let files = list_synced(capture.path())?;
        let dir = self.root.join(&stem);
        fs::rename(capture.take(), &dir)?;
        meta.snapshot_id = format!("{DISK_SNAPSHOT_ID_PREFIX}{group}-{index}-{suffix}");
        let manifest = DiskManifest {
            group,
            meta,
            files,
            source: dir,
        };
        let bytes = manifest.encode()?;
        write_durable(&self.root.join(format!("{stem}{MANIFEST_SUFFIX}")), &bytes)?;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let older = name.starts_with(CHECKPOINT)
                && !name.contains(".send-")
                && name != stem
                && name != format!("{stem}{MANIFEST_SUFFIX}");
            if older {
                remove_entry(&entry.path())?;
            }
        }
        sync_dir(&self.root)?;
        Ok(Published { bytes, manifest })
    }

    /// Record, before a swap, the position the store will have after it.
    pub(crate) fn write_installed(&self, log_id: &LogIdOf<TypeConfig>) -> io::Result<()> {
        let bytes = serde_json::to_vec(log_id).map_err(io::Error::other)?;
        write_durable(&self.root.join(INSTALLED), &bytes)
    }

    pub(crate) fn read_installed(&self) -> io::Result<Option<LogIdOf<TypeConfig>>> {
        match fs::read(self.root.join(INSTALLED)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| invalid(error.to_string())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

pub(super) fn remove_entry(path: &Path) -> io::Result<()> {
    let result = if fs::symlink_metadata(path)?.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    };
    match result {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}
