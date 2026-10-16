//! Durable local identity. Opening is synchronous and belongs before runtime startup.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{ClusterLayout, LayoutConfig, ManifestDigest, NodeId, SlotError};

pub const MANIFEST_FILE_NAME: &str = "slot-manifest.json";
const CREATION_FILE_NAME: &str = ".slot-manifest.creating";
const MAX_MANIFEST_BYTES: u64 = 512 * 1024 * 1024;

/// Validated local identity. Local node identity is separate from cluster layout identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotManifest {
    layout: ClusterLayout,
    local_node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskManifest {
    layout: LayoutConfig,
    layout_digest: ManifestDigest,
    local_node_id: NodeId,
}

impl SlotManifest {
    /// Create only in an empty directory, or verify an existing identity exactly.
    ///
    /// Writes use create-new files and sync both file and directory. The creation
    /// marker serializes competing initializers; a crash leaves evidence and fails
    /// closed on restart. Interrupted creation requires explicit operator recovery.
    /// No existing files are overwritten or silently adopted.
    pub fn open_or_create(
        data_dir: impl AsRef<Path>,
        layout: &ClusterLayout,
        local_node_id: NodeId,
    ) -> Result<Self, SlotError> {
        layout.node(local_node_id)?;
        let data_dir = data_dir.as_ref();
        let newly_created = match fs::create_dir(data_dir) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(error) => return Err(error.into()),
        };
        if newly_created {
            // Persist the directory entry before storing identities inside it.
            File::open(
                data_dir
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )?
            .sync_all()?;
        }
        let manifest_path = data_dir.join(MANIFEST_FILE_NAME);
        let marker_path = data_dir.join(CREATION_FILE_NAME);
        if marker_path.try_exists()? {
            return Err(SlotError::CreationInProgress);
        }
        if manifest_path.try_exists()? {
            let existing = Self::read_existing(&manifest_path, layout, local_node_id)?;
            if marker_path.try_exists()? {
                return Err(SlotError::CreationInProgress);
            }
            return Ok(existing);
        }
        if fs::read_dir(data_dir)?.next().transpose()?.is_some() {
            return Err(SlotError::DataDirectoryNotEmpty);
        }
        let marker = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker_path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(SlotError::CreationInProgress)
            }
            Err(error) => return Err(error.into()),
        };
        marker.sync_all()?;
        File::open(data_dir)?.sync_all()?;
        // A competing initializer may have completed between our first check and lock acquisition.
        if manifest_path.try_exists()? {
            let existing = Self::read_existing(&manifest_path, layout, local_node_id);
            fs::remove_file(&marker_path)?;
            File::open(data_dir)?.sync_all()?;
            return existing;
        }
        for entry in fs::read_dir(data_dir)? {
            if entry?.file_name() != CREATION_FILE_NAME {
                return Err(SlotError::DataDirectoryNotEmpty);
            }
        }
        let disk = DiskManifest {
            layout: layout.config().clone(),
            layout_digest: layout.digest(),
            local_node_id,
        };
        let bytes = serde_json::to_vec(&disk)
            .map_err(|error| SlotError::InvalidManifest(error.to_string()))?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(SlotError::InvalidManifest(
                "manifest exceeds size limit".into(),
            ));
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&manifest_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        File::open(data_dir)?.sync_all()?;
        fs::remove_file(marker_path)?;
        File::open(data_dir)?.sync_all()?;
        Ok(Self {
            layout: layout.clone(),
            local_node_id,
        })
    }

    fn read_existing(
        path: &Path,
        expected: &ClusterLayout,
        local_node_id: NodeId,
    ) -> Result<Self, SlotError> {
        let file = File::open(path)?;
        let mut bytes = Vec::new();
        file.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(SlotError::InvalidManifest(
                "manifest exceeds size limit".into(),
            ));
        }
        let disk: DiskManifest = serde_json::from_slice(&bytes)
            .map_err(|error| SlotError::InvalidManifest(error.to_string()))?;
        let persisted = ClusterLayout::new(disk.layout)
            .map_err(|error| SlotError::InvalidManifest(error.to_string()))?;
        if persisted.digest() != disk.layout_digest {
            return Err(SlotError::InvalidManifest(
                "stored digest disagrees with layout".into(),
            ));
        }
        if &persisted != expected {
            return Err(SlotError::ManifestMismatch("cluster layout"));
        }
        if disk.local_node_id != local_node_id {
            return Err(SlotError::ManifestMismatch("local node identity"));
        }
        Ok(Self {
            layout: persisted,
            local_node_id,
        })
    }

    pub fn layout(&self) -> &ClusterLayout {
        &self.layout
    }
    pub fn layout_digest(&self) -> ManifestDigest {
        self.layout.digest()
    }
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }
}
