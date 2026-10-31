//! New-space creation facts. Legacy identities are never converted or adopted here.
use crate::{
    ClusterLayout, LayoutConfig, ManifestDigest, NodeId, RouteDigest, RuntimeError,
    SlotLocalIdentity, SlotStartupDisposition, StrictSlotStartup, MANIFEST_FILE_NAME,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

const FORMAT: u32 = 1;
const MARKER: &str = ".strict-slot.creating";
const COMPLETE: &str = ".strict-slot.genesis-complete";
const MAX_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    strict_slot_format: u32,
    layout: LayoutConfig,
    genesis_digest: ManifestDigest,
    route_digest: RouteDigest,
    local_node_id: NodeId,
    identity: SlotLocalIdentity,
    candidate: bool,
}
impl Manifest {
    fn expected(layout: &ClusterLayout, node: NodeId, startup: &StrictSlotStartup) -> Self {
        Self {
            strict_slot_format: FORMAT,
            layout: layout.config().clone(),
            genesis_digest: layout.digest(),
            route_digest: layout.route_digest(),
            local_node_id: node,
            identity: startup.identity.clone(),
            candidate: startup.disposition == SlotStartupDisposition::Candidate,
        }
    }
}
fn reject() -> RuntimeError {
    RuntimeError::ManifestMismatch
}
fn io<T>(value: std::io::Result<T>) -> Result<T, RuntimeError> {
    value.map_err(|_| reject())
}
fn read(path: &Path) -> Result<Manifest, RuntimeError> {
    let file = io(File::open(path))?;
    let mut bytes = Vec::new();
    io(file.take(MAX_BYTES + 1).read_to_end(&mut bytes))?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(reject());
    }
    serde_json::from_slice(&bytes).map_err(|_| reject())
}
fn publish(path: &Path, manifest: &Manifest) -> Result<(), RuntimeError> {
    let bytes = serde_json::to_vec(manifest).map_err(|_| reject())?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(reject());
    }
    let mut file = io(OpenOptions::new().write(true).create_new(true).open(path))?;
    io(file.write_all(&bytes))?;
    io(file.sync_all())?;
    io(io(File::open(path.parent().ok_or_else(reject)?))?.sync_all())
}

/// Persist a one-time local creation fact before any listener or native Group exists.
pub(crate) fn prepare(
    dir: &Path,
    layout: &ClusterLayout,
    node: NodeId,
    startup: &StrictSlotStartup,
) -> Result<(), RuntimeError> {
    let expected = Manifest::expected(layout, node, startup);
    let path = dir.join(MANIFEST_FILE_NAME);
    let marker = dir.join(MARKER);
    if io(marker.try_exists())? {
        return Err(reject());
    }
    if io(path.try_exists())? {
        if startup.disposition == SlotStartupDisposition::Genesis || read(&path)? != expected {
            return Err(reject());
        }
        if startup.identity.protocol_version == crate::CONTROLLED_SLOT_PROTOCOL_VERSION {
            // Controlled native receipts and anchors independently validate every recovery.
            // Candidate identities may now contain an authorized learner generation.
            return Ok(());
        }
        if expected.candidate {
            // Candidate has no native state, receipts, snapshots or creation debris to adopt.
            for entry in io(fs::read_dir(dir))? {
                let entry = io(entry)?;
                if entry.file_name() == MANIFEST_FILE_NAME {
                    continue;
                }
                // The empty native owner creates its catalog root, but no Group images.
                if entry.file_name() != "snapshots"
                    || !io(entry.file_type())?.is_dir()
                    || io(fs::read_dir(entry.path()))?
                        .next()
                        .transpose()
                        .map_err(|_| reject())?
                        .is_some()
                {
                    return Err(reject());
                }
            }
        } else if read(&dir.join(COMPLETE))? != expected {
            // An interrupted genesis is not evidence of permission to initialize again.
            return Err(reject());
        }
        return Ok(());
    }
    if startup.disposition == SlotStartupDisposition::RecoverExisting {
        return Err(reject());
    }
    match fs::create_dir(dir) {
        Ok(()) => {
            let parent = dir
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            io(io(File::open(parent))?.sync_all())?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(reject()),
    }
    if io(fs::read_dir(dir))?
        .next()
        .transpose()
        .map_err(|_| reject())?
        .is_some()
    {
        return Err(reject());
    }
    let file = io(OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker))?;
    io(file.sync_all())?;
    io(io(File::open(dir))?.sync_all())?;
    // Recheck after exclusive create: no competing writer's files may be adopted.
    for entry in io(fs::read_dir(dir))? {
        if io(entry)?.file_name() != MARKER {
            return Err(reject());
        }
    }
    publish(&path, &expected)?;
    io(fs::remove_file(marker))?;
    io(io(File::open(dir))?.sync_all())
}

/// Complete only after the local committed genesis basis and both listener owners are ready.
pub(crate) fn complete(
    dir: &Path,
    layout: &ClusterLayout,
    node: NodeId,
    startup: &StrictSlotStartup,
) -> Result<(), RuntimeError> {
    if startup.disposition == SlotStartupDisposition::Genesis {
        publish(
            &dir.join(COMPLETE),
            &Manifest::expected(layout, node, startup),
        )?;
    }
    Ok(())
}
