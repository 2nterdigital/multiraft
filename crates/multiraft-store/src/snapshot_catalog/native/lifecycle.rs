//! Candidate ownership is serialized with publication, including equal-content stages.
use super::{bounded_read, invalid, Active, NativeSnapshotStage, META_LIMIT, VERSION};
use crate::durability::sync_dir;
use multiraft_fsm::GroupId;
use std::{
    collections::BTreeMap,
    fs, io,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock, Weak,
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Snapshot validation has its own runtime admission. This also bounds direct
// catalog users, including duplicate handles to a content-addressed generation.
const STAGE_LIMIT: usize = 16;

// Reopened catalogs in one process must share candidate ownership as well as
// clones; otherwise one could discard files still owned by another catalog.
type Registries = Mutex<BTreeMap<PathBuf, Weak<Mutex<NativeStageRegistry>>>>;
static REGISTRIES: OnceLock<Registries> = OnceLock::new();

pub(in crate::snapshot_catalog) fn shared_registry(
    root: &std::path::Path,
) -> Arc<Mutex<NativeStageRegistry>> {
    let absolute = if root.is_absolute() {
        root.to_owned()
    } else {
        std::env::current_dir().unwrap_or_default().join(root)
    };
    let normalized = absolute
        .components()
        .fold(PathBuf::new(), |mut path, part| {
            match part {
                std::path::Component::CurDir => (),
                std::path::Component::ParentDir => {
                    path.pop();
                }
                part => path.push(part.as_os_str()),
            }
            path
        });
    // Canonicalize the deepest existing ancestor as the root may not exist yet.
    // This keeps /var and /private/var aliases on macOS in one ownership domain.
    let mut ancestor = normalized.clone();
    let mut suffix = Vec::new();
    let key = loop {
        if let Ok(mut existing) = fs::canonicalize(&ancestor) {
            for component in suffix.iter().rev() {
                existing.push(component);
            }
            break existing;
        }
        let Some(name) = ancestor.file_name().map(ToOwned::to_owned) else {
            break normalized;
        };
        suffix.push(name);
        ancestor.pop();
    };
    let mut registries = REGISTRIES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    registries.retain(|_, registry| registry.strong_count() != 0);
    if let Some(registry) = registries.get(&key).and_then(Weak::upgrade) {
        return registry;
    }
    let registry = Arc::new(Mutex::new(NativeStageRegistry::default()));
    registries.insert(key, Arc::downgrade(&registry));
    registry
}

#[derive(Debug)]
pub(in crate::snapshot_catalog) struct NativeStageRegistry {
    owners: BTreeMap<(GroupId, String), usize>,
    admission: Arc<Semaphore>,
    cleanup_failed: BTreeMap<GroupId, Arc<AtomicBool>>,
}

impl Default for NativeStageRegistry {
    fn default() -> Self {
        Self {
            owners: BTreeMap::new(),
            admission: Arc::new(Semaphore::new(STAGE_LIMIT)),
            cleanup_failed: BTreeMap::new(),
        }
    }
}

impl NativeStageRegistry {
    pub(super) fn ensure_clean(&self, group: GroupId) -> io::Result<()> {
        if self
            .cleanup_failed
            .get(&group)
            .is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            return Err(io::Error::other(
                "native snapshot cleanup requires successful provenance scan",
            ));
        }
        Ok(())
    }

    pub(super) fn admit(&self, group: GroupId) -> io::Result<OwnedSemaphorePermit> {
        self.ensure_clean(group)?;
        self.admission.clone().try_acquire_owned().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "native snapshot stage admission exhausted",
            )
        })
    }

    pub(super) fn acquire(&mut self, group: GroupId, generation: &str) {
        *self
            .owners
            .entry((group, generation.to_owned()))
            .or_default() += 1;
    }

    pub(super) fn owns(&self, group: GroupId, generation: &str) -> bool {
        self.owners.contains_key(&(group, generation.to_owned()))
    }

    pub(super) fn cleanup_flag(&mut self, group: GroupId) -> Arc<AtomicBool> {
        self.cleanup_failed.entry(group).or_default().clone()
    }

    pub(super) fn cleanup_completed(&mut self, group: GroupId) {
        if let Some(flag) = self.cleanup_failed.get(&group) {
            // In-flight temporary writers still hold this exact Arc. Preserve
            // it so a later cleanup failure cannot signal a detached flag.
            flag.store(false, Ordering::Release);
        }
        self.cleanup_failed
            .retain(|_, flag| flag.load(Ordering::Acquire) || Arc::strong_count(flag) != 1);
    }

    fn prune_clean(&mut self, group: GroupId) {
        if self
            .cleanup_failed
            .get(&group)
            .is_some_and(|flag| !flag.load(Ordering::Acquire) && Arc::strong_count(flag) == 1)
        {
            self.cleanup_failed.remove(&group);
        }
    }

    fn release(&mut self, group: GroupId, generation: &str) -> bool {
        let key = (group, generation.to_owned());
        let Some(count) = self.owners.get_mut(&key) else {
            return false;
        };
        *count -= 1;
        if *count != 0 {
            return false;
        }
        self.owners.remove(&key);
        true
    }
}

impl NativeSnapshotStage {
    /// Discard a candidate and report durable cleanup errors. The active
    /// checkpoint and other owners of the same generation always remain intact.
    pub fn discard(mut self) -> io::Result<()> {
        self.release()
    }

    fn release(&mut self) -> io::Result<()> {
        if self.released {
            return Ok(());
        }
        let mut registry = self
            .lifecycle
            .lock()
            .map_err(|_| invalid("snapshot activation poisoned"))?;
        self.released = true;
        if !registry.release(self.group, &self.active.generation) {
            return Ok(());
        }
        let result = self.discard_inactive();
        if result.is_err() {
            // Publish cleanup debt before releasing admission's lock.
            registry
                .cleanup_flag(self.group)
                .store(true, Ordering::Release);
        } else {
            registry.prune_clean(self.group);
        }
        result
    }

    fn discard_inactive(&self) -> io::Result<()> {
        // Never delete a candidate based on an unreadable authority manifest.
        // Preserve those bytes for fail-closed startup diagnosis instead.
        for name in ["active.json", "active.pending"] {
            let bytes = match bounded_read(&self.root.join(name), META_LIMIT) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let active: Active = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
            if active.version != VERSION || active.group != self.group {
                return Err(invalid("incompatible snapshot manifest"));
            }
            if active.generation == self.active.generation {
                if name == "active.json" {
                    return Ok(());
                }
                fs::remove_file(self.root.join(name))?;
            }
        }
        match fs::remove_dir_all(self.root.join(&self.active.generation)) {
            Ok(()) => sync_dir(&self.root),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl Drop for NativeSnapshotStage {
    fn drop(&mut self) {
        if let Err(error) = self.release() {
            tracing::warn!(%error, group = self.group, "native snapshot candidate cleanup failed");
        }
    }
}

/// Own incomplete construction as soon as its directory is created. This also
/// covers errors before a public candidate handle can be returned to its caller.
pub(super) struct TemporaryGeneration {
    pub(super) path: Option<PathBuf>,
    pub(super) cleanup_failed: Arc<AtomicBool>,
}

impl Drop for TemporaryGeneration {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else { return };
        let result =
            fs::remove_dir_all(&path).and_then(|()| path.parent().map_or(Ok(()), sync_dir));
        if let Err(error) = result {
            self.cleanup_failed.store(true, Ordering::Release);
            tracing::warn!(%error, "incomplete native snapshot cleanup failed");
        }
    }
}
