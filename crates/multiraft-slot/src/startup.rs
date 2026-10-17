//! Native preference startup plan and the pre-start local Raft state probe.
//!
//! The bootstrap decision (which slots may initialize) belongs to the caller:
//! it probes local state with [`SlotNodeConfig::probe_local_state`] before
//! starting, chooses an [`InitializePolicy`] per slot, and passes the result in
//! [`SlotNodeConfig::startup`]. This crate never derives policy on its own.
use crate::{NodeId, SlotId, SlotNodeConfig};
use multiraft_core::{StartupProvenance, TypeConfig};
use multiraft_net::InitializePolicy;
use multiraft_store::{FileLogStoreOf, RocksLogDb, SnapshotCatalog};
use std::{collections::BTreeMap, io, path::Path, time::Duration};

/// Accepted [`SlotStartupPlan::bootstrap_grace`] range (the native admission bounds).
pub const BOOTSTRAP_GRACE_RANGE: std::ops::RangeInclusive<Duration> =
    Duration::from_millis(100)..=Duration::from_millis(2000);
/// Upper bound of [`SlotStartupPlan::recovery_timeout`] (the native cleanup budget).
pub const MAX_RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// How one [`crate::SlotNodeOwner::start`] brings up the local voter slots.
///
/// All local voter slots are registered first and then recovered concurrently
/// by one native `start_groups_with_preference` batch. Each slot carries the
/// layout's `preferred_initializer` and the policy chosen here:
///
/// - [`InitializePolicy::Allowed`] (default for every slot): a pristine slot
///   initializes the layout membership — the preferred initializer at once,
///   any other voter only after `bootstrap_grace` if the group is still
///   uninitialized (native fallback, reported as `fallback == true`).
/// - [`InitializePolicy::Forbidden`]: a pristine slot never initializes; it
///   must be caught up by an existing leader, with the native rejoin vote
///   guard armed until it has applied the leader-commit anchor. Startup fails
///   if that does not happen within `recovery_timeout`.
///
/// Persisted slots only recover under either policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotStartupPlan {
    /// Per-slot policy; a local voter slot absent here is `Allowed`. Every key
    /// must be a slot this node votes in, otherwise startup is rejected.
    pub initialize: BTreeMap<SlotId, InitializePolicy>,
    /// Native grace before a non-preferred voter falls back to initializing.
    /// Must be within [`BOOTSTRAP_GRACE_RANGE`]; default 1000 ms.
    pub bootstrap_grace: Duration,
    /// Per-group recovery budget measured from native registration, nonzero
    /// and at most [`MAX_RECOVERY_TIMEOUT`]; default 30 s. The effective budget
    /// is additionally clamped to the `deadline` passed to `start`.
    pub recovery_timeout: Duration,
}

impl Default for SlotStartupPlan {
    fn default() -> Self {
        Self {
            initialize: BTreeMap::new(),
            bootstrap_grace: Duration::from_millis(1000),
            recovery_timeout: MAX_RECOVERY_TIMEOUT,
        }
    }
}

impl SlotStartupPlan {
    /// Set one slot's policy.
    #[must_use]
    pub fn with_policy(mut self, slot: SlotId, policy: InitializePolicy) -> Self {
        self.initialize.insert(slot, policy);
        self
    }

    /// The policy that applies to `slot` (`Allowed` when not listed).
    pub fn policy(&self, slot: SlotId) -> InitializePolicy {
        self.initialize.get(&slot).copied().unwrap_or_default()
    }

    pub(crate) fn validate(&self, local_slots: &[SlotId]) -> bool {
        BOOTSTRAP_GRACE_RANGE.contains(&self.bootstrap_grace)
            && !self.recovery_timeout.is_zero()
            && self.recovery_timeout <= MAX_RECOVERY_TIMEOUT
            && self.initialize.keys().all(|s| local_slots.contains(s))
    }
}

/// Failure of [`SlotNodeConfig::probe_local_state`]. Unknown or inconsistent
/// on-disk state is an error, never `Pristine`.
#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    #[error("invalid slot runtime configuration")]
    InvalidConfig,
    #[error("local Raft state of slot {slot:?} cannot be classified")]
    Storage {
        /// `None` when the failure is node-wide (log backend selection/open).
        slot: Option<SlotId>,
        #[source]
        source: io::Error,
    },
}

impl SlotNodeConfig {
    /// Classify each local voter slot's Raft storage as `Pristine` or
    /// `Persisted` without starting anything and without creating, locking or
    /// modifying any file.
    ///
    /// The classification is the one the native batch reports as
    /// `GroupStartupReport::provenance`, composed from the same storage
    /// classifiers as `MultiRaft::local_provenance` / `log_provenance` in
    /// multiraft-net (`multiraft/startup.rs`, `multiraft/recovery.rs`):
    ///
    /// - RocksDB log: `Persisted` iff the group's metadata has `ever_written`
    ///   (a vote, committed id, purge or any appended entry was ever stored).
    ///   An absent database (no `raft-log-backend` marker and no
    ///   `raft-rocksdb/`) is pristine; a marker without `raft-rocksdb/CURRENT`
    ///   or an unknown marker is an error, as in the native opener.
    /// - Native snapshot catalog (`snapshots/`): `Persisted` iff a validated
    ///   active snapshot exists. Unrecognized namespaces are errors.
    /// - A RocksDB-pristine group with persisted legacy FileLog or snapshot
    ///   history is an error (the native runtime refuses to start it).
    ///
    /// `slot-manifest.json` is neither read nor written: a crash after the
    /// manifest was created but before any Raft state was stored leaves a
    /// manifest-only directory, which is `Pristine`. Nonexistent `data_dir`
    /// means every slot is `Pristine`.
    ///
    /// Call it only while no runtime owns `data_dir` (before
    /// [`crate::SlotNodeOwner::start`] or after shutdown completed).
    ///
    /// # Errors
    ///
    /// [`ProbeError::InvalidConfig`] when the configuration would be rejected
    /// by `start`; [`ProbeError::Storage`] for unreadable, unknown or
    /// inconsistent local state.
    pub fn probe_local_state(&self) -> Result<BTreeMap<SlotId, StartupProvenance>, ProbeError> {
        self.validate().map_err(|_| ProbeError::InvalidConfig)?;
        let groups = local_voter_groups(self)?;
        probe(&self.data_dir, self.native.max_snapshot_bytes, &groups)
    }
}

fn local_voter_groups(config: &SlotNodeConfig) -> Result<Vec<(SlotId, u64)>, ProbeError> {
    local_voter_slots(&config.layout, config.node_id)
        .into_iter()
        .map(|slot| {
            config
                .layout
                .group_for_slot(slot)
                .map(|group| (slot, group))
                .map_err(|_| ProbeError::InvalidConfig)
        })
        .collect()
}

pub(crate) fn local_voter_slots(layout: &crate::ClusterLayout, node: NodeId) -> Vec<SlotId> {
    layout
        .config()
        .slots
        .iter()
        .filter(|a| a.voters.contains(&node))
        .map(|a| a.slot)
        .collect()
}

fn storage(slot: Option<SlotId>) -> impl FnOnce(io::Error) -> ProbeError {
    move |source| ProbeError::Storage { slot, source }
}

/// Mirrors `open_node_log` backend selection, read-only.
fn open_log(root: &Path) -> io::Result<Option<RocksLogDb<TypeConfig>>> {
    let database = root.join("raft-rocksdb");
    let marked = match std::fs::read(root.join("raft-log-backend")) {
        Ok(bytes) if bytes == b"rocksdb-v1\n" => true,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported Raft log backend marker",
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error),
    };
    if database.join("CURRENT").is_file() {
        return RocksLogDb::open_read_only(&database).map(Some);
    }
    if marked {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RocksDB Raft log database missing after backend selection",
        ));
    }
    Ok(None)
}

fn probe(
    root: &Path,
    max_snapshot_bytes: usize,
    groups: &[(SlotId, u64)],
) -> Result<BTreeMap<SlotId, StartupProvenance>, ProbeError> {
    let db = open_log(root).map_err(storage(None))?;
    let catalog = SnapshotCatalog::new(root.join("snapshots"), 1);
    let mut out = BTreeMap::new();
    for &(slot, group) in groups {
        let classify = || -> io::Result<StartupProvenance> {
            let log = match &db {
                Some(db) => db.startup_provenance(group)?,
                None => StartupProvenance::Pristine,
            };
            let snapshots = catalog.probe_provenance(group, max_snapshot_bytes)?;
            if log == StartupProvenance::Pristine {
                let legacy =
                    FileLogStoreOf::startup_provenance(root.join(format!("group-{group}")))?;
                if legacy == StartupProvenance::Persisted
                    || snapshots == StartupProvenance::Persisted
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "RocksDB group has no local log basis but persisted FileLog or snapshot history exists",
                    ));
                }
            }
            Ok(if snapshots == StartupProvenance::Persisted {
                snapshots
            } else {
                log
            })
        };
        out.insert(slot, classify().map_err(storage(Some(slot)))?);
    }
    Ok(out)
}
