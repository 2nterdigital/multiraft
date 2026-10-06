//! Consumer validation of the native-recovered local application image.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    pub(crate) async fn validate_recovered(&self, group: GroupId) -> Result<(), MultiRaftError> {
        let context = FsmFactoryContext {
            node_id: self.node_id,
            group_id: group,
        };
        let result = self
            .with_fsm(group, |fsm| {
                // Convert consumer panic to a startup rejection while keeping the
                // native Group in the owner registry for cancellation-safe cleanup.
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.fsm_factory.validate_recovered(context, fsm)
                }))
                .unwrap_or_else(|_| Err(anyhow::anyhow!("recovery validation callback panicked")))
            })
            .await
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        result.map_err(|source| {
            tracing::warn!(target: "multiraft::recovery", operation = "recovery_validation",
                phase = "rejected", node_id = self.node_id, group_id = group,
                "consumer rejected recovered application image");
            MultiRaftError::Other(source.context(format!(
                "validate recovered FSM for node {}, group {}",
                self.node_id, group
            )))
        })
    }
}

/// Classify native facts without exporting or logging application payload text.
pub(super) fn native_recovery_error(
    group: GroupId,
    stage: multiraft_core::RecoveryStage,
    source: multiraft_core::typ::Fatal,
) -> MultiRaftError {
    use multiraft_core::{NativeFailure, RecoveryError, RecoveryFailure};
    let failure = match &source {
        openraft::error::Fatal::Stopped => RecoveryFailure::Closed,
        openraft::error::Fatal::Panicked => RecoveryFailure::Backend(NativeFailure::Panicked),
        openraft::error::Fatal::StorageError(_) => RecoveryFailure::Backend(NativeFailure::Storage),
    };
    RecoveryError::new(group, stage, failure, Some(source.into())).into()
}

pub(super) fn durable_local_mode(config: &multiraft_core::ClusterConfig) -> bool {
    !config.data_dir.as_os_str().is_empty()
        && (config.log_storage_backend == LogStorageBackend::RocksDb
            || config.file_log_sync_level != multiraft_core::FileLogSyncLevel::Os)
}

/// Snapshot metadata and forced native commit record, before the constructor can
/// expose this Group to any peer. This is construction provenance, not a new
/// application watermark/current authority. Native construction still performs
/// restoration and committed suffix replay.
pub(super) async fn construction_basis<
    S: StateMachine,
    L: openraft::storage::RaftLogStorage<TypeConfig>,
>(
    group: GroupId,
    log: &mut L,
    sm: &StateMachineStore<S>,
) -> Result<Option<openraft::alias::LogIdOf<TypeConfig>>, MultiRaftError> {
    let storage_error = |source: std::io::Error| {
        MultiRaftError::from(multiraft_core::RecoveryError::new(
            group,
            multiraft_core::RecoveryStage::Construct,
            multiraft_core::RecoveryFailure::Backend(multiraft_core::NativeFailure::Storage),
            Some(source.into()),
        ))
    };
    let committed = log.read_committed().await.map_err(storage_error)?;
    let snapshot = sm
        .native_snapshot_info()
        .await
        .map_err(storage_error)?
        .and_then(|snapshot| snapshot.meta.last_log_id);
    Ok(committed.max(snapshot))
}

impl<S: StateMachine> MultiRaft<S> {
    /// Owned startup's durable-local seam is distinct from the legacy cluster-tail
    /// confirmation. Weak/memory grades retain that existing stronger wait.
    pub(crate) async fn wait_for_owned_recovery(
        &self,
        group: GroupId,
        timeout: Duration,
    ) -> Result<(), MultiRaftError> {
        if !durable_local_mode(&self.config) {
            return self.wait_for_recovery(group, timeout).await;
        }
        let target = self
            .construction_recovery
            .lock()
            .unwrap()
            .get(&group)
            .copied()
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        self.ensure_recovery_running(group)?;
        let result = raft
            .wait(Some(timeout))
            .applied_index_at_least(
                target.map(|id| id.index),
                "durable native constructor basis applied",
            )
            .await;
        if let Err(source) = result {
            self.ensure_recovery_running(group)?;
            let failure = match &source {
                openraft::metrics::WaitError::Timeout(..) => {
                    multiraft_core::RecoveryFailure::Deadline
                }
                openraft::metrics::WaitError::ShuttingDown => {
                    multiraft_core::RecoveryFailure::Closed
                }
            };
            return Err(multiraft_core::RecoveryError::new(
                group,
                multiraft_core::RecoveryStage::Await,
                failure,
                Some(source.into()),
            )
            .into());
        }
        self.ensure_recovery_running(group)?;
        tracing::info!(target: "multiraft::recovery", operation = "owned_recovery", phase = "complete",
            node_id = self.node_id, group_id = group, recovery_basis = "durable_native_constructor",
            applied_target_index = ?target.map(|id| id.index), "native local recovery basis confirmed");
        Ok(())
    }

    pub(crate) fn ensure_recovery_running(&self, group: GroupId) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let state = raft.metrics().borrow_watched().running_state.clone();
        state.map_err(|source| {
            native_recovery_error(group, multiraft_core::RecoveryStage::Await, source)
        })
    }
}

/// Finite source log labels come from the typed recovery result, never its text.
pub(super) fn diagnostic_fields(error: &MultiRaftError) -> (&'static str, &'static str) {
    use multiraft_core::{NativeFailure, RecoveryFailure, RecoveryStage};
    let MultiRaftError::Recovery(recovery) = error else {
        return ("unknown", "unknown");
    };
    let phase = match recovery.stage {
        RecoveryStage::Construct => "construct",
        RecoveryStage::Await => "await",
        _ => "unknown",
    };
    let failure = match recovery.failure {
        RecoveryFailure::Deadline => "deadline",
        RecoveryFailure::Closed => "closed",
        RecoveryFailure::Backend(NativeFailure::Storage) => "native_storage",
        RecoveryFailure::Backend(NativeFailure::Panicked) => "native_panicked",
        _ => "unknown",
    };
    (phase, failure)
}

/// Open the node-owned engine before publishing ingress. The marker survives
/// deletion of the database and prevents silently replacing consensus history.
pub(super) async fn open_node_log(
    config: &ClusterConfig,
) -> anyhow::Result<Option<RocksLogDb<TypeConfig>>> {
    if config.data_dir.as_os_str().is_empty() {
        return Ok(None);
    }
    let root = config.data_dir.clone();
    let backend = config.log_storage_backend;
    let sync_all = config.file_log_sync_level == multiraft_core::FileLogSyncLevel::All;
    let snapshot_cap = config.max_snapshot_bytes;
    tokio::task::spawn_blocking(move || {
        let marker = root.join("raft-log-backend");
        let database = root.join("raft-rocksdb");
        let marked = match std::fs::read(&marker) {
            Ok(bytes) if bytes == b"rocksdb-v1\n" => true,
            Ok(_) => anyhow::bail!("unsupported Raft log backend marker"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if backend == LogStorageBackend::FileLog {
            anyhow::ensure!(
                !marked,
                "cannot switch RocksDB Raft logs to FileLog"
            );
            return Ok(None);
        }
        anyhow::ensure!(
            !marked || database.join("CURRENT").is_file(),
            "RocksDB Raft log database missing after backend selection"
        );
        // Check legacy roots before opening a new database or selecting a backend.
        let mut groups = BTreeSet::new();
        if root.exists() {
            for entry in std::fs::read_dir(&root)? {
                let entry = entry?;
                if let Some(id) = entry.file_name().to_str().and_then(|name| name.strip_prefix("group-")).and_then(|id| id.parse::<GroupId>().ok()) {
                    groups.insert(id);
                }
            }
        }
        let snapshots = root.join("snapshots");
        if snapshots.exists() {
            for entry in std::fs::read_dir(&snapshots)? {
                let entry = entry?;
                if let Some(id) = entry.file_name().to_str().and_then(|id| id.parse::<GroupId>().ok()) { groups.insert(id); }
            }
        }
        let catalog = SnapshotCatalog::new(snapshots, 1);
        let mut persisted = Vec::new();
        for group in groups {
            if FileLogStoreOf::startup_provenance(root.join(format!("group-{group}")))? == multiraft_core::StartupProvenance::Persisted
                || catalog.startup_provenance(group, snapshot_cap)? == multiraft_core::StartupProvenance::Persisted {
                persisted.push(group);
            }
        }
        anyhow::ensure!(database.exists() || persisted.is_empty(), "cannot select RocksDB over persisted FileLog or snapshot history; explicit migration required");
        let db = RocksLogDb::open_with_sync_all(&database, sync_all)?;
        for group in persisted {
            anyhow::ensure!(db.startup_provenance(group)? == multiraft_core::StartupProvenance::Persisted,
                "RocksDB group has no local log basis but persisted FileLog or snapshot history exists; explicit migration or recovery required");
        }
        if !marked {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&marker)?;
            file.write_all(b"rocksdb-v1\n")?;
            file.sync_all()?;
            std::fs::File::open(&root)?.sync_all()?;
        }
        Ok(Some(db))
    })
    .await?
}

impl<S: StateMachine> MultiRaft<S> {
    pub(super) fn log_provenance(
        &self,
        group: GroupId,
    ) -> std::io::Result<multiraft_core::StartupProvenance> {
        use multiraft_core::StartupProvenance;
        let legacy = || {
            FileLogStoreOf::startup_provenance(self.config.data_dir.join(format!("group-{group}")))
        };
        let db = self.rocks_log_db.lock().unwrap().clone();
        let Some(db) = db else {
            if self.config.log_storage_backend == LogStorageBackend::RocksDb {
                return Err(std::io::Error::other("RocksDB log owner has stopped"));
            }
            return legacy();
        };
        let provenance = db.startup_provenance(group)?;
        if provenance == StartupProvenance::Pristine {
            let snapshots = SnapshotCatalog::new(self.config.data_dir.join("snapshots"), 1)
                .startup_provenance(group, self.config.max_snapshot_bytes)?;
            if legacy()? == StartupProvenance::Persisted
                || snapshots == StartupProvenance::Persisted
            {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
                    "RocksDB group has no local log basis but persisted FileLog or snapshot history exists; explicit migration or recovery required"));
            }
        }
        Ok(provenance)
    }

    pub(super) fn open_native_log(
        &self,
        group: GroupId,
    ) -> Result<NativeRaftLogStore, MultiRaftError> {
        if self.config.data_dir.as_os_str().is_empty() {
            return Ok(NativeRaftLogStore::Memory(MemLogStore::default()));
        }
        let db = self.rocks_log_db.lock().unwrap().clone();
        if let Some(db) = db {
            self.log_provenance(group)
                .map_err(|error| MultiRaftError::Other(error.into()))?;
            return db
                .group(group)
                .map(NativeRaftLogStore::Rocks)
                .map_err(|error| MultiRaftError::Other(error.into()));
        }
        if self.config.log_storage_backend == LogStorageBackend::RocksDb {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "RocksDB log owner has stopped"
            )));
        }
        FileLogStoreOf::open_with_full_options(
            self.config.data_dir.join(format!("group-{group}")),
            self.config.file_log_coalesce_us,
            self.config.file_log_sync_level,
            FileLogStreamOptions {
                stream_buf_bytes: self.config.file_log_stream_buf_bytes,
                stream_flush_ms: self.config.file_log_stream_flush_ms,
                hold_overlap: self.config.file_log_hold_overlap,
            },
        )
        .map(NativeRaftLogStore::File)
        .map_err(|error| MultiRaftError::Other(error.into()))
    }
}
