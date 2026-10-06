//! One sampler's validated provider/native/log facts, never application payloads.
use super::*;

pub(super) enum LogInspection {
    Memory,
    File(PathBuf),
    Rocks(RocksLogDb<TypeConfig>),
}

pub(super) async fn collect<S: StateMachine>(
    node: NodeId,
    group: GroupId,
    mode: SnapshotMode,
    raft: Raft<S>,
    sm: StateMachineStore<S>,
    inspection: LogInspection,
    state: Option<Arc<Mutex<(CompactionProgress, bool)>>>,
) -> Result<LocalStorageStatus, CompactionRejection> {
    tracing::trace!(target: "multiraft::maintenance", operation = "storage_status",
        phase = "sample_start", node_id = node, group_id = group, "sampling local storage facts");
    let durable_snapshot = sm
        .native_snapshot_info()
        .await
        .map_err(|_| CompactionRejection::StorageFailure)?
        .map(|snapshot| DurableSnapshotObservation {
            last_log_id: snapshot.meta.last_log_id.map(observed),
            snapshot_id: snapshot.meta.snapshot_id,
            bytes: snapshot.size,
        });
    let metrics = raft.metrics().borrow_watched().clone();
    let log_storage_backend = match &inspection {
        LogInspection::Memory => None,
        LogInspection::File(_) => Some(LogStorageBackend::FileLog),
        LogInspection::Rocks(_) => Some(LogStorageBackend::RocksDb),
    };
    let retained_log_bytes = tokio::task::spawn_blocking(move || match inspection {
        LogInspection::Memory => Ok(None),
        LogInspection::File(directory) => {
            FileLogStoreOf::measure_retained_log_bytes(directory).map(Some)
        }
        LogInspection::Rocks(db) => db.retained_log_bytes(group).map(Some),
    })
    .await
    .map_err(|_| CompactionRejection::StorageFailure)?
    .map_err(|_| CompactionRejection::StorageFailure)?;
    let state = state
        .map(|state| *state.lock().unwrap())
        .unwrap_or((CompactionProgress::Idle, false));
    Ok(LocalStorageStatus {
        group_id: group,
        local_node_id: node,
        mode,
        durable_snapshot,
        native_snapshot: metrics.snapshot.map(observed),
        purged: metrics.purged.map(observed),
        retained_log_bytes_kind: match log_storage_backend {
            None => "unavailable",
            Some(LogStorageBackend::FileLog) => "physical_file",
            Some(LogStorageBackend::RocksDb) => "encoded_entries",
        },
        log_storage_backend,
        retained_log_bytes,
        progress: state.0,
        no_purge_needed: state.1,
    })
}
fn observed(id: openraft::alias::LogIdOf<TypeConfig>) -> ObservedLogId {
    ObservedLogId::new(
        id.committed_leader_id().term,
        id.committed_leader_id().node_id,
        id.index,
    )
}
