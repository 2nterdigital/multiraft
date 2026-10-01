//! One sampler's validated provider/native/log facts, never application payloads.
use super::*;

pub(super) async fn collect<S: StateMachine>(
    node: NodeId,
    group: GroupId,
    mode: SnapshotMode,
    raft: Raft<S>,
    sm: StateMachineStore<S>,
    directory: Option<PathBuf>,
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
    let retained_log_bytes = if let Some(directory) = directory {
        Some(
            tokio::task::spawn_blocking(move || {
                FileLogStoreOf::measure_retained_log_bytes(directory)
            })
            .await
            .map_err(|_| CompactionRejection::StorageFailure)?
            .map_err(|_| CompactionRejection::StorageFailure)?,
        )
    } else {
        None
    };
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
