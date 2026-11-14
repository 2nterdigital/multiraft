//! One sampler's validated provider/native/log facts, never application payloads.
use super::*;

pub(super) enum LogInspection {
    Memory,
    File(PathBuf),
    Rocks(RocksLogDb<TypeConfig>),
}

// One private call site; the permit is moved into the blocking worker it admits.
#[allow(clippy::too_many_arguments)]
pub(super) async fn collect<S: StateMachine>(
    node: NodeId,
    group: GroupId,
    mode: SnapshotMode,
    raft: Raft<S>,
    sm: StateMachineStore<S>,
    inspection: LogInspection,
    state: Option<Arc<Mutex<(CompactionProgress, bool)>>>,
    permit: tokio::sync::OwnedSemaphorePermit,
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
    // The FSM sample is opportunistic and never waits for apply or capture; the
    // permit covers all blocking work and is released only when it ends.
    let (retained_log_bytes, fsm_memory, raft_engine_memory) = sm
        .try_with_storage_memory_stats(move |fsm_memory| {
            let _permit = permit;
            let fsm_memory = memory_observation(node, group, "fsm", fsm_memory);
            let (retained, raft_memory) = match inspection {
                LogInspection::Memory => (None, None),
                LogInspection::File(directory) => {
                    let retained = FileLogStoreOf::measure_retained_log_bytes(directory)?;
                    (Some(retained), None)
                }
                LogInspection::Rocks(db) => {
                    let retained = match db.lifecycle_receipt(group)? {
                        Some(receipt) => {
                            db.retained_log_bytes_generation(group, receipt.generation)?
                        }
                        None => db.retained_log_bytes(group)?,
                    };
                    let raft_memory = db.storage_memory_stats().map(Some);
                    let raft_memory = memory_observation(node, group, "raft_engine", raft_memory);
                    (Some(retained), raft_memory)
                }
            };
            Ok::<_, std::io::Error>((retained, fsm_memory, raft_memory))
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
        fsm_memory,
        raft_engine_memory,
        progress: state.0,
        no_purge_needed: state.1,
    })
}
/// Memory facts are best effort: a busy or failed sample is reported absent,
/// never as zero and never as a failed status.
fn memory_observation(
    node: NodeId,
    group: GroupId,
    source: &'static str,
    sample: std::io::Result<Option<multiraft_fsm::StorageMemoryStats>>,
) -> Option<multiraft_fsm::StorageMemoryStats> {
    sample
        .inspect_err(|error| {
            tracing::debug!(target: "multiraft::maintenance", operation = "storage_status",
                node_id = node, group_id = group, source, %error, "memory sample unavailable");
        })
        .ok()
        .flatten()
}
fn observed(id: openraft::alias::LogIdOf<TypeConfig>) -> ObservedLogId {
    ObservedLogId::new(
        id.committed_leader_id().term,
        id.committed_leader_id().node_id,
        id.index,
    )
}
