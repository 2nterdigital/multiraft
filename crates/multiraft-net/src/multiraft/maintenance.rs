//! Disposable per-Group submission/observation, over native execution owners.
use super::*;
use crate::ObservedLogId;
use multiraft_fsm::CaptureRefusal;
use multiraft_store::NativeCaptureError;
use openraft::alias::LogIdOf;
use std::sync::atomic::Ordering;

pub const NATIVE_SNAPSHOT_COMPACTION_CONTRACT: &str = "native-snapshot-compaction-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionRejection {
    UnknownGroup,
    Disabled,
    Busy,
    SizeLimit,
    UnsupportedCapture,
    StorageFailure,
    SubmissionFailed,
    ShuttingDown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionProgress {
    Idle,
    Preparing,
    Submitted,
    CompletedObserved,
    Unconfirmed,
}
#[derive(Debug, Clone)]
pub struct CompactionSubmission {
    pub target: Option<ObservedLogId>,
}
#[derive(Debug, Clone)]
pub struct DurableSnapshotObservation {
    pub last_log_id: Option<ObservedLogId>,
    pub snapshot_id: String,
    pub bytes: u64,
}
#[derive(Debug, Clone)]
pub struct LocalStorageStatus {
    pub group_id: GroupId,
    pub local_node_id: NodeId,
    pub mode: SnapshotMode,
    pub durable_snapshot: Option<DurableSnapshotObservation>,
    pub native_snapshot: Option<ObservedLogId>,
    pub purged: Option<ObservedLogId>,
    pub retained_log_bytes: Option<u64>,
    pub progress: CompactionProgress,
    pub no_purge_needed: bool,
}

pub(super) struct Operation {
    state: Mutex<(CompactionProgress, bool)>,
    pub(super) task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Default for Operation {
    fn default() -> Self {
        Self {
            state: Mutex::new((CompactionProgress::Idle, false)),
            task: Mutex::new(None),
        }
    }
}
fn observed(id: LogIdOf<TypeConfig>) -> ObservedLogId {
    ObservedLogId::new(
        id.committed_leader_id().term,
        id.committed_leader_id().node_id,
        id.index,
    )
}
fn refused(error: NativeCaptureError) -> CompactionRejection {
    match error {
        NativeCaptureError::Refused(CaptureRefusal::Busy) => CompactionRejection::Busy,
        NativeCaptureError::Refused(CaptureRefusal::SizeLimit) => CompactionRejection::SizeLimit,
        NativeCaptureError::Refused(CaptureRefusal::Unsupported) => {
            CompactionRejection::UnsupportedCapture
        }
        NativeCaptureError::Io(_) => CompactionRejection::StorageFailure,
    }
}

impl<S: StateMachine> MultiRaft<S> {
    /// Submit one local operation. Dropping this waiter never cancels native work.
    pub async fn request_compaction(
        &self,
        group: GroupId,
    ) -> Result<CompactionSubmission, CompactionRejection> {
        if self.snapshot_rt.stopping.load(Ordering::SeqCst) {
            return Err(CompactionRejection::ShuttingDown);
        }
        let (raft, sm) = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|app| (app.raft.clone(), app.state_machine.clone()))
            .ok_or(CompactionRejection::UnknownGroup)?;
        if self.config.snapshot_mode != SnapshotMode::NativeDurable {
            return Err(CompactionRejection::Disabled);
        }
        let operation = self
            .snapshot_rt
            .operations
            .lock()
            .unwrap()
            .entry(group)
            .or_default()
            .clone();
        {
            let mut state = operation.state.lock().unwrap();
            if matches!(
                state.0,
                CompactionProgress::Preparing | CompactionProgress::Submitted
            ) {
                return Err(CompactionRejection::Busy);
            }
            *state = (CompactionProgress::Preparing, false);
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task_operation = operation.clone();
        let retain = self.config.retain_log_entries;
        let task = tokio::spawn(async move {
            let reservation = match sm.reserve_native_compaction().await {
                Ok(reservation) => reservation,
                Err(error) => {
                    task_operation.state.lock().unwrap().0 = CompactionProgress::Idle;
                    let _ = sender.send(Err(refused(error)));
                    return;
                }
            };
            let target = reservation.target;
            let mut completion = reservation.completion;
            let previous_purged = raft.metrics().borrow_watched().purged;
            if raft.trigger().snapshot().await.is_err() {
                sm.release_native_reservation();
                task_operation.state.lock().unwrap().0 = CompactionProgress::Unconfirmed;
                let _ = sender.send(Err(CompactionRejection::SubmissionFailed));
                return;
            }
            task_operation.state.lock().unwrap().0 = CompactionProgress::Submitted;
            let _ = sender.send(Ok(CompactionSubmission {
                target: target.map(observed),
            }));
            loop {
                tokio::select! {
                    result = &mut completion => {
                        if result.is_err() { task_operation.state.lock().unwrap().0 = CompactionProgress::Unconfirmed; return; }
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if raft.metrics().borrow_watched().running_state.is_err() {
                            sm.release_native_reservation();
                            task_operation.state.lock().unwrap().0 = CompactionProgress::Unconfirmed; return;
                        }
                    }
                }
            }
            loop {
                let metrics = raft.metrics().borrow_watched().clone();
                if metrics.running_state.is_err() {
                    sm.release_native_reservation();
                    task_operation.state.lock().unwrap().0 = CompactionProgress::Unconfirmed;
                    return;
                }
                // Native positions alone (especially None/None) are not proof
                // that a durable checkpoint exists. Verify the provider as well.
                let durable = match sm.native_snapshot_info().await {
                    Ok(snapshot) => snapshot,
                    Err(_) => {
                        task_operation.state.lock().unwrap().0 = CompactionProgress::Unconfirmed;
                        return;
                    }
                };
                if let Some(no_purge_needed) = completion_observed(
                    target,
                    retain,
                    metrics.snapshot,
                    metrics.purged,
                    previous_purged,
                    durable.as_ref().map(|snapshot| &snapshot.meta),
                ) {
                    *task_operation.state.lock().unwrap() =
                        (CompactionProgress::CompletedObserved, no_purge_needed);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        *operation.task.lock().unwrap() = Some(task);
        receiver
            .await
            .unwrap_or(Err(CompactionRejection::SubmissionFailed))
    }

    /// Sample native/provider/log facts; this does not claim health or causality.
    pub async fn local_storage_status(
        &self,
        group: GroupId,
    ) -> Result<LocalStorageStatus, CompactionRejection> {
        let (raft, sm) = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|app| (app.raft.clone(), app.state_machine.clone()))
            .ok_or(CompactionRejection::UnknownGroup)?;
        let durable_snapshot = if self.config.snapshot_mode == SnapshotMode::NativeDurable {
            sm.native_snapshot_info()
                .await
                .map_err(|_| CompactionRejection::StorageFailure)?
                .map(|snapshot| DurableSnapshotObservation {
                    last_log_id: snapshot.meta.last_log_id.map(observed),
                    snapshot_id: snapshot.meta.snapshot_id,
                    bytes: snapshot.size,
                })
        } else {
            None
        };
        let metrics = raft.metrics().borrow_watched().clone();
        let retained_log_bytes = if self.config.data_dir.as_os_str().is_empty() {
            None
        } else {
            let dir = self.config.data_dir.join(format!("group-{group}"));
            Some(
                tokio::task::spawn_blocking(move || {
                    FileLogStoreOf::measure_retained_log_bytes(dir)
                })
                .await
                .map_err(|_| CompactionRejection::StorageFailure)?
                .map_err(|_| CompactionRejection::StorageFailure)?,
            )
        };
        let state = self
            .snapshot_rt
            .operations
            .lock()
            .unwrap()
            .get(&group)
            .map(|op| *op.state.lock().unwrap())
            .unwrap_or((CompactionProgress::Idle, false));
        Ok(LocalStorageStatus {
            group_id: group,
            local_node_id: self.node_id,
            mode: self.config.snapshot_mode,
            durable_snapshot,
            native_snapshot: metrics.snapshot.map(observed),
            purged: metrics.purged.map(observed),
            retained_log_bytes,
            progress: state.0,
            no_purge_needed: state.1,
        })
    }
}

fn completion_observed(
    target: Option<LogIdOf<TypeConfig>>,
    retain: u64,
    native_snapshot: Option<LogIdOf<TypeConfig>>,
    purged: Option<LogIdOf<TypeConfig>>,
    previous_purged: Option<LogIdOf<TypeConfig>>,
    durable_snapshot: Option<&openraft::alias::SnapshotMetaOf<TypeConfig>>,
) -> Option<bool> {
    let durable = durable_snapshot?;
    if durable.last_log_id < target || native_snapshot < target {
        return None;
    }
    let purge_target = target.and_then(|id| id.index.checked_sub(retain));
    let no_purge_needed =
        purge_target.is_none() || previous_purged.is_some_and(|id| Some(id.index) >= purge_target);
    if purge_target.is_none() || purged.is_some_and(|id| Some(id.index) >= purge_target) {
        Some(no_purge_needed)
    } else {
        None
    }
}

#[cfg(test)]
mod completion_tests {
    use super::*;
    #[test]
    fn absent_native_positions_do_not_prove_an_empty_checkpoint_was_published() {
        assert_eq!(
            completion_observed(None, 1024, None, None, None, None),
            None
        );
        let durable = openraft::alias::SnapshotMetaOf::<TypeConfig>::default();
        assert_eq!(
            completion_observed(None, 1024, None, None, None, Some(&durable)),
            Some(true)
        );
    }
}
