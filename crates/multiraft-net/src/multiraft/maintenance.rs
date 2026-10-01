//! Owned local native maintenance admission and canonical observations.
use super::*;
use crate::ObservedLogId;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::Instant;

mod operation;
mod status;

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
    /// The local submission budget expired before a native trigger was invoked.
    Deadline,
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

impl std::fmt::Display for CompactionRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "native maintenance rejected: {self:?}")
    }
}
impl std::error::Error for CompactionRejection {}

pub(super) struct Operation {
    // Task futures retain only these independent facts, never the registry owner.
    state: Arc<Mutex<(CompactionProgress, bool)>>,
    active: Arc<AtomicBool>,
}
impl Default for Operation {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new((CompactionProgress::Idle, false))),
            active: Arc::new(AtomicBool::new(false)),
        }
    }
}

const OPERATION_BUDGET: Duration = Duration::from_secs(30);

impl<S: StateMachine> MultiRaft<S> {
    /// Submit one local operation. Dropping this waiter never revokes native work.
    /// Submission is distinct from builder/provider/native/purge completion.
    pub async fn request_compaction(
        &self,
        group: GroupId,
    ) -> Result<CompactionSubmission, CompactionRejection> {
        self.request_compaction_until(group, Instant::now() + OPERATION_BUDGET)
            .await
    }

    pub(crate) async fn request_compaction_until(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<CompactionSubmission, CompactionRejection> {
        if self.snapshot_rt.stopping.load(Ordering::Acquire) {
            return Err(CompactionRejection::ShuttingDown);
        }
        if Instant::now() >= deadline {
            return Err(CompactionRejection::Deadline);
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
        if operation
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(CompactionRejection::Busy);
        }
        *operation.state.lock().unwrap() = (CompactionProgress::Preparing, false);
        let terminal = operation::TerminalObservation::new(&operation);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let stopping = self.snapshot_rt.stopping.clone();
        let retain = self.config.retain_log_entries;
        if !self.snapshot_rt.maintenance_tasks.spawn(operation::run(
            raft, sm, terminal, stopping, retain, deadline, sender,
        )) {
            return Err(CompactionRejection::ShuttingDown);
        }
        receiver
            .await
            .unwrap_or(Err(CompactionRejection::SubmissionFailed))
    }

    /// Sample native/provider/log facts with one retained sampler per Node.
    /// A canceled waiter does not free its slot while provider/log IO is executing.
    pub async fn local_storage_status(
        &self,
        group: GroupId,
    ) -> Result<LocalStorageStatus, CompactionRejection> {
        if self.snapshot_rt.stopping.load(Ordering::Acquire) {
            return Err(CompactionRejection::ShuttingDown);
        }
        let (raft, sm) = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|app| (app.raft.clone(), app.state_machine.clone()))
            .ok_or(CompactionRejection::UnknownGroup)?;
        let permit = self
            .snapshot_rt
            .sampler_budget
            .clone()
            .try_acquire_owned()
            .map_err(|_| CompactionRejection::Busy)?;
        let state = self
            .snapshot_rt
            .operations
            .lock()
            .unwrap()
            .get(&group)
            .map(|operation| operation.state.clone());
        let directory = (!self.config.data_dir.as_os_str().is_empty())
            .then(|| self.config.data_dir.join(format!("group-{group}")));
        let node_id = self.node_id;
        let mode = self.config.snapshot_mode;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        if !self.snapshot_rt.maintenance_tasks.spawn(async move {
            let _permit = permit;
            let result = status::collect(node_id, group, mode, raft, sm, directory, state).await;
            let _ = sender.send(result);
        }) {
            return Err(CompactionRejection::ShuttingDown);
        }
        receiver
            .await
            .unwrap_or(Err(CompactionRejection::StorageFailure))
    }
}
