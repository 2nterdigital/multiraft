//! Native reservation, one trigger and retained completion observation.
use super::*;
use multiraft_fsm::CaptureRefusal;
use multiraft_store::NativeCaptureError;
use openraft::alias::LogIdOf;

type OperationState = Arc<Mutex<(CompactionProgress, bool)>>;

pub(super) struct TerminalObservation {
    state: OperationState,
    active: Arc<AtomicBool>,
}
impl TerminalObservation {
    pub(super) fn new(operation: &Operation) -> Self {
        Self {
            state: operation.state.clone(),
            active: operation.active.clone(),
        }
    }
}
impl Drop for TerminalObservation {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        if matches!(
            state.0,
            CompactionProgress::Preparing | CompactionProgress::Submitted
        ) {
            state.0 = CompactionProgress::Unconfirmed;
        }
        self.active.store(false, Ordering::Release);
    }
}
struct ReservationCleanup<S: StateMachine>(StateMachineStore<S>);
impl<S: StateMachine> Drop for ReservationCleanup<S> {
    fn drop(&mut self) {
        self.0.release_native_reservation();
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

pub(super) async fn run<S: StateMachine>(
    raft: Raft<S>,
    sm: StateMachineStore<S>,
    terminal: TerminalObservation,
    stopping: Arc<AtomicBool>,
    retain: u64,
    deadline: Instant,
    sender: tokio::sync::oneshot::Sender<Result<CompactionSubmission, CompactionRejection>>,
) {
    if stopping.load(Ordering::Acquire) || Instant::now() >= deadline {
        terminal.state.lock().unwrap().0 = CompactionProgress::Idle;
        let _ = sender.send(Err(if stopping.load(Ordering::Acquire) {
            CompactionRejection::ShuttingDown
        } else {
            CompactionRejection::Deadline
        }));
        return;
    }
    let reservation = match tokio::time::timeout_at(deadline, sm.reserve_native_compaction()).await
    {
        Ok(Ok(reservation)) => reservation,
        outcome => {
            terminal.state.lock().unwrap().0 = CompactionProgress::Idle;
            let rejection = match outcome {
                Ok(Err(error)) if !stopping.load(Ordering::Acquire) => refused(error),
                Ok(_) => CompactionRejection::ShuttingDown,
                Err(_) => CompactionRejection::Deadline,
            };
            let _ = sender.send(Err(rejection));
            return;
        }
    };
    let _reservation = ReservationCleanup(sm.clone());
    if stopping.load(Ordering::Acquire) || Instant::now() >= deadline {
        terminal.state.lock().unwrap().0 = CompactionProgress::Idle;
        let _ = sender.send(Err(if stopping.load(Ordering::Acquire) {
            CompactionRejection::ShuttingDown
        } else {
            CompactionRejection::Deadline
        }));
        return;
    }
    let target = reservation.target;
    let mut completion = reservation.completion;
    let previous_purged = raft.metrics().borrow_watched().purged;
    // After invoking the native trigger, failure/timeout remains unconfirmed.
    if raft.trigger().snapshot().await.is_ok() {
        terminal.state.lock().unwrap().0 = CompactionProgress::Submitted;
        let _ = sender.send(Ok(CompactionSubmission {
            target: target.map(observed),
        }));
    } else {
        let _ = sender.send(Err(CompactionRejection::SubmissionFailed));
        return;
    }
    let observation_deadline = Instant::now() + OPERATION_BUDGET;
    loop {
        tokio::select! {
            result = &mut completion => { if result.is_err() { return; } break; }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                if stopping.load(Ordering::Acquire) || Instant::now() >= observation_deadline
                    || raft.metrics().borrow_watched().running_state.is_err() { return; }
            }
        }
    }
    // The provider completion notification precedes the builder's final return.
    // Reuse the native transition seam to observe actual builder quiescence before
    // accepting durable/provider/native/purge facts, even for repeated same cuts.
    sm.wait_native_quiescent().await;
    loop {
        let metrics = raft.metrics().borrow_watched().clone();
        if stopping.load(Ordering::Acquire)
            || Instant::now() >= observation_deadline
            || metrics.running_state.is_err()
        {
            return;
        }
        let Ok(durable) = sm.native_snapshot_info().await else {
            return;
        };
        if stopping.load(Ordering::Acquire) || Instant::now() >= observation_deadline {
            return;
        }
        if let Some(no_purge_needed) = completion_observed(
            target,
            retain,
            metrics.snapshot,
            metrics.purged,
            previous_purged,
            durable.as_ref().map(|snapshot| &snapshot.meta),
        ) {
            *terminal.state.lock().unwrap() =
                (CompactionProgress::CompletedObserved, no_purge_needed);
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
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
