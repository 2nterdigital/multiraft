//! Shared native confirmation is owned independently from every application's read waiter.
mod barrier;
use super::*;
use crate::read_observation::{ReadGuard, ReadObserver, ReadOutcome, ReadStage};
use barrier::ReadBarrier;
use multiraft_core::{NativeFailure, ReadIndexFailure};
use tokio::time::Instant;

const ROUND_BOUND: Duration = Duration::from_secs(10);
#[derive(Clone)]
enum Confirmation {
    Ready,
    NotLeader(Option<NodeId>),
    Failed(ReadIndexFailure),
}
#[derive(Default)]
pub(super) struct ReadRuntime {
    barriers: Mutex<BTreeMap<GroupId, Arc<ReadBarrier<Confirmation>>>>,
    tasks: tasks::OwnedTasks,
    closed: std::sync::atomic::AtomicBool,
}
impl ReadRuntime {
    fn barrier(&self, group: GroupId) -> Result<Arc<ReadBarrier<Confirmation>>, MultiRaftError> {
        let mut barriers = self.barriers.lock().unwrap();
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ReadIndexFailure::Closed.into());
        }
        Ok(barriers
            .entry(group)
            .or_insert_with(|| ReadBarrier::new(4))
            .clone())
    }
    pub(super) fn stop(&self) {
        // Fence acquisition with the same lock that publishes new barriers.
        let _barriers = self.barriers.lock().unwrap();
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.tasks.abort();
    }
    pub(super) async fn join(&self) {
        // A round panic is reported to its subscribers; it does not poison other Groups/shutdown.
        let _ = self.tasks.join().await;
        self.barriers.lock().unwrap().clear();
    }
}
fn native_failure(error: openraft::error::Fatal<TypeConfig>) -> ReadIndexFailure {
    match error {
        openraft::error::Fatal::Stopped => ReadIndexFailure::Closed,
        openraft::error::Fatal::Panicked => ReadIndexFailure::Backend(NativeFailure::Panicked),
        openraft::error::Fatal::StorageError(_) => {
            ReadIndexFailure::Backend(NativeFailure::Storage)
        }
    }
}
async fn confirm<S: StateMachine>(raft: Raft<S>) -> Confirmation {
    match tokio::time::timeout(ROUND_BOUND, raft.ensure_linearizable(ReadPolicy::ReadIndex)).await {
        Ok(Ok(_)) => Confirmation::Ready,
        Ok(Err(error)) => {
            if let Some(fwd) = error.forward_to_leader() {
                return Confirmation::NotLeader(fwd.leader_id);
            }
            Confirmation::Failed(match error {
                RaftError::APIError(openraft::error::LinearizableReadError::QuorumNotEnough(
                    error,
                )) => ReadIndexFailure::QuorumUnavailable {
                    responders: error.got,
                },
                RaftError::Fatal(error) => native_failure(error),
                // The sole remaining API error is ForwardToLeader, handled above.
                RaftError::APIError(openraft::error::LinearizableReadError::ForwardToLeader(
                    error,
                )) => return Confirmation::NotLeader(error.leader_id),
            })
        }
        Err(_) => Confirmation::Failed(ReadIndexFailure::RoundTimeout),
    }
}
impl<S: StateMachine> MultiRaft<S> {
    /// Shared ReadIndex confirmation, followed by this caller's own local FSM read.
    /// Each call is bounded by ten seconds and never consumes a round started before arrival.
    pub async fn read_linearizable<R>(
        &self,
        group: GroupId,
        query: impl FnOnce(&S) -> R,
    ) -> Result<R, MultiRaftError> {
        self.read_linearizable_at(group, Instant::now() + ROUND_BOUND, query)
            .await
    }
    /// Absolute-deadline ReadIndex + FSM read. The synchronous query must be bounded.
    pub async fn read_linearizable_at<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
    ) -> Result<R, MultiRaftError> {
        self.read_observed(group, deadline, query, None, None, |_| {
            ReadOutcome::Completed
        })
        .await
    }
    pub(crate) async fn read_observed<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
        observer: Option<&dyn ReadObserver>,
        closed: Option<tokio::sync::watch::Receiver<bool>>,
        application_outcome: impl FnOnce(&R) -> ReadOutcome,
    ) -> Result<R, MultiRaftError> {
        let mut stage = ReadGuard::new(
            group,
            self.node_id,
            ReadStage::ReadIndex,
            deadline,
            observer,
            closed.clone(),
        );
        let result = async {
            if Instant::now() >= deadline {
                return Err(ReadIndexFailure::Deadline.into());
            }
            let raft = self
                .raft(group)
                .ok_or(MultiRaftError::UnknownGroup(group))?;
            let barrier = self.reads.barrier(group)?;
            let outcome = tokio::time::timeout_at(
                deadline,
                barrier.confirm(&self.reads.tasks, move || confirm(raft.clone())),
            )
            .await
            .map_err(|_| MultiRaftError::from(ReadIndexFailure::Deadline))?;
            match outcome {
                Ok(Confirmation::Ready) => Ok(()),
                Ok(Confirmation::NotLeader(hint)) => Err(MultiRaftError::NotLeader { hint }),
                Ok(Confirmation::Failed(error)) => Err(error.into()),
                Err(_) if self.reads.closed.load(std::sync::atomic::Ordering::Acquire) => {
                    Err(ReadIndexFailure::Closed.into())
                }
                Err(_) => Err(ReadIndexFailure::Abandoned.into()),
            }
        }
        .await;
        stage.finish_source(&result);
        result?;
        let mut stage = ReadGuard::new(
            group,
            self.node_id,
            ReadStage::StateMachine,
            deadline,
            observer,
            closed,
        );
        let result = tokio::time::timeout_at(deadline, async {
            if Instant::now() >= deadline {
                return Err(ReadIndexFailure::Deadline.into());
            }
            self.with_fsm(group, query)
                .await
                .ok_or_else(|| ReadIndexFailure::FsmUnavailable.into())
        })
        .await
        .map_err(|_| MultiRaftError::from(ReadIndexFailure::Deadline))
        .and_then(|r| r);
        match &result {
            Ok(value) => stage.finish(application_outcome(value), None, None),
            Err(_) => stage.finish_source(&result),
        }
        result
    }
}

impl<S: StateMachine> MultiRaft<S> {
    pub(crate) fn leader_hint_receiver(
        &self,
        group: GroupId,
        closed: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<crate::LeaderHintReceiver, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        Ok(crate::LeaderHintReceiver::new(
            group,
            raft.metrics(),
            closed,
        ))
    }
}
