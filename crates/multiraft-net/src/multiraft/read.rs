//! One native ReadIndex per caller, using its single absolute budget through FSM query.
use super::*;
use crate::read_observation::{ReadGuard, ReadObserver, ReadOutcome, ReadStage};
use multiraft_core::{NativeFailure, ReadIndexFailure};
use tokio::time::Instant;

const READ_BOUND: Duration = Duration::from_secs(10);
fn native_failure(error: openraft::error::Fatal<TypeConfig>) -> ReadIndexFailure {
    match error {
        openraft::error::Fatal::Stopped => ReadIndexFailure::Closed,
        openraft::error::Fatal::Panicked => ReadIndexFailure::Backend(NativeFailure::Panicked),
        openraft::error::Fatal::StorageError(_) => {
            ReadIndexFailure::Backend(NativeFailure::Storage)
        }
    }
}
async fn confirm<S: StateMachine>(raft: &Raft<S>, deadline: Instant) -> Result<(), MultiRaftError> {
    match tokio::time::timeout_at(deadline, raft.ensure_linearizable(ReadPolicy::ReadIndex)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => {
            if let Some(fwd) = error.forward_to_leader() {
                return Err(MultiRaftError::NotLeader {
                    hint: fwd.leader_id,
                });
            }
            Err(match error {
                RaftError::APIError(openraft::error::LinearizableReadError::QuorumNotEnough(
                    error,
                )) => ReadIndexFailure::QuorumUnavailable {
                    responders: error.got,
                }
                .into(),
                RaftError::Fatal(error) => native_failure(error).into(),
                RaftError::APIError(openraft::error::LinearizableReadError::ForwardToLeader(
                    error,
                )) => MultiRaftError::NotLeader {
                    hint: error.leader_id,
                },
            })
        }
        Err(_) => Err(ReadIndexFailure::Deadline.into()),
    }
}
impl<S: StateMachine> MultiRaft<S> {
    /// Independent native ReadIndex confirmation followed by this caller's FSM query.
    /// One ten-second absolute budget covers both stages; no sharing, pool or queue.
    pub async fn read_linearizable<R>(
        &self,
        group: GroupId,
        query: impl FnOnce(&S) -> R,
    ) -> Result<R, MultiRaftError> {
        self.read_linearizable_at(group, Instant::now() + READ_BOUND, query)
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
            confirm(&raft, deadline).await
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
